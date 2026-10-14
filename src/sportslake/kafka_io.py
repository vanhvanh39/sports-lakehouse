"""Tiện ích Kafka: producer idempotent, mã hóa Avro theo định dạng Confluent (magic byte + schema id)."""
from __future__ import annotations

import io
import json
import logging
import struct
import time
from typing import Any, Callable

import fastavro

from .config import AVRO_TOPICS, SCHEMA_DIR, TOPIC_SPECS, Settings

log = logging.getLogger(__name__)


def load_schema(topic: str) -> dict:
    with open(SCHEMA_DIR / AVRO_TOPICS[topic], encoding="utf-8") as f:
        return json.load(f)


class AvroEncoder:
    """Mã hóa bản ghi thành Avro nhị phân có tiền tố Confluent 5 byte.

    Dùng fastavro (nhanh hơn AvroSerializer mặc định khoảng 3-5 lần) nhưng vẫn đăng ký
    lược đồ vào Schema Registry để có schema id và kiểm tra tương thích phiên bản.
    """

    def __init__(self, topic: str, schema_registry_url: str | None = None, schema_id: int | None = None):
        self.topic = topic
        self.raw_schema = load_schema(topic)
        self.parsed = fastavro.parse_schema(self.raw_schema)
        if schema_id is None:
            if not schema_registry_url:
                raise ValueError("Cần schema_registry_url hoặc schema_id")
            schema_id = register_schema(schema_registry_url, f"{topic}-value", self.raw_schema)
        self.schema_id = schema_id
        self._header = struct.pack(">bI", 0, schema_id)

    def encode(self, record: dict) -> bytes:
        buf = io.BytesIO()
        buf.write(self._header)
        fastavro.schemaless_writer(buf, self.parsed, record)
        return buf.getvalue()

    def decode(self, data: bytes) -> dict:
        return decode_confluent(data, self.parsed)


def decode_confluent(data: bytes, parsed_schema: Any) -> dict:
    if len(data) < 5 or data[0] != 0:
        raise ValueError("Không phải định dạng Confluent Avro")
    return fastavro.schemaless_reader(io.BytesIO(data[5:]), parsed_schema)


def register_schema(sr_url: str, subject: str, schema: dict, retries: int = 30) -> int:
    """Đăng ký lược đồ (idempotent: đăng ký lại lược đồ giống hệt trả về id cũ)."""
    import requests

    body = {"schemaType": "AVRO", "schema": json.dumps(schema)}
    last_exc: Exception | None = None
    for _ in range(retries):
        try:
            r = requests.post(f"{sr_url}/subjects/{subject}/versions", json=body,
                              headers={"Content-Type": "application/vnd.schemaregistry.v1+json"}, timeout=10)
            if r.status_code == 200:
                return int(r.json()["id"])
            if r.status_code == 409:
                raise RuntimeError(f"Lược đồ mới không tương thích với {subject}: {r.text}")
            last_exc = RuntimeError(f"{r.status_code} {r.text}")
        except RuntimeError:
            raise
        except Exception as exc:  # Schema Registry chưa sẵn sàng
            last_exc = exc
        time.sleep(2)
    raise RuntimeError(f"Không đăng ký được lược đồ {subject}: {last_exc}")


def make_producer(settings: Settings, client_id: str, extra: dict | None = None):
    """Producer idempotent: acks=all, enable.idempotence=true -> không trùng do retry phía producer."""
    from confluent_kafka import Producer

    conf = {
        "bootstrap.servers": settings.kafka_bootstrap,
        "client.id": client_id,
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",
        "linger.ms": 20,
        "batch.num.messages": 10000,
        "queue.buffering.max.messages": 500000,
        "message.timeout.ms": 300000,  # chờ tối đa 5 phút khi broker sự cố (thí nghiệm E3)
        "retries": 2147483647,
    }
    if extra:
        conf.update(extra)
    return Producer(conf)


class DeliveryTracker:
    def __init__(self):
        self.ok = 0
        self.failed = 0
        self.last_error: str | None = None

    def callback(self) -> Callable:
        def _cb(err, msg):
            if err is not None:
                self.failed += 1
                self.last_error = str(err)
                log.error("Gửi Kafka thất bại: %s", err)
            else:
                self.ok += 1
        return _cb


def ensure_topics(settings: Settings, timeout_s: int = 180) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    deadline = time.time() + timeout_s
    while True:
        try:
            md = admin.list_topics(timeout=10)
            if len(md.brokers) >= 1:
                break
        except Exception as exc:  # noqa: BLE001
            log.info("Chờ Kafka... (%s)", exc)
        if time.time() > deadline:
            raise RuntimeError("Kafka không sẵn sàng")
        time.sleep(3)
    rf = min(3, len(md.brokers))
    existing = set(md.topics.keys())
    new = []
    for name, (parts, cfg) in TOPIC_SPECS.items():
        if name in existing:
            continue
        c = dict(cfg)
        c["min.insync.replicas"] = str(min(2, rf))
        new.append(NewTopic(name, num_partitions=parts, replication_factor=rf, config=c))
    if new:
        for name, fut in admin.create_topics(new).items():
            try:
                fut.result()
                log.info("Đã tạo topic %s", name)
            except Exception as exc:  # noqa: BLE001
                if "TOPIC_ALREADY_EXISTS" not in str(exc):
                    raise


def recreate_topic(settings: Settings, name: str, partitions: int) -> None:
    """Xóa và tạo lại topic với số partition mới (dùng cho thí nghiệm E2)."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap})
    if name in admin.list_topics(timeout=10).topics:
        admin.delete_topics([name])[name].result()
        for _ in range(30):
            if name not in admin.list_topics(timeout=10).topics:
                break
            time.sleep(1)
    _, cfg = TOPIC_SPECS[name]
    c = dict(cfg)
    c["min.insync.replicas"] = "2"
    for _ in range(30):
        try:
            admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=3, config=c)])[name].result()
            return
        except Exception as exc:  # noqa: BLE001 - topic có thể đang bị xóa
            if "TOPIC_ALREADY_EXISTS" in str(exc) or "MARKED_FOR_DELETION" in str(exc) or "being deleted" in str(exc):
                time.sleep(1)
                continue
            raise
