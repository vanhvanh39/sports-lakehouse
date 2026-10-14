"""Tiện ích quản trị nhỏ.

  python -m sportslake.tools.admin topics                       # liệt kê topic, partition, số bản ghi
  python -m sportslake.tools.admin recreate-topic match.tracking 12
  python -m sportslake.tools.admin dlq --last 20                # xem bản ghi trong dead-letter topic
  python -m sportslake.tools.admin tables                       # số dòng & phiên bản các bảng Delta
"""
from __future__ import annotations

import argparse
import json

from ..config import TOPIC_DLQ, get_settings
from ..kafka_io import recreate_topic


def topics() -> None:
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient

    s = get_settings()
    md = AdminClient({"bootstrap.servers": s.kafka_bootstrap}).list_topics(timeout=10)
    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": "admin-peek", "enable.auto.commit": False})
    print(f"Brokers: {len(md.brokers)}")
    for name in sorted(md.topics):
        if name.startswith("_"):
            continue
        t = md.topics[name]
        total = 0
        for p in t.partitions:
            lo, hi = c.get_watermark_offsets(TopicPartition(name, p), timeout=10)
            total += hi - lo
        isr = min(len(p.isrs) for p in t.partitions.values()) if t.partitions else 0
        print(f"{name:20s} partitions={len(t.partitions):3d} min_isr_now={isr} messages={total:,}")
    c.close()


def dlq(last: int) -> None:
    from confluent_kafka import Consumer, TopicPartition

    s = get_settings()
    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": "admin-dlq-peek", "enable.auto.commit": False})
    md = c.list_topics(TOPIC_DLQ, timeout=10)
    parts = []
    for p in md.topics[TOPIC_DLQ].partitions:
        lo, hi = c.get_watermark_offsets(TopicPartition(TOPIC_DLQ, p), timeout=10)
        parts.append(TopicPartition(TOPIC_DLQ, p, max(lo, hi - last)))
    c.assign(parts)
    rows = []
    for _ in range(last * 3):
        m = c.poll(1.0)
        if m is None:
            break
        if not m.error():
            rows.append(json.loads(m.value()))
    for r in rows[-last:]:
        rec = r.get("record") or {}
        print(f"[{r.get('stage')}] {r.get('reason')} match={rec.get('match_id', r.get('match_id'))} "
              f"event={rec.get('event_id', '')} minute={rec.get('minute', '')}")
    print(f"({len(rows)} bản ghi)")
    c.close()


def tables() -> None:
    from ..serving.lake import TABLES, delta_table

    for name, key in TABLES.items():
        try:
            dt = delta_table(key)
            n = dt.to_pyarrow_dataset().count_rows()
            print(f"{name:32s} version={dt.version():6d} rows={n:,}")
        except Exception:  # noqa: BLE001
            print(f"{name:32s} (chưa có)")


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    sub.add_parser("topics")
    r = sub.add_parser("recreate-topic")
    r.add_argument("topic")
    r.add_argument("partitions", type=int)
    d = sub.add_parser("dlq")
    d.add_argument("--last", type=int, default=20)
    sub.add_parser("tables")
    a = ap.parse_args()
    if a.cmd == "topics":
        topics()
    elif a.cmd == "recreate-topic":
        recreate_topic(get_settings(), a.topic, a.partitions)
        print(f"Đã tạo lại {a.topic} với {a.partitions} partition")
    elif a.cmd == "dlq":
        dlq(a.last)
    elif a.cmd == "tables":
        tables()


if __name__ == "__main__":
    main()
