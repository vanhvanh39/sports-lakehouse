"""Khởi tạo hạ tầng một lần: topic Kafka, lược đồ Schema Registry, bucket MinIO, index Elasticsearch.

Idempotent: chạy lại nhiều lần không sao.
"""
from __future__ import annotations

import time

import requests

from ..config import AVRO_TOPICS, get_settings
from ..kafka_io import ensure_topics, load_schema, register_schema
from ..logutil import setup_logging

log = setup_logging("init")

KW = {"type": "keyword"}
DT = {"type": "date"}
FL = {"type": "float"}
IN = {"type": "integer"}
LG = {"type": "long"}
BL = {"type": "boolean"}
TX = {"type": "text", "fields": {"kw": {"type": "keyword", "ignore_above": 256}}}

INDICES = {
    "match_events": {
        "event_id": KW, "match_id": KW, "source": KW, "seq": LG, "op": KW, "is_deleted": BL, "category": KW,
        "event_type": KW, "event_detail": KW, "period": IN, "minute": IN, "second": IN, "match_clock_s": IN,
        "team_id": KW, "team_name": KW, "player_id": KW, "player_name": KW, "related_player_name": KW,
        "x": FL, "y": FL, "xg": FL, "outcome": KW, "source_ts": DT, "ingest_ts": DT, "kafka_ts": DT,
        "processed_ts": DT, "first_seen_ts": DT, "updated_ts": DT, "es_indexed_ts": DT, "version_count": IN,
        "batch_id": LG,
    },
    "match_state": {
        "match_id": KW, "match_label": TX, "league_name": KW, "season": KW, "round": KW, "home_team": KW,
        "away_team": KW, "home_goals": IN, "away_goals": IN, "status_short": KW, "status_long": KW, "elapsed": IN,
        "extra": IN, "kickoff_ts": DT, "venue": KW, "source_ts": DT, "ingest_ts": DT, "kafka_ts": DT,
        "processed_ts": DT, "updated_ts": DT, "es_indexed_ts": DT, "seq": LG,
    },
    "excitement": {
        "match_id": KW, "match_label": KW, "batch_id": LG, "match_clock_s": IN, "minute": IN, "score": FL,
        "level": KW, "ts": DT, "es_indexed_ts": DT,
    },
    "alerts": {
        "alert_id": KW, "match_id": KW, "match_label": KW, "version": KW, "kind": KW, "minute": IN,
        "match_clock_s": IN, "score": FL, "message": TX, "triggering_event_id": KW, "source_ts": DT,
        "ingest_ts": DT, "kafka_ts": DT, "processed_ts": DT, "alert_ts": DT, "alert_sent_ts": DT,
        "source_to_alert_s": FL,
    },
    "gold_standings": {"league_name": KW, "season": KW, "team_name": KW, "rank": IN, "points": IN, "played": IN,
                       "won": IN, "drawn": IN, "lost": IN, "gf": IN, "ga": IN, "gd": IN, "xg_for": FL, "updated_ts": DT},
    "gold_team_form": {"league_name": KW, "season": KW, "team_name": KW, "form_latest_first": KW, "form_points": IN,
                       "avg_xg": FL, "updated_ts": DT},
    "gold_players": {"player_name": KW, "team_name": KW, "goals": IN, "shots": IN, "xg": FL, "matches": IN,
                     "goals_minus_xg": FL, "updated_ts": DT},
}


def wait_http(url: str, name: str, timeout: int = 300) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(url, timeout=5).status_code < 500:
                log.info("%s sẵn sàng", name)
                return
        except requests.RequestException:
            pass
        log.info("Chờ %s ...", name)
        time.sleep(3)
    raise RuntimeError(f"{name} không sẵn sàng sau {timeout}s")


def main() -> None:
    s = get_settings()
    # 1. Kafka topics
    ensure_topics(s)
    # 2. Schema Registry
    wait_http(f"{s.schema_registry_url}/subjects", "Schema Registry")
    for topic in AVRO_TOPICS:
        subject = f"{topic}-value"
        sid = register_schema(s.schema_registry_url, subject, load_schema(topic))
        requests.put(f"{s.schema_registry_url}/config/{subject}", json={"compatibility": "BACKWARD"}, timeout=10)
        log.info("Lược đồ %s id=%s (tương thích BACKWARD)", subject, sid)
    # 3. MinIO bucket
    import boto3
    from botocore.client import Config

    s3 = boto3.client("s3", endpoint_url=s.s3_endpoint, aws_access_key_id=s.s3_access_key,
                      aws_secret_access_key=s.s3_secret_key, config=Config(signature_version="s3v4"),
                      region_name="us-east-1")
    for _ in range(60):
        try:
            names = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
            if s.bucket not in names:
                s3.create_bucket(Bucket=s.bucket)
                log.info("Đã tạo bucket %s", s.bucket)
            else:
                log.info("Bucket %s đã có", s.bucket)
            break
        except Exception as exc:  # noqa: BLE001
            log.info("Chờ MinIO ... (%s)", exc)
            time.sleep(3)
    else:
        raise RuntimeError("MinIO không sẵn sàng")
    # 4. Elasticsearch
    wait_http(s.es_url, "Elasticsearch")
    for name, props in INDICES.items():
        r = requests.put(f"{s.es_url}/{name}", json={
            "settings": {"number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "1s"},
            "mappings": {"dynamic": True, "properties": props}}, timeout=30)
        if r.status_code == 200:
            log.info("Tạo index %s", name)
        elif "resource_already_exists_exception" in r.text:
            log.info("Index %s đã có", name)
        else:
            log.warning("Index %s: %s %s", name, r.status_code, r.text[:300])
    log.info("Khởi tạo hạ tầng xong.")


if __name__ == "__main__":
    main()
