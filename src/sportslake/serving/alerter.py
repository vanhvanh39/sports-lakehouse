"""Nhận cảnh báo từ topic match.alerts -> Telegram / webhook / Elasticsearch (index alerts).

Bỏ trùng theo alert_id (pipeline có thể gửi lại khi một vi lô được xử lý lại sau sự cố).
"""
from __future__ import annotations

import json
import time
from collections import OrderedDict
from datetime import datetime, timezone

import requests
from prometheus_client import Counter, Histogram, start_http_server

from ..config import TOPIC_ALERTS, get_settings
from ..logutil import setup_logging

log = setup_logging("alerter")
M_SENT = Counter("alerter_sent_total", "Cảnh báo đã gửi", ["channel"])
M_DUP = Counter("alerter_duplicates_total", "Cảnh báo trùng bị bỏ qua")
M_LAT = Histogram("alerter_source_to_alert_seconds", "Độ trễ từ sự kiện ở nguồn tới khi cảnh báo được gửi",
                  buckets=(1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233))


class LRU(OrderedDict):
    def __init__(self, cap=50000):
        super().__init__()
        self.cap = cap

    def seen(self, k) -> bool:
        if k in self:
            self.move_to_end(k)
            return True
        self[k] = True
        if len(self) > self.cap:
            self.popitem(last=False)
        return False


def _parse_ts(v):
    if not v:
        return None
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None


def main() -> None:
    from confluent_kafka import Consumer
    from elasticsearch import Elasticsearch

    s = get_settings()
    start_http_server(9110)
    es = Elasticsearch(s.es_url, request_timeout=30, retry_on_timeout=True)
    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": "alerter", "auto.offset.reset": "earliest",
                  "enable.auto.commit": False})
    c.subscribe([TOPIC_ALERTS])
    seen = LRU()
    log.info("Alerter: Telegram=%s, webhook=%s", bool(s.telegram_token and s.telegram_chat_id), bool(s.webhook_url))
    labels: dict[str, str] = {}
    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            log.warning("Kafka: %s", msg.error())
            continue
        try:
            a = json.loads(msg.value())
        except ValueError:
            c.commit(msg)
            continue
        aid = a.get("alert_id")
        if seen.seen(aid):
            M_DUP.inc()
            c.commit(msg)
            continue
        now = time.time()
        a["alert_sent_ts"] = datetime.now(timezone.utc).isoformat()
        src = _parse_ts(a.get("source_ts"))
        if src:
            a["source_to_alert_s"] = round(now - src, 3)
            M_LAT.observe(max(0.0, now - src))
        # nhãn trận (tên đội) để tin nhắn dễ đọc
        mid = a.get("match_id")
        if mid not in labels:
            try:
                d = es.get(index="match_state", id=mid)["_source"]
                labels[mid] = f"{d.get('home_team')} vs {d.get('away_team')}"
            except Exception:  # noqa: BLE001
                pass
        a["match_label"] = labels.get(mid, mid)
        text = f"[{a['version']}] {a['match_label']}\n{a['message']}"
        if s.telegram_token and s.telegram_chat_id:
            try:
                requests.post(f"https://api.telegram.org/bot{s.telegram_token}/sendMessage",
                              json={"chat_id": s.telegram_chat_id, "text": text}, timeout=10)
                M_SENT.labels("telegram").inc()
            except requests.RequestException as exc:
                log.warning("Telegram lỗi: %s", exc)
        if s.webhook_url:
            try:
                requests.post(s.webhook_url, json={"content": text, "alert": a}, timeout=10)
                M_SENT.labels("webhook").inc()
            except requests.RequestException as exc:
                log.warning("Webhook lỗi: %s", exc)
        try:
            es.index(index="alerts", id=aid, document=a)
            M_SENT.labels("elasticsearch").inc()
        except Exception as exc:  # noqa: BLE001
            log.warning("ES lỗi: %s", exc)
        log.info("%s", text.replace("\n", " | "))
        c.commit(msg)


if __name__ == "__main__":
    main()
