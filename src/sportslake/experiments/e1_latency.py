"""Thí nghiệm E1 (RQ1): phân rã độ trễ đầu-cuối theo từng khâu.

Mỗi sự kiện mang mốc thời gian của từng khâu:
  source_ts      lúc sự kiện xuất hiện ở nguồn (Mock: lúc xảy ra trong kịch bản; replay: lúc đến hạn phát)
  ingest_ts      lúc collector/replay gửi vào Kafka
  kafka_ts       lúc broker ghi nhận (LogAppendTime)
  processed_ts   lúc vi lô Spark bắt đầu xử lý
  commit_ts      lúc giao dịch Delta (MERGE INTO) được commit  <- lấy từ Change Data Feed (_commit_timestamp)
  es_indexed_ts  lúc tài liệu vào Elasticsearch
  alert_sent_ts  lúc cảnh báo được alerter gửi đi (với sự kiện sinh cảnh báo V1)

Chỉ tính các sự kiện chưa từng bị sửa (version_count = 1) để mốc thời gian là của lần ghi đầu.

  python -m sportslake.experiments.e1_latency --source mock --since-min 60 --label mock_x10
  python -m sportslake.experiments.e1_latency --source statsbomb --since-min 30 --label replay_x1
"""
from __future__ import annotations

import argparse
import time
from datetime import datetime, timezone

from .. import config as C
from ..config import get_settings
from ..serving.lake import cdf, delta_table, history
from .common import md_table, plt, results_dir, summary, write_csv, write_json

STAGES = [
    ("source→ingest", "source_ts", "ingest_ts"),
    ("ingest→kafka", "ingest_ts", "kafka_ts"),
    ("kafka→spark", "kafka_ts", "processed_ts"),
    ("spark→commit", "processed_ts", "commit_ts"),
    ("commit→ES", "commit_ts", "es_indexed_ts"),
    ("ĐẦU-CUỐI nguồn→commit", "source_ts", "commit_ts"),
    ("ĐẦU-CUỐI nguồn→ES", "source_ts", "es_indexed_ts"),
]


def _ms(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp() * 1000
        except ValueError:
            return None
    if hasattr(v, "timestamp"):
        if getattr(v, "tzinfo", None) is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.timestamp() * 1000
    return None


def _start_version(since_ms: float) -> int:
    hist = history(C.T_SILVER_EVENTS)
    versions = sorted(((h.get("version"), h.get("timestamp")) for h in hist if h.get("version") is not None))
    start = 0
    for v, ts in versions:
        if ts is not None and ts < since_ms - 60_000:
            start = v
    return start


def es_times(event_ids: list[str]) -> dict[str, float]:
    out = {}
    try:
        from elasticsearch import Elasticsearch

        es = Elasticsearch(get_settings().es_url, request_timeout=60)
        for i in range(0, len(event_ids), 1000):
            chunk = event_ids[i:i + 1000]
            r = es.mget(index="match_events", ids=chunk, source_includes=["es_indexed_ts", "version_count"])
            for d in r["docs"]:
                if d.get("found"):
                    out[d["_id"]] = _ms(d["_source"].get("es_indexed_ts"))
    except Exception as exc:  # noqa: BLE001
        print(f"(Bỏ qua Elasticsearch: {exc})")
    return out


def alert_latencies(since_ms: float, source_prefix: str | None) -> list[float]:
    try:
        from elasticsearch import Elasticsearch

        es = Elasticsearch(get_settings().es_url, request_timeout=60)
        r = es.search(index="alerts", size=5000, query={"bool": {"filter": [
            {"term": {"version": "V1"}}, {"range": {"alert_sent_ts": {"gte": int(since_ms)}}}]}})
        return [h["_source"]["source_to_alert_s"] for h in r["hits"]["hits"]
                if h["_source"].get("source_to_alert_s") is not None]
    except Exception:  # noqa: BLE001
        return []


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=None, help="mock | apifootball | statsbomb | replay-api | inject")
    ap.add_argument("--since-min", type=float, default=60)
    ap.add_argument("--match-prefix", default=None)
    ap.add_argument("--label", default="run")
    ap.add_argument("--no-es", action="store_true")
    a = ap.parse_args()
    since_ms = (time.time() - a.since_min * 60) * 1000

    import pyarrow.compute as pc

    tbl = delta_table(C.T_SILVER_EVENTS).to_pyarrow_table(columns=[
        "event_id", "match_id", "source", "category", "version_count", "source_ts", "ingest_ts", "kafka_ts",
        "processed_ts", "first_seen_ts"])
    rows = [r for r in tbl.to_pylist() if r["version_count"] == 1 and _ms(r["first_seen_ts"]) and _ms(r["first_seen_ts"]) >= since_ms
            and (a.source is None or r["source"] == a.source)
            and (a.match_prefix is None or str(r["match_id"]).startswith(a.match_prefix))]
    if not rows:
        raise SystemExit("Không có sự kiện phù hợp trong khoảng thời gian này.")
    ids = {r["event_id"] for r in rows}
    ch = cdf(C.T_SILVER_EVENTS, _start_version(since_ms))
    ch = ch.filter(pc.equal(ch["_change_type"], "insert")).select(["event_id", "_commit_timestamp"]).to_pylist()
    commit = {}
    for c in ch:
        if c["event_id"] in ids:
            commit[c["event_id"]] = _ms(c["_commit_timestamp"])
    es_t = {} if a.no_es else es_times(list(ids))
    for r in rows:
        r["commit_ts"] = commit.get(r["event_id"])
        r["es_indexed_ts"] = es_t.get(r["event_id"])
        for k in ("source_ts", "ingest_ts", "kafka_ts", "processed_ts"):
            r[k] = _ms(r[k])

    out = []
    stage_vals = {}
    for name, a_col, b_col in STAGES:
        vals = [(r[b_col] - r[a_col]) / 1000 for r in rows if r.get(a_col) is not None and r.get(b_col) is not None]
        stage_vals[name] = vals
        out.append({"label": a.label, "stage": name, **summary(vals)})
    al = alert_latencies(since_ms, a.match_prefix)
    if al:
        stage_vals["ĐẦU-CUỐI nguồn→cảnh báo"] = al
        out.append({"label": a.label, "stage": "ĐẦU-CUỐI nguồn→cảnh báo", **summary(al)})
    write_csv("e1_latency.csv", out, append=True)
    write_json(f"e1_latency_{a.label}.json", {"label": a.label, "source": a.source, "events": len(rows), "stages": out})
    print(f"{len(rows)} sự kiện, nhãn {a.label}\n")
    print(md_table(out, ["stage", "n", "p50", "p95", "p99", "max"]))

    p = plt()
    fig, ax = p.subplots(figsize=(7.5, 3.6))
    names = [n for n, v in stage_vals.items() if v]
    ax.boxplot([stage_vals[n] for n in names], orientation="horizontal", showfliers=False)
    ax.set_yticks(range(1, len(names) + 1), names)
    ax.set_xlabel("giây")
    ax.set_title(f"E1 - độ trễ theo khâu ({a.label})")
    fig.tight_layout()
    fig.savefig(results_dir() / f"e1_latency_{a.label}.png")
    print(f"\nKết quả: {results_dir()}")


if __name__ == "__main__":
    main()
