"""Thí nghiệm E3 (RQ2): kiểm chứng tính đúng sau sự cố - không mất, không trùng, đúng sau hiệu chỉnh.

Hai nguồn đáp án:
  --run-id X   luồng bơm có đáp án (sportslake.ingestion.inject)
  --mock       các trận Mock API đã kết thúc (đáp án từ /_admin/truth)

Đồng thời tính thời gian phục hồi từ data/experiments/e3/faults.jsonl (do e3.ps1 ghi) và
data/metrics/streaming_progress.jsonl.

  python -m sportslake.experiments.e3_verify --run-id e3a
  python -m sportslake.experiments.e3_verify --mock
"""
from __future__ import annotations

import argparse
import json
from collections import Counter

import requests

from .. import config as C
from ..config import DATA_DIR, get_settings
from ..serving.lake import delta_table
from .common import md_table, read_jsonl, write_csv, write_json

E3_DIR = DATA_DIR / "experiments" / "e3"


def silver_rows(match_id: str) -> list[dict]:
    import pyarrow.compute as pc

    t = delta_table(C.T_SILVER_EVENTS).to_pyarrow_table(
        columns=["event_id", "match_id", "seq", "op", "is_deleted", "player_name", "version_count", "category"],
        filters=[("match_id", "=", match_id)])
    del pc
    return t.to_pylist()


def bronze_count(match_id: str) -> int | None:
    try:
        t = delta_table(C.T_BRONZE_RAW).to_pyarrow_table(columns=["key", "topic"],
                                                        filters=[("topic", "=", C.TOPIC_EVENTS)])
        return sum(1 for k in t.column("key").to_pylist() if k == match_id)
    except Exception:  # noqa: BLE001
        return None


def dlq_count(match_id: str) -> int | None:
    try:
        return _dlq_count(match_id)
    except Exception as exc:  # noqa: BLE001
        print(f"(Không đọc được DLQ: {exc})")
        return None


def _dlq_count(match_id: str) -> int:
    from confluent_kafka import Consumer, TopicPartition

    s = get_settings()
    c = Consumer({"bootstrap.servers": s.kafka_bootstrap, "group.id": "e3-verify-peek", "enable.auto.commit": False})
    md = c.list_topics(C.TOPIC_DLQ, timeout=10)
    parts = [TopicPartition(C.TOPIC_DLQ, p, 0) for p in md.topics[C.TOPIC_DLQ].partitions]
    c.assign(parts)
    n, idle = 0, 0
    while idle < 5:
        m = c.poll(1.0)
        if m is None:
            idle += 1
            continue
        idle = 0
        if m.error():
            continue
        try:
            r = json.loads(m.value())
        except ValueError:
            continue
        rec = r.get("record") or {}
        if rec.get("match_id") == match_id and r.get("stage") == "silver_events":
            n += 1
    c.close()
    return n


def verify_injected(run_id: str) -> dict:
    data = json.loads((E3_DIR / f"{run_id}_truth.json").read_text(encoding="utf-8"))
    truth, summ = data["truth"], data["summary"]
    mid = summ["match_id"]
    rows = silver_rows(mid)
    by_id = Counter(r["event_id"] for r in rows)
    dup_rows = sum(v - 1 for v in by_id.values() if v > 1)
    got = {r["event_id"]: r for r in rows}
    missing = wrong_state = wrong_value = 0
    for eid, exp in truth.items():
        r = got.get(eid)
        if r is None:
            missing += 1
            continue
        if bool(r["is_deleted"]) != exp["deleted"]:
            wrong_state += 1
        elif not exp["deleted"] and r["player_name"] != exp["player_name"]:
            wrong_value += 1
    unexpected = sum(1 for eid in got if eid not in truth)
    ok = len(truth) - missing - wrong_state - wrong_value
    res = {"run_id": run_id, "messages_sent": summ["messages"], "unique_events": len(truth),
           "bronze_raw_messages": bronze_count(mid), "silver_rows": len(rows), "duplicate_rows": dup_rows,
           "missing": missing, "wrong_deleted_state": wrong_state, "wrong_value_after_update": wrong_value,
           "unexpected_rows": unexpected, "bad_sent": summ["bad"], "dlq_received": dlq_count(mid),
           "final_state_correct_pct": round(100 * ok / len(truth), 2)}
    return res


def verify_mock() -> list[dict]:
    s = get_settings()
    sc = requests.get(f"{s.mock_api_url}/_admin/scenarios", timeout=10).json()["scenarios"]
    out = []
    st = {r["match_id"]: r for r in delta_table(C.T_SILVER_STATE).to_pyarrow_table(
        columns=["match_id", "home_goals", "away_goals", "status_short"]).to_pylist()}
    for x in sc:
        if x["status"] != "FT":
            continue
        fid = str(x["fixture_id"])
        t = requests.get(f"{s.mock_api_url}/_admin/truth", params={"fixture": fid}, timeout=10).json()
        exp = {e["event_id"] for e in t["events"]}
        rows = silver_rows(fid)
        live = {r["event_id"] for r in rows if not r["is_deleted"]}
        state = st.get(fid) or {}
        out.append({"fixture": fid, "match": f"{x['home']} - {x['away']}", "expected_events": len(exp),
                    "silver_live_events": len(live), "missing": len(exp - live), "extra": len(live - exp),
                    "retracted_rows": sum(1 for r in rows if r["is_deleted"]),
                    "updated_rows": sum(1 for r in rows if (r["version_count"] or 0) > 1),
                    "score_expected": f"{t['home_goals']}-{t['away_goals']}",
                    "score_silver": f"{state.get('home_goals')}-{state.get('away_goals')}" if state else None,
                    "status_silver": state.get("status_short"),
                    "correct": exp == live and state.get("home_goals") == t["home_goals"] and state.get("away_goals") == t["away_goals"]})
    return out


def recovery_times() -> list[dict]:
    faults = read_jsonl(E3_DIR / "faults.jsonl")
    prog = read_jsonl(DATA_DIR / "metrics" / "streaming_progress.jsonl")
    out = []
    for f in faults:
        if f.get("action") != "recover":
            continue
        t_rec = f["ts_ms"]
        after = [p for p in prog if p.get("query") == "silver_events" and p.get("timestamp")]
        first = None
        for p in after:
            from datetime import datetime

            ts = datetime.fromisoformat(p["timestamp"].replace("Z", "+00:00")).timestamp() * 1000
            if ts >= t_rec and (p.get("num_input_rows") or 0) > 0:
                first = ts if first is None else min(first, ts)
        down = next((x for x in faults if x.get("target") == f.get("target") and x.get("action") == "fail"
                     and x["ts_ms"] <= t_rec), None)
        out.append({"target": f.get("target"), "downtime_s": round((t_rec - down["ts_ms"]) / 1000, 1) if down else None,
                    "recovery_s": round((first - t_rec) / 1000, 1) if first else None})
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id")
    ap.add_argument("--mock", action="store_true")
    a = ap.parse_args()
    result = {}
    if a.run_id:
        r = verify_injected(a.run_id)
        result["injected"] = r
        write_csv("e3_correctness.csv", [r], append=True)
        print(md_table([r]))
    if a.mock:
        rows = verify_mock()
        result["mock"] = rows
        write_csv("e3_mock_truth.csv", rows)
        print(md_table(rows))
    rec = recovery_times()
    if rec:
        result["recovery"] = rec
        write_csv("e3_recovery.csv", rec)
        print("\nThời gian phục hồi:\n" + md_table(rec))
    write_json(f"e3_{a.run_id or 'mock'}.json", result)


if __name__ == "__main__":
    main()
