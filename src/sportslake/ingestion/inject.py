"""Bơm luồng sự kiện kiểm thử có đáp án biết trước (thí nghiệm E3).

Luồng gồm: sự kiện mới, bản trùng (gửi lại nguyên văn), sửa (seq lớn hơn), thu hồi (RETRACT),
đảo thứ tự (bản cũ đến sau bản mới), và bản ghi lỗi (phải vào DLQ). Gửi rải đều trong
--duration-s giây để có thể dừng broker/executor/driver giữa chừng.

  python -m sportslake.ingestion.inject --run-id e3a --events 400 --duration-s 240
Đáp án ghi ở data/experiments/e3/<run-id>_truth.json; kiểm chứng bằng
  python -m sportslake.experiments.e3_verify --run-id e3a
"""
from __future__ import annotations

import argparse
import json
import random
import time

from ..canonical import make_event, match_clock
from ..config import DATA_DIR, TOPIC_EVENTS, get_settings
from ..kafka_io import AvroEncoder, DeliveryTracker, ensure_topics, make_producer
from ..logutil import setup_logging

log = setup_logging("inject")
CATS = ["OTHER"] * 6 + ["SHOT"] * 3 + ["FOUL", "CARD_YELLOW", "SUB", "GOAL"]


def plan(run_id: str, n: int, rates: dict, seed: int):
    rng = random.Random(seed)
    mid = f"e3-{run_id}"
    msgs = []  # (thứ tự gửi, record)
    truth = {}
    for i in range(n):
        minute = int(i * 90 / n)
        period = 1 if minute < 45 else 2
        cat = rng.choice(CATS)
        base = dict(event_id=f"{mid}-ev{i:05d}", match_id=mid, source="inject", category=cat,
                    event_type=cat.title(), period=period, minute=minute, second=rng.randint(0, 59),
                    team_id="T1" if rng.random() < 0.5 else "T2", player_name=f"P{rng.randint(1, 22)}")
        base["match_clock_s"] = match_clock(period, minute, base["second"])
        order = i * 10
        v1 = make_event(**base, op="UPSERT", seq=1)
        seq_msgs = [(order, v1)]
        final = {"present": True, "deleted": False, "player_name": base["player_name"], "seq": 1}
        r = rng.random()
        if r < rates["update"]:
            v2 = make_event(**{**base, "player_name": base["player_name"] + "-fixed"}, op="UPSERT", seq=2)
            if rng.random() < rates["out_of_order"]:
                seq_msgs = [(order, v2), (order + 3, v1)]  # bản mới tới trước, bản cũ tới sau
            else:
                seq_msgs.append((order + 3, v2))
            final.update(player_name=base["player_name"] + "-fixed", seq=2)
        elif r < rates["update"] + rates["retract"]:
            v3 = make_event(**base, op="RETRACT", seq=3)
            if rng.random() < rates["out_of_order"]:
                seq_msgs = [(order, v3), (order + 4, v1)]  # RETRACT tới trước UPSERT
            else:
                seq_msgs.append((order + 4, v3))
            final.update(deleted=True, seq=3)
        # bản trùng gửi lại nguyên văn
        dups = []
        for o, m in seq_msgs:
            if rng.random() < rates["dup"]:
                dups.append((o + rng.choice([1, 2, 7, 25]), m))
        msgs.extend(seq_msgs + dups)
        truth[base["event_id"]] = final
    # bản ghi lỗi -> DLQ
    n_bad = int(n * rates["bad"])
    for j in range(n_bad):
        bad = make_event(event_id=f"{mid}-bad{j:04d}", match_id=mid, source="inject", category="SHOT",
                         event_type="Shot", period=2, minute=500 + j, op="UPSERT", seq=1)
        msgs.append((rng.randint(0, n * 10), bad))
    msgs.sort(key=lambda x: x[0])
    return mid, [m for _, m in msgs], truth, n_bad


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--events", type=int, default=400)
    ap.add_argument("--duration-s", type=float, default=240)
    ap.add_argument("--dup-rate", type=float, default=0.3)
    ap.add_argument("--update-rate", type=float, default=0.15)
    ap.add_argument("--retract-rate", type=float, default=0.10)
    ap.add_argument("--out-of-order-rate", type=float, default=0.3)
    ap.add_argument("--bad-rate", type=float, default=0.02)
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()
    rates = {"dup": a.dup_rate, "update": a.update_rate, "retract": a.retract_rate,
             "out_of_order": a.out_of_order_rate, "bad": a.bad_rate}
    mid, msgs, truth, n_bad = plan(a.run_id, a.events, rates, a.seed)
    out_dir = DATA_DIR / "experiments" / "e3"
    out_dir.mkdir(parents=True, exist_ok=True)
    s = get_settings()
    ensure_topics(s)
    enc = AvroEncoder(TOPIC_EVENTS, s.schema_registry_url)
    producer = make_producer(s, f"inject-{a.run_id}")
    tracker = DeliveryTracker()
    gap = a.duration_s / max(1, len(msgs))
    t0 = time.time()
    for i, m in enumerate(msgs):
        m = dict(m)
        now = int(time.time() * 1000)
        m["source_ts"] = m["ingest_ts"] = now
        while True:
            try:
                producer.produce(TOPIC_EVENTS, key=mid.encode(), value=enc.encode(m), on_delivery=tracker.callback())
                break
            except BufferError:
                producer.poll(0.2)
        producer.poll(0)
        if i % 100 == 0:
            log.info("%d/%d thông điệp (delivered=%d, failed=%d)", i, len(msgs), tracker.ok, tracker.failed)
        time.sleep(max(0.0, t0 + (i + 1) * gap - time.time()))
    producer.flush(300)
    summary = {"run_id": a.run_id, "match_id": mid, "messages": len(msgs), "unique_events": len(truth),
               "bad": n_bad, "delivered": tracker.ok, "failed": tracker.failed,
               "started_ms": int(t0 * 1000), "finished_ms": int(time.time() * 1000), "rates": rates}
    (out_dir / f"{a.run_id}_truth.json").write_text(json.dumps({"summary": summary, "truth": truth}, indent=1),
                                                     encoding="utf-8")
    log.info("Xong: %s", json.dumps(summary))


if __name__ == "__main__":
    main()
