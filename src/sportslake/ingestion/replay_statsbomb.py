"""Phát lại dữ liệu sự kiện StatsBomb (lịch sử, nhiều mùa giải) vào Kafka theo đúng nhịp thời gian thật,
có thể tăng tốc x10 ... x1000 để kiểm thử tải.

  python -m sportslake.ingestion.replay_statsbomb --competition 43 --season 106 --matches 4 --speed 10
  python -m sportslake.ingestion.replay_statsbomb --all-downloaded --parallel 8 --speed 1000 --only-key
  python -m sportslake.ingestion.replay_statsbomb --match-ids 3869685 --speed 60 --id-suffix run2

Mỗi trận được phát song song (lệch giờ bóng lăn --stagger-s giây), sinh:
  * match.events: mọi sự kiện (hoặc chỉ sự kiện quan trọng với --only-key)
  * match.state : trạng thái trận (NS, 1H, HT, 2H, FT, tỷ số) khi đổi hiệp hoặc có bàn thắng
"""
from __future__ import annotations

import argparse
import heapq
import json
import signal
import time

from ..config import DATA_DIR, TOPIC_EVENTS, TOPIC_STATE, get_settings
from ..kafka_io import AvroEncoder, DeliveryTracker, ensure_topics, make_producer
from ..logutil import setup_logging
from .statsbomb import (categorize, downloaded_matches, load_events, load_matches, period_durations, real_elapsed_s,
                        sb_match_id, to_canonical)

log = setup_logging("replay-statsbomb")
STOP = False


def _stop(*_):
    global STOP
    STOP = True


def build_timeline(match: dict, only_key: bool, suffix: str):
    """Danh sách (giây thực kể từ bóng lăn, loại, dữ liệu) của một trận."""
    events = load_events(DATA_DIR, match["match_id"])
    events.sort(key=lambda e: e.get("index", 0))
    dur = period_durations(events)
    items = []
    for i, ev in enumerate(events):
        t = real_elapsed_s(ev, dur)
        tname = (ev.get("type") or {}).get("name")
        if tname in ("Half Start", "Half End"):
            items.append((t, "period", (int(ev["period"]), tname, int(ev.get("minute") or 0))))
            continue
        if only_key:
            cat, *_ = categorize(ev)
            if cat == "OTHER":
                continue
        items.append((t, "event", (i, ev)))
    items.sort(key=lambda x: x[0])
    return items


class MatchReplayer:
    def __init__(self, match: dict, suffix: str, start_wall: float, speed: float, only_key: bool):
        self.match = match
        self.suffix = suffix
        self.mid = sb_match_id(match) + (f"-{suffix}" if suffix else "")
        self.items = build_timeline(match, only_key, suffix)
        self.start = start_wall
        self.speed = speed
        self.i = 0
        self.home_id = match["home_team"]["home_team_id"]
        self.h = self.a = 0
        self.status = "NS"
        self.elapsed = 0
        self.seq = 0
        self.ended_periods = set()
        self.max_period = max([x[2][0] for x in self.items if x[1] == "period"] + [2])

    def next_wall(self) -> float | None:
        if self.i >= len(self.items):
            return None
        return self.start + self.items[self.i][0] / self.speed

    def state_record(self, src_ms: int) -> dict:
        m = self.match
        comp, season = m.get("competition") or {}, m.get("season") or {}
        self.seq += 1
        return {"match_id": self.mid, "source": "statsbomb", "seq": src_ms * 1000 + self.seq % 1000,
                "league_id": str(comp.get("competition_id")), "league_name": comp.get("competition_name"),
                "season": season.get("season_name"), "round": (m.get("competition_stage") or {}).get("name"),
                "home_team_id": f"sb-{self.home_id}", "home_team": m["home_team"]["home_team_name"],
                "away_team_id": f"sb-{m['away_team']['away_team_id']}", "away_team": m["away_team"]["away_team_name"],
                "home_goals": self.h, "away_goals": self.a, "status_short": self.status, "status_long": None,
                "elapsed": self.elapsed, "extra": None, "kickoff_ts": int(self.start * 1000),
                "venue": (m.get("stadium") or {}).get("name"), "source_ts": src_ms, "ingest_ts": int(time.time() * 1000),
                "schema_version": 1}

    def step(self, enc_ev: AvroEncoder, enc_st: AvroEncoder, producer, tracker) -> int:
        t, kind, payload = self.items[self.i]
        self.i += 1
        src_ms = int((self.start + t / self.speed) * 1000)
        if kind == "period":
            period, name, minute = payload
            if name == "Half Start":
                self.status = {1: "1H", 2: "2H", 3: "ET", 4: "ET", 5: "P"}.get(period, "2H")
            else:
                self.ended_periods.add(period)
                if period >= self.max_period:
                    self.status = "PEN" if period == 5 else ("AET" if period >= 3 else "FT")
                elif period == 1:
                    self.status = "HT"
                else:
                    self.status = "BT"
            self.elapsed = max(self.elapsed, minute)
            _produce(producer, TOPIC_STATE, self.mid, enc_st.encode(self.state_record(src_ms)), tracker)
            return 1
        idx, ev = payload
        now_ms = int(time.time() * 1000)
        rec = to_canonical(ev, self.match, seq=idx, source_ts=src_ms, ingest_ts=now_ms)
        if self.suffix:
            rec["match_id"] = self.mid
            rec["event_id"] = f"{rec['event_id']}-{self.suffix}"
        _produce(producer, TOPIC_EVENTS, self.mid, enc_ev.encode(rec), tracker)
        n = 1
        self.elapsed = max(self.elapsed, int(ev.get("minute") or 0) + 1)
        if rec["category"] in ("GOAL", "OWN_GOAL") and int(ev.get("period") or 1) < 5:
            if (ev.get("team") or {}).get("id") == self.home_id:
                self.h += 1
            else:
                self.a += 1
            _produce(producer, TOPIC_STATE, self.mid, enc_st.encode(self.state_record(src_ms)), tracker)
            n += 1
        return n


def _produce(producer, topic: str, key: str, value: bytes, tracker) -> None:
    while True:
        try:
            producer.produce(topic, key=key.encode(), value=value, on_delivery=tracker.callback())
            return
        except BufferError:
            producer.poll(0.2)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--competition", type=int)
    ap.add_argument("--season", type=int)
    ap.add_argument("--match-ids", default="")
    ap.add_argument("--all-downloaded", action="store_true")
    ap.add_argument("--matches", type=int, default=4, help="số trận tối đa")
    ap.add_argument("--parallel", type=int, default=4, help="số trận phát đồng thời")
    ap.add_argument("--stagger-s", type=float, default=60, help="lệch giờ bóng lăn giữa các trận (giây thực trước khi chia speed)")
    ap.add_argument("--speed", type=float, default=10.0)
    ap.add_argument("--only-key", action="store_true", help="chỉ phát sự kiện quan trọng (bỏ chuyền bóng, rê bóng...)")
    ap.add_argument("--id-suffix", default="", help="hậu tố id để phát lại cùng trận như dữ liệu mới (kiểm thử tải)")
    ap.add_argument("--max-seconds", type=float, default=0, help="dừng sau N giây (0 = phát hết)")
    a = ap.parse_args()
    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    if a.match_ids:
        wanted = {int(x) for x in a.match_ids.split(",")}
        matches = [m for m in downloaded_matches(DATA_DIR) if m["match_id"] in wanted]
    elif a.all_downloaded:
        matches = downloaded_matches(DATA_DIR)
    else:
        if a.competition is None or a.season is None:
            ap.error("cần --competition và --season, hoặc --match-ids, hoặc --all-downloaded")
        all_m = load_matches(DATA_DIR, a.competition, a.season)
        have = {m["match_id"] for m in downloaded_matches(DATA_DIR)}
        matches = [m for m in sorted(all_m, key=lambda m: (m["match_date"], m.get("kick_off") or "")) if m["match_id"] in have]
    matches = matches[: a.matches] if a.matches else matches
    if not matches:
        raise SystemExit("Không có trận nào đã tải. Chạy: python -m sportslake.tools.download_data")

    s = get_settings()
    ensure_topics(s)
    producer = make_producer(s, "replay-statsbomb")
    tracker = DeliveryTracker()
    enc_ev = AvroEncoder(TOPIC_EVENTS, s.schema_registry_url)
    enc_st = AvroEncoder(TOPIC_STATE, s.schema_registry_url)

    pending = list(matches)
    active: list[MatchReplayer] = []
    t0 = time.time()
    sent = 0
    last_log = t0
    k = 0
    while (pending or active) and not STOP:
        if a.max_seconds and time.time() - t0 > a.max_seconds:
            log.info("Đủ %ss theo --max-seconds, dừng", a.max_seconds)
            break
        while pending and len(active) < a.parallel:
            m = pending.pop(0)
            start = time.time() + 2 + (k % a.parallel) * a.stagger_s / a.speed
            r = MatchReplayer(m, a.id_suffix, start, a.speed, a.only_key)
            log.info("Bắt đầu %s: %s vs %s (%d mục)", r.mid, m["home_team"]["home_team_name"],
                     m["away_team"]["away_team_name"], len(r.items))
            _produce(producer, TOPIC_STATE, r.mid, enc_st.encode(r.state_record(int(time.time() * 1000))), tracker)
            sent += 1
            active.append(r)
            k += 1
        # phát mọi mục đã tới hạn của mọi trận, theo thứ tự thời gian
        heap = [(r.next_wall(), id(r), r) for r in active if r.next_wall() is not None]
        heapq.heapify(heap)
        now = time.time()
        burst = 0
        while heap and heap[0][0] <= now and burst < 50000:
            _, _, r = heapq.heappop(heap)
            sent += r.step(enc_ev, enc_st, producer, tracker)
            burst += 1
            nw = r.next_wall()
            if nw is not None:
                heapq.heappush(heap, (nw, id(r), r))
        producer.poll(0)
        for r in [r for r in active if r.next_wall() is None]:
            log.info("Xong %s, tỷ số %d-%d", r.mid, r.h, r.a)
            active.remove(r)
        if time.time() - last_log > 10:
            el = time.time() - t0
            log.info("Đã gửi %d bản ghi (%.0f/s), đang phát %d trận, còn chờ %d trận | lỗi gửi=%d",
                     sent, sent / el, len(active), len(pending), tracker.failed)
            last_log = time.time()
        if burst == 0:
            nxt = min([x[0] for x in heap], default=now + 0.05)
            time.sleep(max(0.0, min(0.2, nxt - time.time())))
    producer.flush(60)
    el = time.time() - t0
    summary = {"sent": sent, "seconds": round(el, 1), "rate_per_s": round(sent / max(el, 1e-9), 1),
               "delivered": tracker.ok, "failed": tracker.failed, "matches": len(matches), "speed": a.speed}
    log.info("Hoàn tất: %s", json.dumps(summary))


if __name__ == "__main__":
    main()
