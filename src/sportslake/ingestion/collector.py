"""Collector: gọi API-Football (live) hoặc Mock API theo bộ lập lịch, suy ra sự kiện và gửi vào Kafka.

Chạy:  python -m sportslake.ingestion.collector [--mode mock|live|replay|off]
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from ..config import DATA_DIR, TOPIC_EVENTS, TOPIC_STATE, TOPIC_STATS, get_settings
from ..kafka_io import AvroEncoder, DeliveryTracker, ensure_topics, make_producer
from ..logutil import setup_logging
from .apifootball import ApiFootballClient, ApiResponse, QuotaExceeded
from .differ import SnapshotDiffer
from .recorder import RawRecorder, find_recordings, iter_recorded
from .scheduler import LIVE_STATUSES, Scheduler, wait_until

log = setup_logging("collector")

M_REQ = Counter("collector_api_requests_total", "Số request API", ["endpoint"])
M_QUOTA = Gauge("collector_quota_remaining", "Hạn mức còn lại trong ngày")
M_EVENTS = Counter("collector_events_total", "Sự kiện đã gửi vào Kafka", ["op", "category"])
M_STATES = Counter("collector_states_total", "Bản ghi trạng thái đã gửi")
M_INTERVAL = Gauge("collector_poll_interval_seconds", "Khoảng cách giữa hai lần gọi live gần nhất")
M_WEIGHT = Gauge("collector_scheduler_weight", "Trọng số hiện tại của bộ lập lịch")
M_DELAY = Histogram("collector_detection_delay_seconds", "Độ trễ từ lúc sự kiện xảy ra tới lúc collector phát hiện",
                    buckets=(1, 2, 5, 10, 20, 30, 60, 90, 120, 180, 300, 600))
M_LIVE = Gauge("collector_live_fixtures", "Số trận đang live")
M_ROLLBACK = Counter("collector_rollbacks_total", "Số lượt gọi bị hoàn tác vì gửi Kafka thất bại")
FLUSH_MAX_S = 330  # > message.timeout.ms của producer (300 s): sau đó mọi thông điệp hoặc đã tới hoặc đã báo lỗi

STATS_MAP = {
    "Total Shots": "shots_total", "Shots on Goal": "shots_on_target", "Ball Possession": "possession_pct",
    "Corner Kicks": "corners", "Fouls": "fouls", "Yellow Cards": "yellow_cards", "Red Cards": "red_cards",
    "expected_goals": "xg",
}


def parse_stats(fixture_id: str, item: dict, seq: int, source: str, ts: int) -> dict:
    team = item.get("team") or {}
    row = {"match_id": str(fixture_id), "team_id": str(team.get("id")), "team_name": team.get("name"),
           "source": source, "seq": seq, "shots_total": None, "shots_on_target": None, "possession_pct": None,
           "corners": None, "fouls": None, "yellow_cards": None, "red_cards": None, "xg": None,
           "stats_json": json.dumps(item.get("statistics") or [], ensure_ascii=False),
           "source_ts": ts, "ingest_ts": int(time.time() * 1000), "schema_version": 1}
    for s in item.get("statistics") or []:
        key = STATS_MAP.get(s.get("type"))
        val = s.get("value")
        if key is None or val is None:
            continue
        try:
            if isinstance(val, str):
                val = val.replace("%", "").strip()
            row[key] = float(val) if key in ("possession_pct", "xg") else int(float(val))
        except ValueError:
            pass
    return row


class Collector:
    def __init__(self, mode: str):
        self.settings = s = get_settings()
        self.mode = mode
        self.source = {"live": "apifootball", "mock": "mock", "replay": "replay-api"}[mode]
        self.stop = False
        ensure_topics(s)
        self.producer = make_producer(s, f"collector-{mode}")
        self.tracker = DeliveryTracker()
        self.enc_events = AvroEncoder(TOPIC_EVENTS, s.schema_registry_url)
        self.enc_state = AvroEncoder(TOPIC_STATE, s.schema_registry_url)
        self.enc_stats = AvroEncoder(TOPIC_STATS, s.schema_registry_url)
        # replay luôn bắt đầu từ trạng thái rỗng để phát lại toàn bộ sự kiện
        self.differ = SnapshotDiffer(self.source, None if mode == "replay" else DATA_DIR / "state" / f"differ_{mode}.json")
        self.recorder = RawRecorder(DATA_DIR, "apifootball" if mode == "live" else "mock") \
            if (s.record_raw and mode in ("live", "mock")) else None
        self.poll_log = DATA_DIR / "metrics" / f"collector_polls_{mode}.jsonl"
        self.poll_log.parent.mkdir(parents=True, exist_ok=True)
        self._last_logged: dict[str, tuple] = {}
        self._pending_final: set[str] = set()
        self._finalized_now: list[str] = []
        self.client: ApiFootballClient | None = None
        self.time_scale = 1.0
        if mode == "live":
            self.client = ApiFootballClient(s.apifootball_base_url, s.apifootball_key)
        elif mode == "mock":
            self.client = ApiFootballClient(s.mock_api_url, s.apifootball_key or "mock-key")
            self.time_scale = self._mock_speed()
        self.scheduler = Scheduler(policy=s.scheduler_policy, daily_budget=s.daily_budget,
                                   minute_limit=s.minute_limit, reserve=s.budget_reserve,
                                   fixed_interval_s=s.fixed_interval_s, time_scale=self.time_scale)

    def _mock_speed(self) -> float:
        for _ in range(60):
            try:
                cfg = requests.get(f"{self.settings.mock_api_url}/_admin/config", timeout=5).json()
                log.info("Mock API: %s", cfg)
                return float(cfg.get("speed", 1.0))
            except Exception as exc:  # noqa: BLE001
                log.info("Chờ Mock API... (%s)", exc)
                time.sleep(2)
        raise RuntimeError("Mock API không phản hồi")

    # ------------------------------------------------------------------
    def _call(self, fn, *args) -> ApiResponse | None:
        try:
            resp = fn(*args)
        except QuotaExceeded as exc:
            log.error("Hết hạn mức API: %s", exc)
            self.scheduler.record_request(time.time(), remaining_day=0)
            return None
        except requests.RequestException as exc:
            log.error("Lỗi mạng khi gọi API: %s", exc)
            return None
        now = time.time()
        self.scheduler.record_request(now, resp.remaining_day)
        M_REQ.labels(resp.endpoint).inc()
        if resp.remaining_day is not None:
            M_QUOTA.set(resp.remaining_day)
        if self.recorder:
            self.recorder.write(resp.endpoint, resp.params, resp.body, resp.received_ts_ms)
        with self.poll_log.open("a", encoding="utf-8") as f:
            f.write(json.dumps({"ts": resp.received_ts_ms, "endpoint": resp.endpoint, "params": resp.params,
                                "policy": self.scheduler.policy, "remaining_day": resp.remaining_day,
                                "weight": self.scheduler.current_weight(now), "latency_ms": round(resp.latency_ms, 1)}) + "\n")
        return resp

    def _produce(self, topic: str, enc: AvroEncoder, key: str, rec: dict) -> None:
        while True:
            try:
                self.producer.produce(topic, key=key.encode(), value=enc.encode(rec), on_delivery=self.tracker.callback())
                break
            except BufferError:
                self.producer.poll(0.5)
        self.producer.poll(0)

    def handle_fixture(self, fx: dict, ts_ms: int, events_override: list | None = None) -> None:
        events, state = self.differ.diff_fixture(fx, ts_ms, events_override=events_override,
                                                 ingest_ts_ms=int(time.time() * 1000))
        for ev in events:
            self._produce(TOPIC_EVENTS, self.enc_events, ev["match_id"], ev)
            M_EVENTS.labels(ev["op"], ev["category"]).inc()
            if ev["op"] == "UPSERT" and ev["source_ts"] < ev["ingest_ts"]:
                M_DELAY.observe((ev["ingest_ts"] - ev["source_ts"]) / 1000.0)
        if state:
            self._produce(TOPIC_STATE, self.enc_state, state["match_id"], state)
            M_STATES.inc()
        fid = str(fx["fixture"]["id"])
        key = (state["status_short"], state["home_goals"], state["away_goals"]) if state else None
        if events or (key and key != self._last_logged.get(fid)):
            if key:
                self._last_logged[fid] = key
            ops = ", ".join(f"{e['op']} {e['category']} {e['minute']}'" for e in events[:6])
            log.info("Trận %s: %d thay đổi [%s]%s", fid, len(events), ops,
                     f" | {state['status_short']} {state['home_goals']}-{state['away_goals']} ({state['elapsed']}')" if state else "")

    def handle_stats(self, fixture_id: str, resp: ApiResponse) -> None:
        for item in resp.items:
            row = parse_stats(fixture_id, item, resp.received_ts_ms, self.source, resp.received_ts_ms)
            self._produce(TOPIC_STATS, self.enc_stats, row["match_id"], row)

    def _in_scope(self, fx: dict) -> bool:
        if not self.settings.league_ids:
            return True
        return str((fx.get("league") or {}).get("id")) in self.settings.league_ids

    # ------------------------------------------------------------------
    def discovery(self) -> None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        resp = self._call(self.client.fixtures_by_date, today)
        now = time.time()
        self.scheduler.last_discovery_ts = now
        if not resp:
            return
        n = 0
        for fx in resp.items:
            if not self._in_scope(fx):
                continue
            f = fx["fixture"]
            status = (f.get("status") or {}).get("short") or "NS"
            if status in ("FT", "AET", "PEN", "PST", "CANC") and str(f["id"]) not in self.scheduler.fixtures:
                continue
            self.scheduler.upsert_fixture(str(f["id"]), float(f.get("timestamp") or now), status,
                                          (f.get("status") or {}).get("elapsed"),
                                          (fx.get("goals") or {}).get("home"), (fx.get("goals") or {}).get("away"), now)
            n += 1
        log.info("Discovery %s: theo dõi %d trận trong các giải %s", today, n, ",".join(self.settings.league_ids))

    # ------------------------------------------------------------------ giao dịch một lượt gọi
    def _flush_all(self) -> int:
        """Chờ tới khi mọi thông điệp đã được Kafka xác nhận hoặc báo lỗi. Trả về số còn tồn."""
        t0 = time.time()
        remaining = self.producer.flush(10)
        while remaining > 0 and time.time() - t0 < FLUSH_MAX_S and not self.stop:
            log.warning("Chờ Kafka xác nhận %d thông điệp...", remaining)
            remaining = self.producer.flush(10)
        return remaining

    def _transaction(self, work) -> bool:
        """Chạy một lượt xử lý; chỉ lưu trạng thái differ khi MỌI thông điệp đã gửi thành công.

        Nếu có thông điệp lỗi/chưa tới, bộ nhớ differ được hoàn tác về trước lượt này, nên lượt sau
        phát hiện lại đúng các thay đổi đó và gửi lại (MERGE INTO phía Spark là idempotent)."""
        cp = self.differ.checkpoint()
        failed_before = self.tracker.failed
        self._finalized_now = []
        work()
        remaining = self._flush_all()
        if remaining > 0 or self.tracker.failed > failed_before:
            self.differ.restore(cp)
            for fid in self._finalized_now:
                # chốt trận chưa tới Kafka -> sẽ chốt lại
                self._pending_final.add(fid)
                self.scheduler.fixtures[fid].stats_done.discard("FINAL")
            M_ROLLBACK.inc()
            log.error("Gửi Kafka thất bại (%d lỗi mới, %d chưa xác nhận: %s) -> hoàn tác, sẽ gửi lại ở lượt sau",
                      self.tracker.failed - failed_before, remaining, self.tracker.last_error)
            return False
        self.differ.save()
        return True

    def live_poll(self) -> None:
        self._transaction(self._live_poll_work)

    def retry_pending_finals(self) -> None:
        """Chốt lại các trận mà lần chốt trước không gửi được tới Kafka."""
        for fid in list(self._pending_final):
            wait_until(self.scheduler.minute_slot_free_at(time.time()), lambda: self.stop)
            self._pending_final.discard(fid)
            self._transaction(lambda fid=fid: self.finalize(fid))

    def _live_poll_work(self) -> None:
        resp = self._call(self.client.live_fixtures, self.settings.league_ids)
        now = time.time()
        if self.scheduler.last_live_poll_ts:
            M_INTERVAL.set(now - self.scheduler.last_live_poll_ts)
        self.scheduler.last_live_poll_ts = now
        if not resp:
            return
        live_ids = set()
        for fx in resp.items:
            if not self._in_scope(fx):
                continue
            f = fx["fixture"]
            fid = str(f["id"])
            live_ids.add(fid)
            self.handle_fixture(fx, resp.received_ts_ms)
            st = (f.get("status") or {}).get("short") or "NS"
            tf = self.scheduler.upsert_fixture(fid, float(f.get("timestamp") or now), st,
                                               (f.get("status") or {}).get("elapsed"),
                                               (fx.get("goals") or {}).get("home"),
                                               (fx.get("goals") or {}).get("away"), now)
            if st == "HT" and "HT" not in tf.stats_done and self.scheduler.stats_calls:
                tf.stats_done.add("HT")
                r = self._call(self.client.fixture_statistics, fid)
                if r:
                    self.handle_stats(fid, r)
        for fid in self.scheduler.mark_missing_from_live(live_ids, now):
            self.finalize(fid)
        M_LIVE.set(len(live_ids))
        M_WEIGHT.set(self.scheduler.current_weight(now))

    def finalize(self, fid: str) -> None:
        """Trận rời danh sách live: gọi /fixtures?id= một lần để chốt sự kiện cuối và trạng thái FT."""
        tf = self.scheduler.fixtures[fid]
        if "FINAL" not in tf.stats_done:
            tf.stats_done.add("FINAL")
            self._finalized_now.append(fid)
            r = self._call(self.client.get, "fixtures", {"id": fid})
            if r and r.items:
                self.handle_fixture(r.items[0], r.received_ts_ms)
                st = r.items[0]["fixture"]["status"]["short"]
                tf.status = st if st in ("FT", "AET", "PEN") else "FT"
        if self.scheduler.stats_calls and "FT" not in tf.stats_done:
            tf.stats_done.add("FT")
            r = self._call(self.client.fixture_statistics, fid)
            if r:
                self.handle_stats(fid, r)
        log.info("Trận %s kết thúc", fid)

    def run_polling(self) -> None:
        log.info("Collector chế độ %s, chính sách %s, time_scale=%.1f", self.mode, self.scheduler.policy, self.time_scale)
        while not self.stop:
            now = time.time()
            if self.scheduler.need_discovery(now):
                self.discovery()
                continue
            if self._pending_final:
                time.sleep(5)
                self.retry_pending_finals()
                continue
            nxt = self.scheduler.next_live_poll(now)
            if nxt is None:
                if self.scheduler.remaining_budget(now) - self.scheduler.reserve <= 0:
                    log.warning("Đã dùng hết ngân sách request hôm nay, chờ sang ngày mới (UTC)")
                # không còn trận: chờ tới lần discovery tiếp theo
                wait_until(now + min(self.scheduler.discovery_every_s, 600), lambda: self.stop)
                continue
            nxt = max(nxt, self.scheduler.minute_slot_free_at(nxt))
            if nxt - now > 1:
                log.debug("Lần gọi kế tiếp sau %.1fs | %s", nxt - now, self.scheduler.describe(now))
            wait_until(nxt, lambda: self.stop)
            if self.stop:
                break
            self.live_poll()
        self.producer.flush(30)

    def run_replay(self, speed: float) -> None:
        paths = find_recordings(DATA_DIR, os.environ.get("REPLAY_RAW_SOURCE", "apifootball"),
                                os.environ.get("REPLAY_DAY") or None)
        if not paths:
            log.error("Không có tệp ghi trong %s/raw. Hãy chạy live hoặc mock trước (RECORD_RAW=true).", DATA_DIR)
            return
        log.info("Replay %d tệp với tốc độ x%.1f", len(paths), speed)
        prev_ts = None
        t_start = time.time()
        first_ts = None
        for row in iter_recorded(paths):
            if self.stop:
                break
            if first_ts is None:
                first_ts = row["ts"]
            # giữ đúng nhịp thời gian gốc, chia cho speed
            target = t_start + (row["ts"] - first_ts) / 1000.0 / speed
            wait_until(target, lambda: self.stop)
            now_ms = int(time.time() * 1000)
            body = row["body"]
            ep = row["endpoint"]
            if ep == "fixtures" and ("live" in row["params"] or "id" in row["params"]):
                for fx in body.get("response") or []:
                    self.handle_fixture(fx, now_ms)
            elif ep == "fixtures/statistics":
                fid = str(row["params"].get("fixture"))
                for item in body.get("response") or []:
                    self._produce(TOPIC_STATS, self.enc_stats, fid, parse_stats(fid, item, now_ms, self.source, now_ms))
            prev_ts = row["ts"]
        self.producer.flush(30)
        log.info("Replay xong (bản ghi cuối ts=%s)", prev_ts)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default=os.environ.get("COLLECTOR_MODE", "mock"), choices=["mock", "live", "replay", "off"])
    ap.add_argument("--speed", type=float, default=float(os.environ.get("REPLAY_SPEED", "10")))
    ap.add_argument("--metrics-port", type=int, default=int(os.environ.get("METRICS_PORT", "9109")))
    args = ap.parse_args()
    if args.mode == "off":
        log.info("COLLECTOR_MODE=off -> collector không chạy")
        while True:
            time.sleep(3600)
    start_http_server(args.metrics_port)
    c = Collector(args.mode)

    def _stop(*_):
        c.stop = True
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    if args.mode == "replay":
        c.run_replay(args.speed)
    else:
        c.run_polling()


if __name__ == "__main__":
    main()
