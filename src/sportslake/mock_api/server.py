"""Mock API-Football v3: cùng đường dẫn, tham số và định dạng phản hồi, có bộ đếm hạn mức.

Chạy:  uvicorn sportslake.mock_api.server:app --host 0.0.0.0 --port 8000

Đường dẫn giống API thật:
  GET /fixtures?live=all | live=39-140 | date=YYYY-MM-DD | id=<fixture>
  GET /fixtures/events?fixture=<id>
  GET /fixtures/statistics?fixture=<id>
  GET /status                (không tính vào hạn mức, như API thật)
Đường dẫn quản trị (không tính hạn mức):
  GET  /_admin/config, /_admin/scenarios, /_admin/truth?fixture=<id>, /_admin/quota
  POST /_admin/reset_quota, /_admin/restart?matches=3&gap_min=2&speed=10&seed=42&delay_s=30
"""
from __future__ import annotations

import os
import random
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from ..config import DATA_DIR
from ..ingestion.differ import _cancelled_goal_ids, assign_event_ids
from .scenario import Scenario, statsbomb_scenario, synthetic_scenario, visible_events

app = FastAPI(title="Mock API-Football", version="1.0")


class MockState:
    def __init__(self):
        self.lock = threading.Lock()
        self.speed = float(os.environ.get("MOCK_SPEED", "10"))
        self.daily_limit = int(os.environ.get("MOCK_DAILY_LIMIT", "100"))
        self.minute_limit = int(os.environ.get("MOCK_MINUTE_LIMIT", "10"))
        self.provider_delay_s = float(os.environ.get("MOCK_PROVIDER_DELAY_S", "20"))  # giây trận
        self.scenarios: dict[int, Scenario] = {}
        self.day_key = ""
        self.used_today = 0
        self.recent: deque[float] = deque()
        self.restart(int(os.environ.get("MOCK_MATCHES", "3")), float(os.environ.get("MOCK_KICKOFF_GAP_MIN", "2")),
                     self.speed, int(os.environ.get("MOCK_SEED", "42")),
                     float(os.environ.get("MOCK_FIRST_KICKOFF_DELAY_S", "45")),
                     os.environ.get("MOCK_SCENARIO_SOURCE", "synthetic"),
                     float(os.environ.get("MOCK_VAR_PROB", "0.7")))

    def restart(self, n: int, gap_min: float, speed: float, seed: int, delay_s: float, source: str, var_prob: float):
        with self.lock:
            self.speed = speed
            start = time.time()
            base_id = 900_000_000 + (int(start) // 60 % 1_000_000) * 10
            self.scenarios = {}
            sb_matches = []
            if source == "statsbomb":
                try:
                    from ..ingestion.statsbomb import downloaded_matches
                    sb_matches = downloaded_matches(DATA_DIR)
                    random.Random(seed).shuffle(sb_matches)
                except Exception:  # noqa: BLE001
                    sb_matches = []
            for i in range(n):
                fid = base_id + i
                ko = start + delay_s + i * gap_min * 60
                if i < len(sb_matches):
                    sc = statsbomb_scenario(fid, ko, DATA_DIR, sb_matches[i], seed + i, var_prob)
                else:
                    sc = synthetic_scenario(fid, ko, seed + i, var_prob)
                self.scenarios[fid] = sc
            self.source = source if sb_matches or source != "statsbomb" else "synthetic"

    # ---------- hạn mức ----------
    def consume(self) -> tuple[bool, dict, dict]:
        now = time.time()
        with self.lock:
            key = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            if key != self.day_key:
                self.day_key, self.used_today = key, 0
            while self.recent and now - self.recent[0] >= 60:
                self.recent.popleft()
            headers = {}
            if self.used_today >= self.daily_limit:
                err = {"requests": "You have reached the request limit for the day, Go to https://dashboard.api-football.com to upgrade your plan."}
                ok = False
            elif len(self.recent) >= self.minute_limit:
                err = {"rateLimit": f"Too many requests. Your rate limit is {self.minute_limit} requests per minute."}
                ok = False
            else:
                self.used_today += 1
                self.recent.append(now)
                err, ok = {}, True
            headers["x-ratelimit-requests-limit"] = str(self.daily_limit)
            headers["x-ratelimit-requests-remaining"] = str(max(0, self.daily_limit - self.used_today))
            headers["X-RateLimit-Limit"] = str(self.minute_limit)
            headers["X-RateLimit-Remaining"] = str(max(0, self.minute_limit - len(self.recent)))
            return ok, err, headers

    def script_t(self, sc: Scenario, now: float | None = None) -> float:
        return ((now or time.time()) - sc.kickoff_ts) * self.speed


STATE = MockState()
LIVE_SET = {"1H", "HT", "2H", "ET", "BT", "P"}


def _fixture_json(sc: Scenario, now: float, with_events: bool) -> dict:
    t = STATE.script_t(sc, now)
    short, long_, elapsed, extra = sc.status_at(t)
    evs, hg, ag = visible_events(sc, t, STATE.provider_delay_s, STATE.speed) if t >= 0 else ([], 0, 0)
    finished = short == "FT"
    body = {
        "fixture": {
            "id": sc.fixture_id, "referee": sc.referee, "timezone": "UTC",
            "date": datetime.fromtimestamp(sc.kickoff_ts, tz=timezone.utc).isoformat(),
            "timestamp": int(sc.kickoff_ts),
            "periods": {"first": int(sc.kickoff_ts) if t >= 0 else None,
                        "second": int(sc.kickoff_ts + (sc.h1_len + 900) / STATE.speed) if short in ("2H", "FT") else None},
            "venue": {"id": None, "name": sc.venue, "city": None},
            "status": {"long": long_, "short": short, "elapsed": elapsed, "extra": extra},
        },
        "league": sc.league,
        "teams": {"home": {**sc.home, "winner": (hg > ag) if finished else None},
                  "away": {**sc.away, "winner": (ag > hg) if finished else None}},
        "goals": {"home": hg if t >= 0 else None, "away": ag if t >= 0 else None},
        "score": {"halftime": {"home": None, "away": None}, "fulltime": {"home": hg if finished else None, "away": ag if finished else None},
                  "extratime": {"home": None, "away": None}, "penalty": {"home": None, "away": None}},
    }
    if with_events:
        body["events"] = evs
    return body


def _wrap(endpoint: str, params: dict, response: list, errors=None, headers=None) -> JSONResponse:
    return JSONResponse({"get": endpoint, "parameters": params, "errors": errors or [], "results": len(response),
                         "paging": {"current": 1, "total": 1}, "response": response}, headers=headers or {})


def _guard(request: Request, endpoint: str, params: dict):
    if not request.headers.get("x-apisports-key"):
        return _wrap(endpoint, params, [], {"token": "Error/Missing application key. Go to https://www.api-football.com/documentation-v3 to learn how to get your API application key."})
    ok, err, headers = STATE.consume()
    if not ok:
        return _wrap(endpoint, params, [], err, headers)
    return headers


@app.get("/fixtures")
def fixtures(request: Request, live: str | None = None, date: str | None = None, id: int | None = None,  # noqa: A002
             league: int | None = None):
    params = {k: v for k, v in {"live": live, "date": date, "id": id, "league": league}.items() if v is not None}
    g = _guard(request, "fixtures", params)
    if isinstance(g, JSONResponse):
        return g
    now = time.time()
    out = []
    for sc in STATE.scenarios.values():
        if league and int(sc.league["id"]) != league:
            continue
        if id is not None:
            if sc.fixture_id == id:
                out.append(_fixture_json(sc, now, True))
            continue
        if live is not None:
            if live != "all" and str(sc.league["id"]) not in live.split("-"):
                continue
            fx = _fixture_json(sc, now, True)
            if fx["fixture"]["status"]["short"] in LIVE_SET:
                out.append(fx)
            continue
        if date is not None:
            if datetime.fromtimestamp(sc.kickoff_ts, tz=timezone.utc).strftime("%Y-%m-%d") == date:
                out.append(_fixture_json(sc, now, False))
            continue
    return _wrap("fixtures", params, out, headers=g)


@app.get("/fixtures/events")
def fixture_events(request: Request, fixture: int):
    params = {"fixture": fixture}
    g = _guard(request, "fixtures/events", params)
    if isinstance(g, JSONResponse):
        return g
    sc = STATE.scenarios.get(fixture)
    if not sc:
        return _wrap("fixtures/events", params, [], headers=g)
    t = STATE.script_t(sc)
    evs, _, _ = visible_events(sc, t, STATE.provider_delay_s, STATE.speed) if t >= 0 else ([], 0, 0)
    return _wrap("fixtures/events", params, evs, headers=g)


@app.get("/fixtures/statistics")
def fixture_statistics(request: Request, fixture: int):
    params = {"fixture": fixture}
    g = _guard(request, "fixtures/statistics", params)
    if isinstance(g, JSONResponse):
        return g
    sc = STATE.scenarios.get(fixture)
    if not sc:
        return _wrap("fixtures/statistics", params, [], headers=g)
    t = max(0.0, STATE.script_t(sc))
    frac = min(1.0, t / sc.total_len)
    evs, hg, ag = visible_events(sc, t, STATE.provider_delay_s, STATE.speed)
    rng = random.Random(sc.seed * 7 + int(frac * 20))
    out = []
    poss_home = rng.randint(38, 62)
    for side, team, goals in (("home", sc.home, hg), ("away", sc.away, ag)):
        shots = goals * 2 + int(frac * rng.randint(6, 14))
        cards = [e for e in evs if e["team"]["id"] == team["id"] and e["type"] == "Card"]
        out.append({"team": {"id": team["id"], "name": team["name"], "logo": None}, "statistics": [
            {"type": "Shots on Goal", "value": goals + int(shots * 0.25)},
            {"type": "Total Shots", "value": shots},
            {"type": "Fouls", "value": int(frac * rng.randint(6, 14))},
            {"type": "Corner Kicks", "value": int(frac * rng.randint(2, 9))},
            {"type": "Ball Possession", "value": f"{poss_home if side == 'home' else 100 - poss_home}%"},
            {"type": "Yellow Cards", "value": sum(1 for c in cards if "Yellow" in c["detail"])},
            {"type": "Red Cards", "value": sum(1 for c in cards if "Red" in c["detail"])},
            {"type": "expected_goals", "value": f"{goals * 0.6 + shots * 0.08:.2f}"},
        ]})
    return _wrap("fixtures/statistics", params, out, headers=g)


@app.get("/status")
def status(request: Request):
    return _wrap("status", {}, [{"account": {"firstname": "Mock", "lastname": "User"},
                                 "requests": {"current": STATE.used_today, "limit_day": STATE.daily_limit}}])


# ------------------------------------------------------------------ quản trị
@app.get("/_admin/config")
def admin_config():
    return {"speed": STATE.speed, "daily_limit": STATE.daily_limit, "minute_limit": STATE.minute_limit,
            "provider_delay_s": STATE.provider_delay_s, "matches": len(STATE.scenarios), "source": STATE.source}


@app.get("/_admin/quota")
def admin_quota():
    return {"used_today": STATE.used_today, "daily_limit": STATE.daily_limit, "last_minute": len(STATE.recent)}


@app.post("/_admin/reset_quota")
def admin_reset_quota():
    with STATE.lock:
        STATE.used_today = 0
        STATE.recent.clear()
    return {"ok": True}


@app.post("/_admin/restart")
def admin_restart(matches: int = 3, gap_min: float = 2, speed: float | None = None, seed: int = 42,
                  delay_s: float = 30, source: str = "synthetic", var_prob: float = 0.7):
    STATE.restart(matches, gap_min, speed or STATE.speed, seed, delay_s, source, var_prob)
    return admin_scenarios()


@app.get("/_admin/scenarios")
def admin_scenarios():
    now = time.time()
    out = []
    for sc in STATE.scenarios.values():
        t = STATE.script_t(sc, now)
        st = sc.status_at(t)
        out.append({"fixture_id": sc.fixture_id, "home": sc.home["name"], "away": sc.away["name"],
                    "kickoff_utc": datetime.fromtimestamp(sc.kickoff_ts, tz=timezone.utc).isoformat(),
                    "status": st[0], "elapsed": st[2], "script_events": len(sc.events),
                    "ends_in_s": max(0.0, (sc.total_len - t) / STATE.speed)})
    return {"speed": STATE.speed, "scenarios": out}


@app.get("/_admin/truth")
def admin_truth(fixture: int):
    """Trạng thái cuối cùng đúng của trận (sau mọi hiệu chỉnh) - dùng để kiểm chứng ở thí nghiệm E3."""
    sc = STATE.scenarios.get(fixture)
    if not sc:
        return JSONResponse({"error": "not found"}, status_code=404)
    t_end = sc.total_len + 10_000
    evs, hg, ag = visible_events(sc, t_end, STATE.provider_delay_s, STATE.speed)
    with_ids = assign_event_ids(str(fixture), evs)
    cancelled = _cancelled_goal_ids(with_ids)
    final = [{"event_id": eid, "type": ev["type"], "detail": ev["detail"], "elapsed": ev["time"]["elapsed"],
              "extra": ev["time"]["extra"], "team": ev["team"]["name"], "player": ev["player"]["name"]}
             for eid, ev in with_ids if eid not in cancelled]
    t_now = STATE.script_t(sc)
    return {"fixture": fixture, "finished": t_now >= sc.total_len, "home_goals": hg, "away_goals": ag,
            "events": final, "cancelled_event_ids": sorted(cancelled),
            "var_cancellations": sum(1 for e in sc.events if e.var_cancel_after_s is not None),
            "corrections": sum(1 for e in sc.events if e.correction_after_s is not None)}
