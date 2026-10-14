"""Mô hình sự kiện chuẩn hóa dùng chung cho mọi nguồn (API-Football, Mock, StatsBomb)."""
from __future__ import annotations

import hashlib
import json
import time
from typing import Any

# Offset (giây) của từng hiệp trên "đồng hồ trận liên tục". Khoảng cách giữa các hiệp đủ lớn
# để thời gian bù giờ hiệp 1 không chồng lên đầu hiệp 2; độ suy giảm của chỉ số kịch tính
# vì thế tự về 0 qua giờ nghỉ.
PERIOD_OFFSET_S = {1: 0, 2: 60 * 60, 3: 120 * 60, 4: 140 * 60, 5: 160 * 60}
PERIOD_START_MIN = {1: 0, 2: 45, 3: 90, 4: 105, 5: 120}

CATEGORIES = ("GOAL", "OWN_GOAL", "SHOT", "CARD_YELLOW", "CARD_RED", "PENALTY_AWARDED",
              "PENALTY_MISSED", "SUB", "VAR", "FOUL", "OTHER")
HIGHLIGHT_CATEGORIES = {"GOAL", "OWN_GOAL", "CARD_RED", "PENALTY_AWARDED", "VAR"}


def now_ms() -> int:
    return int(time.time() * 1000)


def stable_id(*parts: Any, length: int = 24) -> str:
    raw = "|".join("" if p is None else str(p) for p in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:length]


def match_clock(period: int, minute: int, second: int = 0) -> int:
    """Đổi (hiệp, phút cộng dồn, giây) thành đồng hồ trận liên tục (giây)."""
    period = int(period or 1)
    start = PERIOD_START_MIN.get(period, 120)
    in_period = max(0, (int(minute) - start) * 60 + int(second or 0))
    return PERIOD_OFFSET_S.get(period, 160 * 60) + in_period


def period_from_elapsed(elapsed: int, extra: int | None = None, status: str | None = None) -> int:
    """Suy ra hiệp từ trường time.elapsed của API-Football."""
    if status == "P" or elapsed is None:
        return 5 if status == "P" else 1
    if elapsed <= 45:
        return 1
    if elapsed <= 90:
        return 2
    if elapsed <= 105:
        return 3
    if elapsed <= 120:
        return 4
    return 5


def apifootball_clock(elapsed: int, extra: int | None, status: str | None = None) -> tuple[int, int, int, int]:
    """(period, minute, second, match_clock_s) cho sự kiện API-Football (độ phân giải phút)."""
    elapsed = int(elapsed or 0)
    extra = int(extra or 0)
    period = period_from_elapsed(elapsed, extra, status)
    minute = elapsed + extra
    clock = PERIOD_OFFSET_S[period] + max(0, (elapsed - PERIOD_START_MIN[period]) * 60) + extra * 60
    return period, minute, 0, clock


def apifootball_category(ev_type: str, detail: str | None) -> str:
    t = (ev_type or "").lower()
    d = (detail or "").lower()
    if t == "goal":
        if "own" in d:
            return "OWN_GOAL"
        if "missed" in d:
            return "PENALTY_MISSED"
        return "GOAL"
    if t == "card":
        return "CARD_RED" if ("red" in d or "second yellow" in d) else "CARD_YELLOW"
    if t == "subst":
        return "SUB"
    if t == "var":
        if "penalty confirmed" in d:
            return "PENALTY_AWARDED"
        return "VAR"
    return "OTHER"


def make_event(**kw) -> dict:
    """Tạo bản ghi đúng lược đồ match_event.avsc, điền mặc định cho trường thiếu."""
    ev = {
        "event_id": kw["event_id"],
        "match_id": str(kw["match_id"]),
        "source": kw.get("source", "unknown"),
        "op": kw.get("op", "UPSERT"),
        "seq": int(kw.get("seq", 0)),
        "category": kw.get("category", "OTHER"),
        "event_type": kw.get("event_type", "Unknown"),
        "event_detail": kw.get("event_detail"),
        "period": int(kw.get("period", 1)),
        "minute": int(kw.get("minute", 0)),
        "second": int(kw.get("second", 0)),
        "match_clock_s": int(kw.get("match_clock_s", 0)),
        "team_id": _s(kw.get("team_id")),
        "team_name": kw.get("team_name"),
        "player_id": _s(kw.get("player_id")),
        "player_name": kw.get("player_name"),
        "related_player_name": kw.get("related_player_name"),
        "x": _f(kw.get("x")),
        "y": _f(kw.get("y")),
        "xg": _f(kw.get("xg")),
        "outcome": kw.get("outcome"),
        "payload": kw.get("payload") if isinstance(kw.get("payload"), (str, type(None))) else json.dumps(kw.get("payload"), ensure_ascii=False),
        "source_ts": int(kw.get("source_ts") or now_ms()),
        "ingest_ts": int(kw.get("ingest_ts") or now_ms()),
        "schema_version": 1,
    }
    return ev


def _s(v):
    return None if v is None else str(v)


def _f(v):
    return None if v is None else float(v)
