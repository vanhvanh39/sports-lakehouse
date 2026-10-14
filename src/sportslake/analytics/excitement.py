"""Chỉ số kịch tính (excitement index) và bộ phát hiện diễn biến nổi bật.

Dùng chung cho pipeline Spark (tính cho các trận bị ảnh hưởng trong mỗi vi lô) và cho
đánh giá offline (thí nghiệm E6), nên chỉ dùng Python thuần.

* V1 - luật: cảnh báo ngay khi có sự kiện thuộc nhóm nổi bật
  (bàn thắng, phản lưới, thẻ đỏ, phạt đền, VAR).
* V2 - chấm điểm: tổng trọng số các sự kiện trong cửa sổ trượt 10 phút trận, suy giảm theo
  hàm mũ (tau = 3 phút), nhân hệ số bối cảnh (cuối trận, tỷ số sát nút, hiệp phụ).
  Cảnh báo khi điểm vượt ngưỡng theo chiều đi lên (có trễ hồi và thời gian chờ để tránh lặp).
"""
from __future__ import annotations

import math
from dataclasses import dataclass

from ..canonical import HIGHLIGHT_CATEGORIES, stable_id

WINDOW_S = 600
TAU_S = 180.0
BASE_WEIGHTS = {
    "GOAL": 5.0, "OWN_GOAL": 5.0, "PENALTY_AWARDED": 4.0, "CARD_RED": 3.5, "VAR": 3.0, "PENALTY_MISSED": 3.0,
    "SHOT": 0.6, "CARD_YELLOW": 0.6, "FOUL": 0.1, "SUB": 0.1,
}
LEVELS = ((10.0, "VERY_HIGH"), (6.0, "HIGH"), (3.0, "MEDIUM"), (0.0, "LOW"))


@dataclass
class EventLite:
    category: str
    clock_s: int
    xg: float | None = None
    outcome: str | None = None
    minute: int = 0
    event_id: str = ""


def event_weight(e: EventLite) -> float:
    w = BASE_WEIGHTS.get(e.category, 0.0)
    if e.category == "SHOT":
        xg = e.xg or 0.0
        w += 8.0 * xg  # cú sút xG 0.3 ~ 3.0 điểm
        if e.outcome in ("Saved", "Saved To Post", "Post", "Saved Off Target"):
            w += 0.7
    return w


def context_multiplier(minute: int, home_goals: int, away_goals: int, period: int | None = None) -> float:
    m = 1.0
    if (period or 0) >= 3 or minute > 90:
        m += 0.5
    elif minute >= 75:
        m += 0.3
    if abs(home_goals - away_goals) <= 1:
        m += 0.3 if minute >= 60 else 0.1
    return m


def score_window(events: list[EventLite], now_clock: int, minute: int, home_goals: int, away_goals: int,
                 period: int | None = None) -> float:
    total = 0.0
    for e in events:
        dt = now_clock - e.clock_s
        if dt < 0 or dt > WINDOW_S:
            continue
        total += event_weight(e) * math.exp(-dt / TAU_S)
    return round(total * context_multiplier(minute, home_goals, away_goals, period), 3)


def level_of(score: float) -> str:
    for thr, name in LEVELS:
        if score >= thr:
            return name
    return "LOW"


@dataclass
class DetectorState:
    armed: bool = True
    last_alert_clock: int = -10 ** 9


SUPPRESS_AFTER_V1_S = 60


def v2_decide(state: DetectorState, score: float, clock: int, threshold: float, rearm_ratio: float = 0.6,
              cooldown_s: int = 180, last_v1_clock: int | None = None,
              suppress_s: int = SUPPRESS_AFTER_V1_S) -> tuple[bool, DetectorState]:
    """Cảnh báo khi điểm vượt ngưỡng lúc đang 'armed'; chỉ armed lại khi điểm < ngưỡng*rearm_ratio.

    Nếu vừa có cảnh báo V1 của cùng trận trong ``suppress_s`` giây trận trước đó, V2 không phát
    thêm (khoảnh khắc đã được báo) nhưng vẫn chuyển sang trạng thái đã cảnh báo. Quy tắc này
    dùng chung cho pipeline và đánh giá offline (E6)."""
    fire = False
    armed = state.armed
    last = state.last_alert_clock
    if armed and score >= threshold and clock - last >= cooldown_s:
        armed = False
        last = clock
        covered = last_v1_clock is not None and 0 <= clock - last_v1_clock <= suppress_s
        fire = not covered
    elif not armed and score < threshold * rearm_ratio:
        armed = True
    return fire, DetectorState(armed, last)


def v1_is_highlight(category: str) -> bool:
    return category in HIGHLIGHT_CATEGORIES


V1_MESSAGES = {
    "GOAL": "⚽ BÀN THẮNG", "OWN_GOAL": "⚽ PHẢN LƯỚI", "CARD_RED": "🟥 THẺ ĐỎ",
    "PENALTY_AWARDED": "🎯 PHẠT ĐỀN", "VAR": "📺 VAR",
}


def alert_id(match_id: str, version: str, anchor) -> str:
    return stable_id(match_id, version, anchor)


# ------------------------------------------------------------------ mô phỏng offline (E6)
def simulate_match(events: list[EventLite], score_timeline: list[tuple[int, int, int]], threshold: float,
                   versions=("V1", "V2"), step_s: int | None = None) -> list[dict]:
    """Chạy bộ phát hiện trên toàn trận như khi sự kiện đến tuần tự.

    score_timeline: [(clock_s, home_goals, away_goals)] tăng dần theo thời gian.
    Trả về danh sách cảnh báo {version, clock_s, minute, score}.
    """
    events = sorted(events, key=lambda e: e.clock_s)
    alerts = []
    st = DetectorState()
    window: list[EventLite] = []
    si = 0
    h = a = 0
    eval_points = [(e.clock_s, e) for e in events]
    last_v1 = None
    for clock, e in eval_points:
        while si < len(score_timeline) and score_timeline[si][0] <= clock:
            _, h, a = score_timeline[si]
            si += 1
        window.append(e)
        window = [x for x in window if clock - x.clock_s <= WINDOW_S]
        if "V1" in versions and v1_is_highlight(e.category):
            alerts.append({"version": "V1", "clock_s": clock, "minute": e.minute, "score": None, "category": e.category})
            last_v1 = clock
        if "V2" in versions:
            period = 1 if clock < 3600 else (2 if clock < 7200 else 3)
            sc = score_window(window, clock, e.minute, h, a, period)
            fire, st = v2_decide(st, sc, clock, threshold, last_v1_clock=last_v1)
            if fire:
                alerts.append({"version": "V2", "clock_s": clock, "minute": e.minute, "score": sc, "category": e.category})
    return alerts
