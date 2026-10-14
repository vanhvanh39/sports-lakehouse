"""Kịch bản trận đấu cho Mock API-Football.

Một kịch bản là danh sách sự kiện theo "giây kịch bản" (tính từ lúc bóng lăn, gồm cả giờ nghỉ).
Mock API dựng lại đúng định dạng phản hồi của API-Football tại bất kỳ thời điểm nào, kể cả:

* độ trễ của nhà cung cấp (sự kiện chỉ xuất hiện sau ``provider_delay_s`` giây trận),
* VAR hủy bàn thắng (kiểu "remove": bàn thắng biến mất; kiểu "keep": vẫn còn nhưng có sự kiện Var hủy),
* sửa tên người ghi bàn sau vài phút (nguồn hiệu chỉnh dữ liệu).
"""
from __future__ import annotations

import math
import random
from dataclasses import dataclass, field
from pathlib import Path

HT_BREAK_S = 15 * 60

TEAMS = [
    (33, "Manchester United"), (34, "Newcastle"), (35, "Bournemouth"), (36, "Fulham"), (39, "Wolves"),
    (40, "Liverpool"), (42, "Arsenal"), (45, "Everton"), (47, "Tottenham"), (48, "West Ham"),
    (49, "Chelsea"), (50, "Manchester City"), (51, "Brighton"), (52, "Crystal Palace"), (55, "Brentford"),
    (65, "Nottingham Forest"), (66, "Aston Villa"), (63, "Leeds"), (44, "Burnley"), (746, "Sunderland"),
]
SURNAMES = ["Nguyen", "Tran", "Silva", "Smith", "Martinez", "Kane", "Saka", "Rice", "Foden", "Palmer", "Isak",
            "Watkins", "Son", "Salah", "Gordon", "Bowen", "Mbeumo", "Wissa", "Eze", "Olise", "Mitoma", "Diaz",
            "Haaland", "Odegaard", "Fernandes", "Mount", "Trossard", "Jesus", "Nunez", "Gakpo"]
INITIALS = "ABCDEFGHJKLMNPRSTVW"


@dataclass
class ScriptEvent:
    period: int
    minute: int          # phút cộng dồn kiểu StatsBomb (0-based)
    second: int
    side: str            # home | away
    type: str            # Goal | Card | subst | Var
    detail: str
    player: dict
    assist: dict = field(default_factory=lambda: {"id": None, "name": None})
    var_cancel_after_s: float | None = None   # VAR hủy bàn thắng sau X giây trận
    var_style: str = "remove"
    correction_after_s: float | None = None   # sửa người ghi bàn sau X giây trận
    corrected_player: dict | None = None


@dataclass
class Scenario:
    fixture_id: int
    kickoff_ts: float
    home: dict
    away: dict
    league: dict
    venue: str
    referee: str
    events: list[ScriptEvent]
    ht_extra: int = 2
    ft_extra: int = 4
    seed: int = 0

    # ---------- thời gian ----------
    @property
    def h1_len(self) -> int:
        return (45 + self.ht_extra) * 60

    @property
    def h2_len(self) -> int:
        return (45 + self.ft_extra) * 60

    @property
    def total_len(self) -> int:
        return self.h1_len + HT_BREAK_S + self.h2_len

    def script_t(self, period: int, minute: int, second: int) -> int:
        if period == 1:
            return min(minute * 60 + second, self.h1_len - 1)
        return self.h1_len + HT_BREAK_S + max(0, (minute - 45) * 60 + second)

    def status_at(self, t: float) -> tuple[str, str, int | None, int | None]:
        if t < 0:
            return "NS", "Not Started", None, None
        if t < self.h1_len:
            m = int(t // 60) + 1
            return "1H", "First Half", min(45, m), (m - 45 if m > 45 else None)
        if t < self.h1_len + HT_BREAK_S:
            return "HT", "Halftime", 45, None
        if t < self.total_len:
            m = 45 + int((t - self.h1_len - HT_BREAK_S) // 60) + 1
            return "2H", "Second Half", min(90, m), (m - 90 if m > 90 else None)
        return "FT", "Match Finished", 90, None


def _api_time(ev: ScriptEvent) -> dict:
    elapsed = ev.minute + 1
    if ev.period == 1 and elapsed > 45:
        return {"elapsed": 45, "extra": elapsed - 45}
    if ev.period == 2 and elapsed > 90:
        return {"elapsed": 90, "extra": elapsed - 90}
    return {"elapsed": elapsed, "extra": None}


def visible_events(sc: Scenario, t: float, provider_delay_s: float, speed: float) -> tuple[list[dict], int, int]:
    """Danh sách sự kiện kiểu API-Football nhìn thấy tại giây kịch bản t, kèm tỷ số."""
    out: list[tuple[float, dict]] = []
    goals = {"home": 0, "away": 0}
    teams = {"home": sc.home, "away": sc.away}
    for ev in sc.events:
        if ev.period > 2:
            continue
        occ = sc.script_t(ev.period, ev.minute, ev.second)
        if occ + provider_delay_s > t:
            continue
        occurred_ms = int((sc.kickoff_ts + occ / speed) * 1000)
        player = ev.player
        if ev.correction_after_s is not None and t >= occ + ev.correction_after_s + provider_delay_s:
            player = ev.corrected_player or player
        item = {"time": _api_time(ev), "team": dict(teams[ev.side]), "player": dict(player),
                "assist": dict(ev.assist), "type": ev.type, "detail": ev.detail, "comments": None,
                "_occurred_at_ms": occurred_ms}
        cancelled = ev.var_cancel_after_s is not None and t >= occ + ev.var_cancel_after_s + provider_delay_s
        if cancelled:
            c_occ = occ + ev.var_cancel_after_s
            c_ev = ScriptEvent(ev.period, ev.minute + int(ev.var_cancel_after_s // 60), ev.second, ev.side,
                               "Var", "Goal cancelled", player)
            var_item = {"time": _api_time(c_ev), "team": dict(teams[ev.side]), "player": dict(player),
                        "assist": {"id": None, "name": None}, "type": "Var", "detail": "Goal cancelled",
                        "comments": None, "_occurred_at_ms": int((sc.kickoff_ts + c_occ / speed) * 1000)}
            if ev.var_style == "keep":
                out.append((occ, item))
            out.append((c_occ, var_item))
        else:
            out.append((occ, item))
            if ev.type == "Goal" and "missed" not in ev.detail.lower():
                goals[ev.side] += 1
    out.sort(key=lambda x: x[0])
    return [x[1] for x in out], goals["home"], goals["away"]


# ------------------------------------------------------------------ sinh kịch bản
def _player(rng: random.Random, team_id: int, number: int) -> dict:
    return {"id": team_id * 1000 + number, "name": f"{rng.choice(INITIALS)}. {rng.choice(SURNAMES)}"}


def _poisson(rng: random.Random, lam: float) -> int:
    l, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= l:
            return k
        k += 1


def _rand_time(rng: random.Random, ht_extra: int, ft_extra: int) -> tuple[int, int, int]:
    if rng.random() < 0.45:
        m = rng.randint(0, 44 + ht_extra)
        return 1, m, rng.randint(0, 59)
    m = rng.randint(45, 89 + ft_extra)
    return 2, m, rng.randint(0, 59)


def synthetic_scenario(fixture_id: int, kickoff_ts: float, seed: int, var_prob: float = 0.7,
                       correction_prob: float = 0.4) -> Scenario:
    rng = random.Random(seed)
    (hid, hname), (aid, aname) = rng.sample(TEAMS, 2)
    home, away = {"id": hid, "name": hname, "logo": None}, {"id": aid, "name": aname, "logo": None}
    ht_extra, ft_extra = rng.randint(1, 4), rng.randint(3, 7)
    roster = {s: [_player(rng, t["id"], n) for n in range(1, 19)] for s, t in (("home", home), ("away", away))}
    events: list[ScriptEvent] = []
    for side, lam in (("home", 1.5), ("away", 1.2)):
        for _ in range(_poisson(rng, lam)):
            p, m, s = _rand_time(rng, ht_extra, ft_extra)
            scorer = rng.choice(roster[side][1:11])
            assist = rng.choice(roster[side][1:11]) if rng.random() < 0.7 else {"id": None, "name": None}
            detail = "Penalty" if rng.random() < 0.12 else "Normal Goal"
            ev = ScriptEvent(p, m, s, side, "Goal", detail, scorer, assist)
            if rng.random() < correction_prob:
                ev.correction_after_s = rng.randint(60, 240)
                ev.corrected_player = rng.choice(roster[side][1:11])
            events.append(ev)
        for _ in range(_poisson(rng, 1.8)):
            p, m, s = _rand_time(rng, ht_extra, ft_extra)
            events.append(ScriptEvent(p, m, s, side, "Card", "Yellow Card", rng.choice(roster[side][1:11])))
        if rng.random() < 0.1:
            p, m, s = _rand_time(rng, ht_extra, ft_extra)
            events.append(ScriptEvent(p, max(m, 30), s, side, "Card", "Red Card", rng.choice(roster[side][1:11])))
        # 5 lần thay người, có lần thay 2 người cùng phút (kiểm thử khóa tự nhiên trùng nhóm)
        sub_minutes = sorted([45] + [rng.randint(58, 86) for _ in range(3)])
        sub_minutes.insert(2, sub_minutes[2])
        for i, m in enumerate(sub_minutes, start=1):
            out_p = roster[side][i]
            in_p = roster[side][10 + i]
            events.append(ScriptEvent(2, m, 0 if m == 45 else rng.randint(0, 59), side, "subst",
                                      f"Substitution {i}", out_p, in_p))
    if rng.random() < var_prob:
        side = rng.choice(["home", "away"])
        p, m, s = _rand_time(rng, ht_extra, ft_extra)
        ev = ScriptEvent(p, min(m, 85), s, side, "Goal", "Normal Goal", rng.choice(roster[side][1:11]))
        ev.var_cancel_after_s = rng.randint(90, 200)
        ev.var_style = rng.choice(["remove", "keep"])
        events.append(ev)
    events.sort(key=lambda e: (e.period, e.minute, e.second))
    return Scenario(fixture_id, kickoff_ts, home, away,
                    {"id": 39, "name": "Premier League", "country": "England", "logo": None, "flag": None,
                     "season": 2026, "round": f"Regular Season - {rng.randint(1, 38)}"},
                    venue=f"{hname} Stadium", referee=f"{rng.choice(INITIALS)}. {rng.choice(SURNAMES)}",
                    events=events, ht_extra=ht_extra, ft_extra=ft_extra, seed=seed)


def statsbomb_scenario(fixture_id: int, kickoff_ts: float, data_dir: Path, match: dict, seed: int,
                       var_prob: float = 0.5) -> Scenario:
    from ..ingestion.statsbomb import api_like_script, load_events

    rng = random.Random(seed)
    sb_events = load_events(data_dir, match["match_id"])
    script = api_like_script(sb_events, match)
    home = {"id": match["home_team"]["home_team_id"], "name": match["home_team"]["home_team_name"], "logo": None}
    away = {"id": match["away_team"]["away_team_id"], "name": match["away_team"]["away_team_name"], "logo": None}
    max1 = max([e["minute"] for e in script if e["period"] == 1] + [45])
    max2 = max([e["minute"] for e in script if e["period"] == 2] + [90])
    events = [ScriptEvent(e["period"], e["minute"], e["second"], e["side"], e["type"], e["detail"], e["player"],
                          e.get("assist") or {"id": None, "name": None}) for e in script if e["period"] <= 2]
    if rng.random() < var_prob:
        side = rng.choice(["home", "away"])
        ev = ScriptEvent(2, rng.randint(50, 85), rng.randint(0, 59), side, "Goal", "Normal Goal",
                         {"id": 999000 + rng.randint(1, 999), "name": "VAR Test"})
        ev.var_cancel_after_s = rng.randint(90, 200)
        ev.var_style = rng.choice(["remove", "keep"])
        events.append(ev)
        events.sort(key=lambda e: (e.period, e.minute, e.second))
    comp = match.get("competition") or {}
    season = match.get("season") or {}
    return Scenario(fixture_id, kickoff_ts, home, away,
                    {"id": 2, "name": comp.get("competition_name", "StatsBomb"), "country": comp.get("country_name"),
                     "logo": None, "flag": None, "season": season.get("season_name"),
                     "round": (match.get("competition_stage") or {}).get("name")},
                    venue=(match.get("stadium") or {}).get("name", "Stadium"),
                    referee=(match.get("referee") or {}).get("name", "Referee"),
                    events=events, ht_extra=max(1, max1 - 44), ft_extra=max(3, max2 - 89), seed=seed)
