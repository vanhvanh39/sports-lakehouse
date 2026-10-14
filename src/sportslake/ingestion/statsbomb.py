"""Đọc dữ liệu StatsBomb Open Data và chuyển sang mô hình sự kiện chuẩn hóa.

Dữ liệu StatsBomb Open Data được cung cấp miễn phí cho mục đích nghiên cứu, kèm điều kiện
ghi nguồn "Data provided by StatsBomb" (xem LICENSE trong repo statsbomb/open-data).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from ..canonical import make_event, match_clock

SB_DIR_NAME = "statsbomb"


def sb_root(data_dir: Path) -> Path:
    return data_dir / SB_DIR_NAME


def load_competitions(data_dir: Path) -> list[dict]:
    return json.loads((sb_root(data_dir) / "competitions.json").read_text(encoding="utf-8"))


def load_matches(data_dir: Path, competition_id: int, season_id: int) -> list[dict]:
    p = sb_root(data_dir) / "matches" / str(competition_id) / f"{season_id}.json"
    return json.loads(p.read_text(encoding="utf-8"))


def load_events(data_dir: Path, match_id: int | str) -> list[dict]:
    p = sb_root(data_dir) / "events" / f"{match_id}.json"
    return json.loads(p.read_text(encoding="utf-8"))


def downloaded_matches(data_dir: Path) -> list[dict]:
    """Mọi trận đã tải đủ (có file events) kèm thông tin trận."""
    root = sb_root(data_dir)
    out = []
    for mf in sorted((root / "matches").rglob("*.json")):
        for m in json.loads(mf.read_text(encoding="utf-8")):
            if (root / "events" / f"{m['match_id']}.json").exists():
                out.append(m)
    return out


def sb_match_id(m: dict | int | str) -> str:
    mid = m["match_id"] if isinstance(m, dict) else m
    return f"sb-{mid}"


def _name(d: dict | None) -> str | None:
    return (d or {}).get("name")


def categorize(ev: dict) -> tuple[str, str | None, float | None, str | None]:
    """(category, detail, xg, outcome) cho một sự kiện StatsBomb."""
    t = _name(ev.get("type"))
    if t == "Shot":
        shot = ev.get("shot") or {}
        outcome = _name(shot.get("outcome"))
        xg = shot.get("statsbomb_xg")
        detail = _name(shot.get("type"))
        if outcome == "Goal":
            return "GOAL", detail, xg, outcome
        if detail == "Penalty":
            return "PENALTY_MISSED", detail, xg, outcome
        return "SHOT", detail, xg, outcome
    if t == "Own Goal For":
        return "OWN_GOAL", "Own Goal", None, "Goal"
    if t in ("Foul Committed", "Bad Behaviour"):
        block = ev.get("foul_committed") if t == "Foul Committed" else ev.get("bad_behaviour")
        block = block or {}
        card = _name(block.get("card"))
        if card in ("Red Card", "Second Yellow"):
            return "CARD_RED", card, None, None
        if block.get("penalty"):
            return "PENALTY_AWARDED", "Penalty", None, None
        if card == "Yellow Card":
            return "CARD_YELLOW", card, None, None
        return ("FOUL" if t == "Foul Committed" else "OTHER"), None, None, None
    if t == "Substitution":
        return "SUB", "Substitution", None, None
    return "OTHER", None, None, None


def to_canonical(ev: dict, match: dict, seq: int, source_ts: int, ingest_ts: int) -> dict:
    cat, detail, xg, outcome = categorize(ev)
    loc = ev.get("location") or [None, None]
    related = None
    if ev.get("substitution"):
        related = _name(ev["substitution"].get("replacement"))
    elif ev.get("pass") and ev["pass"].get("recipient"):
        related = _name(ev["pass"]["recipient"])
    period = int(ev.get("period") or 1)
    minute, second = int(ev.get("minute") or 0), int(ev.get("second") or 0)
    team = ev.get("team") or {}
    player = ev.get("player") or {}
    return make_event(
        event_id=f"sb-{ev['id']}", match_id=sb_match_id(match), source="statsbomb", op="UPSERT", seq=seq,
        category=cat, event_type=_name(ev.get("type")) or "Unknown", event_detail=detail,
        period=period, minute=minute, second=second, match_clock_s=match_clock(period, minute, second),
        team_id=f"sb-{team.get('id')}" if team.get("id") is not None else None, team_name=team.get("name"),
        player_id=f"sb-{player.get('id')}" if player.get("id") is not None else None, player_name=player.get("name"),
        related_player_name=related, x=loc[0] if len(loc) > 0 else None, y=loc[1] if len(loc) > 1 else None,
        xg=xg, outcome=outcome,
        payload=json.dumps({k: ev[k] for k in ("possession", "play_pattern", "under_pressure") if k in ev}, ensure_ascii=False),
        source_ts=source_ts, ingest_ts=ingest_ts,
    )


def _ts_seconds(ts: str) -> float:
    h, m, s = ts.split(":")
    return int(h) * 3600 + int(m) * 60 + float(s)


def period_durations(events: Iterable[dict]) -> dict[int, float]:
    """Độ dài thực tế mỗi hiệp (giây) theo trường timestamp."""
    dur: dict[int, float] = {}
    for ev in events:
        p = int(ev.get("period") or 1)
        dur[p] = max(dur.get(p, 0.0), _ts_seconds(ev.get("timestamp", "00:00:00.000")))
    return dur


BREAK_BEFORE_S = {2: 15 * 60, 3: 5 * 60, 4: 60, 5: 5 * 60}


def real_elapsed_s(ev: dict, durations: dict[int, float]) -> float:
    """Số giây "thời gian thực" tính từ lúc bóng lăn (gồm nghỉ giữa hiệp)."""
    p = int(ev.get("period") or 1)
    total = 0.0
    for q in range(1, p):
        total += durations.get(q, 0.0) + BREAK_BEFORE_S.get(q + 1, 0)
    return total + _ts_seconds(ev.get("timestamp", "00:00:00.000"))


def score_state(events: list[dict], match: dict) -> list[tuple[int, int, int]]:
    """Diễn biến tỷ số [(index sự kiện, bàn đội nhà, bàn đội khách)] tại các bàn thắng."""
    home_id = match["home_team"]["home_team_id"]
    h = a = 0
    out = []
    for i, ev in enumerate(events):
        if int(ev.get("period") or 1) >= 5:
            continue
        cat, *_ = categorize(ev)
        if cat in ("GOAL", "OWN_GOAL"):
            if (ev.get("team") or {}).get("id") == home_id:
                h += 1
            else:
                a += 1
            out.append((i, h, a))
    return out


def key_moments(events: list[dict], big_chance_xg: float = 0.3, merge_window_s: int = 30) -> list[dict]:
    """Các khoảnh khắc then chốt dùng làm nhãn đánh giá (thí nghiệm E6).

    Gồm: bàn thắng, phản lưới, thẻ đỏ, phạt đền được thổi, cơ hội lớn (cú sút xG >= ngưỡng).
    Các khoảnh khắc cùng trận cách nhau < merge_window_s được gộp làm một.
    """
    raw = []
    for ev in events:
        p = int(ev.get("period") or 1)
        if p >= 5:
            continue
        cat, detail, xg, _ = categorize(ev)
        kind = None
        if cat in ("GOAL", "OWN_GOAL"):
            kind = cat
        elif cat == "CARD_RED":
            kind = "RED_CARD"
        elif cat == "PENALTY_AWARDED":
            kind = "PENALTY"
        elif cat in ("SHOT", "PENALTY_MISSED") and xg is not None and xg >= big_chance_xg:
            kind = "BIG_CHANCE"
        if kind:
            raw.append({"clock_s": match_clock(p, ev["minute"], ev["second"]), "kind": kind,
                        "minute": ev["minute"], "team": (ev.get("team") or {}).get("name")})
    raw.sort(key=lambda r: r["clock_s"])
    merged: list[dict] = []
    for r in raw:
        if merged and r["clock_s"] - merged[-1]["clock_s"] < merge_window_s:
            merged[-1]["kinds"].append(r["kind"])
            continue
        merged.append({**r, "kinds": [r["kind"]]})
    return merged


def api_like_script(events: list[dict], match: dict) -> list[dict]:
    """Chuyển diễn biến StatsBomb thành sự kiện kiểu API-Football (bàn thắng, thẻ, thay người)
    để Mock API phát lại diễn biến thật của trận."""
    out = []
    home_id = match["home_team"]["home_team_id"]
    sub_count: dict[int, int] = {}
    for ev in events:
        p = int(ev.get("period") or 1)
        if p >= 5:
            continue
        cat, detail, _, _ = categorize(ev)
        team = ev.get("team") or {}
        side = "home" if team.get("id") == home_id else "away"
        player = {"id": (ev.get("player") or {}).get("id"), "name": (ev.get("player") or {}).get("name")}
        base = {"period": p, "minute": ev["minute"], "second": ev["second"], "side": side, "player": player}
        if cat == "GOAL":
            out.append({**base, "type": "Goal", "detail": "Penalty" if detail == "Penalty" else "Normal Goal"})
        elif cat == "OWN_GOAL":
            out.append({**base, "type": "Goal", "detail": "Own Goal"})
        elif cat == "CARD_RED":
            out.append({**base, "type": "Card", "detail": "Red Card" if detail == "Red Card" else "Second Yellow card"})
        elif cat == "CARD_YELLOW":
            out.append({**base, "type": "Card", "detail": "Yellow Card"})
        elif cat == "SUB":
            n = sub_count.get(team.get("id"), 0) + 1
            sub_count[team.get("id")] = n
            rep = (ev.get("substitution") or {}).get("replacement") or {}
            out.append({**base, "type": "subst", "detail": f"Substitution {n}",
                        "assist": {"id": rep.get("id"), "name": rep.get("name")}})
    return out
