"""So sánh ảnh chụp trạng thái liên tiếp của API-Football để suy ra sự kiện mới / thay đổi / bị thu hồi.

API-Football không trả về luồng sự kiện mà trả về *toàn bộ danh sách sự kiện hiện tại* của trận
mỗi lần gọi, và sự kiện không có id. Vì vậy:

* Mỗi sự kiện được gán một *khóa tự nhiên* ổn định:
  (fixture, loại, đội, phút, phút bù giờ, thứ tự trong nhóm trùng) -> event_id = sha1(khóa).
  Khóa cố ý KHÔNG chứa cầu thủ, để khi nguồn sửa tên người ghi bàn ta sinh UPSERT (sửa)
  thay vì RETRACT + sự kiện mới.
* Nội dung (cầu thủ, chi tiết, kiến tạo...) được băm để phát hiện thay đổi -> UPSERT với seq mới.
* Sự kiện có trong ảnh chụp trước nhưng không còn trong ảnh chụp mới -> RETRACT.
* Bàn thắng bị VAR hủy nhưng nguồn vẫn giữ trong danh sách (kèm sự kiện "Var - Goal cancelled")
  cũng được coi là đã thu hồi.
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from ..canonical import apifootball_category, apifootball_clock, make_event, stable_id

log = logging.getLogger(__name__)

CANCEL_DETAILS = ("goal cancelled", "goal disallowed")


@dataclass
class FixtureMemory:
    events: dict[str, str] = field(default_factory=dict)  # event_id -> content hash
    event_rows: dict[str, dict] = field(default_factory=dict)  # event_id -> last emitted canonical event
    state_hash: str | None = None


def _content_hash(ev: dict) -> str:
    # băm cả id lẫn tên: nguồn có thể sửa tên cầu thủ mà giữ nguyên id (hoặc ngược lại)
    p, a = ev.get("player") or {}, ev.get("assist") or {}
    keep = {
        "player": [p.get("id"), p.get("name")],
        "assist": [a.get("id"), a.get("name")],
        "detail": ev.get("detail"),
        "comments": ev.get("comments"),
    }
    return stable_id(json.dumps(keep, sort_keys=True, ensure_ascii=False), length=16)


def _group_key(fixture_id: str, ev: dict) -> tuple:
    t = ev.get("time") or {}
    return (str(fixture_id), (ev.get("type") or "").lower(), str((ev.get("team") or {}).get("id")),
            int(t.get("elapsed") or 0), int(t.get("extra") or 0))


def assign_event_ids(fixture_id: str, events: list[dict]) -> list[tuple[str, dict]]:
    """Gán event_id ổn định cho danh sách sự kiện của một trận."""
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for ev in events:
        groups[_group_key(fixture_id, ev)].append(ev)
    out = []
    for key, evs in groups.items():
        # thứ tự ổn định trong nhóm: theo vị trí xuất hiện trong danh sách nguồn
        for ordinal, ev in enumerate(evs):
            out.append((stable_id(*key, ordinal), ev))
    return out


def _cancelled_goal_ids(with_ids: list[tuple[str, dict]]) -> set[str]:
    """Bàn thắng bị VAR hủy (nguồn vẫn giữ sự kiện Goal nhưng có thêm sự kiện Var hủy)."""
    cancels = [ev for _, ev in with_ids
               if (ev.get("type") or "").lower() == "var" and any(c in (ev.get("detail") or "").lower() for c in CANCEL_DETAILS)]
    if not cancels:
        return set()
    out = set()
    goals = [(eid, ev) for eid, ev in with_ids if (ev.get("type") or "").lower() == "goal"]
    for c in cancels:
        c_t = c.get("time") or {}
        c_min = int(c_t.get("elapsed") or 0) + int(c_t.get("extra") or 0)
        c_team = str((c.get("team") or {}).get("id"))
        best = None
        for eid, g in goals:
            if eid in out:
                continue
            g_t = g.get("time") or {}
            g_min = int(g_t.get("elapsed") or 0) + int(g_t.get("extra") or 0)
            same_team = str((g.get("team") or {}).get("id")) == c_team
            if same_team and 0 <= c_min - g_min <= 4:
                if best is None or g_min > best[1]:
                    best = (eid, g_min)
        if best:
            out.add(best[0])
    return out


class SnapshotDiffer:
    def __init__(self, source: str, state_file: Path | None = None):
        self.source = source
        self.state_file = state_file
        self.memory: dict[str, FixtureMemory] = {}
        if state_file and state_file.exists():
            try:
                raw = json.loads(state_file.read_text(encoding="utf-8"))
                for fid, m in raw.items():
                    self.memory[fid] = FixtureMemory(events=m.get("events", {}), event_rows=m.get("event_rows", {}),
                                                     state_hash=m.get("state_hash"))
                log.info("Khôi phục trạng thái differ cho %d trận", len(self.memory))
            except Exception as exc:  # noqa: BLE001
                log.warning("Không đọc được %s: %s", state_file, exc)

    def save(self) -> None:
        if not self.state_file:
            return
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.state_file.with_suffix(".tmp")
        tmp.write_text(json.dumps({fid: {"events": m.events, "event_rows": m.event_rows, "state_hash": m.state_hash}
                                   for fid, m in self.memory.items()}), encoding="utf-8")
        tmp.replace(self.state_file)

    def checkpoint(self) -> dict:
        """Ảnh chụp bộ nhớ differ trước một lượt gọi API (để hoàn tác nếu gửi Kafka thất bại)."""
        import copy

        return copy.deepcopy(self.memory)

    def restore(self, cp: dict) -> None:
        """Hoàn tác về ảnh chụp: lượt sau sẽ phát hiện lại và gửi lại các thay đổi chưa tới Kafka."""
        self.memory = cp

    def forget(self, fixture_id: str) -> None:
        self.memory.pop(str(fixture_id), None)

    # ------------------------------------------------------------------
    def diff_fixture(self, fx: dict, poll_ts_ms: int, events_override: list[dict] | None = None,
                     ingest_ts_ms: int | None = None) -> tuple[list[dict], dict | None]:
        """Trả về (danh sách sự kiện chuẩn hóa cần gửi, trạng thái trận nếu thay đổi)."""
        fixture = fx.get("fixture") or {}
        fid = str(fixture.get("id"))
        status = ((fixture.get("status") or {}).get("short")) or "NS"
        mem = self.memory.setdefault(fid, FixtureMemory())
        raw_events = events_override if events_override is not None else (fx.get("events") or [])
        with_ids = assign_event_ids(fid, raw_events)
        cancelled = _cancelled_goal_ids(with_ids)
        ingest = ingest_ts_ms or poll_ts_ms

        out: list[dict] = []
        current: dict[str, str] = {}
        for eid, ev in with_ids:
            if eid in cancelled:
                continue
            h = _content_hash(ev)
            current[eid] = h
            if mem.events.get(eid) == h:
                continue  # không đổi
            t = ev.get("time") or {}
            period, minute, second, clock = apifootball_clock(t.get("elapsed"), t.get("extra"), status)
            team = ev.get("team") or {}
            player = ev.get("player") or {}
            assist = ev.get("assist") or {}
            occurred = ev.get("_occurred_at_ms")  # chỉ Mock API có (để đo độ trễ nguồn -> thu thập)
            row = make_event(
                event_id=eid, match_id=fid, source=self.source, op="UPSERT", seq=poll_ts_ms,
                category=apifootball_category(ev.get("type"), ev.get("detail")),
                event_type=ev.get("type") or "Unknown", event_detail=ev.get("detail"),
                period=period, minute=minute, second=second, match_clock_s=clock,
                team_id=team.get("id"), team_name=team.get("name"),
                player_id=player.get("id"), player_name=player.get("name"),
                related_player_name=assist.get("name"),
                payload={k: v for k, v in ev.items() if not k.startswith("_")},
                source_ts=int(occurred) if occurred else poll_ts_ms, ingest_ts=ingest,
            )
            out.append(row)
            mem.event_rows[eid] = row

        for eid in list(mem.events):
            if eid not in current:
                prev = dict(mem.event_rows.get(eid) or {"event_id": eid, "match_id": fid})
                prev.update({"op": "RETRACT", "seq": poll_ts_ms, "source_ts": poll_ts_ms, "ingest_ts": ingest,
                             "source": self.source})
                out.append(make_event(**prev))
                mem.event_rows.pop(eid, None)
        mem.events = current

        state = self._state(fx, fid, status, poll_ts_ms, ingest)
        h = stable_id(state["home_goals"], state["away_goals"], state["status_short"], state["elapsed"], state["extra"])
        changed = h != mem.state_hash
        mem.state_hash = h
        return out, (state if changed else None)

    def _state(self, fx: dict, fid: str, status: str, poll_ts_ms: int, ingest: int) -> dict:
        fixture = fx.get("fixture") or {}
        league = fx.get("league") or {}
        teams = fx.get("teams") or {}
        goals = fx.get("goals") or {}
        st = fixture.get("status") or {}
        return {
            "match_id": fid, "source": self.source, "seq": poll_ts_ms,
            "league_id": _s(league.get("id")), "league_name": league.get("name"),
            "season": _s(league.get("season")), "round": league.get("round"),
            "home_team_id": _s((teams.get("home") or {}).get("id")), "home_team": (teams.get("home") or {}).get("name"),
            "away_team_id": _s((teams.get("away") or {}).get("id")), "away_team": (teams.get("away") or {}).get("name"),
            "home_goals": int(goals.get("home") or 0), "away_goals": int(goals.get("away") or 0),
            "status_short": status, "status_long": st.get("long"),
            "elapsed": st.get("elapsed"), "extra": st.get("extra"),
            "kickoff_ts": int(fixture["timestamp"]) * 1000 if fixture.get("timestamp") else None,
            "venue": (fixture.get("venue") or {}).get("name"),
            "source_ts": poll_ts_ms, "ingest_ts": ingest, "schema_version": 1,
        }


def _s(v):
    return None if v is None else str(v)
