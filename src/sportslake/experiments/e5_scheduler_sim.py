"""Thí nghiệm E5 (RQ3): bộ lập lịch thích ứng so với lịch cố định, cùng ngân sách 100 request/ngày.

Mô phỏng offline trên lịch thi đấu THẬT của các giải đã tải (StatsBomb: giờ bóng lăn, diễn biến
bàn thắng/thẻ/thay người theo đúng thời điểm). Mỗi ngày thi đấu là một lần chạy độc lập:
"API giả lập" trả về trạng thái các trận đang diễn ra tại thời điểm gọi (có độ trễ nhà cung cấp).

So sánh:
  adaptive     bộ lập lịch thích ứng (dùng đúng lớp Scheduler của collector)
  fixed-fair   gọi đều đặn với khoảng cách chọn sao cho vừa khít ngân sách của ngày đó
  fixed-60s    gọi mỗi 60 giây (cách làm ngây thơ, hết ngân sách giữa chừng)

Chỉ số: số request, độ trễ phát hiện sự kiện (p50/p95, riêng bàn thắng), số sự kiện chỉ phát hiện
được ở lần chốt cuối trận, ngân sách còn dư.

  python -m sportslake.experiments.e5_scheduler_sim
  python -m sportslake.experiments.e5_scheduler_sim --budget 100 --provider-delay-s 20 --no-stats
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone

from ..config import DATA_DIR
from ..ingestion.scheduler import Scheduler
from ..ingestion.statsbomb import categorize, downloaded_matches, load_events, period_durations, real_elapsed_s
from .common import md_table, plt, results_dir, summary, write_csv, write_json

BREAK_S = {2: 15 * 60, 3: 5 * 60, 4: 60}


class SimMatch:
    def __init__(self, m: dict):
        self.id = str(m["match_id"])
        self.label = f"{m['home_team']['home_team_name']} - {m['away_team']['away_team_name']}"
        ko = m.get("kick_off") or "15:00:00.000"
        self.kickoff = datetime.fromisoformat(f"{m['match_date']}T{ko[:8]}").replace(tzinfo=timezone.utc).timestamp()
        evs = load_events(DATA_DIR, m["match_id"])
        dur = period_durations(evs)
        self.periods = []  # (start, end, status, start_minute)
        t = self.kickoff
        start_min = {1: 0, 2: 45, 3: 90, 4: 105}
        for p in sorted(x for x in dur if x <= 4):
            if p > 1:
                t += BREAK_S.get(p, 0)
            self.periods.append((t, t + dur[p], {1: "1H", 2: "2H"}.get(p, "ET"), start_min[p]))
            t += dur[p]
        self.end = t
        home_id = m["home_team"]["home_team_id"]
        # sự kiện mà API-Football có báo: bàn thắng, thẻ, thay người -> thời điểm thực xảy ra
        self.events = []
        for ev in evs:
            p = int(ev.get("period") or 1)
            if p > 4:
                continue
            cat, detail, _, _ = categorize(ev)
            typ = {"GOAL": "Goal", "OWN_GOAL": "Goal", "CARD_RED": "Card", "CARD_YELLOW": "Card", "SUB": "subst"}.get(cat)
            if not typ:
                continue
            side = "home" if (ev.get("team") or {}).get("id") == home_id else "away"
            self.events.append({"t": self.kickoff + real_elapsed_s(ev, dur), "type": typ, "side": side, "detail": detail})
        self.events.sort(key=lambda e: e["t"])

    def status_at(self, t: float):
        if t < self.kickoff:
            return "NS", 0
        for i, (s, e, st, m0) in enumerate(self.periods):
            if s <= t < e:
                return st, int(m0 + (t - s) // 60) + 1
            nxt = self.periods[i + 1][0] if i + 1 < len(self.periods) else None
            if nxt and e <= t < nxt:
                return ("HT" if st == "1H" else "BT"), m0 + 45
        return "FT", 90

    def score_at(self, t: float, delay: float):
        h = a = 0
        for e in self.events:
            if e["t"] + delay <= t and e["type"] == "Goal":
                if e["side"] == "home":
                    h += 1
                else:
                    a += 1
        return h, a


def simulate_day(matches: list[SimMatch], policy: str, budget: int, delay: float, stats: bool,
                 fixed_interval: float = 60.0) -> dict:
    sch = Scheduler(policy="fixed" if policy.startswith("fixed") else "adaptive", daily_budget=budget,
                    minute_limit=10, reserve=5, fixed_interval_s=fixed_interval, time_scale=1.0, stats_calls=stats)
    t = min(m.kickoff for m in matches) - 3600
    # discovery: 1 request lấy lịch thi đấu trong ngày
    total_req = 0
    orig_record = sch.record_request

    def record(now, remaining_day=None):
        nonlocal total_req
        total_req += 1
        orig_record(now, remaining_day)
    sch.record_request = record
    sch.record_request(t)
    sch.last_discovery_ts = t
    for m in matches:
        sch.upsert_fixture(m.id, m.kickoff, "NS", 0, 0, 0, t)
    seen: dict[str, set] = defaultdict(set)
    delays, goal_delays, late_final = [], [], 0
    polls = 0
    stats_req = 0
    by_id = {m.id: m for m in matches}

    def detect(m: SimMatch, now: float, final: bool = False):
        nonlocal late_final
        for i, e in enumerate(m.events):
            if i in seen[m.id] or e["t"] + delay > now:
                continue
            seen[m.id].add(i)
            d = (min(now, max(t, m.end + delay)) if final else now) - e["t"]
            delays.append(d)
            if e["type"] == "Goal":
                goal_delays.append(d)
            if final:
                late_final += 1

    guard = 0
    while guard < 5000:
        guard += 1
        nxt = sch.next_live_poll(t)
        if nxt is None:
            break
        nxt = max(nxt, sch.minute_slot_free_at(nxt))
        t = nxt
        sch.record_request(t)
        sch.last_live_poll_ts = t
        polls += 1
        live_ids = set()
        for m in matches:
            st, minute = m.status_at(t)
            if st in ("1H", "HT", "2H", "ET", "BT"):
                live_ids.add(m.id)
                h, a = m.score_at(t, delay)
                f = sch.upsert_fixture(m.id, m.kickoff, st, minute, h, a, t)
                detect(m, t)
                if st == "HT" and stats and "HT" not in f.stats_done:
                    f.stats_done.add("HT")
                    sch.record_request(t)
                    stats_req += 1
        for fid in sch.mark_missing_from_live(live_ids, t):
            f = sch.fixtures[fid]
            f.stats_done.add("FINAL")
            sch.record_request(t)
            # lần chốt sau trận (/fixtures?id=) thấy mọi sự kiện; độ trễ tính tới thời điểm chốt
            detect(by_id[fid], max(t, by_id[fid].end + delay), final=True)
            if stats:
                f.stats_done.add("FT")
                sch.record_request(t)
                stats_req += 1
        if total_req >= budget:
            break
    # trận chưa từng được chốt (hết ngân sách) -> sự kiện bị bỏ lỡ
    total_events = sum(len(m.events) for m in matches)
    missed = total_events - sum(len(v) for v in seen.values())
    return {"policy": policy, "matches": len(matches), "requests": total_req, "live_polls": polls,
            "stats_requests": stats_req, "budget_left": budget - total_req, "events": total_events,
            "missed": missed, "late_only_at_final": late_final,
            "delay": summary(delays), "goal_delay": summary(goal_delays)}


def live_union_seconds(matches: list[SimMatch]) -> float:
    # khoảng thời gian trận được coi là "live" (gồm cả nghỉ giữa hiệp) - lịch cố định gọi suốt khoảng này
    segs = sorted((m.kickoff, m.end) for m in matches)
    tot, cur_s, cur_e = 0.0, None, None
    for s, e in segs:
        if cur_e is None or s > cur_e:
            if cur_e is not None:
                tot += cur_e - cur_s
            cur_s, cur_e = s, e
        else:
            cur_e = max(cur_e, e)
    if cur_e is not None:
        tot += cur_e - cur_s
    return tot


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--budget", type=int, default=100)
    ap.add_argument("--provider-delay-s", type=float, default=20)
    ap.add_argument("--no-stats", action="store_true")
    ap.add_argument("--max-days", type=int, default=0)
    a = ap.parse_args()
    stats = not a.no_stats
    days: dict[str, list] = defaultdict(list)
    for m in downloaded_matches(DATA_DIR):
        days[m["match_date"]].append(m)
    if not days:
        raise SystemExit("Chưa có dữ liệu StatsBomb. Chạy: python -m sportslake.tools.download_data")
    day_keys = sorted(days)[: a.max_days or None]
    per_day, agg = [], defaultdict(lambda: defaultdict(list))
    for d in day_keys:
        sims = [SimMatch(m) for m in days[d]]
        # fixed-fair: chia đều ngân sách khả dụng cho thời gian có trận diễn ra (cả giờ nghỉ)
        span = live_union_seconds(sims)
        usable = a.budget - 5 - 1 - len(sims) * (4 if stats else 2)
        fair = max(6.3, span / max(1, usable))
        for pol, fi in (("adaptive", 0), ("fixed-fair", fair), ("fixed-60s", 60.0)):
            r = simulate_day(sims, pol, a.budget, a.provider_delay_s, stats, fi or 60.0)
            row = {"day": d, "policy": pol, "matches": r["matches"], "requests": r["requests"],
                   "budget_left": r["budget_left"], "events": r["events"], "missed": r["missed"],
                   "late_only_at_final": r["late_only_at_final"],
                   "delay_p50_s": r["delay"].get("p50"), "delay_p95_s": r["delay"].get("p95"),
                   "goal_delay_p50_s": r["goal_delay"].get("p50"), "goal_delay_p95_s": r["goal_delay"].get("p95"),
                   "fixed_interval_s": round(fi, 1) if fi else None}
            per_day.append(row)
            for k in ("requests", "budget_left", "missed", "late_only_at_final", "delay_p50_s", "delay_p95_s",
                      "goal_delay_p50_s", "goal_delay_p95_s"):
                if row[k] is not None:
                    agg[pol][k].append(row[k])
    totals = []
    for pol, d in agg.items():
        totals.append({"policy": pol, "days": len(day_keys),
                       **{f"avg_{k}": round(sum(v) / len(v), 1) for k, v in d.items() if v}})
    write_csv("e5_scheduler_per_day.csv", per_day)
    write_csv("e5_scheduler_summary.csv", totals)
    write_json("e5_scheduler_summary.json", {"params": vars(a), "summary": totals})
    print(md_table(totals))

    p = plt()
    import numpy as np

    fig, ax = p.subplots(1, 2, figsize=(9.5, 3.6))
    pols = [t["policy"] for t in totals]
    x = np.arange(len(pols))
    w = 0.38
    ax[0].bar(x - w / 2, [t.get("avg_goal_delay_p50_s", 0) for t in totals], w, label="p50")
    ax[0].bar(x + w / 2, [t.get("avg_goal_delay_p95_s", 0) for t in totals], w, label="p95")
    ax[0].set_xticks(x, pols)
    ax[0].set_ylabel("giây")
    ax[0].set_title("Độ trễ phát hiện bàn thắng (TB theo ngày)")
    ax[0].legend()
    ax[1].bar(x - w / 2, [t.get("avg_requests", 0) for t in totals], w, color="#4a9", label="request/ngày")
    ax[1].bar(x + w / 2, [t.get("avg_missed", 0) for t in totals], w, color="#d55", label="sự kiện bỏ lỡ/ngày")
    ax[1].set_xticks(x, pols)
    ax[1].set_title(f"Ngân sách {a.budget} request/ngày")
    ax[1].set_ylim(0, a.budget * 1.3)
    ax[1].legend(loc="upper left", ncol=2)
    fig.tight_layout()
    out = results_dir() / "e5_scheduler.png"
    fig.savefig(out)
    print(f"Kết quả: {results_dir()}")


if __name__ == "__main__":
    main()
