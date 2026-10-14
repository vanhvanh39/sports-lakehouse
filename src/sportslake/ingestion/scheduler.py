"""Bộ lập lịch gọi API thích ứng theo vòng đời trận và ngân sách request.

Ý tưởng: một lần gọi ``/fixtures?live=all`` trả về mọi trận đang diễn ra, nên chi phí là
theo *thời gian*, không theo *số trận*. Bộ lập lịch:

1. Dự đoán dòng thời gian còn lại của các trận đang theo dõi (hiệp 1, nghỉ giữa hiệp,
   hiệp 2) và gán trọng số cho từng phút: nghỉ giữa hiệp = 0, hiệp 1 = 1.0, hiệp 2 = 1.1,
   15 phút cuối = 1.3, cuối trận và tỷ số sát nút = 1.5, hiệp phụ = 1.5 (bàn thắng xảy ra
   nhiều hơn ở cuối trận; trọng số đã hiệu chỉnh bằng mô phỏng E5).
   Khi chỉ còn phần dự phòng, bộ lập lịch chỉ hẹn thêm một lần gọi sau giờ kết thúc dự kiến
   để chốt kết quả, tránh bỏ lỡ sự kiện cuối trận.
2. Tổng "số phút có trọng số" còn lại W chia cho ngân sách còn lại B (đã trừ dự phòng
   và các request bắt buộc khác) cho ra khoảng cách cơ sở giữa hai lần gọi.
3. Khoảng cách hiện tại = khoảng cách cơ sở / trọng số hiện tại, kẹp trong
   [giới hạn theo phút của API, 10 phút trận]. Trong giờ nghỉ, lần gọi kế tiếp được hẹn
   đúng lúc dự kiến hiệp 2 bắt đầu.

Chính sách ``fixed`` gọi đều đặn mỗi ``fixed_interval_s`` giây trong suốt thời gian có trận
(kể cả giờ nghỉ) và dùng làm mốc so sánh trong thí nghiệm E5.

Mọi thời gian tính bằng giây epoch "tường"; ``time_scale`` > 1 khi đồng hồ trận chạy nhanh
hơn thời gian thực (Mock API, mô phỏng).
"""
from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone

LIVE_STATUSES = {"1H", "HT", "2H", "ET", "BT", "P", "LIVE", "INT", "SUSP"}
DONE_STATUSES = {"FT", "AET", "PEN", "PST", "CANC", "ABD", "AWD", "WO"}

HALF_MIN = 48      # độ dài dự kiến hiệp 1 (gồm bù giờ), phút trận
HT_MIN = 15        # nghỉ giữa hiệp
SECOND_HALF_END = 98  # phút kết thúc dự kiến của hiệp 2 (bù giờ hiện nay thường 6-8 phút)


@dataclass
class TrackedFixture:
    fixture_id: str
    kickoff_ts: float
    status: str = "NS"
    elapsed: int = 0
    home_goals: int = 0
    away_goals: int = 0
    last_seen_ts: float = 0.0
    ht_seen_ts: float | None = None
    stats_done: set = field(default_factory=set)  # {"HT","FT"}

    @property
    def close_game(self) -> bool:
        return abs(self.home_goals - self.away_goals) <= 1

    @property
    def done(self) -> bool:
        return self.status in DONE_STATUSES


def weight_at(status: str, minute: int, close: bool) -> float:
    if status in ("HT", "BT", "NS") or status in DONE_STATUSES:
        return 0.0
    if status in ("ET", "P"):
        return 1.5
    if status == "1H":
        return 1.0
    # 2H
    if minute >= 75:
        return 1.5 if close else 1.3
    return 1.1


class Scheduler:
    def __init__(self, policy: str = "adaptive", daily_budget: int = 100, minute_limit: int = 10,
                 reserve: int = 5, fixed_interval_s: float = 90.0, time_scale: float = 1.0,
                 max_interval_match_min: float = 10.0, stats_calls: bool = True, discovery_every_s: float = 6 * 3600):
        self.policy = policy
        self.daily_budget = daily_budget
        self.minute_limit = minute_limit
        self.reserve = reserve
        self.fixed_interval_s = fixed_interval_s
        self.time_scale = max(1e-6, time_scale)
        self.max_interval_match_min = max_interval_match_min
        self.stats_calls = stats_calls
        self.discovery_every_s = discovery_every_s / self.time_scale
        self.fixtures: dict[str, TrackedFixture] = {}
        self.used_today = 0
        self.remaining_day_hint: int | None = None
        self.day_key: str | None = None
        self.recent: deque[float] = deque()
        self.last_discovery_ts: float | None = None
        self.last_live_poll_ts: float | None = None

    # ---------------- ngân sách ----------------
    def _roll_day(self, now: float) -> None:
        key = datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y-%m-%d")
        if key != self.day_key:
            self.day_key = key
            self.used_today = 0
            self.remaining_day_hint = None

    def record_request(self, now: float, remaining_day: int | None = None) -> None:
        self._roll_day(now)
        self.used_today += 1
        self.recent.append(now)
        if remaining_day is not None:
            self.remaining_day_hint = remaining_day

    def remaining_budget(self, now: float) -> int:
        self._roll_day(now)
        computed = self.daily_budget - self.used_today
        if self.remaining_day_hint is not None:
            computed = min(computed, self.remaining_day_hint)
        return max(0, computed)

    def minute_slot_free_at(self, now: float) -> float:
        """Thời điểm sớm nhất được gọi tiếp mà không vượt giới hạn theo phút (cửa sổ trượt 60 giây)."""
        while self.recent and now - self.recent[0] >= 60:
            self.recent.popleft()
        if len(self.recent) < self.minute_limit:
            return now
        return self.recent[0] + 60.0 + 0.05

    # ---------------- trạng thái trận ----------------
    def upsert_fixture(self, fixture_id: str, kickoff_ts: float, status: str, elapsed: int | None,
                       home_goals: int, away_goals: int, now: float) -> TrackedFixture:
        f = self.fixtures.get(fixture_id)
        if f is None:
            f = TrackedFixture(fixture_id, kickoff_ts)
            self.fixtures[fixture_id] = f
        f.kickoff_ts = kickoff_ts or f.kickoff_ts
        if status == "HT" and f.status != "HT":
            f.ht_seen_ts = now
        f.status = status
        f.elapsed = int(elapsed or f.elapsed or 0)
        f.home_goals, f.away_goals = int(home_goals or 0), int(away_goals or 0)
        f.last_seen_ts = now
        return f

    def mark_missing_from_live(self, live_ids: set[str], now: float) -> list[str]:
        """Trận đã từng live nhưng không còn trong /fixtures?live=all -> coi như kết thúc."""
        ended = []
        for fid, f in self.fixtures.items():
            if f.status in LIVE_STATUSES and fid not in live_ids:
                f.status = "FT"
                ended.append(fid)
        return ended

    def active_fixtures(self, now: float) -> list[TrackedFixture]:
        return [f for f in self.fixtures.values() if not f.done]

    # ---------------- dự đoán dòng thời gian ----------------
    def _wall(self, match_minutes: float) -> float:
        return match_minutes * 60.0 / self.time_scale

    def _timeline(self, f: TrackedFixture, now: float) -> list[tuple[float, float, str, int]]:
        """Danh sách các đoạn (start_ts, end_ts, status, minute_at_start) dự kiến từ hiện tại."""
        segs = []
        t = now
        if f.status == "NS":
            start = max(now, f.kickoff_ts)
            segs.append((start, start + self._wall(HALF_MIN), "1H", 0))
            t = start + self._wall(HALF_MIN)
            segs.append((t, t + self._wall(HT_MIN), "HT", 45))
            t += self._wall(HT_MIN)
            segs.append((t, t + self._wall(SECOND_HALF_END - 45), "2H", 45))
        elif f.status == "1H":
            rem = max(2, HALF_MIN - f.elapsed)
            segs.append((t, t + self._wall(rem), "1H", f.elapsed))
            t += self._wall(rem)
            segs.append((t, t + self._wall(HT_MIN), "HT", 45))
            t += self._wall(HT_MIN)
            segs.append((t, t + self._wall(SECOND_HALF_END - 45), "2H", 45))
        elif f.status == "HT":
            resume = (f.ht_seen_ts or now) + self._wall(HT_MIN)
            if resume > now:
                segs.append((now, resume, "HT", 45))
                t = resume
            segs.append((t, t + self._wall(SECOND_HALF_END - 45), "2H", 45))
        elif f.status == "2H":
            rem = max(2, SECOND_HALF_END - f.elapsed)
            segs.append((t, t + self._wall(rem), "2H", f.elapsed))
        elif f.status in ("ET", "BT", "P"):
            rem = max(2, 125 - max(f.elapsed, 90))
            segs.append((t, t + self._wall(rem), "ET", max(f.elapsed, 90)))
        return segs

    def weighted_minutes_remaining(self, now: float) -> tuple[float, float]:
        """(tổng số phút trận có trọng số còn lại, thời điểm kết thúc dự kiến)."""
        tls = [(f, self._timeline(f, now)) for f in self.active_fixtures(now)]
        tls = [x for x in tls if x[1]]
        if not tls:
            return 0.0, now
        end = max(seg[1] for _, tl in tls for seg in tl)
        step = self._wall(1.0)  # 1 phút trận
        total = 0.0
        t = now
        while t < end:
            w = 0.0
            for f, tl in tls:
                for s, e, st, m0 in tl:
                    if s <= t < e:
                        minute = m0 + int((t - s) / step)
                        w = max(w, weight_at(st, minute, f.close_game))
                        break
            total += w
            t += step
        return total, end

    def current_weight(self, now: float) -> float:
        w = 0.0
        for f in self.active_fixtures(now):
            if f.status == "NS":
                continue
            w = max(w, weight_at(f.status, f.elapsed, f.close_game))
        return w

    # ---------------- quyết định ----------------
    def planned_extra_requests(self, now: float) -> int:
        n = 0
        # mỗi trận cần: 1 lần chốt kết quả khi rời danh sách live (FINAL)
        # và nếu bật thống kê: 1 lần lấy thống kê lúc nghỉ giữa hiệp (HT) + 1 lần lúc kết thúc (FT)
        needed = {"HT", "FT", "FINAL"} if self.stats_calls else {"FINAL"}
        for f in self.active_fixtures(now):
            n += len(needed - f.stats_done)
            # lần gọi lúc phát hiện nghỉ giữa hiệp không nằm trong phần "có trọng số"
            if f.status in ("NS", "1H"):
                n += 1
        # dự phòng 1 lần discovery (gọi lịch thi đấu) sau khi các trận hiện tại kết thúc
        if self.last_discovery_ts is not None:
            n += 1
        return n

    def need_discovery(self, now: float) -> bool:
        if self.last_discovery_ts is None:
            return True
        if datetime.fromtimestamp(self.last_discovery_ts, tz=timezone.utc).date() != datetime.fromtimestamp(now, tz=timezone.utc).date():
            return True
        if now - self.last_discovery_ts >= self.discovery_every_s and not self.active_fixtures(now):
            return True
        return False

    def next_live_poll(self, now: float) -> float | None:
        """Thời điểm nên gọi /fixtures?live=all tiếp theo; None nếu không còn trận nào hôm nay."""
        active = self.active_fixtures(now)
        if not active:
            return None
        live = [f for f in active if f.status in LIVE_STATUSES]
        upcoming = [f for f in active if f.status == "NS"]
        last = self.last_live_poll_ts
        remaining = self.remaining_budget(now) - self.reserve
        if remaining <= 0:
            # hết ngân sách thường: dùng phần dự phòng chỉ để chốt kết quả các trận đang live,
            # hẹn một lần gọi ngay sau thời điểm kết thúc dự kiến
            if not live or self.remaining_budget(now) <= 0:
                return None
            ends = [max(seg[1] for seg in tl) for tl in (self._timeline(f, now) for f in live) if tl]
            return max(now, min(ends) + self._wall(3)) if ends else None

        if not live:
            # chưa trận nào bắt đầu: hẹn đúng giờ bóng lăn (+30 giây trận)
            first_kick = min(f.kickoff_ts for f in upcoming)
            return max(now, first_kick + self._wall(0.5))

        if self.policy == "fixed":
            nxt = (last + self.fixed_interval_s) if last else now
            return max(now, nxt)

        # adaptive
        w_now = self.current_weight(now)
        if w_now <= 0:
            # tất cả trận live đang nghỉ giữa hiệp -> hẹn lúc hiệp sau bắt đầu
            resumes = []
            for f in live:
                if f.status == "HT":
                    resumes.append((f.ht_seen_ts or now) + self._wall(HT_MIN) + self._wall(0.5))
                elif f.status == "BT":
                    resumes.append(now + self._wall(5))
            nxt = min(resumes) if resumes else now + self._wall(self.max_interval_match_min)
            if upcoming:
                nxt = min(nxt, max(now, min(f.kickoff_ts for f in upcoming) + self._wall(0.5)))
            return max(now, nxt)

        budget = remaining - self.planned_extra_requests(now)
        weighted, _ = self.weighted_minutes_remaining(now)
        if budget <= 0:
            interval_match_min = self.max_interval_match_min
        else:
            interval_match_min = (weighted / budget) / w_now
        interval = self._wall(interval_match_min)
        min_wall = 60.0 / max(1, self.minute_limit) * 1.05
        interval = max(min_wall, min(interval, self._wall(self.max_interval_match_min)))
        nxt = (last + interval) if last else now
        return max(now, nxt)

    def describe(self, now: float) -> dict:
        w, end = self.weighted_minutes_remaining(now)
        return {
            "policy": self.policy, "used_today": self.used_today, "remaining": self.remaining_budget(now),
            "weighted_minutes_remaining": round(w, 1), "current_weight": self.current_weight(now),
            "active": len(self.active_fixtures(now)),
            "live": [f.fixture_id for f in self.active_fixtures(now) if f.status in LIVE_STATUSES],
        }


def wait_until(ts: float, stop_flag=None) -> None:
    while True:
        d = ts - time.time()
        if d <= 0 or (stop_flag and stop_flag()):
            return
        time.sleep(min(d, 1.0))
