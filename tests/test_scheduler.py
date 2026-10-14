from sportslake.ingestion.scheduler import Scheduler, weight_at

T0 = 1_760_000_000.0  # một thời điểm cố định


def test_weights_order():
    assert weight_at("HT", 45, True) == 0
    assert weight_at("1H", 20, False) < weight_at("2H", 60, False) < weight_at("2H", 80, False) <= weight_at("2H", 85, True)


def test_waits_for_kickoff_then_polls():
    s = Scheduler(daily_budget=100)
    s.last_discovery_ts = T0
    s.upsert_fixture("1", T0 + 3600, "NS", 0, 0, 0, T0)
    nxt = s.next_live_poll(T0)
    assert T0 + 3600 <= nxt <= T0 + 3600 + 60


def test_halftime_skips_to_resume():
    s = Scheduler(daily_budget=100)
    s.last_discovery_ts = T0
    s.upsert_fixture("1", T0 - 47 * 60, "HT", 45, 0, 0, T0)
    s.last_live_poll_ts = T0
    nxt = s.next_live_poll(T0)
    assert nxt >= T0 + 15 * 60  # không gọi trong giờ nghỉ


def test_interval_respects_budget_and_minute_limit():
    s = Scheduler(daily_budget=100, minute_limit=10)
    s.last_discovery_ts = T0
    s.upsert_fixture("1", T0 - 10 * 60, "1H", 10, 0, 0, T0)
    s.last_live_poll_ts = T0
    nxt = s.next_live_poll(T0)
    assert 6.0 <= nxt - T0 <= 600
    # dùng gần hết ngân sách -> khoảng cách giãn ra
    for i in range(80):
        s.record_request(T0 - 3000 + i)
    nxt2 = s.next_live_poll(T0)
    assert nxt2 - T0 > nxt - T0


def test_minute_window():
    s = Scheduler(minute_limit=10)
    for i in range(10):
        s.record_request(T0 + i)
    assert s.minute_slot_free_at(T0 + 10) >= T0 + 60


def test_fixed_policy_constant_interval():
    s = Scheduler(policy="fixed", fixed_interval_s=90)
    s.last_discovery_ts = T0
    s.upsert_fixture("1", T0 - 600, "1H", 10, 0, 0, T0)
    s.last_live_poll_ts = T0
    assert s.next_live_poll(T0 + 1) == T0 + 90


def test_reserve_used_only_to_finalize():
    s = Scheduler(daily_budget=20, reserve=5)
    s.last_discovery_ts = T0
    s.upsert_fixture("1", T0 - 80 * 60, "2H", 70, 1, 0, T0)
    for i in range(16):
        s.record_request(T0 - 100 + i)
    s.last_live_poll_ts = T0
    nxt = s.next_live_poll(T0)
    assert nxt is not None and nxt > T0 + 20 * 60  # một lần chốt sau giờ kết thúc dự kiến
