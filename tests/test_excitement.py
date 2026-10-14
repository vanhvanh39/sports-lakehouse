from sportslake.analytics.excitement import (DetectorState, EventLite, level_of, score_window, simulate_match,
                                             v2_decide)


def test_decay_and_window():
    evs = [EventLite("GOAL", 1000), EventLite("SHOT", 1100, xg=0.4)]
    s_now = score_window(evs, 1100, 20, 1, 0)
    s_later = score_window(evs, 1500, 25, 1, 0)
    s_out = score_window(evs, 1000 + 700, 30, 1, 0)
    assert s_now > s_later > s_out >= 0


def test_context_increases_late_close():
    evs = [EventLite("SHOT", 5000, xg=0.3)]
    assert score_window(evs, 5000, 85, 1, 1) > score_window(evs, 5000, 20, 3, 0)


def test_hysteresis():
    st = DetectorState()
    fire, st = v2_decide(st, 7.0, 100, 6.0)
    assert fire and not st.armed
    fire, st = v2_decide(st, 7.5, 150, 6.0)
    assert not fire
    fire, st = v2_decide(st, 2.0, 400, 6.0)
    assert not fire and st.armed
    fire, st = v2_decide(st, 8.0, 500, 6.0)
    assert fire


def test_simulate_v1_v2():
    evs = [EventLite("SHOT", 4000 + i * 20, xg=0.35, minute=80) for i in range(4)] + [EventLite("GOAL", 4100, minute=80)]
    alerts = simulate_match(evs, [(4100, 1, 1)], 6.0)
    assert any(a["version"] == "V1" for a in alerts)
    assert any(a["version"] == "V2" for a in alerts)
    assert level_of(11) == "VERY_HIGH"


def test_v2_suppressed_right_after_v1():
    st = DetectorState()
    fire, st2 = v2_decide(st, 9.0, 1000, 6.0, last_v1_clock=980)
    assert not fire and not st2.armed          # khoảnh khắc đã được V1 báo -> V2 im lặng nhưng vẫn "đã báo"
    fire, _ = v2_decide(DetectorState(), 9.0, 1000, 6.0, last_v1_clock=800)
    assert fire                                 # V1 đã quá 60 giây -> V2 báo bình thường
