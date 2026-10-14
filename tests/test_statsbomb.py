from sportslake.canonical import match_clock
from sportslake.ingestion.statsbomb import categorize, key_moments, real_elapsed_s, to_canonical


def sb(i, typ, period, minute, second, ts, **kw):
    e = {"id": f"id{i}", "index": i, "type": {"name": typ}, "period": period, "minute": minute, "second": second,
         "timestamp": ts, "team": {"id": 1, "name": "A"}, "player": {"id": 9, "name": "P"}, "location": [100.0, 40.0]}
    e.update(kw)
    return e


EVENTS = [
    sb(1, "Half Start", 1, 0, 0, "00:00:00.000"),
    sb(2, "Shot", 1, 10, 5, "00:10:05.000", shot={"outcome": {"name": "Goal"}, "statsbomb_xg": 0.5, "type": {"name": "Open Play"}}),
    sb(3, "Shot", 1, 30, 0, "00:30:00.000", shot={"outcome": {"name": "Saved"}, "statsbomb_xg": 0.35, "type": {"name": "Open Play"}}),
    sb(4, "Foul Committed", 2, 60, 0, "00:15:00.000", foul_committed={"penalty": True}),
    sb(5, "Bad Behaviour", 2, 70, 0, "00:25:00.000", bad_behaviour={"card": {"name": "Red Card"}}),
    sb(6, "Pass", 2, 70, 10, "00:25:10.000"),
]


def test_categories():
    cats = [categorize(e)[0] for e in EVENTS]
    assert cats == ["OTHER", "GOAL", "SHOT", "PENALTY_AWARDED", "CARD_RED", "OTHER"]


def test_key_moments():
    km = key_moments(EVENTS)
    kinds = [k for m in km for k in m["kinds"]]
    assert kinds == ["GOAL", "BIG_CHANCE", "PENALTY", "RED_CARD"]


def test_canonical_and_clock():
    m = {"match_id": 77, "home_team": {"home_team_id": 1}}
    c = to_canonical(EVENTS[1], m, seq=2, source_ts=1, ingest_ts=2)
    assert c["match_id"] == "sb-77" and c["event_id"] == "sb-id2" and c["category"] == "GOAL" and c["xg"] == 0.5
    assert c["match_clock_s"] == match_clock(1, 10, 5)
    dur = {1: 47 * 60.0, 2: 49 * 60.0}
    assert real_elapsed_s(EVENTS[3], dur) == 47 * 60 + 15 * 60 + 15 * 60
