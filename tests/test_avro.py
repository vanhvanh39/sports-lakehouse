import fastavro

from sportslake.canonical import make_event
from sportslake.config import TOPIC_EVENTS, TOPIC_STATE, TOPIC_STATS, TOPIC_TRACKING
from sportslake.ingestion.collector import parse_stats
from sportslake.kafka_io import AvroEncoder


def roundtrip(topic, rec):
    enc = AvroEncoder(topic, schema_id=7)
    data = enc.encode(rec)
    assert data[0] == 0 and int.from_bytes(data[1:5], "big") == 7
    return enc.decode(data)


def test_event_roundtrip():
    rec = make_event(event_id="e1", match_id="m1", source="t", category="GOAL", event_type="Goal", xg=0.4,
                     payload={"a": 1})
    out = roundtrip(TOPIC_EVENTS, rec)
    assert out["event_id"] == "e1" and out["xg"] == 0.4 and out["payload"] == '{"a": 1}'


def test_state_stats_tracking_roundtrip():
    st = {"match_id": "1", "source": "t", "seq": 1, "home_goals": 1, "away_goals": 0, "status_short": "1H",
          "source_ts": 1, "ingest_ts": 2}
    assert roundtrip(TOPIC_STATE, st)["home_goals"] == 1
    row = parse_stats("9", {"team": {"id": 5, "name": "A"}, "statistics": [
        {"type": "Ball Possession", "value": "55%"}, {"type": "Total Shots", "value": 7},
        {"type": "expected_goals", "value": "1.25"}]}, 1, "t", 1)
    out = roundtrip(TOPIC_STATS, row)
    assert out["possession_pct"] == 55.0 and out["shots_total"] == 7 and out["xg"] == 1.25
    tr = {"match_id": "m", "period": 1, "frame": 1, "time_s": 0.04, "ball_x": 0.5, "ball_y": None,
          "players": [{"team": "home", "player": "p1", "x": 0.1, "y": 0.2}], "source_ts": 1, "ingest_ts": 1}
    assert roundtrip(TOPIC_TRACKING, tr)["players"][0]["player"] == "p1"


def test_backward_compatible_reader():
    """Lược đồ mới thêm trường có mặc định vẫn đọc được dữ liệu cũ (tương thích BACKWARD)."""
    enc = AvroEncoder(TOPIC_EVENTS, schema_id=1)
    old = enc.raw_schema
    new = dict(old, fields=old["fields"] + [{"name": "new_field", "type": ["null", "string"], "default": None}])
    data = enc.encode(make_event(event_id="x", match_id="m"))
    import io
    rec = fastavro.schemaless_reader(io.BytesIO(data[5:]), fastavro.parse_schema(old), fastavro.parse_schema(new))
    assert rec["new_field"] is None
