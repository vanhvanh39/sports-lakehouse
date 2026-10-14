"""Kiểm thử các phép biến đổi Spark (cần pyspark; tự bỏ qua nếu môi trường không có)."""
import pytest

pyspark = pytest.importorskip("pyspark")


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    s = SparkSession.builder.master("local[2]").config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.session.timeZone", "UTC").getOrCreate()
    yield s
    s.stop()


def test_dq_and_latest(spark):
    from pyspark.sql import functions as F

    from sportslake.streaming import transforms as T

    rows = [
        ("e1", "m", "UPSERT", 1, "GOAL", 10, 1, False, 100),
        ("e1", "m", "UPSERT", 2, "GOAL", 10, 1, False, 101),
        ("e2", "m", "UPSERT", 3, "SHOT", 20, 1, False, 102),
        ("e2", "m", "RETRACT", 3, "SHOT", 20, 1, False, 103),
        ("e3", "m", "UPSERT", 1, "SHOT", 500, 2, False, 104),
        ("e4", "m", "BOGUS", 1, "SHOT", 5, 1, False, 105),
    ]
    df = spark.createDataFrame(rows, "event_id string, match_id string, op string, seq long, category string, "
                                     "minute int, period int, _corrupt boolean, kafka_offset long")
    for c in T.EVENT_COLS:
        if c not in df.columns:
            df = df.withColumn(c, F.lit(None).cast("string"))
    df = (df.withColumn("source_ts", F.current_timestamp()).withColumn("ingest_ts", F.current_timestamp())
          .withColumn("kafka_ts", F.current_timestamp()).withColumn("processed_ts", F.current_timestamp())
          .withColumn("kafka_partition", F.lit(0)))
    q = T.dq_events(df)
    bad = {r["event_id"]: r["dq_error"] for r in q.where("dq_error is not null").collect()}
    assert bad == {"e3": "minute_out_of_range", "e4": "invalid_op"}
    src = {r["event_id"]: r for r in T.events_merge_source(q.where("dq_error is null"), 5).collect()}
    assert src["e1"]["seq"] == 2 and src["e2"]["op"] == "RETRACT" and src["e1"]["batch_id"] == 5


# ---------------------------------------------------------------- dựng DataFrame giống đầu ra from_avro
def _spark_type(t):
    from pyspark.sql import types as ST

    if isinstance(t, list):
        non_null = [x for x in t if x != "null"]
        return _spark_type(non_null[0])
    if isinstance(t, dict):
        if t["type"] == "array":
            return ST.ArrayType(_spark_type(t["items"]))
        if t["type"] == "record":
            return ST.StructType([ST.StructField(f["name"], _spark_type(f["type"])) for f in t["fields"]])
    return {"string": ST.StringType(), "long": ST.LongType(), "int": ST.IntegerType(),
            "double": ST.DoubleType()}[t]


def decoded_df(spark, topic, records):
    from pyspark.sql import types as ST

    from sportslake.kafka_io import load_schema

    rtype = _spark_type(load_schema(topic))
    schema = ST.StructType([ST.StructField("kafka_key", ST.StringType()), ST.StructField("topic", ST.StringType()),
                            ST.StructField("kafka_partition", ST.IntegerType()), ST.StructField("kafka_offset", ST.LongType()),
                            ST.StructField("kafka_ts", ST.TimestampType()), ST.StructField("raw_value", ST.BinaryType()),
                            ST.StructField("r", rtype)])
    import datetime as dt
    now = dt.datetime(2026, 10, 14, 8, 0, 0)
    rows = [("m", topic, 0, i, now, b"\x00\x00\x00\x00\x01abc", r) for i, r in enumerate(records)]
    return spark.createDataFrame(rows, schema)


def test_flatten_events_and_dlq(spark):
    import json

    from sportslake.canonical import make_event
    from sportslake.config import TOPIC_EVENTS
    from sportslake.streaming import transforms as T

    recs = [make_event(event_id="a", match_id="m", category="GOAL", minute=10, source_ts=1_760_000_000_000),
            make_event(event_id="a", match_id="m", category="GOAL", minute=10, seq=5, source_ts=1_760_000_000_000),
            None]
    df = T.dq_events(T.flatten_events(decoded_df(spark, TOPIC_EVENTS, recs)))
    rows = df.collect()
    assert [r["dq_error"] for r in rows] == [None, None, "avro_decode_failed"]
    assert str(rows[0]["source_ts"]).startswith("2025-10-09")
    dlq = T.dlq_records(df.where("dq_error is not null"), "silver_events").collect()
    body = json.loads(dlq[0]["value"])
    assert body["reason"] == "avro_decode_failed" and body["raw_value_b64"]


def test_tracking_and_state(spark):
    from sportslake.config import TOPIC_STATE, TOPIC_TRACKING
    from sportslake.streaming import transforms as T

    tr = [{"match_id": "x", "period": 1, "frame": 1, "time_s": 0.04, "ball_x": 0.5, "ball_y": 0.5,
           "players": [{"team": "home", "player": "h1", "x": 0.1, "y": 0.2}, {"team": "away", "player": "a1", "x": 0.3, "y": 0.4}],
           "source_ts": 1, "ingest_ts": 1}]
    pos = T.tracking_positions(decoded_df(spark, TOPIC_TRACKING, tr)).collect()
    assert sorted(r["player"] for r in pos) == ["a1", "ball", "h1"]
    st = [{"match_id": "1", "source": "t", "seq": 1, "home_goals": -1, "away_goals": 0, "status_short": "1H",
           "elapsed": 10, "source_ts": 1, "ingest_ts": 1, "kickoff_ts": None}]
    s = T.dq_state(T.flatten_state(decoded_df(spark, TOPIC_STATE, st))).collect()
    assert s[0]["dq_error"] == "negative_goals"


def test_gold_tables(spark):
    from sportslake.batch import gold_job as G

    ev = spark.createDataFrame([
        ("m1", "A", "Kane", "GOAL", None, 0.6, False, 1),
        ("m1", "A", "Kane", "SHOT", "Saved", 0.2, False, 1),
        ("m1", "B", "Son", "CARD_YELLOW", None, None, False, 2),
        ("m1", "B", "Son", "GOAL", None, 0.3, True, 2),   # bị VAR hủy -> không tính
        ("m2", "B", "Son", "GOAL", None, 0.4, False, 1),
    ], "match_id string, team_id string, player_name string, category string, outcome string, xg double, "
       "is_deleted boolean, period int").withColumn("team_name", __import__("pyspark.sql.functions", fromlist=["col"]).col("team_id"))
    st = spark.createDataFrame([
        ("m1", "L", "2026", "FT", None, "A", "A", "B", "B", 1, 0),
        ("m2", "L", "2026", "FT", None, "B", "B", "A", "A", 1, 0),
    ], "match_id string, league_name string, season string, status_short string, kickoff_ts timestamp, "
       "home_team_id string, home_team string, away_team_id string, away_team string, home_goals int, away_goals int")
    stats = spark.createDataFrame([], "match_id string, team_id string, shots_total int, shots_on_target int, xg double, "
                                      "yellow_cards int, red_cards int, possession_pct double")
    tms = G.team_match_summary_df(ev, st, stats)
    by = {(r["match_id"], r["team_id"]): r for r in tms.collect()}
    assert by[("m1", "A")]["result"] == "W" and by[("m1", "A")]["shots"] == 2 and by[("m1", "A")]["xg"] == 0.8
    assert by[("m1", "B")]["points"] == 0 and by[("m1", "B")]["yellow_cards"] == 1
    table = {r["team_name"]: r for r in G.standings(tms).collect()}
    assert table["A"]["points"] == 3 and table["B"]["points"] == 3 and table["A"]["played"] == 2
    form = {r["team_name"]: r for r in G.team_form(tms).collect()}
    assert sorted(form["A"]["form_latest_first"]) == ["L", "W"]
    ps = {r["player_name"]: r for r in G.player_summary_df(ev).collect()}
    assert ps["Son"]["goals"] == 1 and ps["Kane"]["shots"] == 2


def test_player_distance(spark):
    from sportslake.batch import gold_job as G

    # 25 khung hình, mỗi khung tiến 0.004 * 105 m = 0.42 m (10.5 m/s)
    rows = [("x", 1, f, f * 0.04, "home", "h1", 0.3 + f * 0.004, 0.5) for f in range(1, 26)]
    pos = spark.createDataFrame(rows, "match_id string, period int, frame long, time_s double, team string, "
                                      "player string, x double, y double")
    d = G.player_distance_df(pos).collect()[0]
    assert abs(d["distance_km"] - 24 * 0.42 / 1000) < 0.001
    assert 36 < d["top_speed_kmh"] < 39
