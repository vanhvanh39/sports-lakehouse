"""Định nghĩa và khởi tạo các bảng Delta Lake (bronze - silver - gold)."""
from __future__ import annotations

import logging

from pyspark.sql.types import (BinaryType, BooleanType, DateType, DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType, TimestampType)

from .. import config as C
from .spark_utils import table_path

log = logging.getLogger(__name__)


def _f(name, t, nullable=True):
    return StructField(name, t, nullable)


S, I, L, D, B, TS = StringType(), IntegerType(), LongType(), DoubleType(), BooleanType(), TimestampType()

BRONZE_RAW = StructType([
    _f("topic", S), _f("partition", I), _f("offset", L), _f("kafka_ts", TS), _f("key", S), _f("value", BinaryType()),
    _f("ingest_date", DateType()),
])

BRONZE_TRACKING = StructType([
    _f("match_id", S), _f("period", I), _f("frame", L), _f("time_s", D), _f("team", S), _f("player", S),
    _f("x", D), _f("y", D), _f("source_ts", TS), _f("ingest_ts", TS), _f("kafka_ts", TS), _f("processed_ts", TS),
])

SILVER_EVENTS = StructType([
    _f("event_id", S, False), _f("match_id", S), _f("source", S), _f("seq", L), _f("op", S), _f("is_deleted", B),
    _f("category", S), _f("event_type", S), _f("event_detail", S), _f("period", I), _f("minute", I), _f("second", I),
    _f("match_clock_s", I), _f("team_id", S), _f("team_name", S), _f("player_id", S), _f("player_name", S),
    _f("related_player_name", S), _f("x", D), _f("y", D), _f("xg", D), _f("outcome", S), _f("payload", S),
    _f("source_ts", TS), _f("ingest_ts", TS), _f("kafka_ts", TS), _f("processed_ts", TS),
    _f("first_seen_ts", TS), _f("updated_ts", TS), _f("version_count", I), _f("batch_id", L),
    _f("kafka_partition", I), _f("kafka_offset", L),
])

SILVER_STATE = StructType([
    _f("match_id", S, False), _f("source", S), _f("seq", L), _f("league_id", S), _f("league_name", S), _f("season", S),
    _f("round", S), _f("home_team_id", S), _f("home_team", S), _f("away_team_id", S), _f("away_team", S),
    _f("home_goals", I), _f("away_goals", I), _f("status_short", S), _f("status_long", S), _f("elapsed", I),
    _f("extra", I), _f("kickoff_ts", TS), _f("venue", S), _f("source_ts", TS), _f("ingest_ts", TS),
    _f("kafka_ts", TS), _f("processed_ts", TS), _f("updated_ts", TS),
])

SILVER_STATS = StructType([
    _f("match_id", S, False), _f("team_id", S, False), _f("team_name", S), _f("source", S), _f("seq", L),
    _f("shots_total", I), _f("shots_on_target", I), _f("possession_pct", D), _f("corners", I), _f("fouls", I),
    _f("yellow_cards", I), _f("red_cards", I), _f("xg", D), _f("stats_json", S), _f("source_ts", TS),
    _f("ingest_ts", TS), _f("kafka_ts", TS), _f("processed_ts", TS),
])

SILVER_EXCITEMENT = StructType([
    _f("match_id", S, False), _f("score", D), _f("level", S), _f("armed", B), _f("last_alert_clock", I),
    _f("last_clock", I), _f("minute", I), _f("home_goals", I), _f("away_goals", I), _f("updated_ts", TS),
])

SILVER_EXC_HISTORY = StructType([
    _f("match_id", S), _f("batch_id", L), _f("match_clock_s", I), _f("minute", I), _f("score", D), _f("level", S),
    _f("ts", TS),
])

SILVER_ALERTS = StructType([
    _f("alert_id", S, False), _f("match_id", S), _f("version", S), _f("kind", S), _f("minute", I),
    _f("match_clock_s", I), _f("score", D), _f("message", S), _f("triggering_event_id", S),
    _f("source_ts", TS), _f("ingest_ts", TS), _f("kafka_ts", TS), _f("processed_ts", TS), _f("alert_ts", TS),
])

TABLES = {
    C.T_BRONZE_RAW: (BRONZE_RAW, ["topic", "ingest_date"]),
    C.T_BRONZE_TRACKING: (BRONZE_TRACKING, ["match_id"]),
    C.T_SILVER_EVENTS: (SILVER_EVENTS, []),
    C.T_SILVER_STATE: (SILVER_STATE, []),
    C.T_SILVER_STATS: (SILVER_STATS, []),
    C.T_SILVER_EXCITEMENT: (SILVER_EXCITEMENT, []),
    C.T_SILVER_EXC_HISTORY: (SILVER_EXC_HISTORY, []),
    C.T_SILVER_ALERTS: (SILVER_ALERTS, []),
}


def ensure_tables(spark, which=None) -> None:
    from delta.tables import DeltaTable

    for key, (schema, parts) in TABLES.items():
        if which and key not in which:
            continue
        path = table_path(key)
        b = (DeltaTable.createIfNotExists(spark).location(path).addColumns(schema)
             .property("delta.enableChangeDataFeed", "true")
             .property("delta.appendOnly", "false")
             .property("description", f"{key[0]}.{key[1]}"))
        if parts:
            b = b.partitionedBy(*parts)
        b.execute()
        log.info("Bảng sẵn sàng: %s", path)
