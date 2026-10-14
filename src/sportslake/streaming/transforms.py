"""Các phép biến đổi DataFrame thuần (không I/O) dùng trong pipeline - tách riêng để dễ kiểm thử."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .merge_sql import (EVENT_COLS, INSERT_RETRACT_COND, INSERT_UPSERT_COND, RETRACT_COND,  # noqa: F401
                        UPSERT_COND, insert_values, retract_set, upsert_set)

VALID_OPS = ("UPSERT", "RETRACT")
VALID_CATEGORIES = ("GOAL", "OWN_GOAL", "SHOT", "CARD_YELLOW", "CARD_RED", "PENALTY_AWARDED", "PENALTY_MISSED",
                    "SUB", "VAR", "FOUL", "OTHER")


def strip_confluent_header(col: str = "value"):
    """Bỏ 5 byte đầu (magic byte + schema id) của định dạng Confluent trước khi from_avro."""
    return F.expr(f"substring({col}, 6, length({col}) - 5)")


def decode_avro(df: DataFrame, schema_json: str) -> DataFrame:
    from pyspark.sql.avro.functions import from_avro

    return df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("topic"), F.col("partition").alias("kafka_partition"), F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_ts"), F.col("value").alias("raw_value"),
        from_avro(strip_confluent_header("value"), schema_json, {"mode": "PERMISSIVE"}).alias("r"),
    )


def ms_to_ts(col: str):
    return F.expr(f"timestamp_millis({col})")


def flatten_events(decoded: DataFrame) -> DataFrame:
    """Sau decode_avro: bung struct r, đổi epoch ms -> timestamp. Bản ghi không giải mã được có r = null."""
    return decoded.select(
        "kafka_key", "kafka_partition", "kafka_offset", "kafka_ts", "raw_value",
        F.col("r").isNull().alias("_corrupt"),
        *[F.col(f"r.{c}").alias(c) for c in EVENT_COLS],
        ms_to_ts("r.source_ts").alias("source_ts"), ms_to_ts("r.ingest_ts").alias("ingest_ts"),
    )


def dq_events(df: DataFrame) -> DataFrame:
    """Thêm cột dq_error (null nếu hợp lệ). Quy tắc: giải mã được, có khóa, op hợp lệ,
    phút trận trong [0, 130], hiệp trong [1, 5], loại sự kiện thuộc danh mục."""
    err = (F.when(F.col("_corrupt"), F.lit("avro_decode_failed"))
           .when(F.col("event_id").isNull() | (F.length("event_id") == 0), F.lit("missing_event_id"))
           .when(F.col("match_id").isNull(), F.lit("missing_match_id"))
           .when(~F.col("op").isin(*VALID_OPS), F.lit("invalid_op"))
           .when((F.col("minute") < 0) | (F.col("minute") > 130), F.lit("minute_out_of_range"))
           .when((F.col("period") < 1) | (F.col("period") > 5), F.lit("period_out_of_range"))
           .when(~F.col("category").isin(*VALID_CATEGORIES), F.lit("unknown_category")))
    return df.withColumn("dq_error", err)


def latest_per_key(df: DataFrame, keys: list[str], order_desc: list[str]) -> DataFrame:
    """Giữ bản ghi mới nhất cho mỗi khóa trong vi lô (khi trùng seq, RETRACT thắng UPSERT)."""
    w = Window.partitionBy(*keys).orderBy(*[F.col(c).desc() for c in order_desc])
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def events_merge_source(valid: DataFrame, batch_id: int) -> DataFrame:
    with_rank = valid.withColumn("_op_rank", F.when(F.col("op") == "RETRACT", 1).otherwise(0))
    latest = latest_per_key(with_rank, ["event_id"], ["seq", "_op_rank", "kafka_offset"]).drop("_op_rank")
    return latest.select(
        *EVENT_COLS, "source_ts", "ingest_ts", "kafka_ts", "processed_ts", "kafka_partition", "kafka_offset",
        F.lit(batch_id).cast("long").alias("batch_id"),
    )


STATE_COLS = ["match_id", "source", "seq", "league_id", "league_name", "season", "round", "home_team_id", "home_team",
              "away_team_id", "away_team", "home_goals", "away_goals", "status_short", "status_long", "elapsed",
              "extra"]


def flatten_state(decoded: DataFrame) -> DataFrame:
    return decoded.select(
        "kafka_ts", "kafka_partition", "kafka_offset", "raw_value", F.col("r").isNull().alias("_corrupt"),
        *[F.col(f"r.{c}").alias(c) for c in STATE_COLS],
        ms_to_ts("r.kickoff_ts").alias("kickoff_ts"), F.col("r.venue").alias("venue"),
        ms_to_ts("r.source_ts").alias("source_ts"), ms_to_ts("r.ingest_ts").alias("ingest_ts"),
    )


def dq_state(df: DataFrame) -> DataFrame:
    err = (F.when(F.col("_corrupt"), F.lit("avro_decode_failed"))
           .when(F.col("match_id").isNull(), F.lit("missing_match_id"))
           .when((F.col("home_goals") < 0) | (F.col("away_goals") < 0), F.lit("negative_goals"))
           .when(F.col("elapsed").isNotNull() & ((F.col("elapsed") < 0) | (F.col("elapsed") > 130)),
                 F.lit("elapsed_out_of_range")))
    return df.withColumn("dq_error", err)


STATS_COLS = ["match_id", "team_id", "team_name", "source", "seq", "shots_total", "shots_on_target", "possession_pct",
              "corners", "fouls", "yellow_cards", "red_cards", "xg", "stats_json"]


def flatten_stats(decoded: DataFrame) -> DataFrame:
    return decoded.select(
        "kafka_ts", "raw_value", F.col("r").isNull().alias("_corrupt"),
        *[F.col(f"r.{c}").alias(c) for c in STATS_COLS],
        ms_to_ts("r.source_ts").alias("source_ts"), ms_to_ts("r.ingest_ts").alias("ingest_ts"),
    )


def tracking_positions(decoded: DataFrame) -> DataFrame:
    base = decoded.where("r is not null").select(
        "kafka_ts", F.col("r.match_id").alias("match_id"), F.col("r.period").alias("period"),
        F.col("r.frame").alias("frame"), F.col("r.time_s").alias("time_s"),
        F.col("r.ball_x").alias("ball_x"), F.col("r.ball_y").alias("ball_y"), F.col("r.players").alias("players"),
        ms_to_ts("r.source_ts").alias("source_ts"), ms_to_ts("r.ingest_ts").alias("ingest_ts"),
    )
    common = ["match_id", "period", "frame", "time_s", "source_ts", "ingest_ts", "kafka_ts"]
    players = base.select(*common, F.explode("players").alias("p")).select(
        *common, F.col("p.team").alias("team"), F.col("p.player").alias("player"),
        F.col("p.x").alias("x"), F.col("p.y").alias("y"))
    ball = base.where("ball_x is not null").select(
        *common, F.lit("ball").alias("team"), F.lit("ball").alias("player"),
        F.col("ball_x").alias("x"), F.col("ball_y").alias("y"))
    out = players.unionByName(ball)
    return out.select("match_id", "period", "frame", "time_s", "team", "player", "x", "y", "source_ts", "ingest_ts",
                      "kafka_ts", F.current_timestamp().alias("processed_ts"))


def dlq_records(df: DataFrame, stage: str) -> DataFrame:
    """Chuẩn bị bản ghi gửi vào match.dlq (JSON)."""
    cols = [c for c in df.columns if c not in ("raw_value", "_corrupt", "dq_error")]
    return df.select(
        F.coalesce(F.col("match_id"), F.lit("unknown")).cast("string").alias("key") if "match_id" in df.columns else F.lit("unknown").alias("key"),
        F.to_json(F.struct(
            F.lit(stage).alias("stage"), F.col("dq_error").alias("reason"),
            F.current_timestamp().cast("string").alias("detected_at"),
            F.base64("raw_value").alias("raw_value_b64"),
            F.struct(*cols).alias("record"),
        )).alias("value"),
    )
