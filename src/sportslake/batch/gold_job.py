"""Job Spark tính các bảng gold từ silver (chạy theo lịch bởi job_runner).

Chế độ:
  full         tính lại toàn bộ
  incremental  đọc Change Data Feed của silver.match_events/match_state/match_stats từ phiên bản đã xử lý
               lần trước, chỉ tính lại các trận bị ảnh hưởng rồi MERGE vào gold.team_match_summary;
               các bảng tổng hợp nhỏ (bảng xếp hạng, phong độ, cầu thủ) được ghi đè từ đó.

Chạy tay:  spark-submit ... /opt/app/src/jobs/run_gold.py --mode full [--tracking]
"""
from __future__ import annotations

import argparse
import json
import logging
import time

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .. import config as C
from ..logutil import setup_logging
from ..streaming.spark_utils import build_spark, table_path

log = setup_logging("gold")
STATE_PATH = f"s3a://{C.get_settings().bucket}/gold/_job_state"
FINISHED = ("FT", "AET", "PEN")


def _exists(spark, path: str) -> bool:
    from delta.tables import DeltaTable

    try:
        return DeltaTable.isDeltaTable(spark, path)
    except Exception:  # noqa: BLE001
        return False


def _latest_version(spark, path: str) -> int:
    from delta.tables import DeltaTable

    return int(DeltaTable.forPath(spark, path).history(1).collect()[0]["version"])


def _read_job_state(spark) -> dict:
    if not _exists(spark, STATE_PATH):
        return {}
    return {r["table_name"]: int(r["version"]) for r in spark.read.format("delta").load(STATE_PATH).collect()}


def _write_job_state(spark, state: dict) -> None:
    rows = [(k, int(v), int(time.time() * 1000)) for k, v in state.items()]
    (spark.createDataFrame(rows, "table_name string, version long, updated_ms long")
     .write.format("delta").mode("overwrite").save(STATE_PATH))


# ------------------------------------------------------------------ tính toán
def team_match_summary(spark, match_ids: list[str] | None = None) -> DataFrame:
    ev = spark.read.format("delta").load(table_path(C.T_SILVER_EVENTS))
    st = spark.read.format("delta").load(table_path(C.T_SILVER_STATE))
    stats = spark.read.format("delta").load(table_path(C.T_SILVER_STATS))
    if match_ids is not None:
        ev = ev.where(F.col("match_id").isin(match_ids))
        st = st.where(F.col("match_id").isin(match_ids))
        stats = stats.where(F.col("match_id").isin(match_ids))
    return team_match_summary_df(ev, st, stats)


def team_match_summary_df(ev: DataFrame, st: DataFrame, stats: DataFrame) -> DataFrame:
    ev = ev.where("NOT is_deleted AND period < 5")

    agg = ev.groupBy("match_id", "team_id").agg(
        F.sum(F.when(F.col("category").isin("SHOT", "GOAL", "PENALTY_MISSED"), 1).otherwise(0)).alias("ev_shots"),
        F.sum(F.when((F.col("category") == "GOAL") | ((F.col("category") == "SHOT") &
                                                     F.col("outcome").isin("Saved", "Saved To Post")), 1).otherwise(0)).alias("ev_on_target"),
        F.sum(F.coalesce("xg", F.lit(0.0))).alias("ev_xg"),
        F.sum(F.when(F.col("category") == "CARD_YELLOW", 1).otherwise(0)).alias("ev_yellow"),
        F.sum(F.when(F.col("category") == "CARD_RED", 1).otherwise(0)).alias("ev_red"),
        F.sum(F.when(F.col("category") == "SUB", 1).otherwise(0)).alias("subs"),
    )
    base = st.select("match_id", "league_name", "season", "status_short", "kickoff_ts", "home_team_id", "home_team",
                     "away_team_id", "away_team", "home_goals", "away_goals")
    home = base.select("match_id", "league_name", "season", "status_short", "kickoff_ts",
                       F.col("home_team_id").alias("team_id"), F.col("home_team").alias("team_name"),
                       F.col("away_team").alias("opponent"), F.lit(True).alias("is_home"),
                       F.col("home_goals").alias("goals_for"), F.col("away_goals").alias("goals_against"))
    away = base.select("match_id", "league_name", "season", "status_short", "kickoff_ts",
                       F.col("away_team_id").alias("team_id"), F.col("away_team").alias("team_name"),
                       F.col("home_team").alias("opponent"), F.lit(False).alias("is_home"),
                       F.col("away_goals").alias("goals_for"), F.col("home_goals").alias("goals_against"))
    teams = home.unionByName(away)
    finished = F.col("status_short").isin(*FINISHED)
    out = (teams.join(agg, ["match_id", "team_id"], "left")
           .join(stats.select("match_id", "team_id", "shots_total", "shots_on_target", "xg", "yellow_cards", "red_cards",
                              "possession_pct"), ["match_id", "team_id"], "left")
           .select(
               "match_id", "team_id", "team_name", "opponent", "is_home", "league_name", "season", "status_short",
               "kickoff_ts", "goals_for", "goals_against",
               F.when(~finished, None).when(F.col("goals_for") > F.col("goals_against"), "W")
                .when(F.col("goals_for") == F.col("goals_against"), "D").otherwise("L").alias("result"),
               F.when(~finished, None).when(F.col("goals_for") > F.col("goals_against"), 3)
                .when(F.col("goals_for") == F.col("goals_against"), 1).otherwise(0).alias("points"),
               F.coalesce("shots_total", "ev_shots").cast("int").alias("shots"),
               F.coalesce("shots_on_target", "ev_on_target").cast("int").alias("shots_on_target"),
               F.round(F.coalesce("xg", "ev_xg"), 3).alias("xg"),
               F.coalesce("yellow_cards", "ev_yellow").cast("int").alias("yellow_cards"),
               F.coalesce("red_cards", "ev_red").cast("int").alias("red_cards"),
               "possession_pct", F.coalesce("subs", F.lit(0)).cast("int").alias("subs"),
               F.current_timestamp().alias("updated_ts")))
    return out


def standings(tms: DataFrame) -> DataFrame:
    fin = tms.where(F.col("result").isNotNull())
    agg = fin.groupBy("league_name", "season", "team_name").agg(
        F.count("*").alias("played"), F.sum(F.when(F.col("result") == "W", 1).otherwise(0)).alias("won"),
        F.sum(F.when(F.col("result") == "D", 1).otherwise(0)).alias("drawn"),
        F.sum(F.when(F.col("result") == "L", 1).otherwise(0)).alias("lost"),
        F.sum("goals_for").alias("gf"), F.sum("goals_against").alias("ga"), F.sum("points").alias("points"),
        F.round(F.sum("xg"), 2).alias("xg_for"))
    agg = agg.withColumn("gd", F.col("gf") - F.col("ga"))
    w = Window.partitionBy("league_name", "season").orderBy(F.desc("points"), F.desc("gd"), F.desc("gf"), "team_name")
    return agg.withColumn("rank", F.row_number().over(w)).withColumn("updated_ts", F.current_timestamp())


def team_form(tms: DataFrame, n: int = 5) -> DataFrame:
    fin = tms.where(F.col("result").isNotNull())
    w = Window.partitionBy("league_name", "season", "team_name").orderBy(F.desc("kickoff_ts"), F.desc("match_id"))
    last = fin.withColumn("rn", F.row_number().over(w)).where(F.col("rn") <= n)
    return (last.groupBy("league_name", "season", "team_name").agg(
        F.concat_ws("", F.transform(F.sort_array(F.collect_list(F.struct("rn", "result"))), lambda x: x["result"])).alias("form_latest_first"),
        F.sum("points").alias("form_points"), F.round(F.avg("xg"), 2).alias("avg_xg"),
        F.round(F.avg("goals_for"), 2).alias("avg_goals_for"), F.round(F.avg("goals_against"), 2).alias("avg_goals_against"),
        F.count("*").alias("matches_in_form"))
        .withColumn("updated_ts", F.current_timestamp()))


def player_summary(spark) -> DataFrame:
    return player_summary_df(spark.read.format("delta").load(table_path(C.T_SILVER_EVENTS)))


def player_summary_df(ev: DataFrame) -> DataFrame:
    ev = ev.where("NOT is_deleted AND player_name IS NOT NULL AND period < 5")
    return (ev.groupBy("player_name", "team_name").agg(
        F.sum(F.when(F.col("category") == "GOAL", 1).otherwise(0)).alias("goals"),
        F.sum(F.when(F.col("category").isin("SHOT", "GOAL", "PENALTY_MISSED"), 1).otherwise(0)).alias("shots"),
        F.round(F.sum(F.coalesce("xg", F.lit(0.0))), 2).alias("xg"),
        F.sum(F.when(F.col("category") == "CARD_YELLOW", 1).otherwise(0)).alias("yellow_cards"),
        F.sum(F.when(F.col("category") == "CARD_RED", 1).otherwise(0)).alias("red_cards"),
        F.countDistinct("match_id").alias("matches"))
        .withColumn("goals_minus_xg", F.round(F.col("goals") - F.col("xg"), 2))
        .withColumn("updated_ts", F.current_timestamp()))


def player_distance(spark) -> DataFrame:
    return player_distance_df(spark.read.format("delta").load(table_path(C.T_BRONZE_TRACKING)))


def player_distance_df(pos: DataFrame) -> DataFrame:
    """Quãng đường chạy và tốc độ tối đa từ dữ liệu vị trí 25 Hz (tọa độ Metrica chuẩn hóa 0..1, sân 105x68 m)."""
    pos = (pos.where("team != 'ball' AND x IS NOT NULL AND y IS NOT NULL")
           .dropDuplicates(["match_id", "period", "frame", "team", "player"]))
    w = Window.partitionBy("match_id", "period", "team", "player").orderBy("frame")
    d = (pos.withColumn("px", F.lag("x").over(w)).withColumn("py", F.lag("y").over(w))
         .withColumn("pt", F.lag("time_s").over(w))
         .withColumn("dist", F.sqrt(((F.col("x") - F.col("px")) * 105) ** 2 + ((F.col("y") - F.col("py")) * 68) ** 2))
         .withColumn("dt", F.col("time_s") - F.col("pt"))
         .where("dt > 0 AND dt < 1")
         .withColumn("speed", F.col("dist") / F.col("dt"))
         .where("speed < 12.5"))  # loại nhiễu > 45 km/h
    return (d.groupBy("match_id", "team", "player").agg(
        F.round(F.sum("dist") / 1000, 3).alias("distance_km"),
        F.round(F.expr("percentile_approx(speed, 0.99)") * 3.6, 1).alias("top_speed_kmh"),
        F.round(F.sum(F.when(F.col("speed") > 19.8 / 3.6, F.col("dist")).otherwise(0)), 0).alias("high_speed_m"),
        F.count("*").alias("frames"))
        .withColumn("updated_ts", F.current_timestamp()))


# ------------------------------------------------------------------ ghi
def _overwrite(df: DataFrame, key) -> None:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(table_path(key))


def _merge_tms(spark, df: DataFrame) -> None:
    from delta.tables import DeltaTable

    path = table_path(C.T_GOLD_TEAM_MATCH)
    if not _exists(spark, path):
        _overwrite(df, C.T_GOLD_TEAM_MATCH)
        return
    (DeltaTable.forPath(spark, path).alias("t")
     .merge(df.alias("s"), "t.match_id = s.match_id AND t.team_id <=> s.team_id")
     .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())


def push_to_es(df: DataFrame, index: str, id_cols: list[str]) -> int:
    from ..streaming.es_sync import _clean, _es

    from elasticsearch.helpers import bulk

    rows = [r.asDict() for r in df.collect()]
    actions = [{"_index": index, "_id": "|".join(str(r[c]) for c in id_cols),
                "_source": {k: _clean(v) for k, v in r.items()}} for r in rows]
    if not actions:
        return 0
    es = _es()
    try:
        es.options(ignore_status=[404]).delete_by_query(index=index, query={"match_all": {}}, refresh=True)
    except Exception:  # noqa: BLE001
        pass
    ok, _ = bulk(es, actions, raise_on_error=False)
    return ok


def run(mode: str, tracking: bool, es: bool) -> dict:
    spark = build_spark(f"gold-{mode}")
    t0 = time.time()
    p_events, p_state = table_path(C.T_SILVER_EVENTS), table_path(C.T_SILVER_STATE)
    if not (_exists(spark, p_events) and _exists(spark, p_state)):
        log.warning("Bảng silver chưa có dữ liệu, bỏ qua lần chạy này")
        return {"skipped": True}
    p_stats = table_path(C.T_SILVER_STATS)
    v_events, v_state = _latest_version(spark, p_events), _latest_version(spark, p_state)
    # bảng stats cũng là đầu vào của gold (cú sút, kiểm soát bóng, xG) -> phải theo dõi thay đổi của nó
    v_stats = _latest_version(spark, p_stats) if _exists(spark, p_stats) else None
    job_state = _read_job_state(spark)
    affected: list[str] | None = None
    if mode == "incremental" and job_state and _exists(spark, table_path(C.T_GOLD_TEAM_MATCH)):
        ids = set()
        sources = [("events", p_events, v_events), ("state", p_state, v_state)]
        if v_stats is not None:
            sources.append(("stats", p_stats, v_stats))
        for key, path, v_now in sources:
            start = job_state.get(key, -1) + 1
            if start > v_now:
                continue
            ch = (spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", start)
                  .option("endingVersion", v_now).load(path).select("match_id").distinct())
            ids.update(r["match_id"] for r in ch.collect())
        affected = sorted(ids)
        log.info("Incremental: %d trận bị ảnh hưởng kể từ version events=%s state=%s stats=%s",
                 len(affected), job_state.get("events"), job_state.get("state"), job_state.get("stats"))
        if affected:
            _merge_tms(spark, team_match_summary(spark, affected))
    else:
        mode = "full"
        _overwrite(team_match_summary(spark), C.T_GOLD_TEAM_MATCH)

    tms = spark.read.format("delta").load(table_path(C.T_GOLD_TEAM_MATCH)).cache()
    stand, form, players = standings(tms), team_form(tms), player_summary(spark)
    _overwrite(stand, C.T_GOLD_STANDINGS)
    _overwrite(form, C.T_GOLD_TEAM_FORM)
    _overwrite(players, C.T_GOLD_PLAYER)
    result = {"mode": mode, "affected_matches": None if affected is None else len(affected),
              "team_match_rows": tms.count()}
    if tracking and _exists(spark, table_path(C.T_BRONZE_TRACKING)):
        t1 = time.time()
        _overwrite(player_distance(spark), C.T_GOLD_TRACKING)
        result["tracking_s"] = round(time.time() - t1, 1)
    new_state = {"events": v_events, "state": v_state}
    if v_stats is not None:
        new_state["stats"] = v_stats
    _write_job_state(spark, new_state)

    # bảo trì bảng gold (job này là nơi ghi duy nhất của gold)
    from delta.tables import DeltaTable
    try:
        DeltaTable.forPath(spark, table_path(C.T_GOLD_TEAM_MATCH)).optimize().executeCompaction()
    except Exception as exc:  # noqa: BLE001
        log.warning("OPTIMIZE gold lỗi: %s", exc)

    if es:
        try:
            result["es_standings"] = push_to_es(spark.read.format("delta").load(table_path(C.T_GOLD_STANDINGS)),
                                                "gold_standings", ["league_name", "season", "team_name"])
            result["es_form"] = push_to_es(spark.read.format("delta").load(table_path(C.T_GOLD_TEAM_FORM)),
                                           "gold_team_form", ["league_name", "season", "team_name"])
            top = spark.read.format("delta").load(table_path(C.T_GOLD_PLAYER)).orderBy(F.desc("goals"), F.desc("xg")).limit(500)
            result["es_players"] = push_to_es(top, "gold_players", ["player_name", "team_name"])
        except Exception as exc:  # noqa: BLE001
            log.warning("Đẩy gold sang Elasticsearch lỗi: %s", exc)
    result["seconds"] = round(time.time() - t0, 1)
    log.info("Gold xong: %s", json.dumps(result))
    tms.unpersist()
    spark.stop()
    return result


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", default="incremental", choices=["incremental", "full"])
    ap.add_argument("--tracking", action="store_true", help="tính thêm gold.player_distance từ dữ liệu vị trí")
    ap.add_argument("--no-es", action="store_true")
    a = ap.parse_args()
    run(a.mode, a.tracking, not a.no_es)


if __name__ == "__main__":
    main()
