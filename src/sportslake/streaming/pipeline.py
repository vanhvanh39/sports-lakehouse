"""Pipeline Spark Structured Streaming duy nhất: Kafka -> Delta Lake (bronze/silver) -> cảnh báo, Elasticsearch.

Các truy vấn streaming chạy trong cùng một ứng dụng Spark (một driver):

  bronze_raw      match.events|state|stats  -> bronze.kafka_raw (append, Delta sink đúng-một-lần)
  silver_events   match.events -> loại trùng (watermark) -> foreachBatch:
                     kiểm tra chất lượng -> DLQ | MERGE INTO silver.match_events | chỉ số kịch tính | cảnh báo
  silver_state    match.state  -> foreachBatch: kiểm tra phút không lùi -> MERGE INTO silver.match_state
  silver_stats    match.stats  -> foreachBatch: MERGE INTO silver.match_stats
  bronze_tracking match.tracking -> explode -> bronze.tracking_positions (append)
  es_*            Change Data Feed của các bảng silver -> Elasticsearch

Mọi thao tác ghi vào bảng silver (kể cả OPTIMIZE/VACUUM định kỳ) đi qua một khóa chung trong
driver: bảng nằm trên S3 (MinIO) nên Delta chỉ bảo đảm an toàn khi chỉ có MỘT driver ghi.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
import traceback

from prometheus_client import Counter, Gauge, Histogram, start_http_server
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .. import config as C
from ..analytics import excitement as X
from ..config import DATA_DIR, get_settings
from ..logutil import setup_logging
from . import transforms as T
from .spark_utils import avro_schema_json, build_spark, checkpoint, table_path
from .tables import SILVER_ALERTS, ensure_tables

log = setup_logging("pipeline")
SETTINGS = get_settings()
SILVER_LOCK = threading.RLock()
METRICS_DIR = DATA_DIR / "metrics"

M_ROWS = Counter("pipeline_rows_total", "Số bản ghi xử lý", ["query"])
M_DLQ = Counter("pipeline_dlq_total", "Số bản ghi vào dead-letter topic", ["stage", "reason"])
M_ALERTS = Counter("pipeline_alerts_total", "Số cảnh báo phát ra", ["version"])
M_MERGE = Histogram("pipeline_merge_seconds", "Thời gian một lần MERGE INTO", ["table"],
                    buckets=(0.25, 0.5, 1, 2, 3, 5, 8, 13, 21, 34))
M_BATCH = Gauge("pipeline_batch_duration_ms", "Thời gian vi lô gần nhất", ["query"])
M_INPUT = Gauge("pipeline_input_rows_per_second", "Tốc độ đầu vào", ["query"])
M_PROC = Gauge("pipeline_processed_rows_per_second", "Tốc độ xử lý", ["query"])
M_LAG = Gauge("pipeline_offsets_behind_latest", "Số offset còn tụt lại sau Kafka (lag)", ["query"])
M_E2E = Histogram("pipeline_source_to_silver_seconds", "Độ trễ từ nguồn tới lúc vi lô bắt đầu xử lý",
                  buckets=(0.5, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144))
M_EXC = Gauge("pipeline_excitement_score", "Chỉ số kịch tính hiện tại", ["match_id"])
M_ACTIVE = Gauge("pipeline_query_active", "1 nếu truy vấn streaming đang chạy", ["query"])


def _jsonl(name: str, obj: dict) -> None:
    try:
        METRICS_DIR.mkdir(parents=True, exist_ok=True)
        with (METRICS_DIR / name).open("a", encoding="utf-8") as f:
            f.write(json.dumps(obj, default=str) + "\n")
    except OSError:
        pass


def kafka_stream(spark, topics: str, max_offsets: int | None = None, starting: str = "earliest"):
    r = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", SETTINGS.kafka_bootstrap)
         .option("subscribe", topics)
         .option("startingOffsets", starting)
         .option("failOnDataLoss", "false")
         .option("includeHeaders", "false"))
    if max_offsets:
        r = r.option("maxOffsetsPerTrigger", str(max_offsets))
    return r.load()


def _sql_in_list(values: list[str]) -> str:
    """Danh sách literal an toàn cho SQL (id chỉ gồm chữ, số, '-', '_')."""
    clean = ["".join(ch for ch in str(v) if ch.isalnum() or ch in "-_.") for v in values]
    return ", ".join(f"'{v}'" for v in clean) or "''"


def write_kafka(df: DataFrame, topic: str) -> None:
    (df.write.format("kafka").option("kafka.bootstrap.servers", SETTINGS.kafka_bootstrap)
     .option("topic", topic).option("kafka.acks", "all").save())


def _merge_with_retry(fn, table: str, attempts: int = 4) -> float:
    for i in range(attempts):
        t0 = time.time()
        try:
            fn()
            dt = time.time() - t0
            M_MERGE.labels(table).observe(dt)
            return dt
        except Exception as exc:  # noqa: BLE001 - xung đột ghi đồng thời của Delta thì thử lại
            name = type(exc).__name__ + str(exc)[:300]
            if "Concurrent" in name and i < attempts - 1:
                log.warning("Xung đột ghi %s, thử lại (%d): %s", table, i + 1, name)
                time.sleep(1 + i)
                continue
            raise
    return 0.0


# ======================================================================== silver_events
class EventsProcessor:
    def __init__(self, spark):
        from delta.tables import DeltaTable

        self.spark = spark
        self.DeltaTable = DeltaTable
        self.p_events = table_path(C.T_SILVER_EVENTS)
        self.p_state = table_path(C.T_SILVER_STATE)
        self.p_exc = table_path(C.T_SILVER_EXCITEMENT)
        self.p_hist = table_path(C.T_SILVER_EXC_HISTORY)
        self.p_alerts = table_path(C.T_SILVER_ALERTS)
        self.threshold = SETTINGS.excitement_threshold
        self.versions = set(SETTINGS.excitement_versions)

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        try:
            self.process(batch_df, batch_id)
        except Exception:
            log.error("Lỗi vi lô silver_events %s:\n%s", batch_id, traceback.format_exc())
            raise

    def process(self, batch_df: DataFrame, batch_id: int) -> None:
        t0 = time.time()
        df = T.dq_events(batch_df.withColumn("processed_ts", F.current_timestamp())).persist()
        n = df.count()
        if n == 0:
            df.unpersist()
            return
        M_ROWS.labels("silver_events").inc(n)

        bad = df.where("dq_error is not null")
        n_bad = bad.count()
        if n_bad:
            for r in bad.groupBy("dq_error").count().collect():
                M_DLQ.labels("events", r["dq_error"]).inc(r["count"])
            write_kafka(T.dlq_records(bad, "silver_events"), C.TOPIC_DLQ)

        src = T.events_merge_source(df.where("dq_error is null"), batch_id).persist()
        tgt = self.DeltaTable.forPath(self.spark, self.p_events)

        match_ids = [r["match_id"] for r in src.select("match_id").distinct().collect()]
        in_list = _sql_in_list(match_ids)

        with SILVER_LOCK:
            # Thứ tự ghi được chọn để chạy lại vi lô (sau sự cố) không làm mất cảnh báo:
            #   (1) cảnh báo V1 -> silver.alerts   (2) MERGE sự kiện   (3) cảnh báo V2 -> silver.alerts
            #   (4) trạng thái bộ phát hiện V2.    Mọi alert_id đều tất định, silver.alerts MERGE theo alert_id,
            #   và việc đẩy sang Kafka do truy vấn "alerts_outbox" đọc CDF của silver.alerts đảm nhiệm.
            existing = (self.spark.read.format("delta").load(self.p_events)
                        .where(F.col("match_id").isin(match_ids)).select("event_id"))
            new_rows = src.where("op = 'UPSERT'").join(existing, "event_id", "left_anti")
            alerts = []
            if "V1" in self.versions:
                alerts = [self._v1_alert(r) for r in
                          new_rows.where(F.col("category").isin(*X.HIGHLIGHT_CATEGORIES)).collect()]
                self._save_alerts(alerts)                                              # (1)

            merge_s = _merge_with_retry(lambda: (                                      # (2)
                # điều kiện có danh sách match_id cố định -> Delta bỏ qua các file không liên quan (data skipping)
                tgt.alias("t").merge(src.alias("s"), f"t.match_id IN ({in_list}) AND t.match_id = s.match_id AND t.event_id = s.event_id")
                .whenMatchedUpdate(condition=T.RETRACT_COND, set=T.retract_set())
                .whenMatchedUpdate(condition=T.UPSERT_COND, set=T.upsert_set())
                .whenNotMatchedInsert(condition=T.INSERT_UPSERT_COND, values=T.insert_values(False))
                # RETRACT đến trước UPSERT (thứ tự đảo): ghi bia mộ để UPSERT cũ hơn không "hồi sinh" sự kiện
                .whenNotMatchedInsert(condition=T.INSERT_RETRACT_COND, values=T.insert_values(True))
                .execute()), "silver.match_events")

            if match_ids:
                v2_alerts, exc_rows, hist_rows = self._excitement(match_ids, batch_id)
                self._save_alerts(v2_alerts)                                           # (3)
                self._persist_excitement(exc_rows, hist_rows, batch_id)                # (4)
                alerts.extend(v2_alerts)
            self._consistency_check(match_ids)

        # đo độ trễ nguồn -> bắt đầu xử lý (mẫu)
        for r in src.select((F.col("processed_ts").cast("double") - F.col("source_ts").cast("double"))
                            .alias("lag")).limit(2000).collect():
            if r["lag"] is not None and r["lag"] >= 0:
                M_E2E.observe(r["lag"])
        src.unpersist()
        df.unpersist()
        _jsonl("batch_metrics.jsonl", {"query": "silver_events", "batch_id": batch_id, "rows": n, "dlq": n_bad,
                                       "merge_s": round(merge_s, 3), "alerts": len(alerts),
                                       "matches": len(match_ids), "batch_s": round(time.time() - t0, 3),
                                       "committed_at_ms": int(time.time() * 1000)})

    # ---------------------------------------------------------------- cảnh báo
    def _v1_alert(self, r) -> dict:
        msg = f"{X.V1_MESSAGES.get(r['category'], r['category'])} {r['minute']}' - {r['team_name'] or ''}"
        if r["player_name"]:
            msg += f" ({r['player_name']})"
        if r["event_detail"]:
            msg += f" [{r['event_detail']}]"
        return {"alert_id": X.alert_id(r["match_id"], "V1", r["event_id"]), "match_id": r["match_id"], "version": "V1",
                "kind": r["category"], "minute": r["minute"], "match_clock_s": r["match_clock_s"], "score": None,
                "message": msg, "triggering_event_id": r["event_id"], "source_ts": r["source_ts"],
                "ingest_ts": r["ingest_ts"], "kafka_ts": r["kafka_ts"], "processed_ts": r["processed_ts"]}

    def _excitement(self, match_ids: list[str], batch_id: int) -> tuple[list, list, list]:
        ev = (self.spark.read.format("delta").load(self.p_events)
              .where(F.col("match_id").isin(match_ids) & ~F.col("is_deleted")))
        mx = ev.groupBy("match_id").agg(F.max("match_clock_s").alias("now_clock"))
        win = (ev.join(mx, "match_id").where(F.col("match_clock_s") >= F.col("now_clock") - X.WINDOW_S)
               .where(F.col("category") != "OTHER")
               .select("match_id", "event_id", "category", "match_clock_s", "minute", "period", "xg", "outcome",
                       "now_clock", "source_ts", "ingest_ts", "kafka_ts", "processed_ts"))
        rows = win.collect()
        # trận chỉ có sự kiện OTHER trong cửa sổ vẫn cần đồng hồ hiện tại
        clocks = {r["match_id"]: r["now_clock"] for r in mx.collect()}
        state = {r["match_id"]: r for r in self.spark.read.format("delta").load(self.p_state)
                 .where(F.col("match_id").isin(match_ids)).select("match_id", "home_goals", "away_goals").collect()}
        prev = {r["match_id"]: r for r in self.spark.read.format("delta").load(self.p_exc)
                .where(F.col("match_id").isin(match_ids)).collect()}
        last_v1 = {r["match_id"]: r["c"] for r in self.spark.read.format("delta").load(self.p_alerts)
                   .where(F.col("match_id").isin(match_ids) & (F.col("version") == "V1"))
                   .groupBy("match_id").agg(F.max("match_clock_s").alias("c")).collect()}
        goals_from_events = {}
        if len(state) < len(match_ids):
            for r in (ev.where(F.col("category").isin("GOAL", "OWN_GOAL")).groupBy("match_id", "team_id").count().collect()):
                goals_from_events.setdefault(r["match_id"], []).append(r["count"])

        by_match: dict[str, list] = {}
        for r in rows:
            by_match.setdefault(r["match_id"], []).append(r)
        alerts, exc_rows, hist_rows = [], [], []
        for mid in match_ids:
            if mid not in clocks or clocks[mid] is None:
                continue
            rs = by_match.get(mid, [])
            now_clock = int(clocks[mid])
            latest = max(rs, key=lambda r: r["match_clock_s"]) if rs else None
            minute = int(latest["minute"]) if latest else now_clock // 60
            period = int(latest["period"]) if latest else 1
            if mid in state:
                h, a = int(state[mid]["home_goals"] or 0), int(state[mid]["away_goals"] or 0)
            else:
                g = sorted(goals_from_events.get(mid, []), reverse=True) + [0, 0]
                h, a = g[0], g[1]
            lite = [X.EventLite(r["category"], int(r["match_clock_s"]), r["xg"], r["outcome"], int(r["minute"]), r["event_id"])
                    for r in rs]
            score = X.score_window(lite, now_clock, minute, h, a, period)
            p = prev.get(mid)
            st = X.DetectorState(armed=bool(p["armed"]) if p is not None and p["armed"] is not None else True,
                                 last_alert_clock=int(p["last_alert_clock"]) if p is not None and p["last_alert_clock"] is not None else -10 ** 9)
            fire, st2 = X.v2_decide(st, score, now_clock, self.threshold,
                                    last_v1_clock=last_v1.get(mid) if "V1" in self.versions else None)
            exc_rows.append((mid, float(score), X.level_of(score), st2.armed, int(st2.last_alert_clock) if st2.last_alert_clock > -10 ** 8 else None,
                             now_clock, minute, h, a))
            hist_rows.append((mid, batch_id, now_clock, minute, float(score), X.level_of(score)))
            M_EXC.labels(mid).set(score)
            if fire and "V2" in self.versions and latest is not None:
                alerts.append({"alert_id": X.alert_id(mid, "V2", now_clock), "match_id": mid, "version": "V2",
                               "kind": X.level_of(score), "minute": minute, "match_clock_s": now_clock,
                               "score": float(score),
                               "message": f"🔥 Trận đang rất kịch tính (điểm {score:.1f}) phút {minute}' - tỷ số {h}-{a}",
                               "triggering_event_id": latest["event_id"], "source_ts": latest["source_ts"],
                               "ingest_ts": latest["ingest_ts"], "kafka_ts": latest["kafka_ts"],
                               "processed_ts": latest["processed_ts"]})
        return alerts, exc_rows, hist_rows

    def _persist_excitement(self, exc_rows: list, hist_rows: list, batch_id: int) -> None:
        if not exc_rows:
            return
        exc_df = self.spark.createDataFrame(
            exc_rows, "match_id string, score double, level string, armed boolean, last_alert_clock int, "
                      "last_clock int, minute int, home_goals int, away_goals int"
        ).withColumn("updated_ts", F.current_timestamp())
        tgt = self.DeltaTable.forPath(self.spark, self.p_exc)
        _merge_with_retry(lambda: (tgt.alias("t").merge(exc_df.alias("s"), "t.match_id = s.match_id")
                                   .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()),
                          "silver.match_excitement")
        hist = self.spark.createDataFrame(
            hist_rows, "match_id string, batch_id long, match_clock_s int, minute int, score double, level string"
        ).withColumn("ts", F.current_timestamp())
        # txnAppId/txnVersion: chạy lại cùng vi lô thì Delta bỏ qua lần ghi trùng
        (hist.write.format("delta").mode("append")
         .option("txnAppId", "excitement_history").option("txnVersion", batch_id).save(self.p_hist))

    def _save_alerts(self, alerts: list[dict]) -> None:
        """Lưu bền cảnh báo vào silver.alerts (MERGE theo alert_id -> ghi lại nhiều lần vẫn một bản).
        Truy vấn alerts_outbox đọc Change Data Feed của bảng này để đẩy sang Kafka match.alerts."""
        if not alerts:
            return
        schema = ("alert_id string, match_id string, version string, kind string, minute int, match_clock_s int, "
                  "score double, message string, triggering_event_id string, source_ts timestamp, "
                  "ingest_ts timestamp, kafka_ts timestamp, processed_ts timestamp")
        df = self.spark.createDataFrame([tuple(a[c.split()[0]] for c in schema.split(", ")) for a in alerts], schema) \
            .withColumn("alert_ts", F.current_timestamp())
        tgt = self.DeltaTable.forPath(self.spark, self.p_alerts)
        _merge_with_retry(lambda: (tgt.alias("t").merge(df.alias("s"), "t.alert_id = s.alert_id")
                                   .whenNotMatchedInsertAll().execute()), "silver.alerts")
        for a in alerts:
            M_ALERTS.labels(a["version"]).inc()
            log.info("CẢNH BÁO [%s] trận %s: %s", a["version"], a["match_id"], a["message"])

    def _consistency_check(self, match_ids: list[str]) -> None:
        """Quy tắc: khi trận đã kết thúc, tổng số bàn thắng (sự kiện) phải khớp tỷ số (trạng thái)."""
        if not match_ids:
            return
        st = (self.spark.read.format("delta").load(self.p_state)
              .where(F.col("match_id").isin(match_ids) & F.col("status_short").isin("FT", "AET", "PEN"))
              .select("match_id", (F.col("home_goals") + F.col("away_goals")).alias("score_goals")))
        if st.limit(1).count() == 0:
            return
        ev = (self.spark.read.format("delta").load(self.p_events)
              .where(F.col("match_id").isin(match_ids) & ~F.col("is_deleted") & F.col("category").isin("GOAL", "OWN_GOAL")
                     & (F.col("period") < 5))
              .groupBy("match_id").agg(F.count("*").alias("event_goals")))
        bad = (st.join(ev, "match_id", "left").fillna(0, ["event_goals"])
               .where("score_goals != event_goals"))
        rows = bad.collect()
        if rows:
            out = bad.select(F.col("match_id").alias("key"), F.to_json(F.struct(
                F.lit("consistency").alias("stage"), F.lit("goals_mismatch").alias("reason"),
                F.current_timestamp().cast("string").alias("detected_at"), "match_id", "score_goals", "event_goals"
            )).alias("value"))
            write_kafka(out, C.TOPIC_DLQ)
            M_DLQ.labels("consistency", "goals_mismatch").inc(len(rows))
            log.warning("Không khớp tỷ số/bàn thắng: %s", [(r["match_id"], r["score_goals"], r["event_goals"]) for r in rows])


# ======================================================================== silver_state / stats
class StateProcessor:
    def __init__(self, spark):
        from delta.tables import DeltaTable

        self.spark = spark
        self.DeltaTable = DeltaTable
        self.path = table_path(C.T_SILVER_STATE)

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        t0 = time.time()
        df = T.dq_state(batch_df.withColumn("processed_ts", F.current_timestamp())).persist()
        n = df.count()
        if n == 0:
            df.unpersist()
            return
        M_ROWS.labels("silver_state").inc(n)
        bad = df.where("dq_error is not null")
        if bad.limit(1).count():
            write_kafka(T.dlq_records(bad, "silver_state"), C.TOPIC_DLQ)
            M_DLQ.labels("state", "invalid").inc(bad.count())
        good = T.latest_per_key(df.where("dq_error is null"), ["match_id"], ["seq", "kafka_offset"])
        cols = T.STATE_COLS + ["kickoff_ts", "venue", "source_ts", "ingest_ts", "kafka_ts", "processed_ts"]
        with SILVER_LOCK:
            cur = self.spark.read.format("delta").load(self.path).select(
                "match_id", F.col("status_short").alias("cur_status"), F.col("elapsed").alias("cur_elapsed"),
                F.col("seq").alias("cur_seq"))
            j = good.join(cur, "match_id", "left")
            # Quy tắc "phút trận không lùi": cùng trạng thái mà phút nhỏ hơn bản ghi đang có -> DLQ
            regress = j.where("cur_status IS NOT NULL AND status_short = cur_status AND elapsed < cur_elapsed AND seq > cur_seq")
            n_reg = regress.count()
            if n_reg:
                write_kafka(T.dlq_records(regress.withColumn("dq_error", F.lit("minute_regression"))
                                          .drop("cur_status", "cur_elapsed", "cur_seq"), "silver_state"), C.TOPIC_DLQ)
                M_DLQ.labels("state", "minute_regression").inc(n_reg)
            src = (j.join(regress.select("match_id"), "match_id", "left_anti")
                   .select(*cols).withColumn("updated_ts", F.col("processed_ts")))
            tgt = self.DeltaTable.forPath(self.spark, self.path)
            merge_s = _merge_with_retry(lambda: (tgt.alias("t").merge(src.alias("s"), "t.match_id = s.match_id")
                                                 .whenMatchedUpdateAll(condition="s.seq > t.seq")
                                                 .whenNotMatchedInsertAll().execute()), "silver.match_state")
        df.unpersist()
        _jsonl("batch_metrics.jsonl", {"query": "silver_state", "batch_id": batch_id, "rows": n, "merge_s": round(merge_s, 3),
                                       "batch_s": round(time.time() - t0, 3), "committed_at_ms": int(time.time() * 1000)})


class StatsProcessor:
    def __init__(self, spark):
        from delta.tables import DeltaTable

        self.spark = spark
        self.DeltaTable = DeltaTable
        self.path = table_path(C.T_SILVER_STATS)

    def __call__(self, batch_df: DataFrame, batch_id: int) -> None:
        df = batch_df.where("not _corrupt AND match_id IS NOT NULL AND team_id IS NOT NULL") \
            .withColumn("processed_ts", F.current_timestamp())
        if df.limit(1).count() == 0:
            return
        src = T.latest_per_key(df, ["match_id", "team_id"], ["seq"]).select(
            *T.STATS_COLS, "source_ts", "ingest_ts", "kafka_ts", "processed_ts")
        with SILVER_LOCK:
            tgt = self.DeltaTable.forPath(self.spark, self.path)
            _merge_with_retry(lambda: (tgt.alias("t").merge(src.alias("s"), "t.match_id = s.match_id AND t.team_id = s.team_id")
                                       .whenMatchedUpdateAll(condition="s.seq > t.seq")
                                       .whenNotMatchedInsertAll().execute()), "silver.match_stats")


# ======================================================================== bảo trì & giám sát
class Maintenance(threading.Thread):
    """OPTIMIZE (gộp file nhỏ, ZORDER BY match_id) và VACUUM định kỳ, chạy trong cùng driver."""

    def __init__(self, spark, interval_min: float, retention_h: float):
        super().__init__(daemon=True, name="maintenance")
        self.spark = spark
        self.interval = interval_min * 60
        self.retention_h = retention_h

    def run(self) -> None:
        from delta.tables import DeltaTable

        if self.interval <= 0:
            return
        while True:
            time.sleep(self.interval)
            for key, zorder in ((C.T_SILVER_EVENTS, "match_id"), (C.T_SILVER_STATE, None), (C.T_SILVER_STATS, None),
                                (C.T_SILVER_EXCITEMENT, None), (C.T_SILVER_EXC_HISTORY, "match_id"),
                                (C.T_SILVER_ALERTS, None)):
                path = table_path(key)
                try:
                    with SILVER_LOCK:
                        t0 = time.time()
                        dt = DeltaTable.forPath(self.spark, path)
                        opt = dt.optimize()
                        res = opt.executeZOrderBy(zorder) if zorder else opt.executeCompaction()
                        m = res.collect()[0]["metrics"]
                        t1 = time.time()
                        dt.vacuum(self.retention_h)
                        t2 = time.time()
                    info = {"table": f"{key[0]}.{key[1]}", "optimize_s": round(t1 - t0, 2), "vacuum_s": round(t2 - t1, 2),
                            "files_removed": m["numFilesRemoved"], "files_added": m["numFilesAdded"],
                            "ts_ms": int(time.time() * 1000)}
                    log.info("Bảo trì %s", info)
                    _jsonl("maintenance.jsonl", info)
                except Exception as exc:  # noqa: BLE001
                    log.warning("Bảo trì %s lỗi: %s", path, exc)


class ProgressMonitor(threading.Thread):
    """Đọc lastProgress của mọi truy vấn -> Prometheus + data/metrics/streaming_progress.jsonl."""

    def __init__(self, spark, every_s: float = 5.0):
        super().__init__(daemon=True, name="progress-monitor")
        self.spark = spark
        self.every = every_s
        self.last: dict[str, int] = {}

    def run(self) -> None:
        while True:
            time.sleep(self.every)
            try:
                active = self.spark.streams.active
                names = {q.name for q in active}
                for n in set(self.last) | names:
                    M_ACTIVE.labels(n).set(1 if n in names else 0)
                for q in active:
                    self.last.setdefault(q.name, None)
                    p = q.lastProgress
                    if not p or self.last.get(q.name) == p.get("batchId"):
                        continue
                    self.last[q.name] = p.get("batchId")
                    lag = 0.0
                    for s in p.get("sources") or []:
                        mt = s.get("metrics") or {}
                        try:
                            lag += float(mt.get("maxOffsetsBehindLatest") or 0)
                        except (TypeError, ValueError):
                            pass
                    dur = (p.get("durationMs") or {}).get("triggerExecution")
                    M_INPUT.labels(q.name).set(p.get("inputRowsPerSecond") or 0)
                    M_PROC.labels(q.name).set(p.get("processedRowsPerSecond") or 0)
                    M_LAG.labels(q.name).set(lag)
                    if dur is not None:
                        M_BATCH.labels(q.name).set(dur)
                    _jsonl("streaming_progress.jsonl", {
                        "query": q.name, "batch_id": p.get("batchId"), "timestamp": p.get("timestamp"),
                        "num_input_rows": p.get("numInputRows"), "input_rps": p.get("inputRowsPerSecond"),
                        "processed_rps": p.get("processedRowsPerSecond"), "trigger_ms": dur,
                        "duration_ms": p.get("durationMs"), "offsets_behind": lag,
                        # số dòng sink ghi ra (sau explode của tracking: số bản ghi vị trí); -1 nếu sink không báo
                        "sink_output_rows": (p.get("sink") or {}).get("numOutputRows"),
                        "run_id": os.environ.get("RUN_ID", ""), "cores_max": os.environ.get("PIPELINE_CORES_MAX", ""),
                    })
            except Exception as exc:  # noqa: BLE001
                log.debug("progress monitor: %s", exc)


# ======================================================================== khởi động
def start(spark) -> list:
    trig = f"{int(os.environ.get('TRIGGER_EVENTS_SECONDS', '2'))} seconds"
    wm = f"{int(os.environ.get('DEDUP_WATERMARK_MINUTES', '10'))} minutes"
    max_off = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "0")) or None
    queries = []

    # ---- bronze: lưu nguyên bản mọi bản ghi (để phát lại / kiểm toán)
    raw = kafka_stream(spark, f"{C.TOPIC_EVENTS},{C.TOPIC_STATE},{C.TOPIC_STATS}", max_off)
    q = (raw.select("topic", "partition", "offset", F.col("timestamp").alias("kafka_ts"),
                    F.col("key").cast("string").alias("key"), "value",
                    F.to_date("timestamp").alias("ingest_date"))
         .writeStream.queryName("bronze_raw").format("delta").outputMode("append")
         .option("checkpointLocation", checkpoint("bronze_raw"))
         .trigger(processingTime="10 seconds").start(table_path(C.T_BRONZE_RAW)))
    queries.append(q)

    # ---- silver events
    ev_schema = avro_schema_json(C.TOPIC_EVENTS)
    ev = T.flatten_events(T.decode_avro(kafka_stream(spark, C.TOPIC_EVENTS, max_off), ev_schema))
    # khóa loại trùng; bản ghi hỏng (event_id null) dùng vị trí Kafka để không bị gộp nhầm với nhau
    ev = ev.withColumn("_dedup_key", F.coalesce(F.col("event_id"), F.concat_ws(
        "-", F.lit("corrupt"), F.col("kafka_partition").cast("string"), F.col("kafka_offset").cast("string"))))
    ev = ev.withWatermark("kafka_ts", wm).dropDuplicatesWithinWatermark(["_dedup_key", "seq", "op"]).drop("_dedup_key")
    q = (ev.writeStream.queryName("silver_events").foreachBatch(EventsProcessor(spark))
         .option("checkpointLocation", checkpoint("silver_events")).trigger(processingTime=trig).start())
    queries.append(q)

    # ---- silver state
    st = T.flatten_state(T.decode_avro(kafka_stream(spark, C.TOPIC_STATE, max_off), avro_schema_json(C.TOPIC_STATE)))
    q = (st.writeStream.queryName("silver_state").foreachBatch(StateProcessor(spark))
         .option("checkpointLocation", checkpoint("silver_state")).trigger(processingTime=trig).start())
    queries.append(q)

    # ---- silver stats
    ss = T.flatten_stats(T.decode_avro(kafka_stream(spark, C.TOPIC_STATS, max_off), avro_schema_json(C.TOPIC_STATS)))
    q = (ss.writeStream.queryName("silver_stats").foreachBatch(StatsProcessor(spark))
         .option("checkpointLocation", checkpoint("silver_stats")).trigger(processingTime="10 seconds").start())
    queries.append(q)

    # ---- outbox cảnh báo: silver.alerts (CDF, chỉ bản ghi mới) -> Kafka match.alerts (ít nhất một lần;
    #      alerter bỏ trùng theo alert_id). Nhờ vậy cảnh báo đã lưu thì chắc chắn được gửi, kể cả khi driver chết.
    alert_cols = [f.name for f in SILVER_ALERTS.fields]
    q = (spark.readStream.format("delta").option("readChangeFeed", "true").option("startingVersion", 0)
         .load(table_path(C.T_SILVER_ALERTS)).where("_change_type = 'insert'")
         .select(F.col("match_id").alias("key"), F.to_json(F.struct(*alert_cols)).alias("value"))
         .writeStream.queryName("alerts_outbox").format("kafka")
         .option("kafka.bootstrap.servers", SETTINGS.kafka_bootstrap).option("kafka.acks", "all")
         .option("topic", C.TOPIC_ALERTS).option("checkpointLocation", checkpoint("alerts_outbox"))
         .trigger(processingTime="2 seconds").start())
    queries.append(q)

    # ---- tracking (khối lượng lớn)
    if os.environ.get("ENABLE_TRACKING_STREAM", "true").lower() == "true":
        run = os.environ.get("TRACKING_RUN_ID", "default")
        trk_max = int(os.environ.get("TRACKING_MAX_OFFSETS", "0")) or None
        tr = T.tracking_positions(T.decode_avro(
            kafka_stream(spark, C.TOPIC_TRACKING, trk_max, os.environ.get("TRACKING_STARTING_OFFSETS", "earliest")),
            avro_schema_json(C.TOPIC_TRACKING)))
        q = (tr.writeStream.queryName("bronze_tracking").format("delta").outputMode("append")
             .partitionBy("match_id")
             .option("checkpointLocation", checkpoint(f"bronze_tracking_{run}"))
             .trigger(processingTime=f"{int(os.environ.get('TRIGGER_TRACKING_SECONDS', '5'))} seconds")
             .start(table_path(C.T_BRONZE_TRACKING)))
        queries.append(q)

    # ---- đồng bộ Elasticsearch từ Change Data Feed
    if os.environ.get("ENABLE_ES_SYNC", "true").lower() == "true":
        from .es_sync import start_es_sync
        queries.extend(start_es_sync(spark))
    return queries


def main() -> None:
    port = int(os.environ.get("PIPELINE_METRICS_PORT", "9108"))
    start_http_server(port)
    spark = build_spark("sports-lakehouse-pipeline")
    ensure_tables(spark)
    queries = start(spark)
    ProgressMonitor(spark).start()
    Maintenance(spark, float(os.environ.get("MAINTENANCE_INTERVAL_MIN", "30")),
                float(os.environ.get("VACUUM_RETENTION_HOURS", "168"))).start()
    log.info("Pipeline đã chạy %d truy vấn: %s | metrics :%d", len(queries), [q.name for q in queries], port)
    # Một truy vấn lỗi -> thoát tiến trình (mã khác 0) để Docker khởi động lại; checkpoint bảo đảm chạy tiếp đúng chỗ.
    spark.streams.awaitAnyTermination()
    for q in spark.streams.active:
        if q.exception():
            log.error("Truy vấn %s dừng vì lỗi: %s", q.name, q.exception())
    raise SystemExit(1)


if __name__ == "__main__":
    main()
