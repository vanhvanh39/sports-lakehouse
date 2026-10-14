"""Công cụ Spark SQL trên lakehouse: truy vấn, lịch sử phiên bản, time travel, bảo trì thủ công.

Ví dụ (trong container jobs):
  python -m sportslake.batch.job_runner tool -- sql "SELECT * FROM silver_match_state"
  python -m sportslake.batch.job_runner tool -- history silver.match_events
  python -m sportslake.batch.job_runner tool -- time-travel silver.match_events --version 3 --match-id 900123450
"""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from .. import config as C
from ..logutil import setup_logging
from ..streaming.spark_utils import build_spark, table_path

log = setup_logging("spark-tools")

ALL = {f"{k[0]}.{k[1]}": k for k in (
    C.T_BRONZE_RAW, C.T_BRONZE_TRACKING, C.T_SILVER_EVENTS, C.T_SILVER_STATE, C.T_SILVER_STATS, C.T_SILVER_EXCITEMENT,
    C.T_SILVER_EXC_HISTORY, C.T_SILVER_ALERTS, C.T_GOLD_TEAM_MATCH, C.T_GOLD_STANDINGS, C.T_GOLD_TEAM_FORM,
    C.T_GOLD_PLAYER, C.T_GOLD_TRACKING)}


def register_views(spark) -> list[str]:
    from delta.tables import DeltaTable

    names = []
    for name, key in ALL.items():
        p = table_path(key)
        try:
            if DeltaTable.isDeltaTable(spark, p):
                v = name.replace(".", "_")
                spark.read.format("delta").load(p).createOrReplaceTempView(v)
                names.append(v)
        except Exception:  # noqa: BLE001
            pass
    return names


def main() -> None:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    q = sub.add_parser("sql")
    q.add_argument("query")
    q.add_argument("--limit", type=int, default=50)
    h = sub.add_parser("history")
    h.add_argument("table", choices=list(ALL))
    tt = sub.add_parser("time-travel", help="so sánh một phiên bản cũ với hiện tại")
    tt.add_argument("table", choices=list(ALL))
    tt.add_argument("--version", type=int)
    tt.add_argument("--timestamp")
    tt.add_argument("--match-id")
    o = sub.add_parser("optimize", help="OPTIMIZE + VACUUM thủ công (chỉ chạy khi pipeline đã dừng)")
    o.add_argument("table", choices=list(ALL))
    o.add_argument("--zorder", default=None)
    o.add_argument("--retain-hours", type=float, default=168)
    a = ap.parse_args()

    spark = build_spark(f"spark-tools-{a.cmd}")
    if a.cmd == "sql":
        log.info("Các view: %s", register_views(spark))
        spark.sql(a.query).show(a.limit, truncate=False)
    elif a.cmd == "history":
        from delta.tables import DeltaTable

        (DeltaTable.forPath(spark, table_path(ALL[a.table])).history()
         .select("version", "timestamp", "operation", "operationMetrics").show(50, truncate=80))
    elif a.cmd == "time-travel":
        p = table_path(ALL[a.table])
        clause = f"VERSION AS OF {a.version}" if a.version is not None else f"TIMESTAMP AS OF '{a.timestamp}'"
        old = spark.sql(f"SELECT * FROM delta.`{p}` {clause}")
        cur = spark.read.format("delta").load(p)
        if a.match_id:
            old, cur = old.where(F.col("match_id") == a.match_id), cur.where(F.col("match_id") == a.match_id)
        print(f"Số dòng {clause}: {old.count()} | hiện tại: {cur.count()}")
        if "event_id" in cur.columns:
            j = old.alias("o").join(cur.alias("c"), "event_id", "full_outer")
            diff = j.select("event_id",
                            F.when(F.col("o.seq").isNull(), "THÊM MỚI")
                             .when(F.col("c.seq").isNull(), "MẤT")
                             .when(~F.col("o.is_deleted") & F.col("c.is_deleted"), "BỊ THU HỒI")
                             .when(F.col("o.seq") != F.col("c.seq"), "BỊ SỬA").otherwise("KHÔNG ĐỔI").alias("change"),
                            F.coalesce("c.category", "o.category").alias("category"),
                            F.col("o.player_name").alias("player_before"), F.col("c.player_name").alias("player_now"),
                            F.coalesce("c.minute", "o.minute").alias("minute"))
            diff.groupBy("change").count().show()
            diff.where("change != 'KHÔNG ĐỔI'").orderBy("minute").show(100, truncate=False)
    elif a.cmd == "optimize":
        from delta.tables import DeltaTable

        dt = DeltaTable.forPath(spark, table_path(ALL[a.table]))
        res = dt.optimize().executeZOrderBy(a.zorder) if a.zorder else dt.optimize().executeCompaction()
        res.select("metrics.numFilesAdded", "metrics.numFilesRemoved").show()
        dt.vacuum(a.retain_hours)
    spark.stop()


if __name__ == "__main__":
    main()
