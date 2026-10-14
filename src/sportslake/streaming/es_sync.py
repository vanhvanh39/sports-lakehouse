"""Đồng bộ bảng silver sang Elasticsearch bằng Change Data Feed (CDF) của Delta Lake.

Mỗi bảng có một truy vấn streaming đọc CDF; foreachBatch lấy phiên bản mới nhất của mỗi khóa
trong vi lô rồi ghi bulk vào Elasticsearch với _id = khóa chính -> ghi lại nhiều lần vẫn đúng.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timezone

from .. import config as C
from ..config import get_settings
from .spark_utils import checkpoint, table_path

log = logging.getLogger(__name__)

INDEX_EVENTS = "match_events"
INDEX_STATE = "match_state"
INDEX_EXCITEMENT = "excitement"


def _es():
    from elasticsearch import Elasticsearch

    return Elasticsearch(get_settings().es_url, request_timeout=60, retry_on_timeout=True, max_retries=5)


def _clean(v):
    if isinstance(v, datetime):
        if v.tzinfo is None:
            v = v.replace(tzinfo=timezone.utc)
        return v.isoformat()
    if isinstance(v, date):
        return v.isoformat()
    return v


class EsSyncError(RuntimeError):
    pass


def _bulk(es, index: str, rows, id_fn, enrich=None) -> int:
    """Ghi bulk với _id ổn định (ghi lại nhiều lần vẫn là một tài liệu).

    * Lỗi tạm thời (429, 5xx, mất kết nối) còn sót sau khi thử lại -> ném lỗi để vi lô thất bại;
      Spark sẽ chạy lại vi lô từ checkpoint nên không mất tài liệu.
    * Lỗi vĩnh viễn của từng tài liệu (4xx, vd. sai kiểu dữ liệu) -> ghi ra data/metrics/es_rejected.jsonl
      và bỏ qua, tránh để một bản ghi hỏng chặn cả pipeline.
    """
    from elasticsearch.helpers import streaming_bulk

    now = datetime.now(timezone.utc).isoformat()

    def actions():
        for r in rows:
            doc = {k: _clean(v) for k, v in r.asDict().items() if not k.startswith("_")}
            doc["es_indexed_ts"] = now
            if enrich:
                enrich(doc)
            yield {"_index": index, "_id": id_fn(doc), "_source": doc}

    ok, transient, rejected = 0, [], []
    for success, info in streaming_bulk(es, actions(), chunk_size=2000, raise_on_error=False,
                                        raise_on_exception=False, max_retries=5, initial_backoff=1):
        if success:
            ok += 1
            continue
        item = next(iter(info.values())) if isinstance(info, dict) and info else {}
        try:
            status = int(item.get("status", 0) or 0)
        except (TypeError, ValueError):
            status = 0  # lỗi kết nối: không có mã HTTP -> coi là tạm thời
        (transient if status in (0, 429) or status >= 500 else rejected).append(item)
    if rejected:
        try:
            from ..config import DATA_DIR

            (DATA_DIR / "metrics").mkdir(parents=True, exist_ok=True)
            with (DATA_DIR / "metrics" / "es_rejected.jsonl").open("a", encoding="utf-8") as f:
                for it in rejected:
                    f.write(json.dumps({"index": index, "id": it.get("_id"), "status": it.get("status"),
                                        "error": it.get("error")}, default=str) + "\n")
        except OSError:
            pass
        log.warning("ES %s: %d tài liệu bị từ chối (4xx), xem data/metrics/es_rejected.jsonl", index, len(rejected))
    if transient:
        raise EsSyncError(f"ES {index}: {len(transient)} tài liệu lỗi tạm thời, vi lô sẽ được chạy lại: {transient[:2]}")
    return ok


def _latest_changes(df, key_cols: list[str]):
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    w = Window.partitionBy(*key_cols).orderBy(F.col("_commit_version").desc())
    return (df.where(F.col("_change_type").isin("insert", "update_postimage"))
            .withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn"))


def _events_batch(df, batch_id):
    rows = _latest_changes(df, ["event_id"]).drop("payload", "_change_type", "_commit_timestamp")
    es = _es()
    n = _bulk(es, INDEX_EVENTS, rows.toLocalIterator(), lambda d: d["event_id"])
    if n:
        log.info("ES %s: %d tài liệu (batch %s)", INDEX_EVENTS, n, batch_id)


def _state_label(doc: dict) -> None:
    doc["match_label"] = f"{doc.get('home_team')} {doc.get('home_goals')}-{doc.get('away_goals')} {doc.get('away_team')}"


def _state_batch(df, batch_id):
    rows = _latest_changes(df, ["match_id"]).drop("_change_type", "_commit_timestamp")
    _bulk(_es(), INDEX_STATE, rows.toLocalIterator(), lambda d: d["match_id"], _state_label)


def _excitement_batch(df, batch_id):
    from pyspark.sql import functions as F

    # nhãn trận để vẽ biểu đồ theo tên trận thay vì id
    spark = df.sparkSession
    st = spark.read.format("delta").load(table_path(C.T_SILVER_STATE)).select(
        "match_id", F.concat_ws(" vs ", "home_team", "away_team").alias("match_label"))
    rows = df.join(st, "match_id", "left").withColumn("match_label", F.coalesce("match_label", "match_id"))
    # _id = trận + vi lô: mỗi vi lô ghi đúng một điểm cho mỗi trận -> chạy lại không sinh bản sao
    _bulk(_es(), INDEX_EXCITEMENT, rows.toLocalIterator(), lambda d: f"{d['match_id']}|{d['batch_id']}")


def start_es_sync(spark) -> list:
    trig = f"{int(os.environ.get('ES_SYNC_TRIGGER_SECONDS', '5'))} seconds"
    out = []
    for name, key, fn, cdf in (("es_events", C.T_SILVER_EVENTS, _events_batch, True),
                               ("es_state", C.T_SILVER_STATE, _state_batch, True),
                               ("es_excitement", C.T_SILVER_EXC_HISTORY, _excitement_batch, False)):
        r = spark.readStream.format("delta")
        if cdf:
            r = r.option("readChangeFeed", "true").option("startingVersion", 0)
        r = r.option("maxFilesPerTrigger", 200)
        q = (r.load(table_path(key)).writeStream.queryName(name).foreachBatch(fn)
             .option("checkpointLocation", checkpoint(name)).trigger(processingTime=trig).start())
        out.append(q)
    return out
