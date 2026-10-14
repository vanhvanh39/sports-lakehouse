"""Biểu thức SQL của câu lệnh MERGE INTO bảng silver.match_events (không phụ thuộc pyspark,
dùng chung cho pipeline Spark và cho kiểm thử bằng delta-rs).

Quy tắc:
  khớp + RETRACT + (seq > seq hiện có, hoặc cùng seq mà chưa bị xóa)
                                       -> đánh dấu is_deleted (xóa logic / "bia mộ")
                                          (RETRACT cùng seq thắng UPSERT; áp lại RETRACT đã áp dụng = không đổi gì)
  khớp + UPSERT  + seq >  seq hiện có  -> cập nhật, giữ first_seen_ts, tăng version_count
  không khớp + UPSERT                  -> thêm mới
  không khớp + RETRACT                 -> thêm bia mộ (RETRACT đến trước UPSERT do đảo thứ tự)
Bản ghi đến muộn có seq nhỏ hơn không thể ghi đè hay "hồi sinh" sự kiện đã thu hồi.
"""

# Điều kiện của các mệnh đề WHEN MATCHED (dùng chung cho Spark và kiểm thử delta-rs)
RETRACT_COND = "s.op = 'RETRACT' AND (s.seq > t.seq OR (s.seq = t.seq AND NOT t.is_deleted))"
UPSERT_COND = "s.op = 'UPSERT' AND s.seq > t.seq"
INSERT_UPSERT_COND = "s.op = 'UPSERT'"
INSERT_RETRACT_COND = "s.op = 'RETRACT'"

EVENT_COLS = ["event_id", "match_id", "source", "seq", "op", "category", "event_type", "event_detail", "period",
              "minute", "second", "match_clock_s", "team_id", "team_name", "player_id", "player_name",
              "related_player_name", "x", "y", "xg", "outcome", "payload"]


# Cột được cập nhật khi UPSERT khớp (giữ nguyên first_seen_ts)
def upsert_set() -> dict:
    s = {c: f"s.{c}" for c in EVENT_COLS if c != "event_id"}
    s.update({"source_ts": "s.source_ts", "ingest_ts": "s.ingest_ts", "kafka_ts": "s.kafka_ts",
              "processed_ts": "s.processed_ts", "updated_ts": "s.processed_ts", "is_deleted": "false",
              "version_count": "t.version_count + 1", "batch_id": "s.batch_id",
              "kafka_partition": "s.kafka_partition", "kafka_offset": "s.kafka_offset"})
    return s


def retract_set() -> dict:
    return {"is_deleted": "true", "op": "s.op", "seq": "s.seq", "updated_ts": "s.processed_ts",
            "processed_ts": "s.processed_ts", "kafka_ts": "s.kafka_ts", "ingest_ts": "s.ingest_ts",
            "version_count": "t.version_count + 1", "batch_id": "s.batch_id",
            "kafka_partition": "s.kafka_partition", "kafka_offset": "s.kafka_offset"}


def insert_values(deleted: bool) -> dict:
    v = {c: f"s.{c}" for c in EVENT_COLS}
    v.update({"source_ts": "s.source_ts", "ingest_ts": "s.ingest_ts", "kafka_ts": "s.kafka_ts",
              "processed_ts": "s.processed_ts", "first_seen_ts": "s.processed_ts", "updated_ts": "s.processed_ts",
              "is_deleted": "true" if deleted else "false", "version_count": "1", "batch_id": "s.batch_id",
              "kafka_partition": "s.kafka_partition", "kafka_offset": "s.kafka_offset"})
    return v


