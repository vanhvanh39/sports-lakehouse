"""Kiểm thử ngữ nghĩa MERGE INTO của bảng silver (cùng điều kiện/biểu thức như pipeline Spark),
chạy bằng deltalake (delta-rs) để không cần cụm Spark: luồng có trùng, sửa, thu hồi, đảo thứ tự
phải hội tụ về đúng đáp án của bộ bơm E3."""
import pyarrow as pa
import pytest

deltalake = pytest.importorskip("deltalake")

from sportslake.ingestion.inject import plan  # noqa: E402
from sportslake.streaming.merge_sql import (INSERT_RETRACT_COND, INSERT_UPSERT_COND, RETRACT_COND,  # noqa: E402
                                            UPSERT_COND, insert_values, retract_set, upsert_set)

COLS = ["event_id", "match_id", "source", "seq", "op", "category", "event_type", "event_detail", "period", "minute",
        "second", "match_clock_s", "team_id", "team_name", "player_id", "player_name", "related_player_name", "x", "y",
        "xg", "outcome", "payload", "source_ts", "ingest_ts", "kafka_ts", "processed_ts", "kafka_partition",
        "kafka_offset", "batch_id"]
TS = ("source_ts", "ingest_ts", "kafka_ts", "processed_ts")


def schema(target: bool) -> pa.Schema:
    f = []
    for c in COLS:
        t = pa.int64() if c in ("seq", "kafka_offset", "batch_id", "source_ts", "ingest_ts", "kafka_ts", "processed_ts") else \
            pa.int32() if c in ("period", "minute", "second", "match_clock_s", "kafka_partition") else \
            pa.float64() if c in ("x", "y", "xg") else pa.string()
        f.append(pa.field(c, t))
    if target:
        f += [pa.field("is_deleted", pa.bool_()), pa.field("first_seen_ts", pa.int64()), pa.field("updated_ts", pa.int64()),
              pa.field("version_count", pa.int32())]
    return pa.schema(f)


def latest_per_event(rows):
    best = {}
    for r in rows:
        k = (r["seq"], 1 if r["op"] == "RETRACT" else 0, r["kafka_offset"])
        if r["event_id"] not in best or k > best[r["event_id"]][0]:
            best[r["event_id"]] = (k, r)
    return [v[1] for v in best.values()]


def merge(path, rows):
    src = pa.Table.from_pylist(latest_per_event(rows), schema=schema(False))
    (deltalake.DeltaTable(path).merge(src, predicate="t.event_id = s.event_id", source_alias="s", target_alias="t")
     .when_matched_update(updates=retract_set(), predicate=RETRACT_COND)
     .when_matched_update(updates=upsert_set(), predicate=UPSERT_COND)
     .when_not_matched_insert(updates=insert_values(False), predicate=INSERT_UPSERT_COND)
     .when_not_matched_insert(updates=insert_values(True), predicate=INSERT_RETRACT_COND)
     .execute())


def run_stream(path, msgs, batch_size, replay_each_batch=False):
    deltalake.write_deltalake(path, pa.Table.from_pylist([], schema=schema(True)))
    offset = 0
    for i in range(0, len(msgs), batch_size):
        batch = []
        for m in msgs[i:i + batch_size]:
            r = {c: m.get(c) for c in COLS if c in m}
            r.update(kafka_ts=offset, processed_ts=i, kafka_partition=0, kafka_offset=offset, batch_id=i)
            offset += 1
            batch.append(r)
        merge(path, batch)
        if replay_each_batch:      # giả lập driver chết sau MERGE, vi lô được chạy lại từ checkpoint
            merge(path, batch)
    return {r["event_id"]: r for r in deltalake.DeltaTable(path).to_pyarrow_table().to_pylist()}


PLAN = plan("t", 120, {"dup": 0.4, "update": 0.2, "retract": 0.15, "out_of_order": 0.5, "bad": 0.0}, seed=3)


@pytest.mark.parametrize("batch_size", [3, 40])
def test_merge_converges(tmp_path, batch_size):
    mid, msgs, truth, n_bad = PLAN
    got = run_stream(str(tmp_path / "silver"), msgs, batch_size)
    assert len(got) == len(truth)
    for eid, exp in truth.items():
        assert got[eid]["is_deleted"] == exp["deleted"], eid
        if not exp["deleted"]:
            assert got[eid]["player_name"] == exp["player_name"], eid
            assert got[eid]["seq"] == exp["seq"]


def test_replayed_batches_change_nothing(tmp_path):
    """Chạy lại mỗi vi lô hai lần phải cho đúng cùng kết quả, kể cả version_count (dùng cho E8)."""
    _, msgs, _, _ = PLAN
    once = run_stream(str(tmp_path / "a"), msgs, 7)
    twice = run_stream(str(tmp_path / "b"), msgs, 7, replay_each_batch=True)
    for eid, r in once.items():
        t = twice[eid]
        assert (r["is_deleted"], r["seq"], r["player_name"], r["version_count"]) == \
               (t["is_deleted"], t["seq"], t["player_name"], t["version_count"]), eid


def test_retract_wins_same_seq_and_is_idempotent(tmp_path):
    path = str(tmp_path / "c")
    deltalake.write_deltalake(path, pa.Table.from_pylist([], schema=schema(True)))
    base = {c: None for c in COLS}
    up = dict(base, event_id="e", match_id="m", op="UPSERT", seq=5, player_name="A", kafka_offset=1, batch_id=1)
    rt = dict(up, op="RETRACT", kafka_offset=2, batch_id=2)
    merge(path, [up])
    merge(path, [rt])          # cùng seq: RETRACT thắng
    merge(path, [rt])          # áp lại: không đổi gì
    merge(path, [up])          # UPSERT cùng seq đến muộn: không hồi sinh
    r = deltalake.DeltaTable(path).to_pyarrow_table().to_pylist()[0]
    assert r["is_deleted"] is True and r["version_count"] == 2
