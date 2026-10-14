"""Đọc bảng Delta trên MinIO từ Python thuần (thư viện deltalake / delta-rs), không cần Spark.

Dùng cho truy vấn ad-hoc bằng DuckDB và cho các script phân tích thí nghiệm.
"""
from __future__ import annotations

import os

from .. import config as C
from ..config import get_settings

# Cho phép đọc bảng Delta trên ổ đĩa cục bộ (kiểm thử): LAKE_LOCAL_ROOT=/duong/dan
LOCAL_ROOT = os.environ.get("LAKE_LOCAL_ROOT")

TABLES = {f"{k[0]}_{k[1]}": k for k in (
    C.T_BRONZE_RAW, C.T_BRONZE_TRACKING, C.T_SILVER_EVENTS, C.T_SILVER_STATE, C.T_SILVER_STATS, C.T_SILVER_EXCITEMENT,
    C.T_SILVER_EXC_HISTORY, C.T_SILVER_ALERTS, C.T_GOLD_TEAM_MATCH, C.T_GOLD_STANDINGS, C.T_GOLD_TEAM_FORM,
    C.T_GOLD_PLAYER, C.T_GOLD_TRACKING)}


def s3_uri(key: tuple[str, str]) -> str:
    if LOCAL_ROOT:
        return f"{LOCAL_ROOT.rstrip('/')}/{key[0]}/{key[1]}"
    return get_settings().table_path(*key, scheme="s3")


def delta_table(key: tuple[str, str], version: int | None = None):
    from deltalake import DeltaTable

    s = get_settings()
    dt = DeltaTable(s3_uri(key), storage_options=None if LOCAL_ROOT else s.delta_storage_options())
    if version is not None:
        dt.load_as_version(version)
    return dt


def exists(key: tuple[str, str]) -> bool:
    try:
        delta_table(key)
        return True
    except Exception:  # noqa: BLE001
        return False


def to_arrow(key: tuple[str, str], columns: list[str] | None = None, version: int | None = None):
    return delta_table(key, version).to_pyarrow_table(columns=columns)


def to_pandas(key: tuple[str, str], columns: list[str] | None = None, version: int | None = None):
    return to_arrow(key, columns, version).to_pandas()


def history(key: tuple[str, str]) -> list[dict]:
    return delta_table(key).history()


def cdf(key: tuple[str, str], starting_version: int = 0, columns: list[str] | None = None):
    """Change Data Feed dạng pyarrow.Table (gồm _change_type, _commit_version, _commit_timestamp)."""
    import pyarrow as pa

    reader = delta_table(key).load_cdf(starting_version=starting_version, columns=columns)
    if isinstance(reader, pa.Table):
        t = reader
    elif isinstance(reader, pa.RecordBatchReader):
        t = reader.read_all()
    else:
        # deltalake >= 1.0 trả về đối tượng arro3 -> chuyển sang pyarrow qua giao diện Arrow C stream
        t = pa.RecordBatchReader.from_stream(reader).read_all()
    return _plain_strings(t)


def _plain_strings(t):
    """Đổi string_view -> string để các hàm pyarrow.compute dùng được."""
    import pyarrow as pa

    is_view = getattr(pa.types, "is_string_view", lambda _t: False)
    if not any(is_view(f.type) for f in t.schema):
        return t
    return t.cast(pa.schema([pa.field(f.name, pa.string() if is_view(f.type) else f.type) for f in t.schema]))


def duckdb_connection(tables: list[str] | None = None, version: dict | None = None):
    """Kết nối DuckDB có sẵn view cho mọi bảng đang tồn tại (tên view: silver_match_events, gold_standings, ...)."""
    import duckdb

    con = duckdb.connect()
    for name, key in TABLES.items():
        if tables and name not in tables:
            continue
        try:
            ds = delta_table(key, (version or {}).get(name)).to_pyarrow_dataset()
        except Exception:  # noqa: BLE001
            continue
        con.register(name, ds)
    return con
