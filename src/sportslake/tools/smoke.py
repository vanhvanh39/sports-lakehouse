"""Kiểm tra nhanh toàn bộ luồng sau khi khởi động cụm (".\\manage.ps1 smoke").

Kiểm tra lần lượt hạ tầng (Kafka, Schema Registry, MinIO, Elasticsearch), pipeline (các truy vấn
streaming đang có tiến độ), dữ liệu (bảng silver có dữ liệu, cảnh báo đã tới Kafka/ES) và TÍNH ĐÚNG
(các trận Mock đã kết thúc khớp đáp án /_admin/truth: sự kiện cuối cùng, tỷ số, VAR, sửa tên).

  python -m sportslake.tools.smoke                  # kiểm tra ngay
  python -m sportslake.tools.smoke --wait-mock 20   # chờ tối đa 20 phút cho các trận Mock kết thúc
"""
from __future__ import annotations

import argparse
import time

import requests

from .. import config as C
from ..config import get_settings
from ..experiments.common import md_table

RESULTS: list[dict] = []


def check(name: str, fn) -> bool:
    try:
        ok, detail = fn()
    except Exception as exc:  # noqa: BLE001
        ok, detail = False, f"{type(exc).__name__}: {exc}"[:200]
    RESULTS.append({"kiểm tra": name, "kết quả": "ĐẠT" if ok else "LỖI", "chi tiết": detail})
    print(f"[{'ĐẠT' if ok else 'LỖI'}] {name}: {detail}")
    return ok


def kafka():
    from confluent_kafka.admin import AdminClient

    md = AdminClient({"bootstrap.servers": get_settings().kafka_bootstrap}).list_topics(timeout=15)
    missing = [t for t in C.TOPIC_SPECS if t not in md.topics]
    return (len(md.brokers) == 3 and not missing), f"{len(md.brokers)} broker, thiếu topic: {missing or 'không'}"


def schema_registry():
    subs = requests.get(f"{get_settings().schema_registry_url}/subjects", timeout=10).json()
    need = [f"{t}-value" for t in C.AVRO_TOPICS]
    miss = [x for x in need if x not in subs]
    return not miss, f"{len(subs)} subject, thiếu: {miss or 'không'}"


def es():
    s = get_settings()
    out = {}
    for idx in ("match_events", "match_state", "excitement", "alerts"):
        r = requests.get(f"{s.es_url}/{idx}/_count", timeout=10)
        out[idx] = r.json().get("count") if r.status_code == 200 else None
    return all(v is not None for v in out.values()), str(out)


EXPECTED_QUERIES = {"bronze_raw", "silver_events", "silver_state", "silver_stats", "alerts_outbox",
                    "bronze_tracking", "es_events", "es_state", "es_excitement"}


def pipeline_queries():
    import re

    txt = requests.get("http://spark-pipeline:9108/metrics", timeout=10).text
    active = {m.group(1) for m in re.finditer(r'pipeline_query_active\{query="([^"]+)"\} 1\.0', txt)}
    expected = set(EXPECTED_QUERIES)
    if "es_events" not in active and "es_state" not in active:
        expected -= {"es_events", "es_state", "es_excitement"}   # ENABLE_ES_SYNC=false
    if "bronze_tracking" not in active:
        expected.discard("bronze_tracking")                      # ENABLE_TRACKING_STREAM=false
    miss = expected - active
    return not miss, f"{len(active)} truy vấn đang chạy; thiếu: {sorted(miss) or 'không'}"


def metrics_endpoint():
    r = requests.get("http://spark-pipeline:9108/metrics", timeout=10)
    return r.status_code == 200 and "pipeline_rows_total" in r.text, f"HTTP {r.status_code}"


def silver_tables():
    from ..serving.lake import delta_table

    counts = {}
    for key in (C.T_SILVER_EVENTS, C.T_SILVER_STATE, C.T_BRONZE_RAW):
        counts[f"{key[0]}.{key[1]}"] = delta_table(key).to_pyarrow_dataset().count_rows()
    return all(v > 0 for v in counts.values()), str(counts)


def alerts_delivered():
    """Mọi cảnh báo trong silver.alerts phải tới được Elasticsearch (qua outbox -> Kafka -> alerter)."""
    from ..serving.lake import delta_table

    ids = set(delta_table(C.T_SILVER_ALERTS).to_pyarrow_table(columns=["alert_id"]).column("alert_id").to_pylist())
    if not ids:
        return True, "chưa có cảnh báo nào"
    s = get_settings()
    found = set()
    lst = list(ids)
    for i in range(0, len(lst), 500):
        r = requests.post(f"{s.es_url}/alerts/_mget", json={"ids": lst[i:i + 500]}, timeout=20).json()
        found |= {d["_id"] for d in r.get("docs", []) if d.get("found")}
    missing = ids - found
    return not missing, f"{len(ids)} cảnh báo trong silver, {len(found)} có trong ES, thiếu {len(missing)}"


def pipeline_speed():
    """Thời gian xử lý các vi lô silver_events gần nhất (để biết pipeline có theo kịp không)."""
    from ..config import DATA_DIR
    from ..experiments.common import read_jsonl, summary

    rows = [r for r in read_jsonl(DATA_DIR / "metrics" / "batch_metrics.jsonl") if r.get("query") == "silver_events"][-30:]
    if not rows:
        return False, "chưa có vi lô silver_events nào ghi xong (pipeline chưa xử lý được dữ liệu)"
    b = summary([r["batch_s"] for r in rows])
    m = summary([r["merge_s"] for r in rows])
    return True, (f"{len(rows)} vi lô gần nhất: thời gian vi lô p50={b['p50']}s max={b['max']}s, "
                  f"MERGE p50={m['p50']}s")


def mock_truth(wait_min: float, settle_min: float):
    from ..experiments.e3_verify import verify_mock

    s = get_settings()
    deadline = time.time() + wait_min * 60
    while True:
        sc = requests.get(f"{s.mock_api_url}/_admin/scenarios", timeout=10).json()["scenarios"]
        if all(x["status"] == "FT" for x in sc) or time.time() > deadline:
            break
        left = max(x["ends_in_s"] for x in sc)
        print(f"  ... chờ các trận Mock kết thúc (còn ~{left:.0f}s)")
        time.sleep(min(60, max(10, left + 5)))
    # Trận kết thúc ở Mock chưa có nghĩa là đã vào bảng silver: collector cần một lượt gọi để thấy trận rời
    # danh sách live và chốt kết quả, rồi pipeline cần vài vi lô để ghi. Kiểm tra lặp lại tới khi khớp
    # hoặc hết thời gian chờ (--settle-min).
    settle_end = time.time() + settle_min * 60
    while True:
        rows = verify_mock()
        bad = [r for r in rows if not r["correct"]]
        if rows and not bad:
            break
        if time.time() > settle_end:
            break
        print(f"  ... {len(rows) - len(bad)}/{len(rows)} trận đã khớp, chờ collector chốt trận và pipeline ghi xong")
        time.sleep(20)
    if not rows:
        return False, "chưa có trận Mock nào kết thúc (thử --wait-mock 20)"
    if bad:
        print(md_table(bad))
    return not bad, f"{len(rows) - len(bad)}/{len(rows)} trận khớp đáp án (sự kiện cuối, tỷ số)"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wait-mock", type=float, default=0, help="số phút tối đa chờ các trận Mock kết thúc")
    ap.add_argument("--settle-min", type=float, default=6,
                    help="sau khi trận kết thúc, số phút tối đa chờ dữ liệu vào silver trước khi kết luận")
    a = ap.parse_args()
    check("Kafka 3 broker + đủ topic", kafka)
    check("Schema Registry có lược đồ", schema_registry)
    check("Elasticsearch có các index", es)
    check("Pipeline xuất metrics Prometheus", metrics_endpoint)
    check("Các truy vấn streaming đang chạy", pipeline_queries)
    check("Bảng bronze/silver có dữ liệu", silver_tables)
    check("Tốc độ xử lý vi lô silver_events", pipeline_speed)
    check("Cảnh báo đã tới Elasticsearch", alerts_delivered)
    check("Trận Mock khớp đáp án", lambda: mock_truth(a.wait_mock, a.settle_min))
    print("\n" + md_table(RESULTS))
    n_bad = sum(1 for r in RESULTS if r["kết quả"] != "ĐẠT")
    print(f"\n{len(RESULTS) - n_bad}/{len(RESULTS)} mục đạt")
    raise SystemExit(1 if n_bad else 0)


if __name__ == "__main__":
    main()