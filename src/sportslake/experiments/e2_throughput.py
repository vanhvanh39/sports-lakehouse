"""Thí nghiệm E2: thông lượng và khả năng mở rộng.

Script PowerShell scripts/experiments/e2.ps1 chạy lần lượt các cấu hình (số partition × số core
executor × tốc độ phát lại). Mỗi lần chạy: pipeline khởi động với RUN_ID riêng, dữ liệu vị trí được
phát với hậu tố id = RUN_ID (match_id = metrica-<game>-<RUN_ID>), rồi script này tổng hợp.

Hai đơn vị được đo TÁCH BIỆT:
  * khung hình (frame) / giây  = số THÔNG ĐIỆP Kafka Spark đọc vào (inputRows của truy vấn bronze_tracking)
  * bản ghi vị trí / giây       = số DÒNG Spark ghi ra bảng bronze.tracking_positions sau explode
    (đếm trực tiếp trong bảng Delta theo match_id của lần chạy; đối chiếu với sink.numOutputRows nếu có)

Tốc độ được tính theo hai cách:
  * busy  = số dòng / tổng thời gian các vi lô có dữ liệu (năng lực xử lý)
  * span  = số dòng / khoảng từ lúc vi lô đầu bắt đầu tới lúc vi lô cuối kết thúc (thông lượng thực tế)

  python -m sportslake.experiments.e2_throughput --run-id p12_c3_x200 --partitions 12 --cores 3 --speed 200
  python -m sportslake.experiments.e2_throughput --report
"""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from datetime import datetime

from .. import config as C
from ..config import DATA_DIR
from .common import md_table, plt, read_jsonl, results_dir, summary, write_csv

PROGRESS = DATA_DIR / "metrics" / "streaming_progress.jsonl"
PRODUCER = DATA_DIR / "metrics" / "tracking_producer.jsonl"


def _ts(s: str) -> float:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()


def positions_written(run_id: str, games: list[int]) -> int | None:
    try:
        import pyarrow.dataset as ds

        from ..serving.lake import delta_table

        d = delta_table(C.T_BRONZE_TRACKING).to_pyarrow_dataset()
        ids = [f"metrica-{g}-{run_id}" for g in games]
        return int(d.count_rows(filter=ds.field("match_id").isin(ids)))
    except Exception as exc:  # noqa: BLE001
        print(f"(Không đếm được bảng tracking: {exc})")
        return None


def analyze(run_id: str, query: str, games: list[int]) -> dict:
    rows = [r for r in read_jsonl(PROGRESS) if r.get("run_id") == run_id and r.get("query") == query]
    busy_rows = [r for r in rows if (r.get("num_input_rows") or 0) > 0]
    if not busy_rows:
        return {}
    frames_in = sum(r["num_input_rows"] for r in busy_rows)
    busy_s = sum((r.get("trigger_ms") or 0) for r in busy_rows) / 1000.0
    t_first = _ts(busy_rows[0]["timestamp"])
    t_last = _ts(busy_rows[-1]["timestamp"]) + (busy_rows[-1].get("trigger_ms") or 0) / 1000.0
    span_s = max(1e-6, t_last - t_first)
    sink_rows = [r.get("sink_output_rows") for r in busy_rows if (r.get("sink_output_rows") or -1) >= 0]
    positions = positions_written(run_id, games)
    trig = [r["trigger_ms"] for r in busy_rows if r.get("trigger_ms")]
    lag = [r.get("offsets_behind") or 0 for r in rows]
    out = {
        "batches": len(busy_rows),
        "frames_in": frames_in,
        "frames_per_s_busy": round(frames_in / busy_s, 1) if busy_s else None,
        "frames_per_s_span": round(frames_in / span_s, 1),
        "positions_written": positions,
        "positions_sink_reported": sum(sink_rows) if sink_rows else None,
        "positions_per_s_busy": round(positions / busy_s, 1) if positions and busy_s else None,
        "positions_per_s_span": round(positions / span_s, 1) if positions else None,
        "trigger_ms_p50": summary(trig).get("p50"), "trigger_ms_p95": summary(trig).get("p95"),
        "lag_max_frames": max(lag) if lag else None, "lag_last_frames": lag[-1] if lag else None,
        "keeps_up": (lag[-1] if lag else 0) <= max(100.0, 0.01 * frames_in),
    }
    return out


def report() -> None:
    p = results_dir() / "e2_throughput.csv"
    if not p.exists():
        raise SystemExit("Chưa có e2_throughput.csv")
    rows = list(csv.DictReader(p.open(encoding="utf-8")))
    print(md_table(rows, ["run_id", "partitions", "cores", "speed", "producer_frames_per_s", "frames_per_s_busy",
                          "positions_per_s_busy", "positions_per_s_span", "trigger_ms_p50", "lag_max_frames", "keeps_up"]))
    plot = plt()
    fig, ax = plot.subplots(figsize=(6, 3.6))
    groups = defaultdict(dict)
    for r in rows:
        try:
            c, v = int(r["cores"]), float(r["positions_per_s_busy"] or 0)
        except (TypeError, ValueError, KeyError):
            continue
        groups[r["partitions"]][c] = max(groups[r["partitions"]].get(c, 0), v)
    for parts, best in sorted(groups.items(), key=lambda x: int(x[0])):
        xs = sorted(best)
        ax.plot(xs, [best[x] for x in xs], "o-", label=f"{parts} partition")
    ax.set_xlabel("số core executor (spark.cores.max)")
    ax.set_ylabel("bản ghi vị trí ghi ra / giây (busy)")
    ax.set_title("E2 - năng lực xử lý theo cấu hình")
    ax.legend()
    fig.tight_layout()
    fig.savefig(results_dir() / "e2_throughput.png")
    print(f"Biểu đồ: {results_dir() / 'e2_throughput.png'}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id")
    ap.add_argument("--query", default="bronze_tracking")
    ap.add_argument("--games", default="1")
    ap.add_argument("--partitions", type=int)
    ap.add_argument("--cores", type=int)
    ap.add_argument("--speed", type=float)
    ap.add_argument("--report", action="store_true")
    a = ap.parse_args()
    if a.report:
        report()
        return
    games = [int(g) for g in a.games.split(",")]
    res = analyze(a.run_id, a.query, games)
    if not res:
        raise SystemExit(f"Không có tiến độ cho RUN_ID={a.run_id} (truy vấn {a.query})")
    prod = [r for r in read_jsonl(PRODUCER) if r.get("id_suffix") == a.run_id]
    last_prod = prod[-1] if prod else {}
    row = {"run_id": a.run_id, "query": a.query, "partitions": a.partitions, "cores": a.cores, "speed": a.speed,
           "producer_frames_per_s": last_prod.get("frames_per_s"),
           "producer_positions_per_s": last_prod.get("positions_per_s"),
           "producer_positions": last_prod.get("positions"), **res}
    if row["producer_positions"] and res.get("positions_written") is not None:
        row["positions_complete"] = res["positions_written"] == row["producer_positions"]
    write_csv("e2_throughput.csv", [row], append=True)
    print(md_table([row]))


if __name__ == "__main__":
    main()
