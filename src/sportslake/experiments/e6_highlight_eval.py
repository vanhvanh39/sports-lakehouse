"""Thí nghiệm E6 (RQ4): chất lượng phát hiện diễn biến nổi bật của chỉ số kịch tính.

Đánh giá offline trên dữ liệu sự kiện StatsBomb: chạy bộ phát hiện tuần tự theo thời gian như
pipeline (cùng mã nguồn analytics/excitement.py), so với nhãn "khoảnh khắc then chốt":
bàn thắng, phản lưới, thẻ đỏ, phạt đền, cơ hội lớn (cú sút xG >= 0.3).

Một cảnh báo khớp một khoảnh khắc nếu nằm trong [t - 30s, t + 120s] (mỗi khoảnh khắc khớp tối đa
một cảnh báo). Precision = cảnh báo khớp / tổng cảnh báo; Recall = khoảnh khắc được phát hiện /
tổng khoảnh khắc; độ trễ = thời điểm cảnh báo - thời điểm khoảnh khắc.

Quy trình tách tập: ngưỡng V2 được CHỌN trên tập điều chỉnh (mặc định World Cup 2022, competition 43)
theo F1 của cấu hình triển khai V1+V2, rồi kết quả chính được BÁO CÁO trên tập kiểm tra tách biệt
(mặc định Euro 2024, competition 55). Nếu không có đủ hai giải, tách 50/50 theo hash match_id.

Không có bước gộp cảnh báo riêng cho đánh giá: quy tắc "V2 không phát nếu vừa có V1 trong 60 giây"
nằm trong analytics/excitement.py và được dùng chung với pipeline.

Lưu ý khi diễn giải: nhãn được suy ra từ chính dữ liệu sự kiện nên V1 (luật trên bàn thắng/thẻ đỏ/
phạt đền) gần như luôn đúng với các loại đó; giá trị của V2 nằm ở khả năng bắt được "cơ hội lớn"
và giai đoạn dồn ép mà luật không có.

  python -m sportslake.experiments.e6_highlight_eval
  python -m sportslake.experiments.e6_highlight_eval --tune-comp 43 --test-comp 55 --thresholds 3,4,5,6,7,8
"""
from __future__ import annotations

import argparse
from collections import defaultdict

from ..analytics.excitement import EventLite, simulate_match
from ..canonical import match_clock
from ..config import DATA_DIR
from ..ingestion.statsbomb import categorize, downloaded_matches, key_moments, load_events
from .common import md_table, plt, results_dir, summary, write_csv, write_json

PRE_S, POST_S = 30, 120


def match_inputs(m: dict):
    evs = load_events(DATA_DIR, m["match_id"])
    evs.sort(key=lambda e: e.get("index", 0))
    home = m["home_team"]["home_team_id"]
    lite, timeline = [], []
    h = a = 0
    for ev in evs:
        p = int(ev.get("period") or 1)
        if p >= 5:
            continue
        cat, detail, xg, outcome = categorize(ev)
        if cat == "OTHER":
            continue
        clk = match_clock(p, ev["minute"], ev["second"])
        lite.append(EventLite(cat, clk, xg, outcome, ev["minute"], ev["id"]))
        if cat in ("GOAL", "OWN_GOAL"):
            if (ev.get("team") or {}).get("id") == home:
                h += 1
            else:
                a += 1
            timeline.append((clk, h, a))
    return lite, timeline, key_moments(evs)


def evaluate(alerts: list[dict], moments: list[dict]) -> dict:
    used = set()
    tp_alerts = 0
    lat = []
    kinds_hit = defaultdict(int)
    for al in sorted(alerts, key=lambda x: x["clock_s"]):
        best = None
        for i, mo in enumerate(moments):
            if i in used:
                continue
            d = al["clock_s"] - mo["clock_s"]
            if -PRE_S <= d <= POST_S and (best is None or abs(d) < abs(best[1])):
                best = (i, d)
        if best:
            used.add(best[0])
            tp_alerts += 1
            lat.append(best[1])
            for k in moments[best[0]]["kinds"]:
                kinds_hit[k] += 1
    return {"alerts": len(alerts), "tp": tp_alerts, "moments": len(moments), "detected": len(used),
            "latency": lat, "kinds_hit": dict(kinds_hit)}


def split_matches(matches: list[dict], tune_comp: int, test_comp: int) -> tuple[list, list, str]:
    comp = lambda m: (m.get("competition") or {}).get("competition_id")  # noqa: E731
    tune = [m for m in matches if comp(m) == tune_comp]
    test = [m for m in matches if comp(m) == test_comp]
    if tune and test:
        return tune, test, f"tune = competition {tune_comp}, test = competition {test_comp}"
    import hashlib

    h = lambda m: int(hashlib.sha1(str(m["match_id"]).encode()).hexdigest(), 16) % 2  # noqa: E731
    return ([m for m in matches if h(m) == 0], [m for m in matches if h(m) == 1],
            "tách 50/50 theo hash match_id (không đủ hai giải)")


def run(inputs, versions, thr) -> dict:
    tot = {"alerts": 0, "tp": 0, "moments": 0, "detected": 0}
    lat, kinds, kinds_total = [], defaultdict(int), defaultdict(int)
    for lite, tl, moments in inputs:
        for mo in moments:
            for k in mo["kinds"]:
                kinds_total[k] += 1
        r = evaluate(simulate_match(lite, tl, thr, versions=versions), moments)
        for k in tot:
            tot[k] += r[k]
        lat += r["latency"]
        for k, v in r["kinds_hit"].items():
            kinds[k] += v
    p = tot["tp"] / tot["alerts"] if tot["alerts"] else 0.0
    rc = tot["detected"] / tot["moments"] if tot["moments"] else 0.0
    f1 = 2 * p * rc / (p + rc) if p + rc else 0.0
    ls = summary(lat)
    return {"version": "+".join(versions), "threshold": thr if "V2" in versions else None,
            "matches": len(inputs), "alerts": tot["alerts"], "alerts_per_match": round(tot["alerts"] / max(1, len(inputs)), 2),
            "moments": tot["moments"], "precision": round(p, 3), "recall": round(rc, 3), "f1": round(f1, 3),
            "latency_p50_s": ls.get("p50"), "latency_p95_s": ls.get("p95"),
            **{f"recall_{k}": round(kinds[k] / kinds_total[k], 3) if kinds_total[k] else None
               for k in ("GOAL", "OWN_GOAL", "RED_CARD", "PENALTY", "BIG_CHANCE")}}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--thresholds", default="3,4,5,6,7,8,9,10,12")
    ap.add_argument("--tune-comp", type=int, default=43, help="giải dùng để chọn ngưỡng (43 = World Cup)")
    ap.add_argument("--test-comp", type=int, default=55, help="giải dùng để báo cáo (55 = Euro)")
    ap.add_argument("--max-matches", type=int, default=0)
    a = ap.parse_args()
    matches = downloaded_matches(DATA_DIR)[: a.max_matches or None]
    if not matches:
        raise SystemExit("Chưa có dữ liệu StatsBomb. Chạy: python -m sportslake.tools.download_data")
    tune_m, test_m, how = split_matches(matches, a.tune_comp, a.test_comp)
    tune, test = [match_inputs(m) for m in tune_m], [match_inputs(m) for m in test_m]
    thresholds = [float(t) for t in a.thresholds.split(",")]

    # 1) chọn ngưỡng trên tập điều chỉnh theo F1 của cấu hình triển khai (V1+V2)
    sweep_tune = [{**run(tune, ("V1", "V2"), t), "set": "tune"} for t in thresholds]
    sweep_tune += [{**run(tune, ("V2",), t), "set": "tune"} for t in thresholds]
    best = max((r for r in sweep_tune if r["version"] == "V1+V2"), key=lambda r: (r["f1"], -r["threshold"]))
    thr = best["threshold"]

    # 2) báo cáo trên tập kiểm tra (chưa từng dùng để chọn tham số)
    main_rows = [{**run(test, ("V1",), thr), "set": "test"}, {**run(test, ("V2",), thr), "set": "test"},
                 {**run(test, ("V1", "V2"), thr), "set": "test"}]
    sweep_test = [{**run(test, ("V2",), t), "set": "test"} for t in thresholds]  # chỉ để tham khảo đường PR

    write_csv("e6_highlight_main.csv", main_rows)
    write_csv("e6_highlight_v2_sweep.csv", sweep_tune + sweep_test)
    write_json("e6_highlight.json", {"split": how, "tune_matches": len(tune), "test_matches": len(test),
                                     "chosen_threshold": thr, "chosen_on": "tune, F1 của V1+V2",
                                     "main_test": main_rows, "sweep": sweep_tune + sweep_test})
    cols = ["set", "version", "threshold", "matches", "alerts_per_match", "precision", "recall", "f1",
            "latency_p50_s", "recall_GOAL", "recall_RED_CARD", "recall_PENALTY", "recall_BIG_CHANCE"]
    print(f"Tách tập: {how} | tune {len(tune)} trận, test {len(test)} trận")
    print(f"Ngưỡng V2 chọn trên tập tune (F1 của V1+V2 cao nhất): {thr}\n")
    print("KẾT QUẢ TRÊN TẬP TEST:\n" + md_table(main_rows, cols))
    print("\nQuét ngưỡng trên tập tune:\n" + md_table([r for r in sweep_tune if r["version"] == "V1+V2"], cols))

    p = plt()
    fig, ax = p.subplots(figsize=(4.8, 3.7))
    st = [r for r in sweep_test if r["version"] == "V2"]
    ax.plot([r["recall"] for r in st], [r["precision"] for r in st], "o-", label="V2 (test, quét ngưỡng)")
    for r in st:
        ax.annotate(str(r["threshold"]), (r["recall"], r["precision"]), fontsize=7, xytext=(3, 3), textcoords="offset points")
    for r, mk in ((main_rows[0], "s"), (main_rows[2], "^")):
        ax.plot([r["recall"]], [r["precision"]], mk, markersize=8, label=f"{r['version']} (test, ngưỡng {thr})")
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_title("E6 - Precision/Recall trên tập test")
    ax.legend(fontsize=7)
    fig.tight_layout()
    fig.savefig(results_dir() / "e6_precision_recall.png")
    print(f"\nKết quả: {results_dir()}")


if __name__ == "__main__":
    main()
