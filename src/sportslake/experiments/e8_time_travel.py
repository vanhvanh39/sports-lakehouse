"""Thí nghiệm E8: kiểm tra tính đúng đắn bằng time travel của Delta Lake.

So sánh bảng silver.match_events tại một phiên bản cũ (VERSION AS OF) với phiên bản hiện tại:
  * tỷ lệ bản ghi đã bị sửa sau lần ghi đầu, bị thu hồi (VAR...), được thêm mới;
  * với từng sự kiện bị sửa: giá trị lúc mới ghi (đọc từ phiên bản lúc nó được ghi lần đầu) so với giá trị cuối;
  * thời gian một truy vấn time travel.

  python -m sportslake.experiments.e8_time_travel                      # tự chọn phiên bản ở giữa lịch sử
  python -m sportslake.experiments.e8_time_travel --version 12 --match-prefix 900
  python -m sportslake.experiments.e8_time_travel --minutes-ago 30
"""
from __future__ import annotations

import argparse
import time
from collections import Counter

from .. import config as C
from ..serving.lake import cdf, delta_table, history
from .common import md_table, write_csv, write_json

COLS = ["event_id", "match_id", "seq", "is_deleted", "player_name", "event_detail", "category", "minute", "version_count"]


def load(version: int | None, prefix: str | None) -> dict[str, dict]:
    t = delta_table(C.T_SILVER_EVENTS, version).to_pyarrow_table(columns=COLS)
    return {r["event_id"]: r for r in t.to_pylist() if prefix is None or str(r["match_id"]).startswith(prefix)}


def classify(old: dict, cur: dict) -> Counter:
    c = Counter()
    for eid in set(old) | set(cur):
        o, n = old.get(eid), cur.get(eid)
        if o is None:
            c["thêm sau phiên bản cũ"] += 1
        elif n is None:
            c["biến mất (không mong đợi)"] += 1
        elif not o["is_deleted"] and n["is_deleted"]:
            c["bị thu hồi"] += 1
        elif o["is_deleted"] and not n["is_deleted"]:
            c["được khôi phục"] += 1
        elif o["seq"] != n["seq"]:
            c["bị sửa"] += 1
        else:
            c["không đổi"] += 1
    return c


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", type=int)
    ap.add_argument("--minutes-ago", type=float)
    ap.add_argument("--match-prefix")
    ap.add_argument("--examples", type=int, default=15)
    a = ap.parse_args()
    hist = sorted(history(C.T_SILVER_EVENTS), key=lambda h: h["version"])
    latest = hist[-1]["version"]
    if a.version is not None:
        v_old = a.version
    elif a.minutes_ago is not None:
        cutoff = (time.time() - a.minutes_ago * 60) * 1000
        older = [h["version"] for h in hist if h.get("timestamp", 0) <= cutoff]
        v_old = older[-1] if older else hist[0]["version"]
    else:
        v_old = hist[len(hist) // 2]["version"]

    t0 = time.time()
    old = load(v_old, a.match_prefix)
    t_tt = time.time() - t0
    t0 = time.time()
    cur = load(None, a.match_prefix)
    t_cur = time.time() - t0
    c = classify(old, cur)
    tot = sum(c.values())
    table = [{"thay đổi": k, "số bản ghi": v, "tỷ lệ %": round(100 * v / tot, 2)} for k, v in c.most_common()]
    print(f"So sánh silver.match_events VERSION AS OF {v_old} với phiên bản hiện tại {latest}")
    print(f"(đọc phiên bản cũ {t_tt:.2f}s, hiện tại {t_cur:.2f}s)\n")
    print(md_table(table))

    # Từng sự kiện bị sửa/thu hồi: giá trị lúc ghi lần đầu, đọc bằng time travel tại phiên bản ghi đầu tiên
    import pyarrow.compute as pc

    changed = [e for e, r in cur.items() if (r["version_count"] or 0) > 1]
    ch = cdf(C.T_SILVER_EVENTS, 0)
    ins = ch.filter(pc.equal(ch["_change_type"], "insert")).select(["event_id", "_commit_version"]).to_pylist()
    first_v = {}
    wanted = set(changed)
    for r in ins:
        if r["event_id"] in wanted:
            first_v[r["event_id"]] = min(first_v.get(r["event_id"], 10 ** 12), r["_commit_version"])
    examples = []
    cache: dict[int, dict] = {}
    for eid in changed[: a.examples]:
        v = first_v.get(eid)
        if v is None:
            continue
        if v not in cache:
            cache[v] = load(v, a.match_prefix)
        before, after = cache[v].get(eid), cur[eid]
        if not before:
            continue
        examples.append({"event_id": eid[:12], "match_id": after["match_id"], "phút": after["minute"],
                         "loại": after["category"], "ghi lần đầu ở version": v,
                         "cầu thủ lúc đầu": before["player_name"], "cầu thủ hiện tại": after["player_name"],
                         "bị thu hồi": after["is_deleted"], "số lần thay đổi": after["version_count"]})
    if examples:
        print("\nVí dụ sự kiện bị hiệu chỉnh (đọc lại bằng time travel):\n")
        print(md_table(examples))
    summary_row = {"version_old": v_old, "version_now": latest, "rows_compared": tot,
                   "pct_updated": round(100 * c["bị sửa"] / tot, 2) if tot else 0,
                   "pct_retracted": round(100 * c["bị thu hồi"] / tot, 2) if tot else 0,
                   "pct_added_after": round(100 * c["thêm sau phiên bản cũ"] / tot, 2) if tot else 0,
                   "events_ever_modified": len(changed), "time_travel_read_s": round(t_tt, 3)}
    write_csv("e8_time_travel.csv", [summary_row], append=True)
    write_json(f"e8_time_travel_v{v_old}.json", {"summary": summary_row, "classes": table, "examples": examples})


if __name__ == "__main__":
    main()
