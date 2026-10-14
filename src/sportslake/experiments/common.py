"""Tiện ích chung cho các script thí nghiệm: thư mục kết quả, phân vị, ghi bảng, vẽ hình."""
from __future__ import annotations

import csv
import json
import math
from datetime import datetime, timezone
from pathlib import Path

from ..config import DATA_DIR

RESULTS = DATA_DIR / "experiments" / "results"


def results_dir() -> Path:
    RESULTS.mkdir(parents=True, exist_ok=True)
    return RESULTS


def pct(values, q: float):
    v = sorted(x for x in values if x is not None and not (isinstance(x, float) and math.isnan(x)))
    if not v:
        return None
    k = (len(v) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    return v[lo] if lo == hi else v[lo] + (v[hi] - v[lo]) * (k - lo)


def summary(values) -> dict:
    v = [x for x in values if x is not None]
    if not v:
        return {"n": 0}
    return {"n": len(v), "mean": round(sum(v) / len(v), 3), "p50": round(pct(v, .5), 3), "p95": round(pct(v, .95), 3),
            "p99": round(pct(v, .99), 3), "max": round(max(v), 3)}


def write_csv(name: str, rows: list[dict], append: bool = False) -> Path:
    p = results_dir() / name
    if not rows:
        return p
    exists = p.exists() and append
    keys = list(rows[0].keys())
    if exists:
        with p.open(encoding="utf-8") as f:
            header = next(csv.reader(f), None)
        if header:
            keys = header + [k for k in keys if k not in header]
    with p.open("a" if exists else "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
        if not exists:
            w.writeheader()
        for r in rows:
            w.writerow(r)
    return p


def write_json(name: str, obj) -> Path:
    p = results_dir() / name
    p.write_text(json.dumps(obj, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return p


def md_table(rows: list[dict], cols: list[str] | None = None) -> str:
    if not rows:
        return "(không có dữ liệu)"
    cols = cols or list(rows[0].keys())
    out = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in rows:
        out.append("| " + " | ".join("" if r.get(c) is None else str(r.get(c)) for c in cols) + " |")
    return "\n".join(out)


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def plt():
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as p

    p.rcParams.update({"figure.dpi": 120, "font.size": 9, "axes.spines.top": False, "axes.spines.right": False,
                       "axes.grid": True, "grid.alpha": 0.3})
    return p


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    out.append(json.loads(line))
                except ValueError:
                    pass
    return out
