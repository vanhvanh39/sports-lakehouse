"""Ghi phản hồi thô của API ra tệp (jsonl.gz) để có thể phát lại (chế độ Replay)."""
from __future__ import annotations

import gzip
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator


class RawRecorder:
    def __init__(self, root: Path, source: str):
        day = datetime.now(timezone.utc).strftime("%Y%m%d")
        self.dir = root / "raw" / source / day
        self.dir.mkdir(parents=True, exist_ok=True)
        self.path = self.dir / f"session_{int(time.time())}.jsonl.gz"

    def write(self, endpoint: str, params: dict, body: dict, ts_ms: int) -> None:
        line = json.dumps({"ts": ts_ms, "endpoint": endpoint, "params": params, "body": body}, ensure_ascii=False)
        # mỗi lần ghi là một gzip member riêng -> an toàn khi tiến trình bị dừng đột ngột
        with gzip.open(self.path, "at", encoding="utf-8") as f:
            f.write(line + "\n")


def iter_recorded(paths: list[Path]) -> Iterator[dict]:
    rows = []
    for p in paths:
        try:
            with gzip.open(p, "rt", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        rows.append(json.loads(line))
        except EOFError:
            pass  # tệp đang ghi dở
    rows.sort(key=lambda r: r["ts"])
    yield from rows


def find_recordings(root: Path, source: str = "apifootball", day: str | None = None) -> list[Path]:
    base = root / "raw" / source
    if day:
        base = base / day
    return sorted(base.rglob("*.jsonl.gz"))
