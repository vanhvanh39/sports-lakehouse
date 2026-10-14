"""Tải dữ liệu mở về thư mục data/.

  python -m sportslake.tools.download_data                       # mặc định: WC 2022 + Euro 2024 (30 trận mỗi giải) + Metrica 1,2
  python -m sportslake.tools.download_data --sb-seasons 2:27 --max-matches 0   # cả mùa Ngoại hạng Anh 2015/16 (380 trận)
  python -m sportslake.tools.download_data --metrica-only

Nguồn:
  StatsBomb Open Data  https://github.com/statsbomb/open-data   (ghi nguồn "Data provided by StatsBomb")
  Metrica Sports       https://github.com/metrica-sports/sample-data
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import time
from pathlib import Path

import requests

from ..config import DATA_DIR
from ..logutil import setup_logging

log = setup_logging("download")
SB = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
METRICA = "https://raw.githubusercontent.com/metrica-sports/sample-data/master/data"


def fetch(url: str, dest: Path, retries: int = 4) -> bool:
    if dest.exists() and dest.stat().st_size > 0:
        return False
    dest.parent.mkdir(parents=True, exist_ok=True)
    for i in range(retries):
        try:
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                tmp = dest.with_suffix(dest.suffix + ".part")
                with tmp.open("wb") as f:
                    for chunk in r.iter_content(1 << 20):
                        f.write(chunk)
                tmp.replace(dest)
            return True
        except requests.RequestException as exc:
            log.warning("Lỗi tải %s (%s), thử lại %d", url, exc, i + 1)
            time.sleep(2 * (i + 1))
    raise RuntimeError(f"Không tải được {url}")


def statsbomb(seasons: list[tuple[int, int]], max_matches: int, workers: int) -> int:
    root = DATA_DIR / "statsbomb"
    fetch(f"{SB}/competitions.json", root / "competitions.json")
    jobs = []
    for comp, season in seasons:
        mp = root / "matches" / str(comp) / f"{season}.json"
        fetch(f"{SB}/matches/{comp}/{season}.json", mp)
        matches = json.loads(mp.read_text(encoding="utf-8"))
        matches.sort(key=lambda m: (m["match_date"], m.get("kick_off") or ""))
        if max_matches:
            matches = matches[:max_matches]
        log.info("StatsBomb %s/%s: %d trận", comp, season, len(matches))
        for m in matches:
            jobs.append((f"{SB}/events/{m['match_id']}.json", root / "events" / f"{m['match_id']}.json"))
    n = 0
    with cf.ThreadPoolExecutor(workers) as ex:
        for i, new in enumerate(ex.map(lambda j: fetch(*j), jobs), start=1):
            n += int(new)
            if i % 20 == 0:
                log.info("  %d/%d file sự kiện", i, len(jobs))
    log.info("StatsBomb xong: %d file mới, tổng %d", n, len(jobs))
    return len(jobs)


def metrica(games: list[int]) -> None:
    for g in games:
        base = DATA_DIR / "metrica" / f"Sample_Game_{g}"
        for kind in ("RawTrackingData_Home_Team", "RawTrackingData_Away_Team", "RawEventsData"):
            name = f"Sample_Game_{g}_{kind}.csv"
            if fetch(f"{METRICA}/Sample_Game_{g}/{name}", base / name):
                log.info("Đã tải %s", name)
    log.info("Metrica xong: game %s", games)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sb-seasons", default="43:106,55:282",
                    help="competition:season, cách nhau dấu phẩy. 43:106=World Cup 2022, 55:282=Euro 2024, "
                         "2:27=Premier League 2015/16, 11:90=La Liga 2020/21, 9:281=Bundesliga 2023/24")
    ap.add_argument("--max-matches", type=int, default=30, help="số trận tối đa mỗi giải (0 = tất cả)")
    ap.add_argument("--metrica-games", default="1,2")
    ap.add_argument("--metrica-only", action="store_true")
    ap.add_argument("--statsbomb-only", action="store_true")
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args()
    if not a.metrica_only:
        seasons = [tuple(int(v) for v in x.split(":")) for x in a.sb_seasons.split(",") if x]
        statsbomb(seasons, a.max_matches, a.workers)
    if not a.statsbomb_only:
        metrica([int(x) for x in a.metrica_games.split(",") if x])


if __name__ == "__main__":
    main()
