"""Phát lại dữ liệu theo dõi vị trí 25 Hz (Metrica Sports sample data) vào topic match.tracking.

Mỗi thông điệp Kafka là một khung hình (vị trí bóng + ~22 cầu thủ). Một trận ~145.000 khung hình
~ 3,3 triệu bản ghi vị trí sau khi Spark "explode". Dùng cho thí nghiệm thông lượng E2.

  python -m sportslake.ingestion.replay_tracking --game 1 --speed 100
  python -m sportslake.ingestion.replay_tracking --game 1,2 --speed 1000 --workers 4 --repeat 2
  python -m sportslake.ingestion.replay_tracking --game 1 --max-rate 0     # nhanh nhất có thể
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import multiprocessing as mp
import time
from pathlib import Path

from ..config import DATA_DIR, TOPIC_TRACKING, get_settings
from ..kafka_io import AvroEncoder, DeliveryTracker, ensure_topics, make_producer
from ..logutil import setup_logging

log = setup_logging("replay-tracking")
FPS = 25.0


def metrica_paths(game: int) -> tuple[Path, Path]:
    base = DATA_DIR / "metrica" / f"Sample_Game_{game}"
    return (base / f"Sample_Game_{game}_RawTrackingData_Home_Team.csv",
            base / f"Sample_Game_{game}_RawTrackingData_Away_Team.csv")


def _num(v: str):
    if v is None or v == "" or v.lower() == "nan":
        return None
    try:
        f = float(v)
        return None if math.isnan(f) else f
    except ValueError:
        return None


def _header(reader) -> list[tuple[str, int]]:
    """Trả về [(tên cầu thủ, chỉ số cột x)] và chỉ số cột bóng (tên 'ball')."""
    next(reader)  # dòng 1: tên đội
    next(reader)  # dòng 2: số áo
    h = next(reader)  # dòng 3: Period, Frame, Time [s], Player.., , Ball,
    cols = []
    for i, name in enumerate(h):
        if i >= 3 and name:
            cols.append((name.strip(), i))
    return cols


def iter_frames(game: int):
    """Sinh (period, frame, time_s, ball_x, ball_y, players[]) từ hai file Home/Away đọc song song."""
    hp, ap = metrica_paths(game)
    with hp.open(newline="", encoding="utf-8") as fh, ap.open(newline="", encoding="utf-8") as fa:
        rh, ra = csv.reader(fh), csv.reader(fa)
        ch, ca = _header(rh), _header(ra)
        for row_h, row_a in zip(rh, ra):
            if not row_h or not row_h[0]:
                continue
            period, frame, t = int(row_h[0]), int(row_h[1]), float(row_h[2])
            players = []
            bx = by = None
            for team, cols, row in (("home", ch, row_h), ("away", ca, row_a)):
                for name, i in cols:
                    x, y = _num(row[i]) if i < len(row) else None, _num(row[i + 1]) if i + 1 < len(row) else None
                    if name.lower() == "ball":
                        if bx is None:
                            bx, by = x, y
                        continue
                    if x is None and y is None:
                        continue  # cầu thủ không có mặt trên sân
                    players.append({"team": team, "player": f"{team}_{name}", "x": x, "y": y})
            yield period, frame, t, bx, by, players


def worker(wid: int, nworkers: int, games: list[int], speed: float, repeat: int, suffix: str, max_rate: float,
           q: "mp.Queue") -> None:
    s = get_settings()
    producer = make_producer(s, f"replay-tracking-{wid}", {"linger.ms": 50, "batch.size": 1048576})
    tracker = DeliveryTracker()
    enc = AvroEncoder(TOPIC_TRACKING, s.schema_registry_url)
    sent = positions = 0
    t0 = time.time()
    for rep in range(repeat):
        for game in games:
            mid = f"metrica-{game}" + (f"-{suffix}" if suffix else "") + (f"-r{rep}" if repeat > 1 else "")
            start = time.time()
            first_t = None
            for period, frame, t, bx, by, players in iter_frames(game):
                if frame % nworkers != wid:
                    continue
                if first_t is None:
                    first_t = t
                if speed > 0:
                    target = start + (t - first_t) / speed
                    d = target - time.time()
                    if d > 0.002:
                        time.sleep(d)
                elif max_rate > 0 and sent and sent / max(1e-6, time.time() - t0) > max_rate / nworkers:
                    time.sleep(0.001)
                now_ms = int(time.time() * 1000)
                rec = {"match_id": mid, "period": period, "frame": frame, "time_s": t, "ball_x": bx, "ball_y": by,
                       "players": players, "source_ts": now_ms, "ingest_ts": now_ms}
                while True:
                    try:
                        producer.produce(TOPIC_TRACKING, key=mid.encode(), value=enc.encode(rec),
                                         on_delivery=tracker.callback())
                        break
                    except BufferError:
                        producer.poll(0.1)
                sent += 1
                positions += len(players) + (1 if bx is not None else 0)
                if sent % 2000 == 0:
                    producer.poll(0)
                    q.put(("progress", wid, sent, positions, time.time() - t0))
    producer.flush(120)
    q.put(("done", wid, sent, positions, time.time() - t0, tracker.failed))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--game", default="1", help="1, 2 hoặc 1,2")
    ap.add_argument("--speed", type=float, default=100.0, help="hệ số tăng tốc so với 25 khung/giây; 0 = không giới hạn")
    ap.add_argument("--max-rate", type=float, default=0, help="khi speed=0: giới hạn khung/giây (0 = tối đa)")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--repeat", type=int, default=1)
    ap.add_argument("--id-suffix", default="")
    ap.add_argument("--result-file", default=str(DATA_DIR / "metrics" / "tracking_producer.jsonl"))
    a = ap.parse_args()
    games = [int(x) for x in a.game.split(",")]
    for g in games:
        for p in metrica_paths(g):
            if not p.exists():
                raise SystemExit(f"Thiếu {p}. Chạy: python -m sportslake.tools.download_data --metrica")
    ensure_topics(get_settings())
    q: mp.Queue = mp.Queue()
    procs = [mp.Process(target=worker, args=(i, a.workers, games, a.speed, a.repeat, a.id_suffix, a.max_rate, q))
             for i in range(a.workers)]
    t0 = time.time()
    for p in procs:
        p.start()
    done = {}
    prog = {}
    last = time.time()
    while len(done) < len(procs):
        try:
            msg = q.get(timeout=1)
        except Exception:  # noqa: BLE001
            if all(not p.is_alive() for p in procs) and len(done) < len(procs):
                break
            continue
        if msg[0] == "progress":
            prog[msg[1]] = msg[2:]
        else:
            done[msg[1]] = msg[2:]
        if time.time() - last > 5:
            fr = sum(v[0] for v in prog.values())
            pos = sum(v[1] for v in prog.values())
            el = time.time() - t0
            log.info("Đã gửi %d khung (%.0f khung/s, %.0f vị trí/s)", fr, fr / el, pos / el)
            last = time.time()
    for p in procs:
        p.join()
    el = time.time() - t0
    frames = sum(v[0] for v in done.values())
    pos = sum(v[1] for v in done.values())
    res = {"ts_ms": int(time.time() * 1000), "id_suffix": a.id_suffix, "games": games, "speed": a.speed,
           "workers": a.workers, "repeat": a.repeat,
           "frames": frames, "positions": pos, "seconds": round(el, 2), "frames_per_s": round(frames / el, 1),
           "positions_per_s": round(pos / el, 1), "failed": sum(v[3] for v in done.values() if len(v) > 3)}
    log.info("Hoàn tất: %s", json.dumps(res))
    Path(a.result_file).parent.mkdir(parents=True, exist_ok=True)
    with open(a.result_file, "a", encoding="utf-8") as f:
        f.write(json.dumps(res) + "\n")


if __name__ == "__main__":
    main()
