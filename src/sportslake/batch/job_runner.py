"""Bộ lập lịch job batch (thay cho Airflow/cron để gọn cụm): chạy job gold định kỳ bằng spark-submit.

  python -m sportslake.batch.job_runner                 # chạy vòng lặp theo GOLD_INTERVAL_MIN
  python -m sportslake.batch.job_runner gold --mode full --tracking
  python -m sportslake.batch.job_runner tool -- sql "SELECT * FROM gold_standings"
"""
from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone

from ..config import DATA_DIR
from ..logutil import setup_logging

log = setup_logging("job-runner")
APP = os.environ.get("APP_ROOT", "/opt/app")


def spark_submit_cmd(script: str, args: list[str], cores: int = 1, mem: str | None = None) -> list[str]:
    host = socket.gethostname()
    return [
        "/opt/spark/bin/spark-submit",
        "--master", os.environ.get("SPARK_MASTER", "spark://spark-master:7077"),
        "--deploy-mode", "client",
        "--conf", f"spark.driver.host={host}",
        "--conf", "spark.driver.bindAddress=0.0.0.0",
        "--conf", f"spark.cores.max={cores}",
        "--conf", "spark.executor.cores=1",
        "--conf", f"spark.executor.memory={mem or os.environ.get('JOB_EXECUTOR_MEMORY', '768m')}",
        "--conf", f"spark.driver.memory={os.environ.get('JOB_DRIVER_MEMORY', '768m')}",
        "--conf", f"spark.executorEnv.PYTHONPATH={APP}/src",
        "--conf", "spark.ui.port=4041",
        f"{APP}/src/jobs/{script}", *args,
    ]


def run(script: str, args: list[str], cores: int = 1) -> int:
    cmd = spark_submit_cmd(script, args, cores)
    log.info("Chạy: %s", " ".join(cmd[-len(args) - 1:]))
    t0 = time.time()
    rc = subprocess.call(cmd)
    rec = {"script": script, "args": args, "rc": rc, "seconds": round(time.time() - t0, 1),
           "finished_utc": datetime.now(timezone.utc).isoformat()}
    log.info("Kết thúc: %s", rec)
    try:
        (DATA_DIR / "metrics").mkdir(parents=True, exist_ok=True)
        with (DATA_DIR / "metrics" / "batch_jobs.jsonl").open("a", encoding="utf-8") as f:
            f.write(json.dumps(rec) + "\n")
    except OSError:
        pass
    return rc


def loop() -> None:
    interval = float(os.environ.get("GOLD_INTERVAL_MIN", "10")) * 60
    mode = os.environ.get("GOLD_MODE", "incremental")
    first_delay = float(os.environ.get("GOLD_FIRST_DELAY_S", "120"))
    log.info("Job gold mỗi %.0f phút (chế độ %s), lần đầu sau %.0fs", interval / 60, mode, first_delay)
    time.sleep(first_delay)
    n = 0
    while True:
        # mỗi 6 lần chạy thì tính lại toàn bộ một lần (kèm dữ liệu vị trí) để sửa sai lệch nếu có
        full = n % 6 == 5
        args = ["--mode", "full", "--tracking"] if full else ["--mode", mode]
        run("run_gold.py", args, cores=int(os.environ.get("JOB_CORES_MAX", "1")))
        n += 1
        time.sleep(interval)


def main() -> None:
    argv = sys.argv[1:]
    if not argv:
        loop()
    elif argv[0] == "gold":
        sys.exit(run("run_gold.py", argv[1:], cores=int(os.environ.get("JOB_CORES_MAX", "1"))))
    elif argv[0] == "tool":
        rest = argv[1:]
        if rest and rest[0] == "--":
            rest = rest[1:]
        sys.exit(run("run_spark_tool.py", rest, cores=int(os.environ.get("JOB_CORES_MAX", "1"))))
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
