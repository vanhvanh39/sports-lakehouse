#!/usr/bin/env bash
# Khởi động pipeline streaming (driver chạy trong container này, executor ở spark-worker).
set -euo pipefail
HOST=$(hostname)
echo "[run-pipeline] master=${SPARK_MASTER} cores.max=${PIPELINE_CORES_MAX:-2} executor.memory=${PIPELINE_EXECUTOR_MEMORY:-768m} RUN_ID=${RUN_ID:-}"
exec /opt/spark/bin/spark-submit \
  --master "${SPARK_MASTER:-spark://spark-master:7077}" \
  --deploy-mode client \
  --name sports-lakehouse-pipeline \
  --driver-memory "${PIPELINE_DRIVER_MEMORY:-1g}" \
  --conf spark.driver.host="${HOST}" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  --conf spark.driver.port=7078 \
  --conf spark.blockManager.port=7079 \
  --conf spark.cores.max="${PIPELINE_CORES_MAX:-2}" \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory="${PIPELINE_EXECUTOR_MEMORY:-768m}" \
  --conf spark.executorEnv.PYTHONPATH=/opt/app/src \
  --conf spark.sql.streaming.metricsEnabled=true \
  --conf spark.ui.port=4040 \
  /opt/app/src/jobs/run_pipeline.py
