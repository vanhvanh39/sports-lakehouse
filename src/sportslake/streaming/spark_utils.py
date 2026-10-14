"""Khởi tạo SparkSession có Delta Lake + S3A (MinIO), và tiện ích lược đồ."""
from __future__ import annotations

import json
import logging
import os

from ..config import AVRO_TOPICS, SCHEMA_DIR, get_settings

log = logging.getLogger(__name__)

CHECKPOINT_ROOT = os.environ.get("CHECKPOINT_ROOT", "/opt/checkpoints")


def build_spark(app_name: str, extra: dict | None = None):
    from pyspark.sql import SparkSession

    s = get_settings()
    conf = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        # MinIO qua S3A
        "spark.hadoop.fs.s3a.endpoint": s.s3_endpoint,
        "spark.hadoop.fs.s3a.access.key": s.s3_access_key,
        "spark.hadoop.fs.s3a.secret.key": s.s3_secret_key,
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "spark.hadoop.fs.s3a.fast.upload": "true",
        # Delta: mọi bảng mới bật Change Data Feed, cho phép VACUUM ngắn trong thí nghiệm
        "spark.databricks.delta.properties.defaults.enableChangeDataFeed": "true",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        "spark.databricks.delta.schema.autoMerge.enabled": "false",
        # cụm nhỏ -> ít partition shuffle
        "spark.sql.shuffle.partitions": os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"),
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.streaming.stateStore.stateSchemaCheck": "true",
        "spark.sql.adaptive.enabled": "true",
    }
    if extra:
        conf.update(extra)
    b = SparkSession.builder.appName(app_name)
    for k, v in conf.items():
        b = b.config(k, v)
    if os.environ.get("SPARK_LOCAL") == "1":
        b = b.master("local[*]")
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


def table_path(layer_name: tuple[str, str]) -> str:
    return get_settings().table_path(*layer_name)


def checkpoint(name: str) -> str:
    return f"{CHECKPOINT_ROOT.rstrip('/')}/{name}"


def avro_schema_json(topic: str) -> str:
    """Lấy lược đồ mới nhất từ Schema Registry; nếu không được thì dùng file .avsc trong repo."""
    s = get_settings()
    try:
        import requests

        r = requests.get(f"{s.schema_registry_url}/subjects/{topic}-value/versions/latest", timeout=10)
        if r.status_code == 200:
            schema = r.json()["schema"]
            log.info("Lược đồ %s-value phiên bản %s từ Schema Registry", topic, r.json().get("version"))
            return schema
    except Exception as exc:  # noqa: BLE001
        log.warning("Không lấy được lược đồ từ Schema Registry (%s), dùng file cục bộ", exc)
    with open(SCHEMA_DIR / AVRO_TOPICS[topic], encoding="utf-8") as f:
        return json.dumps(json.load(f))
