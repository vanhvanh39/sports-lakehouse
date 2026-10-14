"""Cấu hình chung, đọc từ biến môi trường (file .env được docker compose nạp vào container)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _env(name: str, default: str = "") -> str:
    v = os.environ.get(name)
    return default if v is None or v == "" else v


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


def _env_bool(name: str, default: bool) -> bool:
    return _env(name, "true" if default else "false").strip().lower() in ("1", "true", "yes", "on")


def _env_list(name: str, default: str) -> list[str]:
    return [x.strip() for x in _env(name, default).split(",") if x.strip()]


# ---------------- Kafka topics ----------------
TOPIC_EVENTS = "match.events"
TOPIC_STATE = "match.state"
TOPIC_STATS = "match.stats"
TOPIC_TRACKING = "match.tracking"
TOPIC_ALERTS = "match.alerts"
TOPIC_DLQ = "match.dlq"

# topic -> (partitions, extra config). Replication factor 3, min.insync.replicas 2 cho mọi topic.
TOPIC_SPECS: dict[str, tuple[int, dict[str, str]]] = {
    TOPIC_EVENTS: (6, {"message.timestamp.type": "LogAppendTime", "retention.ms": str(30 * 24 * 3600 * 1000)}),
    TOPIC_STATE: (3, {"message.timestamp.type": "LogAppendTime", "retention.ms": str(30 * 24 * 3600 * 1000)}),
    TOPIC_STATS: (3, {"message.timestamp.type": "LogAppendTime", "retention.ms": str(30 * 24 * 3600 * 1000)}),
    TOPIC_TRACKING: (12, {"message.timestamp.type": "LogAppendTime", "retention.ms": str(24 * 3600 * 1000),
                          "compression.type": "producer"}),
    TOPIC_ALERTS: (3, {"retention.ms": str(30 * 24 * 3600 * 1000)}),
    TOPIC_DLQ: (3, {"retention.ms": str(30 * 24 * 3600 * 1000)}),
}

# Topic dùng Avro + Schema Registry (subject = "<topic>-value")
AVRO_TOPICS = {
    TOPIC_EVENTS: "match_event.avsc",
    TOPIC_STATE: "match_state.avsc",
    TOPIC_STATS: "match_stats.avsc",
    TOPIC_TRACKING: "tracking_frame.avsc",
}

PROJECT_ROOT = Path(_env("APP_ROOT", str(Path(__file__).resolve().parents[2])))
SCHEMA_DIR = Path(_env("SCHEMA_DIR", str(PROJECT_ROOT / "schemas")))
DATA_DIR = Path(_env("DATA_DIR", str(PROJECT_ROOT / "data")))


@dataclass
class Settings:
    # chế độ
    collector_mode: str = field(default_factory=lambda: _env("COLLECTOR_MODE", "mock"))

    # API-Football
    apifootball_key: str = field(default_factory=lambda: _env("APIFOOTBALL_KEY"))
    apifootball_base_url: str = field(default_factory=lambda: _env("APIFOOTBALL_BASE_URL", "https://v3.football.api-sports.io"))
    mock_api_url: str = field(default_factory=lambda: _env("MOCK_API_URL", "http://mock-api:8000"))
    league_ids: list[str] = field(default_factory=lambda: _env_list("LEAGUE_IDS", "39,140,2"))
    daily_budget: int = field(default_factory=lambda: _env_int("DAILY_REQUEST_BUDGET", 100))
    minute_limit: int = field(default_factory=lambda: _env_int("MINUTE_REQUEST_LIMIT", 10))
    budget_reserve: int = field(default_factory=lambda: _env_int("BUDGET_RESERVE", 5))
    scheduler_policy: str = field(default_factory=lambda: _env("SCHEDULER_POLICY", "adaptive"))
    fixed_interval_s: float = field(default_factory=lambda: _env_float("FIXED_INTERVAL_SECONDS", 90))
    record_raw: bool = field(default_factory=lambda: _env_bool("RECORD_RAW", True))

    # Mock
    mock_speed: float = field(default_factory=lambda: _env_float("MOCK_SPEED", 10))

    # Kafka
    kafka_bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "localhost:9092"))
    schema_registry_url: str = field(default_factory=lambda: _env("SCHEMA_REGISTRY_URL", "http://localhost:8081"))

    # S3 / lakehouse
    s3_endpoint: str = field(default_factory=lambda: _env("S3_ENDPOINT", "http://localhost:9000"))
    s3_access_key: str = field(default_factory=lambda: _env("S3_ACCESS_KEY", "minioadmin"))
    s3_secret_key: str = field(default_factory=lambda: _env("S3_SECRET_KEY", "minioadmin123"))
    bucket: str = field(default_factory=lambda: _env("LAKEHOUSE_BUCKET", "lakehouse"))

    # Elasticsearch
    es_url: str = field(default_factory=lambda: _env("ES_URL", "http://localhost:9200"))

    # Cảnh báo
    telegram_token: str = field(default_factory=lambda: _env("TELEGRAM_BOT_TOKEN"))
    telegram_chat_id: str = field(default_factory=lambda: _env("TELEGRAM_CHAT_ID"))
    webhook_url: str = field(default_factory=lambda: _env("ALERT_WEBHOOK_URL"))

    # Kịch tính
    excitement_versions: list[str] = field(default_factory=lambda: _env_list("EXCITEMENT_VERSIONS", "V1,V2"))
    excitement_threshold: float = field(default_factory=lambda: _env_float("EXCITEMENT_THRESHOLD", 7.0))

    @property
    def lake_root(self) -> str:
        return f"s3a://{self.bucket}"

    def table_path(self, layer: str, name: str, scheme: str = "s3a") -> str:
        return f"{scheme}://{self.bucket}/{layer}/{name}"

    def delta_storage_options(self) -> dict[str, str]:
        """Tùy chọn cho thư viện deltalake (delta-rs) khi đọc bảng trên MinIO từ Python."""
        return {
            "AWS_ENDPOINT_URL": self.s3_endpoint,
            "AWS_ACCESS_KEY_ID": self.s3_access_key,
            "AWS_SECRET_ACCESS_KEY": self.s3_secret_key,
            "AWS_REGION": "us-east-1",
            "AWS_ALLOW_HTTP": "true",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
        }


def get_settings() -> Settings:
    return Settings()


# Tên bảng Delta (layer, name)
T_BRONZE_RAW = ("bronze", "kafka_raw")
T_BRONZE_TRACKING = ("bronze", "tracking_positions")
T_SILVER_EVENTS = ("silver", "match_events")
T_SILVER_STATE = ("silver", "match_state")
T_SILVER_STATS = ("silver", "match_stats")
T_SILVER_EXCITEMENT = ("silver", "match_excitement")
T_SILVER_EXC_HISTORY = ("silver", "excitement_history")
T_SILVER_ALERTS = ("silver", "alerts")
T_GOLD_TEAM_MATCH = ("gold", "team_match_summary")
T_GOLD_STANDINGS = ("gold", "standings")
T_GOLD_TEAM_FORM = ("gold", "team_form")
T_GOLD_PLAYER = ("gold", "player_summary")
T_GOLD_TRACKING = ("gold", "player_distance")
