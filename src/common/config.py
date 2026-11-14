"""Typed configuration loaded from environment variables.

Docker Compose injects the variables from ``.env`` into every container. Each service
loads only the settings groups it needs. Endpoints and paths have **no defaults**: a
missing variable fails fast at startup instead of silently falling back to a hard-coded
host, port or path. Only tuning knobs have defaults, and they match ``.env.example``.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.common.schemas import MAX_PRODUCER_RATE

_SETTINGS_CONFIG = SettingsConfigDict(case_sensitive=False, extra="ignore", frozen=True)


class KafkaSettings(BaseSettings):
    """Kafka connection and topic names."""

    model_config = _SETTINGS_CONFIG

    kafka_bootstrap_servers: str = Field(min_length=1)
    topic_transactions: str = Field(default="transactions", min_length=1)
    topic_predictions: str = Field(default="predictions", min_length=1)
    topic_fraud_alerts: str = Field(default="fraud-alerts", min_length=1)
    topic_producer_control: str = Field(default="producer-control", min_length=1)

    @property
    def all_topics(self) -> tuple[str, ...]:
        """Every topic the system uses, in a stable order."""
        return (
            self.topic_transactions,
            self.topic_predictions,
            self.topic_fraud_alerts,
            self.topic_producer_control,
        )


class PostgresSettings(BaseSettings):
    """PostgreSQL connection settings."""

    model_config = _SETTINGS_CONFIG

    postgres_host: str = Field(min_length=1)
    postgres_port: int = Field(ge=1, le=65535)
    postgres_db: str = Field(min_length=1)
    postgres_user: str = Field(min_length=1)
    postgres_password: SecretStr

    def connect_kwargs(self) -> dict[str, Any]:
        """Keyword arguments for :func:`psycopg.connect` (contains the password)."""
        return {
            "host": self.postgres_host,
            "port": self.postgres_port,
            "dbname": self.postgres_db,
            "user": self.postgres_user,
            "password": self.postgres_password.get_secret_value(),
        }


class PathSettings(BaseSettings):
    """Filesystem locations inside the containers (bind mounts / named volumes)."""

    model_config = _SETTINGS_CONFIG

    data_dir: Path
    model_dir: Path
    docs_dir: Path
    checkpoint_dir: Path
    heartbeat_dir: Path

    @property
    def raw_dataset_path(self) -> Path:
        return self.data_dir / "raw" / "creditcard.csv"

    @property
    def processed_dir(self) -> Path:
        return self.data_dir / "processed"

    def split_path(self, split: str) -> Path:
        """Parquet file of a data split (``train``, ``val`` or ``test``)."""
        return self.processed_dir / f"{split}.parquet"

    @property
    def test_split_path(self) -> Path:
        return self.split_path("test")

    @property
    def model_path(self) -> Path:
        return self.model_dir / "best_model"

    @property
    def metrics_path(self) -> Path:
        return self.model_dir / "metrics.json"

    @property
    def demo_samples_path(self) -> Path:
        return self.model_dir / "demo_samples.json"

    @property
    def figures_dir(self) -> Path:
        return self.docs_dir / "figures"

    @property
    def results_markdown_path(self) -> Path:
        return self.docs_dir / "RESULTS.md"

    @property
    def stream_checkpoint_path(self) -> Path:
        return self.checkpoint_dir / "stream-processor"

    def heartbeat_path(self, service: str) -> Path:
        """Heartbeat file touched by a service and checked by its Docker healthcheck."""
        return self.heartbeat_dir / f"{service}.heartbeat"


class TrainingSettings(BaseSettings):
    """Offline training job settings."""

    model_config = _SETTINGS_CONFIG

    random_seed: int = 42
    undersample_ratio: float = Field(default=10.0, gt=0)
    spark_master: str = Field(default="local[*]", min_length=1)
    trainer_driver_memory: str = Field(default="2g", pattern=r"^\d+[kmgKMG]$")


class ProducerSettings(BaseSettings):
    """Transaction replay producer settings."""

    model_config = _SETTINGS_CONFIG

    producer_rate: float = Field(default=50.0, gt=0, le=MAX_PRODUCER_RATE)
    producer_loop: bool = True
    producer_log_interval_seconds: float = Field(default=10.0, gt=0)


class StreamingSettings(BaseSettings):
    """Spark Structured Streaming processor settings."""

    model_config = _SETTINGS_CONFIG

    spark_master: str = Field(default="local[*]", min_length=1)
    stream_driver_memory: str = Field(default="1g", pattern=r"^\d+[kmgKMG]$")
    trigger_interval_seconds: float = Field(default=1.0, gt=0)
    max_offsets_per_trigger: int = Field(default=5000, gt=0)
    starting_offsets: Literal["earliest", "latest"] = "earliest"
    alert_threshold_override: float | None = Field(default=None, gt=0, lt=1)

    @field_validator("alert_threshold_override", mode="before")
    @classmethod
    def _blank_means_none(cls, value: Any) -> Any:
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @property
    def trigger_interval(self) -> str:
        """Interval string for ``DataStreamWriter.trigger(processingTime=...)``."""
        return f"{self.trigger_interval_seconds:g} seconds"


class DashboardSettings(BaseSettings):
    """Streamlit dashboard settings."""

    model_config = _SETTINGS_CONFIG

    dashboard_refresh_seconds: float = Field(default=2.0, ge=0.5, le=60)
    manual_test_timeout_seconds: float = Field(default=15.0, gt=0, le=120)


class LoggingSettings(BaseSettings):
    """Logging settings."""

    model_config = _SETTINGS_CONFIG

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"

    @field_validator("log_level", mode="before")
    @classmethod
    def _upper(cls, value: Any) -> Any:
        return value.upper() if isinstance(value, str) else value


# Cached accessors: each settings group is parsed once per process.
@lru_cache(maxsize=1)
def get_kafka_settings() -> KafkaSettings:
    return KafkaSettings()  # type: ignore[call-arg]


@lru_cache(maxsize=1)
def get_postgres_settings() -> PostgresSettings:
    return PostgresSettings()  # type: ignore[call-arg]


@lru_cache(maxsize=1)
def get_path_settings() -> PathSettings:
    return PathSettings()  # type: ignore[call-arg]


@lru_cache(maxsize=1)
def get_training_settings() -> TrainingSettings:
    return TrainingSettings()


@lru_cache(maxsize=1)
def get_producer_settings() -> ProducerSettings:
    return ProducerSettings()


@lru_cache(maxsize=1)
def get_streaming_settings() -> StreamingSettings:
    return StreamingSettings()


@lru_cache(maxsize=1)
def get_dashboard_settings() -> DashboardSettings:
    return DashboardSettings()


@lru_cache(maxsize=1)
def get_logging_settings() -> LoggingSettings:
    return LoggingSettings()
