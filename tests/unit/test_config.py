"""Configuration loading (pydantic-settings)."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from src.common import config


def test_kafka_settings_from_env() -> None:
    kafka = config.get_kafka_settings()
    assert kafka.kafka_bootstrap_servers == "kafka-test:9092"
    assert kafka.all_topics == ("transactions", "predictions", "fraud-alerts", "producer-control")


def test_topic_names_can_be_overridden(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TOPIC_TRANSACTIONS", "tx-test")
    assert config.KafkaSettings().topic_transactions == "tx-test"


def test_missing_endpoint_fails_fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS")
    with pytest.raises(ValidationError):
        config.KafkaSettings()


def test_postgres_connect_kwargs_and_secret_masking() -> None:
    pg = config.get_postgres_settings()
    kwargs = pg.connect_kwargs()
    assert kwargs == {
        "host": "pg-test",
        "port": 5432,
        "dbname": "fraud",
        "user": "fraud",
        "password": "s3cret-test",
    }
    assert "s3cret-test" not in repr(pg)


@pytest.mark.parametrize("port", ["0", "70000", "not-a-port"])
def test_postgres_port_validation(monkeypatch: pytest.MonkeyPatch, port: str) -> None:
    monkeypatch.setenv("POSTGRES_PORT", port)
    with pytest.raises(ValidationError):
        config.PostgresSettings()


def test_path_settings_derived_paths(isolated_env: dict[str, str]) -> None:
    paths = config.get_path_settings()
    data, models = Path(isolated_env["DATA_DIR"]), Path(isolated_env["MODEL_DIR"])
    assert paths.raw_dataset_path == data / "raw" / "creditcard.csv"
    assert paths.test_split_path == data / "processed" / "test.parquet"
    assert paths.split_path("train") == data / "processed" / "train.parquet"
    assert paths.model_path == models / "best_model"
    assert paths.metrics_path == models / "metrics.json"
    assert paths.demo_samples_path == models / "demo_samples.json"
    assert paths.figures_dir == Path(isolated_env["DOCS_DIR"]) / "figures"
    assert paths.results_markdown_path == Path(isolated_env["DOCS_DIR"]) / "RESULTS.md"
    assert paths.stream_checkpoint_path == Path(isolated_env["CHECKPOINT_DIR"]) / "stream-processor"
    assert (
        paths.heartbeat_path("producer")
        == Path(isolated_env["HEARTBEAT_DIR"]) / "producer.heartbeat"
    )


def test_streaming_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in ("ALERT_THRESHOLD_OVERRIDE", "TRIGGER_INTERVAL_SECONDS", "STARTING_OFFSETS"):
        monkeypatch.delenv(name, raising=False)
    streaming = config.StreamingSettings()
    assert streaming.alert_threshold_override is None
    assert streaming.trigger_interval == "1 seconds"
    assert streaming.starting_offsets == "earliest"


@pytest.mark.parametrize(("raw", "expected"), [("", None), ("   ", None), ("0.25", 0.25)])
def test_threshold_override_parsing(
    monkeypatch: pytest.MonkeyPatch, raw: str, expected: float | None
) -> None:
    monkeypatch.setenv("ALERT_THRESHOLD_OVERRIDE", raw)
    assert config.StreamingSettings().alert_threshold_override == expected


@pytest.mark.parametrize("raw", ["0", "1", "1.5", "-0.1"])
def test_threshold_override_out_of_range(monkeypatch: pytest.MonkeyPatch, raw: str) -> None:
    monkeypatch.setenv("ALERT_THRESHOLD_OVERRIDE", raw)
    with pytest.raises(ValidationError):
        config.StreamingSettings()


def test_trigger_interval_string(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRIGGER_INTERVAL_SECONDS", "2.5")
    assert config.StreamingSettings().trigger_interval == "2.5 seconds"


def test_producer_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRODUCER_RATE", "120")
    monkeypatch.setenv("PRODUCER_LOOP", "false")
    producer = config.ProducerSettings()
    assert producer.producer_rate == 120
    assert producer.producer_loop is False


@pytest.mark.parametrize("rate", ["0", "-5", "1000.5"])
def test_producer_rate_bounds(monkeypatch: pytest.MonkeyPatch, rate: str) -> None:
    monkeypatch.setenv("PRODUCER_RATE", rate)
    with pytest.raises(ValidationError):
        config.ProducerSettings()


def test_driver_memory_format_validated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRAINER_DRIVER_MEMORY", "lots")
    with pytest.raises(ValidationError):
        config.TrainingSettings()


def test_log_level_is_case_insensitive(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LOG_LEVEL", "debug")
    assert config.LoggingSettings().log_level == "DEBUG"


def test_dashboard_settings_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DASHBOARD_REFRESH_SECONDS", "0.1")
    with pytest.raises(ValidationError):
        config.DashboardSettings()


def test_accessors_are_cached() -> None:
    assert config.get_kafka_settings() is config.get_kafka_settings()
    assert config.get_training_settings() is config.get_training_settings()
    assert config.get_producer_settings() is config.get_producer_settings()
    assert config.get_streaming_settings() is config.get_streaming_settings()
    assert config.get_dashboard_settings() is config.get_dashboard_settings()
    assert config.get_logging_settings() is config.get_logging_settings()
