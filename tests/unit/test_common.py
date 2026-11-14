"""Shared helpers: heartbeat, Kafka readiness/offsets, PostgreSQL access, logging."""

from __future__ import annotations

import io
import json
import sys
from pathlib import Path
from typing import Any

import psycopg
import pytest
import structlog
from confluent_kafka import KafkaException

from src.common import db, health, kafka_utils
from src.common.config import get_postgres_settings
from src.common.logging_setup import configure_logging, get_logger
from tests.unit.fakes import FakeConnection, FakeConsumer


# --------------------------------------------------------------------------- heartbeat
class FakeClock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def test_heartbeat_is_rate_limited(tmp_path: Path) -> None:
    clock = FakeClock()
    beat = health.Heartbeat(tmp_path / "hb" / "svc.heartbeat", min_interval_s=5.0, clock=clock)
    assert beat.beat() is True
    assert beat.path.is_file()
    clock.now += 1
    assert beat.beat() is False
    assert beat.beat(force=True) is True
    clock.now += 10
    assert beat.beat() is True


def test_heartbeat_age(tmp_path: Path) -> None:
    path = tmp_path / "svc.heartbeat"
    assert health.heartbeat_age_seconds(path) is None
    health.Heartbeat(path).beat()
    age = health.heartbeat_age_seconds(path, now=path.stat().st_mtime + 3)
    assert age == pytest.approx(3.0)


# --------------------------------------------------------------------------- kafka
def test_producer_config_defaults_and_overrides() -> None:
    cfg = kafka_utils.producer_config("k:9092", "client", **{"linger.ms": 0})
    assert cfg["bootstrap.servers"] == "k:9092"
    assert cfg["enable.idempotence"] is True
    assert cfg["acks"] == "all"
    assert cfg["linger.ms"] == 0


class FakeAdmin:
    def __init__(self, outcomes: list[Any]) -> None:
        self.outcomes = outcomes
        self.calls = 0

    def list_topics(self, timeout: float) -> Any:
        self.calls += 1
        outcome = self.outcomes.pop(0) if len(self.outcomes) > 1 else self.outcomes[0]
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_wait_for_kafka_retries_until_topics_exist() -> None:
    from tests.unit.fakes import FakeClusterMetadata

    admin = FakeAdmin(
        [
            KafkaException("broker not available"),
            FakeClusterMetadata({"transactions": [0]}),
            FakeClusterMetadata({"transactions": [0], "producer-control": [0]}),
        ]
    )
    sleeps: list[float] = []
    kafka_utils.wait_for_kafka(
        "k:9092",
        required_topics=("transactions", "producer-control"),
        admin_factory=lambda cfg: admin,
        sleep=sleeps.append,
        clock=lambda: 0.0,
    )
    assert admin.calls == 3
    assert sleeps == [1.0, 2.0]  # exponential backoff


def test_wait_for_kafka_times_out() -> None:
    times = iter([0.0, 5.0, 11.0, 20.0])
    admin = FakeAdmin([KafkaException("down")])
    with pytest.raises(TimeoutError, match="not ready"):
        kafka_utils.wait_for_kafka(
            "k:9092",
            timeout_s=10,
            admin_factory=lambda cfg: admin,
            sleep=lambda s: None,
            clock=lambda: next(times),
        )


def test_topic_end_offsets() -> None:
    consumer = FakeConsumer(
        topics={"transactions": [0, 1, 2]},
        watermarks={
            ("transactions", 0): (0, 10),
            ("transactions", 1): (0, 5),
            ("transactions", 2): (3, 7),
        },
    )
    assert kafka_utils.topic_end_offsets(consumer, "transactions") == {0: 10, 1: 5, 2: 7}
    assert kafka_utils.topic_end_offsets(consumer, "missing") == {}


# --------------------------------------------------------------------------- postgres
def test_insert_sql_placeholders_match_columns() -> None:
    assert db.INSERT_PREDICTIONS_SQL.count("%s") == len(db.PREDICTION_DB_COLUMNS)
    assert "ON CONFLICT (transaction_id) DO NOTHING" in db.INSERT_PREDICTIONS_SQL
    assert "%s::uuid" in db.INSERT_PREDICTIONS_SQL


@pytest.mark.parametrize("value", [0.0, 1.0, -0.2, 1.7, float("nan"), float("inf")])
def test_validate_threshold_rejects_invalid(value: float) -> None:
    with pytest.raises(ValueError):
        db.validate_threshold(value)


def test_threshold_round_trip() -> None:
    conn = FakeConnection()
    assert db.get_threshold(conn) is None
    assert db.set_threshold(conn, 0.42) == 0.42
    assert db.get_threshold(conn) == 0.42


def test_get_threshold_ignores_garbage() -> None:
    assert db.get_threshold(FakeConnection({db.THRESHOLD_KEY: "abc"})) is None
    assert db.get_threshold(FakeConnection({db.THRESHOLD_KEY: "7"})) is None


def test_sync_threshold_override_wins() -> None:
    conn = FakeConnection({db.THRESHOLD_KEY: "0.9", db.MODEL_RUN_KEY: "run-1"})
    value = db.sync_threshold_with_model(
        conn, model_run_id="run-1", default_threshold=0.4, override=0.3
    )
    assert value == 0.3
    assert float(conn.settings[db.THRESHOLD_KEY]) == 0.3


def test_sync_threshold_resets_for_new_model() -> None:
    conn = FakeConnection({db.THRESHOLD_KEY: "0.9", db.MODEL_RUN_KEY: "old-run"})
    assert db.sync_threshold_with_model(conn, model_run_id="new-run", default_threshold=0.4) == 0.4
    assert conn.settings[db.MODEL_RUN_KEY] == "new-run"


def test_sync_threshold_keeps_slider_value_for_same_model() -> None:
    conn = FakeConnection({db.THRESHOLD_KEY: "0.7", db.MODEL_RUN_KEY: "run-1"})
    assert db.sync_threshold_with_model(conn, model_run_id="run-1", default_threshold=0.4) == 0.7


def test_sync_threshold_seeds_missing_value() -> None:
    conn = FakeConnection({db.MODEL_RUN_KEY: "run-1"})
    assert db.sync_threshold_with_model(conn, model_run_id="run-1", default_threshold=0.4) == 0.4


def test_insert_predictions() -> None:
    conn = FakeConnection()
    assert db.insert_predictions(conn, []) == 0
    assert conn.cursors == []
    rows = [("id-1",) * len(db.PREDICTION_DB_COLUMNS), ("id-2",) * len(db.PREDICTION_DB_COLUMNS)]
    assert db.insert_predictions(conn, rows) == 2
    sql, written = conn.cursors[0].executemany_calls[0]
    assert sql == db.INSERT_PREDICTIONS_SQL
    assert written == rows


def test_insert_and_prune_stream_progress() -> None:
    conn = FakeConnection()
    db.insert_stream_progress(
        conn, {"batch_id": 7, "num_input_rows": 3, "end_offsets": {"transactions": {"0": 5}}}
    )
    sql, params = conn.executed[-1]
    assert sql == db.INSERT_PROGRESS_SQL
    assert params[2] == 7
    assert params[-1].obj == {"transactions": {"0": 5}}  # wrapped as Jsonb
    db.prune_stream_progress(conn, keep="2 hours")
    assert conn.executed[-1] == (db.PRUNE_PROGRESS_SQL, ("2 hours",))


def test_connect_passes_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}
    monkeypatch.setattr(db.psycopg, "connect", lambda **kw: captured.update(kw) or "conn")
    assert db.connect(get_postgres_settings(), autocommit=False) == "conn"
    assert captured["host"] == "pg-test"
    assert captured["autocommit"] is False
    assert captured["connect_timeout"] == 5


def test_wait_for_postgres_retries_then_succeeds(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = {"n": 0}

    def fake_connect(settings: Any, **kwargs: Any) -> FakeConnection:
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise psycopg.OperationalError("connection refused")
        return FakeConnection()

    monkeypatch.setattr(db, "connect", fake_connect)
    sleeps: list[float] = []
    db.wait_for_postgres(get_postgres_settings(), sleep=sleeps.append, clock=lambda: 0.0)
    assert attempts["n"] == 3
    assert sleeps == [1.0, 2.0]


def test_wait_for_postgres_times_out(monkeypatch: pytest.MonkeyPatch) -> None:
    def always_fail(settings: Any, **kwargs: Any) -> None:
        raise psycopg.OperationalError("down")

    monkeypatch.setattr(db, "connect", always_fail)
    times = iter([0.0, 100.0, 200.0])
    with pytest.raises(TimeoutError):
        db.wait_for_postgres(
            get_postgres_settings(), timeout_s=50, sleep=lambda s: None, clock=lambda: next(times)
        )


# --------------------------------------------------------------------------- logging
def test_logging_emits_json_with_service(monkeypatch: pytest.MonkeyPatch) -> None:
    buffer = io.StringIO()
    monkeypatch.setattr(sys, "stdout", buffer)
    logger = get_logger("tests.module")  # created before configuration on purpose
    try:
        configure_logging("unit-test", "INFO")
        logger.info("something.happened", answer=42)
        logger.debug("hidden")
    finally:
        structlog.reset_defaults()  # don't leak the StringIO-bound config into other tests
    lines = [line for line in buffer.getvalue().splitlines() if line.startswith("{")]
    record = json.loads(lines[-1])
    assert record["event"] == "something.happened"
    assert record["answer"] == 42
    assert record["service"] == "unit-test"
    assert record["module"] == "tests.module"
    assert all("hidden" not in line for line in lines)


def test_producer_config_compresses() -> None:
    assert kafka_utils.producer_config("k:9092", "c")["compression.type"] == "lz4"


def test_prune_predictions_uses_retention_hours() -> None:
    conn = FakeConnection()
    assert db.prune_predictions(conn, keep_hours=6) == 0
    assert conn.executed[-1] == (db.PRUNE_PREDICTIONS_SQL, (21600.0,))


def test_prediction_columns_match_init_sql() -> None:
    """Contract test: the INSERT column list must match the real table definition."""
    init_sql = Path(__file__).resolve().parents[2] / "docker" / "postgres" / "init.sql"
    text = init_sql.read_text(encoding="utf-8")
    body = text.split("CREATE TABLE IF NOT EXISTS predictions (", 1)[1].split(");", 1)[0]
    columns = [
        line.split()[0]
        for line in body.splitlines()
        if line.strip() and not line.strip().startswith("--")
    ]
    assert set(columns) == {*db.PREDICTION_DB_COLUMNS, "stored_at"}
    assert "UUID PRIMARY KEY" in body
