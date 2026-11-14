"""Dashboard I/O helpers: SQL query wrappers and Kafka publishing/reading (both faked)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest

from src.common.schemas import CONTROL_MESSAGE_KEY, RAW_INPUT_COLUMNS, ControlCommand
from src.dashboard.helpers import kafka_io, queries, samples
from tests.unit.fakes import FakeConnection, FakeConsumer, FakeMessage, FakeProducer

NOW = datetime(2026, 11, 14, 18, 0, tzinfo=UTC)


# --------------------------------------------------------------------------- SQL helpers
def test_fetch_one_and_frame() -> None:
    conn = FakeConnection(
        cursor_results=[
            ([{"total": 5}], ["total"]),
            ([{"a": 1, "b": 2}, {"a": 3, "b": 4}], ["a", "b"]),
            ([], ["a", "b"]),
        ]
    )
    assert queries.fetch_one(conn, "SELECT 1", {"x": 1}) == {"total": 5}
    frame = queries.fetch_frame(conn, "SELECT 2")
    assert frame.to_dict("records") == [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
    empty = queries.fetch_frame(conn, "SELECT 3")
    assert empty.empty and list(empty.columns) == ["a", "b"]
    assert conn.cursors[0].executed == [("SELECT 1", {"x": 1})]


def test_live_snapshot_runs_every_query_with_parameters() -> None:
    conn = FakeConnection(cursor_results=[([{"total": 1}], ["total"]), ([{"tp": 1}], ["tp"])])
    snapshot = queries.live_snapshot(
        conn, 0.4, window_s=30, feed_limit=10, minutes=15, scores_limit=99
    )
    assert set(snapshot) == {"summary", "quality", "feed", "alerts", "per_minute", "scores"}
    executed = [cursor.executed[0] for cursor in conn.cursors]
    assert [sql for sql, _ in executed] == [
        queries.KPI_SUMMARY_SQL,
        queries.QUALITY_SQL,
        queries.RECENT_FEED_SQL,
        queries.RECENT_ALERTS_SQL,
        queries.PER_MINUTE_SQL,
        queries.RECENT_SCORES_SQL,
    ]
    params = [p for _, p in executed]
    assert params[0] == {"threshold": 0.4, "window_s": 30}
    assert params[3] == {"threshold": 0.4, "limit": 10}
    assert params[4] == {"threshold": 0.4, "minutes": 15}
    assert params[5] == {"limit": 99}


def test_kpi_queries_only_count_replay_traffic() -> None:
    for sql in (queries.KPI_SUMMARY_SQL, queries.QUALITY_SQL, queries.PER_MINUTE_SQL):
        assert "source = 'replay'" in sql


def test_fetch_prediction_casts_uuid() -> None:
    conn = FakeConnection(cursor_results=[([{"transaction_id": "abc"}], ["transaction_id"])])
    assert queries.fetch_prediction(conn, "abc") == {"transaction_id": "abc"}
    sql, params = conn.cursors[0].executed[0]
    assert "::uuid" in sql and params == {"transaction_id": "abc"}


# --------------------------------------------------------------------------- Kafka helpers
def _event() -> Any:
    return samples.build_manual_event({c: 1.0 for c in RAW_INPUT_COLUMNS}, 0, NOW)


def test_publish_transaction() -> None:
    producer = FakeProducer()
    event = _event()
    kafka_io.publish_transaction(producer, "transactions", event)
    assert producer.produced == [("transactions", event.key, event.to_json_bytes())]
    assert producer.flushes == 1


def test_publish_control_uses_fixed_key() -> None:
    producer = FakeProducer()
    command = ControlCommand(command="pause", paused=True, rate=50, issued_at=NOW)
    kafka_io.publish_control(producer, "producer-control", command)
    assert producer.produced[0][1] == CONTROL_MESSAGE_KEY


@pytest.mark.parametrize(
    ("producer", "match"),
    [
        (FakeProducer(undelivered=1), "not delivered"),
        (FakeProducer(delivery_error="broker down"), "failed"),
    ],
)
def test_publish_errors_are_raised(producer: FakeProducer, match: str) -> None:
    with pytest.raises(kafka_io.PublishError, match=match):
        kafka_io.publish_transaction(producer, "transactions", _event(), timeout_s=0.1)


def test_read_last_control_empty_topic() -> None:
    consumer = FakeConsumer()
    assert (
        kafka_io.read_last_control("k", "producer-control", consumer_factory=lambda cfg: consumer)
        is None
    )
    assert consumer.closed


def test_read_last_control_returns_latest_command() -> None:
    command = ControlCommand(command="set_rate", paused=False, rate=80, issued_at=NOW)
    consumer = FakeConsumer(
        messages=[FakeMessage(command.to_json_bytes())],
        watermarks={("producer-control", 0): (0, 4)},
    )
    result = kafka_io.read_last_control(
        "k", "producer-control", consumer_factory=lambda cfg: consumer
    )
    assert result == command
    assert consumer.assigned[0].offset == 3  # type: ignore[index]  # the last message only
    assert consumer.closed


def test_read_last_control_invalid_or_missing_message() -> None:
    invalid = FakeConsumer(messages=[FakeMessage(b"junk")], watermarks={("c", 0): (0, 1)})
    assert kafka_io.read_last_control("k", "c", consumer_factory=lambda cfg: invalid) is None
    silent = FakeConsumer(watermarks={("c", 0): (0, 1)})
    assert (
        kafka_io.read_last_control("k", "c", timeout_s=0.05, consumer_factory=lambda cfg: silent)
        is None
    )


def test_topic_overview() -> None:
    consumer = FakeConsumer(
        topics={"transactions": [0, 1], "predictions": [0]},
        watermarks={
            ("transactions", 0): (0, 10),
            ("transactions", 1): (0, 12),
            ("predictions", 0): (0, 22),
        },
    )
    overview = kafka_io.topic_overview(
        "k", ["transactions", "predictions", "missing"], consumer_factory=lambda cfg: consumer
    )
    assert overview == {"transactions": {0: 10, 1: 12}, "predictions": {0: 22}, "missing": {}}
    assert consumer.closed
