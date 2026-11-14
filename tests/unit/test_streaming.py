"""Stream processor logic on static DataFrames (no Kafka or PostgreSQL needed)."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import psycopg
import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

from src.common.config import get_path_settings, get_streaming_settings
from src.common.db import PREDICTION_DB_COLUMNS
from src.common.health import Heartbeat
from src.common.schemas import (
    PREDICTION_FIELDS,
    RAW_INPUT_COLUMNS,
    TRANSACTION_FIELD_TYPES,
    PredictionEvent,
    Source,
    TransactionEvent,
)
from src.streaming import batch as batch_module
from src.streaming import listener as listener_module
from src.streaming import sinks, stream_processor, threshold
from src.streaming.transforms import (
    apply_threshold,
    enrich,
    is_valid_transaction,
    parse_transactions,
    score,
    to_kafka_records,
)
from tests.conftest import make_transactions
from tests.unit.fakes import FakeConnection, FakeLogger

pytestmark = pytest.mark.spark


def event_bytes(row_index: int = 0, *, frauds: bool = False, **overrides: Any) -> bytes:
    """A valid transaction message; ``overrides`` are written into the raw JSON afterwards,
    so they can also inject values the pydantic model itself would reject."""
    frame = make_transactions(3, 3, seed=5)
    row = frame.iloc[row_index + (3 if frauds else 0)]
    event_time = overrides.pop("event_time", None)
    event = TransactionEvent(
        transaction_id=uuid.uuid4(),
        event_time=event_time if isinstance(event_time, datetime) else datetime.now(UTC),
        source=Source.REPLAY,
        replay_pass=1,
        actual_class=int(row["Class"]),
        **{c: float(row[c]) for c in RAW_INPUT_COLUMNS},
    )
    payload = json.loads(event.to_json_bytes())
    if event_time is not None and not isinstance(event_time, datetime):
        payload["event_time"] = event_time
    payload.update(overrides)
    return json.dumps(payload).encode()


def raw_frame(spark, payloads: list[bytes]):  # type: ignore[no-untyped-def]
    return spark.createDataFrame([(p,) for p in payloads], "value binary")


# --------------------------------------------------------------------------- parse / validate
def test_parse_transactions_types(spark) -> None:
    parsed = parse_transactions(raw_frame(spark, [event_bytes()]))
    assert parsed.columns == list(TRANSACTION_FIELD_TYPES)
    assert dict(parsed.dtypes)["event_time"] == "timestamp"
    row = parsed.first()
    assert row["source"] == "replay" and row["replay_pass"] == 1
    assert row["event_time"] is not None


def test_invalid_messages_are_filtered(spark) -> None:
    payloads = [
        event_bytes(),  # the only valid one
        b"garbage",
        b'{"transaction_id": "x"}',  # missing fields
        event_bytes(source="bogus"),
        event_bytes(Amount=-5.0),
        event_bytes(actual_class=3),
        event_bytes(event_time="yesterday"),
        event_bytes().replace(b'"V1":', b'"V1": NaN, "_ignored":'),
        # review R1: values that used to pass validation and then fail the whole batch
        event_bytes(transaction_id="test-1"),  # not a UUID → Postgres ::uuid cast fails
        event_bytes(Time=float("inf")),  # Infinity → NaN hour features → VectorAssembler error
        event_bytes(Amount=float("-inf")),
        event_bytes(V7=float("inf")),
        event_bytes(replay_pass=-1),
    ]
    parsed = parse_transactions(raw_frame(spark, payloads))
    assert parsed.count() == len(payloads)
    assert parsed.filter(is_valid_transaction()).count() == 1


def test_infinity_tokens_are_parsed_then_rejected(spark) -> None:
    """Spark's JSON reader accepts Infinity tokens; the filter must catch them."""
    parsed = parse_transactions(raw_frame(spark, [event_bytes(Time=float("inf"))]))
    assert parsed.first()["Time"] == float("inf")
    assert parsed.filter(is_valid_transaction()).count() == 0


def test_valid_rows_score_without_errors_even_next_to_poison_pills(spark, tiny_model) -> None:
    payloads = [event_bytes(0), event_bytes(Time=float("inf")), event_bytes(transaction_id="x")]
    parsed = parse_transactions(raw_frame(spark, payloads)).filter(is_valid_transaction())
    assert score(parsed, tiny_model).count() == 1  # no VectorAssembler error


def test_scoring_ignores_the_true_label(spark, tiny_model) -> None:
    """Leakage guard: flipping actual_class must not change the score."""
    legit = event_bytes(0)
    flipped = json.loads(legit)
    flipped["actual_class"] = 1 - flipped["actual_class"]
    frame = parse_transactions(raw_frame(spark, [legit, json.dumps(flipped).encode()]))
    probabilities = [r["fraud_probability"] for r in score(frame, tiny_model).collect()]
    assert probabilities[0] == probabilities[1]


# --------------------------------------------------------------------------- score / enrich / label
def test_score_and_enrich(spark, tiny_model) -> None:
    event_time = datetime.now(UTC) - timedelta(seconds=2)
    payloads = [
        event_bytes(0, event_time=event_time),
        event_bytes(0, frauds=True, event_time=event_time),
    ]
    parsed = parse_transactions(raw_frame(spark, payloads))
    scored = score(parsed, tiny_model)
    probabilities = [r["fraud_probability"] for r in scored.select("fraud_probability").collect()]
    assert all(0.0 <= p <= 1.0 for p in probabilities)
    assert probabilities[1] > probabilities[0]  # the fraud scores higher

    enriched = enrich(scored, "test-model").collect()
    for row in enriched:
        assert 2000 <= row["latency_ms"] < 60_000
        assert row["model_name"] == "test-model"
        assert 0 <= row["hour"] <= 23
    assert set(enrich(scored, "m").columns) == set(PREDICTION_FIELDS) - {"prediction", "threshold"}


def test_hour_is_derived_from_time(spark, tiny_model) -> None:
    payload = event_bytes(Time=5 * 3600 + 59 * 60 + 30.0 + 2 * 86400)
    enriched = enrich(score(parse_transactions(raw_frame(spark, [payload])), tiny_model), "m")
    assert enriched.first()["hour"] == 5


def _labelled_input(spark, probabilities: list[float]):  # type: ignore[no-untyped-def]
    now = datetime.now(UTC).replace(tzinfo=None)
    rows = [
        (str(uuid.uuid4()), now, now, 5.0, "replay", 1, 10.0, 3, p, None, "m")
        for p in probabilities
    ]
    schema = (
        "transaction_id string, event_time timestamp, processed_at timestamp, latency_ms double, "
        "source string, replay_pass int, amount double, hour int, fraud_probability double, "
        "actual_class int, model_name string"
    )
    return spark.createDataFrame(rows, schema)


def test_apply_threshold_inclusive_boundary(spark) -> None:
    labelled = apply_threshold(_labelled_input(spark, [0.49, 0.5, 0.51]), 0.5)
    assert labelled.columns == list(PREDICTION_FIELDS)
    assert [r["prediction"] for r in labelled.orderBy("fraud_probability").collect()] == [0, 1, 1]
    assert {r["threshold"] for r in labelled.collect()} == {0.5}


@pytest.mark.parametrize("bad", [0.0, 1.0, -0.1, 1.5])
def test_apply_threshold_rejects_invalid(spark, bad: float) -> None:
    with pytest.raises(ValueError):
        apply_threshold(_labelled_input(spark, [0.5]), bad)


def test_kafka_records_match_prediction_schema(spark) -> None:
    labelled = apply_threshold(_labelled_input(spark, [0.9]), 0.4)
    record = to_kafka_records(labelled).first()
    payload = json.loads(record["value"])
    assert record["key"] == payload["transaction_id"]
    assert "actual_class" not in payload  # null fields are omitted
    assert payload["event_time"].endswith("Z")
    assert PredictionEvent.model_validate(payload).prediction == 1


# --------------------------------------------------------------------------- sinks
def _prediction_row(index: int) -> Row:
    values = {column: f"v{index}" for column in PREDICTION_DB_COLUMNS}
    values["event_time"] = datetime(2026, 11, 14, 18, 0)  # naive, as PySpark returns it
    return Row(**values)


def test_row_to_db_tuple_orders_columns_and_marks_utc() -> None:
    values = _prediction_row(1).asDict()
    as_tuple = sinks.row_to_db_tuple(values)
    assert len(as_tuple) == len(PREDICTION_DB_COLUMNS)
    assert as_tuple[0] == "v1"
    assert as_tuple[1].tzinfo is UTC


def test_write_partition_batches_inserts(monkeypatch: pytest.MonkeyPatch) -> None:
    connections: list[FakeConnection] = []

    def fake_connect(**kwargs: Any) -> FakeConnection:
        assert kwargs["connect_timeout"] == 10 and kwargs["host"] == "pg"
        connections.append(FakeConnection())
        return connections[-1]

    monkeypatch.setattr(sinks.psycopg, "connect", fake_connect)
    sinks.write_partition_to_postgres(
        [_prediction_row(i) for i in range(5)], {"host": "pg"}, batch_size=2
    )
    assert len(connections) == 1
    batches = [len(c.executemany_calls[0][1]) for c in connections[0].cursors]
    assert batches == [2, 2, 1]
    assert connections[0].committed and connections[0].closed


def test_write_partition_skips_empty_partitions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sinks.psycopg, "connect", lambda **kw: pytest.fail("no connection for empty partition")
    )
    sinks.write_partition_to_postgres(iter([]), {"host": "pg"})


def test_write_postgres_uses_foreach_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    frame = MagicMock()
    frame.select.return_value = frame
    sinks.write_postgres(frame, {"host": "pg"})
    frame.select.assert_called_once_with(*PREDICTION_DB_COLUMNS)
    writer = frame.foreachPartition.call_args.args[0]
    connections: list[FakeConnection] = []
    monkeypatch.setattr(
        sinks.psycopg,
        "connect",
        lambda **kw: connections.append(FakeConnection()) or connections[-1],
    )
    writer([_prediction_row(0)])
    assert len(connections) == 1


def test_write_kafka_builds_batch_writer(monkeypatch: pytest.MonkeyPatch) -> None:
    records = MagicMock()
    monkeypatch.setattr(sinks, "to_kafka_records", lambda df: records)
    sinks.write_kafka(MagicMock(), "kafka:9092", "predictions")
    chain = records.write.format
    chain.assert_called_once_with("kafka")
    options = chain.return_value.option
    options.assert_called_once_with("kafka.bootstrap.servers", "kafka:9092")
    options.return_value.option.assert_called_once_with("kafka.compression.type", "lz4")
    topic_option = options.return_value.option.return_value.option
    topic_option.assert_called_once_with("topic", "predictions")
    topic_option.return_value.save.assert_called_once()


# --------------------------------------------------------------------------- threshold provider
def test_threshold_provider_reads_and_falls_back(monkeypatch: pytest.MonkeyPatch) -> None:
    values: list[Any] = [0.3, None, psycopg.OperationalError("db down"), 0.6]

    def fake_get_threshold(conn: Any) -> Any:
        value = values.pop(0)
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(threshold, "get_threshold", fake_get_threshold)
    opened: list[FakeConnection] = []
    logger = FakeLogger()
    provider = threshold.ThresholdProvider(
        lambda: opened.append(FakeConnection()) or opened[-1], 0.5, logger
    )
    assert provider.current() == 0.3  # value from the database
    assert provider.current() == 0.3  # unset → keep last good value
    assert provider.current() == 0.3  # error → keep last good value, drop connection
    assert opened[0].closed
    assert provider.current() == 0.6  # reconnected
    assert len(opened) == 2
    assert logger.events() == ["threshold.changed", "threshold.read_failed", "threshold.changed"]
    provider.close()
    assert opened[1].closed and provider.last == 0.6


def test_threshold_provider_validates_default() -> None:
    with pytest.raises(ValueError):
        threshold.ThresholdProvider(lambda: FakeConnection(), 1.5)


# --------------------------------------------------------------------------- listener
def _progress(**overrides: Any) -> SimpleNamespace:
    values = {
        "id": "query-1",
        "runId": "run-1",
        "batchId": 3,
        "numInputRows": 10,
        "inputRowsPerSecond": float("nan"),
        "processedRowsPerSecond": 5.0,
        "durationMs": {"triggerExecution": 120},
        "sources": [
            SimpleNamespace(endOffset='{"transactions": {"0": 5, "1": 7}}'),
            SimpleNamespace(endOffset=None),
            SimpleNamespace(endOffset="not json"),
        ],
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_progress_to_record() -> None:
    record = listener_module.progress_to_record(_progress())
    assert record == {
        "query_id": "query-1",
        "run_id": "run-1",
        "batch_id": 3,
        "num_input_rows": 10,
        "input_rows_per_second": None,  # NaN is not stored
        "processed_rows_per_second": 5.0,
        "batch_duration_ms": 120,
        "end_offsets": {"transactions": {"0": 5, "1": 7}},
    }
    minimal = listener_module.progress_to_record(_progress(sources=None, durationMs=None))
    assert minimal["end_offsets"] == {} and minimal["batch_duration_ms"] is None


def test_progress_listener(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    stored: list[dict[str, Any]] = []
    pruned: list[Any] = []
    outcomes: list[Exception | None] = [None, RuntimeError("insert failed"), None]

    def fake_insert(conn: Any, record: dict[str, Any]) -> None:
        outcome = outcomes.pop(0)
        if outcome is not None:
            raise outcome
        stored.append(record)

    monkeypatch.setattr(listener_module, "insert_stream_progress", fake_insert)
    monkeypatch.setattr(listener_module, "prune_stream_progress", lambda conn: pruned.append(conn))
    retention: list[float] = []
    monkeypatch.setattr(
        listener_module, "prune_predictions", lambda conn, hours: retention.append(hours) or 42
    )
    monkeypatch.setattr(listener_module, "PRUNE_EVERY_EVENTS", 2)
    connections: list[FakeConnection] = []
    logger = FakeLogger()
    heartbeat = Heartbeat(tmp_path / "stream.heartbeat")
    listener = listener_module.ProgressListener(
        heartbeat,
        lambda: connections.append(FakeConnection()) or connections[-1],
        logger,
        predictions_retention_hours=3,
    )
    listener.onQueryStarted(SimpleNamespace(id="q", runId="r"))
    assert heartbeat.path.is_file()
    for _ in range(3):
        listener.onQueryProgress(SimpleNamespace(progress=_progress()))
    assert len(stored) == 2
    assert len(connections) == 2  # reconnected after the failed insert
    assert pruned  # pruning ran after the 2nd successful insert
    assert retention == [3]  # predictions are pruned to the retention window too
    assert "retention.pruned" in logger.events()
    listener.onQueryIdle(SimpleNamespace())
    listener.onQueryTerminated(SimpleNamespace(id="q", exception=None))
    assert connections[-1].closed
    assert {"query.started", "progress.store_failed", "query.terminated"} <= set(logger.events())


# --------------------------------------------------------------------------- foreachBatch
class StaticThreshold:
    def __init__(self, value: float) -> None:
        self.value = value

    def current(self) -> float:
        return self.value


def test_batch_processor_fans_out_to_all_sinks(
    spark, tiny_model, monkeypatch: pytest.MonkeyPatch
) -> None:
    kafka_writes: dict[str, list[Row]] = {}
    postgres_writes: list[Row] = []
    monkeypatch.setattr(
        batch_module,
        "write_kafka",
        lambda df, bootstrap, topic: kafka_writes.setdefault(topic, []).extend(df.collect()),
    )
    monkeypatch.setattr(
        batch_module, "write_postgres", lambda df, kwargs: postgres_writes.extend(df.collect())
    )
    logger = FakeLogger()
    processor = batch_module.BatchProcessor(
        model=tiny_model,
        model_name="tiny",
        bootstrap_servers="kafka:9092",
        predictions_topic="predictions",
        alerts_topic="fraud-alerts",
        postgres_connect_kwargs={"host": "pg"},
        threshold_provider=StaticThreshold(0.5),  # type: ignore[arg-type]
        logger=logger,
    )
    payloads = (
        [event_bytes(i) for i in range(3)]
        + [event_bytes(i, frauds=True) for i in range(2)]
        + [b"junk"]
    )
    processor(parse_transactions(raw_frame(spark, payloads)), 7)

    assert len(kafka_writes["predictions"]) == 5
    assert len(postgres_writes) == 5
    alerts = kafka_writes["fraud-alerts"]
    assert alerts and all(row["prediction"] == 1 for row in alerts)
    assert {row["actual_class"] for row in alerts} == {1}
    assert all(row["threshold"] == 0.5 and row["model_name"] == "tiny" for row in postgres_writes)
    assert logger.events("warning") == ["batch.invalid_rows"]
    processed = next(fields for _, event, fields in logger.records if event == "batch.processed")
    assert processed["rows"] == 5 and processed["batch_id"] == 7


def test_batch_processor_skips_batches_without_valid_rows(
    spark, tiny_model, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(batch_module, "write_kafka", lambda *a: pytest.fail("nothing to write"))
    monkeypatch.setattr(batch_module, "write_postgres", lambda *a: pytest.fail("nothing to write"))
    processor = batch_module.BatchProcessor(
        model=tiny_model,
        model_name="tiny",
        bootstrap_servers="k",
        predictions_topic="p",
        alerts_topic="a",
        postgres_connect_kwargs={},
        threshold_provider=StaticThreshold(0.5),  # type: ignore[arg-type]
        logger=FakeLogger(),
    )
    processor(parse_transactions(raw_frame(spark, [b"junk"])), 1)
    processor(parse_transactions(raw_frame(spark, [b"x"])).filter(F.lit(False)), 2)  # empty batch


# --------------------------------------------------------------------------- stream processor glue
def test_load_model_metadata(tmp_path: Path) -> None:
    paths = get_path_settings()
    with pytest.raises(stream_processor.ModelNotTrainedError, match=r"run.ps1 train"):
        stream_processor.load_model_metadata(paths.metrics_path, paths.model_path)
    (paths.model_path / "metadata").mkdir(parents=True)
    paths.metrics_path.write_text(
        json.dumps({"run_id": "r1", "best": {"display_name": "RF", "threshold": 0.386}}),
        encoding="utf-8",
    )
    assert stream_processor.load_model_metadata(paths.metrics_path, paths.model_path) == {
        "run_id": "r1",
        "model_name": "RF",
        "default_threshold": 0.386,
    }


def test_build_stream_uses_kafka_source(spark) -> None:
    """Also proves the spark-sql-kafka connector is on the classpath of the image."""
    stream = stream_processor.build_stream(
        spark, "kafka:9092", "transactions", get_streaming_settings()
    )
    assert stream.isStreaming
    assert stream.columns == list(TRANSACTION_FIELD_TYPES)
