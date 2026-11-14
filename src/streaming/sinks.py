"""Batch sinks used inside ``foreachBatch``: Kafka topics and PostgreSQL."""

from __future__ import annotations

import itertools
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime
from functools import partial
from typing import Any

import psycopg
from pyspark.sql import DataFrame

from src.common.db import PREDICTION_DB_COLUMNS, insert_predictions
from src.streaming.transforms import to_kafka_records

POSTGRES_INSERT_BATCH = 500


def write_kafka(predictions: DataFrame, bootstrap_servers: str, topic: str) -> None:
    """Write prediction rows as JSON to a Kafka topic (batch write)."""
    (
        to_kafka_records(predictions)
        .write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("kafka.compression.type", "lz4")
        .option("topic", topic)
        .save()
    )


def _db_value(value: Any) -> Any:
    # PySpark returns naive datetimes in the session/JVM time zone, which is UTC here.
    if isinstance(value, datetime) and value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def row_to_db_tuple(row: Mapping[str, Any]) -> tuple[Any, ...]:
    """Order a prediction row's values as ``PREDICTION_DB_COLUMNS``."""
    return tuple(_db_value(row[column]) for column in PREDICTION_DB_COLUMNS)


def write_partition_to_postgres(
    rows: Iterable[Any], connect_kwargs: Mapping[str, Any], batch_size: int = POSTGRES_INSERT_BATCH
) -> None:
    """Insert one Spark partition in a single transaction (runs on the executors).

    ``ON CONFLICT DO NOTHING`` makes the write idempotent, so a micro-batch replayed after
    a crash never duplicates rows.
    """
    iterator = iter(rows)
    first = next(iterator, None)
    if first is None:
        return  # don't open a connection for an empty partition
    buffer: list[tuple[Any, ...]] = []
    with psycopg.connect(**connect_kwargs, connect_timeout=10) as conn:
        for row in itertools.chain([first], iterator):
            buffer.append(row_to_db_tuple(row.asDict()))
            if len(buffer) >= batch_size:
                insert_predictions(conn, buffer)
                buffer.clear()
        insert_predictions(conn, buffer)
    # leaving the block commits the transaction and closes the connection


def write_postgres(predictions: DataFrame, connect_kwargs: Mapping[str, Any]) -> None:
    """Upsert prediction rows into ``predictions`` via ``foreachPartition``."""
    writer = partial(write_partition_to_postgres, connect_kwargs=dict(connect_kwargs))
    predictions.select(*PREDICTION_DB_COLUMNS).foreachPartition(writer)
