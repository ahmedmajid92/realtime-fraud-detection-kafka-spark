"""Fixtures for end-to-end tests against the running Docker Compose stack.

Run with ``.\\run.ps1 e2e`` (pytest inside the ``e2e`` container on the Compose network).
"""

from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import psycopg
import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from psycopg.rows import dict_row

from src.common import db
from src.common.config import (
    KafkaSettings,
    PathSettings,
    PostgresSettings,
    get_kafka_settings,
    get_path_settings,
    get_postgres_settings,
)
from src.common.kafka_utils import producer_config, topic_end_offsets
from src.common.schemas import RAW_INPUT_COLUMNS, Source, TransactionEvent, utc_now

pytestmark = pytest.mark.e2e


@pytest.fixture(scope="session")
def kafka() -> KafkaSettings:
    return get_kafka_settings()


@pytest.fixture(scope="session")
def pg() -> PostgresSettings:
    return get_postgres_settings()


@pytest.fixture(scope="session")
def paths() -> PathSettings:
    return get_path_settings()


@pytest.fixture(scope="session")
def urls() -> dict[str, str]:
    return {
        "dashboard": os.environ.get("DASHBOARD_URL", "http://dashboard:8501"),
        "kafka_ui": os.environ.get("KAFKA_UI_URL", "http://kafka-ui:8080"),
    }


@pytest.fixture(scope="session")
def metrics(paths: PathSettings) -> dict[str, Any]:
    return json.loads(paths.metrics_path.read_text(encoding="utf-8"))


@pytest.fixture(scope="session")
def demo_samples(paths: PathSettings) -> dict[str, Any]:
    return json.loads(paths.demo_samples_path.read_text(encoding="utf-8"))


@pytest.fixture
def conn(pg: PostgresSettings) -> Iterator[psycopg.Connection[Any]]:
    with db.connect(pg) as connection:
        yield connection


@pytest.fixture(scope="session")
def producer(kafka: KafkaSettings) -> Iterator[Producer]:
    client = Producer(producer_config(kafka.kafka_bootstrap_servers, client_id="e2e"))
    yield client
    client.flush(10)


@pytest.fixture
def consumer_at_end(kafka: KafkaSettings) -> Iterator[Callable[[str], Consumer]]:
    """Factory: a consumer assigned to every partition of a topic at its current end."""
    created: list[Consumer] = []

    def factory(topic: str) -> Consumer:
        consumer = Consumer(
            {
                "bootstrap.servers": kafka.kafka_bootstrap_servers,
                "group.id": f"e2e-{uuid.uuid4().hex[:8]}",
                "enable.auto.commit": False,
            }
        )
        offsets = topic_end_offsets(consumer, topic)
        consumer.assign([TopicPartition(topic, p, o) for p, o in offsets.items()])
        created.append(consumer)
        return consumer

    yield factory
    for consumer in created:
        consumer.close()


def make_event(sample: dict[str, Any], *, source: Source = Source.E2E) -> TransactionEvent:
    """A fresh transaction from a known test-split sample."""
    return TransactionEvent(
        transaction_id=uuid.uuid4(),
        event_time=utc_now(),
        source=source,
        replay_pass=0,
        actual_class=int(sample["Class"]),
        **{column: float(sample[column]) for column in RAW_INPUT_COLUMNS},
    )


def publish(producer: Producer, topic: str, event: TransactionEvent) -> None:
    producer.produce(topic, key=event.key, value=event.to_json_bytes())
    assert producer.flush(10) == 0, "message not delivered to Kafka"


def wait_for_message(
    consumer: Consumer, predicate: Callable[[dict[str, Any]], bool], timeout_s: float
) -> dict[str, Any] | None:
    """Poll until a JSON message satisfying ``predicate`` arrives (or the timeout passes)."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        message = consumer.poll(0.5)
        if message is None or message.error():
            continue
        payload = json.loads(message.value())
        if predicate(payload):
            return payload
    return None


def wait_for_row(
    connection: psycopg.Connection[Any], transaction_id: str, timeout_s: float
) -> dict[str, Any] | None:
    """Poll Postgres until the prediction for ``transaction_id`` is stored."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                "SELECT *, EXTRACT(EPOCH FROM stored_at - event_time) * 1000 AS e2e_ms "
                "FROM predictions WHERE transaction_id = %s::uuid",
                (transaction_id,),
            )
            row = cursor.fetchone()
        if row is not None:
            return row
        time.sleep(0.5)
    return None
