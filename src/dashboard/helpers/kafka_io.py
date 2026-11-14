"""Kafka access from the dashboard: manual test transactions, producer control, topic stats."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

from src.common.kafka_utils import producer_config, topic_end_offsets
from src.common.schemas import CONTROL_MESSAGE_KEY, ControlCommand, TransactionEvent


class PublishError(RuntimeError):
    """A message could not be delivered to Kafka."""


def make_producer(bootstrap_servers: str) -> Producer:
    return Producer(producer_config(bootstrap_servers, client_id="dashboard", **{"linger.ms": 0}))


def _publish(producer: Any, topic: str, key: bytes, value: bytes, timeout_s: float) -> None:
    errors: list[str] = []

    def _on_delivery(error: Any, _message: Any) -> None:
        if error is not None:
            errors.append(str(error))

    producer.produce(topic, key=key, value=value, on_delivery=_on_delivery)
    remaining = producer.flush(timeout_s)
    if remaining:
        raise PublishError(f"message to '{topic}' not delivered within {timeout_s:g}s")
    if errors:
        raise PublishError(f"delivery to '{topic}' failed: {errors[0]}")


def publish_transaction(
    producer: Any, topic: str, event: TransactionEvent, timeout_s: float = 5.0
) -> None:
    """Send one transaction and wait for the broker's acknowledgement."""
    _publish(producer, topic, event.key, event.to_json_bytes(), timeout_s)


def publish_control(
    producer: Any, topic: str, command: ControlCommand, timeout_s: float = 5.0
) -> None:
    """Send a producer control command (key ``producer``, compacted topic)."""
    _publish(producer, topic, CONTROL_MESSAGE_KEY, command.to_json_bytes(), timeout_s)


def read_last_control(
    bootstrap_servers: str,
    topic: str,
    timeout_s: float = 3.0,
    consumer_factory: Callable[[dict[str, Any]], Any] = Consumer,
) -> ControlCommand | None:
    """The most recent control command (``None`` if none was ever sent)."""
    consumer = consumer_factory(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "dashboard-control-reader",
            "enable.auto.commit": False,
        }
    )
    try:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=timeout_s)
        if high <= low:
            return None
        consumer.assign([TopicPartition(topic, 0, high - 1)])
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            message = consumer.poll(0.25)
            if message is None or message.error():
                continue
            try:
                return ControlCommand.model_validate_json(message.value())
            except ValueError:
                return None
        return None
    finally:
        consumer.close()


def topic_overview(
    bootstrap_servers: str,
    topics: Iterable[str],
    timeout_s: float = 3.0,
    consumer_factory: Callable[[dict[str, Any]], Any] = Consumer,
) -> dict[str, dict[int, int]]:
    """``{topic: {partition: end_offset}}`` for each topic (empty dict if a topic is missing).

    Raises:
        KafkaException: if the broker is unreachable.
    """
    consumer = consumer_factory(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "dashboard-health",
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.list_topics(timeout=timeout_s)  # fail fast if the broker is down
        return {topic: topic_end_offsets(consumer, topic, timeout_s) for topic in topics}
    finally:
        consumer.close()


__all__ = [
    "KafkaException",
    "PublishError",
    "make_producer",
    "publish_control",
    "publish_transaction",
    "read_last_control",
    "topic_overview",
]
