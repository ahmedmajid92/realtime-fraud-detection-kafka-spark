"""Kafka helpers built on ``confluent-kafka`` (producer, dashboard, e2e tests)."""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable
from typing import Any

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient


def producer_config(bootstrap_servers: str, client_id: str, **overrides: Any) -> dict[str, Any]:
    """Reliable producer defaults: idempotent, acks from all in-sync replicas, small linger."""
    config: dict[str, Any] = {
        "bootstrap.servers": bootstrap_servers,
        "client.id": client_id,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 10,
        "compression.type": "lz4",
        "message.timeout.ms": 30_000,
    }
    config.update(overrides)
    return config


def wait_for_kafka(
    bootstrap_servers: str,
    *,
    required_topics: Iterable[str] = (),
    timeout_s: float = 180.0,
    logger: Any | None = None,
    admin_factory: Callable[[dict[str, Any]], Any] = AdminClient,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Block until the broker answers and every ``required_topics`` entry exists.

    Retries with exponential backoff (1 s, 2 s, 4 s ... capped at 10 s).

    Raises:
        TimeoutError: if Kafka is not ready within ``timeout_s``.
    """
    required = set(required_topics)
    admin = admin_factory({"bootstrap.servers": bootstrap_servers})
    deadline = clock() + timeout_s
    delay = 1.0
    while True:
        try:
            metadata = admin.list_topics(timeout=5)
            missing = required - set(metadata.topics)
            if not missing:
                if logger is not None:
                    logger.info("kafka.ready", bootstrap_servers=bootstrap_servers)
                return
            reason = f"missing topics {sorted(missing)}"
        except KafkaException as exc:
            reason = str(exc)
        if clock() >= deadline:
            raise TimeoutError(
                f"Kafka at {bootstrap_servers} not ready after {timeout_s:.0f}s: {reason}"
            )
        if logger is not None:
            logger.info("kafka.waiting", reason=reason, retry_in_s=delay)
        sleep(delay)
        delay = min(delay * 2, 10.0)


def topic_end_offsets(client: Consumer, topic: str, timeout_s: float = 5.0) -> dict[int, int]:
    """High-watermark offset of every partition of ``topic`` (``{partition: offset}``).

    Returns an empty dict if the topic does not exist.
    """
    metadata = client.list_topics(topic, timeout=timeout_s)
    topic_metadata = metadata.topics.get(topic)
    if topic_metadata is None or topic_metadata.error is not None:
        return {}
    offsets: dict[int, int] = {}
    for partition in sorted(topic_metadata.partitions):
        _, high = client.get_watermark_offsets(
            TopicPartition(topic, partition), timeout=timeout_s, cached=False
        )
        offsets[partition] = int(high)
    return offsets
