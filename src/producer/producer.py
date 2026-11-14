"""Producer service: replays the held-out **test split only** into the ``transactions`` topic.

Run inside the ``fraud-app`` image::

    python -m src.producer.producer
"""

from __future__ import annotations

import signal
import socket
import sys
import threading
import time
from collections.abc import Callable
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message, Producer

from src.common.config import (
    get_kafka_settings,
    get_logging_settings,
    get_path_settings,
    get_producer_settings,
)
from src.common.health import Heartbeat
from src.common.kafka_utils import producer_config, wait_for_kafka
from src.common.logging_setup import configure_logging, get_logger
from src.common.schemas import TransactionEvent
from src.producer.control import ControlListener, ControlState, control_consumer_config
from src.producer.rate_limiter import RateLimiter
from src.producer.replay import ReplayDataMissingError, TransactionReplayer, load_replay_dataset

SERVICE = "producer"

logger = get_logger(__name__)


class DeliveryStats:
    """Counts delivery reports from librdkafka (called on the producer's poll thread)."""

    def __init__(self) -> None:
        self.delivered = 0
        self.failed = 0

    def __call__(self, error: KafkaError | None, message: Message) -> None:
        if error is None:
            self.delivered += 1
            return
        self.failed += 1
        if self.failed <= 10 or self.failed % 1000 == 0:
            logger.warning("delivery.failed", error=str(error), failed_total=self.failed)


def make_sender(
    producer: Any, topic: str, on_delivery: Callable[[KafkaError | None, Message], None]
) -> Callable[[TransactionEvent], None]:
    """Serialise and enqueue an event, waiting briefly if the local queue is full."""

    def send(event: TransactionEvent) -> None:
        payload, key = event.to_json_bytes(), event.key
        while True:
            try:
                producer.produce(topic, value=payload, key=key, on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)  # let librdkafka drain its queue, then retry
        producer.poll(0)

    return send


class ProgressReporter:
    """Periodic progress log + heartbeat, driven from the replay loop."""

    def __init__(
        self,
        replayer_ref: Callable[[], TransactionReplayer | None],
        state: ControlState,
        stats: DeliveryStats,
        heartbeat: Heartbeat,
        interval_s: float,
    ) -> None:
        self._replayer_ref = replayer_ref
        self._state = state
        self._stats = stats
        self._heartbeat = heartbeat
        self._interval_s = interval_s
        self._last_log = time.monotonic()
        self._last_sent = 0

    def __call__(self) -> None:
        self._heartbeat.beat()
        now = time.monotonic()
        elapsed = now - self._last_log
        if elapsed < self._interval_s:
            return
        replayer = self._replayer_ref()
        sent = replayer.sent if replayer else 0
        paused, rate = self._state.snapshot()
        logger.info(
            "producer.progress",
            sent_total=sent,
            delivered_total=self._stats.delivered,
            failed_total=self._stats.failed,
            actual_rate=round((sent - self._last_sent) / elapsed, 1),
            target_rate=rate,
            paused=paused,
            replay_pass=replayer.replay_pass if replayer else 0,
        )
        self._last_log, self._last_sent = now, sent


def main() -> int:
    configure_logging(SERVICE, get_logging_settings().log_level)
    kafka = get_kafka_settings()
    paths = get_path_settings()
    settings = get_producer_settings()

    stop_event = threading.Event()

    def _request_stop(signum: int, _frame: Any) -> None:
        logger.info("shutdown.requested", signal=signal.Signals(signum).name)
        stop_event.set()

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    try:
        dataset = load_replay_dataset(paths.test_split_path)
    except ReplayDataMissingError as exc:
        logger.error("replay.data_missing", error=str(exc))
        return 2
    logger.info(
        "replay.loaded",
        rows=len(dataset),
        frauds=int(dataset.labels.sum()),
        rate=settings.producer_rate,
        loop=settings.producer_loop,
    )

    wait_for_kafka(
        kafka.kafka_bootstrap_servers,
        required_topics=(kafka.topic_transactions, kafka.topic_producer_control),
        logger=logger,
    )

    state = ControlState(rate=settings.producer_rate)
    group_id = f"producer-control-{socket.gethostname()}"
    listener = ControlListener(
        lambda: Consumer(control_consumer_config(kafka.kafka_bootstrap_servers, group_id)),
        kafka.topic_producer_control,
        state,
        logger,
    )
    listener.start()

    producer = Producer(producer_config(kafka.kafka_bootstrap_servers, client_id=SERVICE))
    stats = DeliveryStats()
    heartbeat = Heartbeat(paths.heartbeat_path(SERVICE))
    heartbeat.beat(force=True)

    replayer_holder: list[TransactionReplayer] = []
    reporter = ProgressReporter(
        lambda: replayer_holder[0] if replayer_holder else None,
        state,
        stats,
        heartbeat,
        settings.producer_log_interval_seconds,
    )
    replayer = TransactionReplayer(
        dataset,
        make_sender(producer, kafka.topic_transactions, stats),
        state,
        RateLimiter(settings.producer_rate),
        stop_event,
        loop=settings.producer_loop,
        on_tick=reporter,
    )
    replayer_holder.append(replayer)

    logger.info("producer.started", topic=kafka.topic_transactions)
    try:
        replayer.run()
    finally:
        listener.stop()
        remaining = producer.flush(10)
        logger.info(
            "shutdown.complete",
            sent_total=replayer.sent,
            delivered_total=stats.delivered,
            failed_total=stats.failed,
            undelivered=remaining,
        )
        listener.join(timeout=5)
    return 0


if __name__ == "__main__":
    sys.exit(main())
