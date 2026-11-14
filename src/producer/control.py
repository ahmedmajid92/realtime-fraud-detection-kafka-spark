"""Live producer control through the compacted ``producer-control`` Kafka topic.

The dashboard publishes :class:`~src.common.schemas.ControlCommand` messages. The producer
reads the topic **from the beginning** at startup, so the last command (e.g. "paused at
20 tx/s") survives a producer restart, and then keeps following new commands.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
from pydantic import ValidationError

from src.common.schemas import ControlCommand


class ControlState:
    """Thread-safe producer state changed by control commands."""

    def __init__(self, rate: float, paused: bool = False) -> None:
        self._lock = threading.Lock()
        self._rate = float(rate)
        self._paused = paused

    def snapshot(self) -> tuple[bool, float]:
        """``(paused, rate)`` read atomically."""
        with self._lock:
            return self._paused, self._rate

    @property
    def paused(self) -> bool:
        return self.snapshot()[0]

    @property
    def rate(self) -> float:
        return self.snapshot()[1]

    def apply(self, command: ControlCommand) -> None:
        """Adopt the full desired state carried by the command."""
        with self._lock:
            self._paused = command.paused
            self._rate = float(command.rate)


def parse_control_message(value: bytes | None, logger: Any | None = None) -> ControlCommand | None:
    """Validate a control message; invalid messages are logged and ignored."""
    if not value:
        return None
    try:
        return ControlCommand.model_validate_json(value)
    except ValidationError as exc:
        if logger is not None:
            logger.warning("control.invalid_message", error=exc.errors(include_url=False)[:3])
        return None


class ControlListener(threading.Thread):
    """Background thread applying control commands to a :class:`ControlState`.

    * ``caught_up`` is set once every message that existed at startup has been applied,
      so the producer can wait for the retained state (e.g. "paused") before replaying,
      instead of sending a burst at the default rate.
    * If consuming fails unexpectedly, the listener logs it, waits (exponential backoff up
      to 30 s) and reconnects, so control is never silently lost while the producer runs.
    """

    def __init__(
        self,
        consumer_factory: Callable[[], Consumer],
        topic: str,
        state: ControlState,
        logger: Any,
        poll_timeout_s: float = 0.5,
        retry_delay_s: float = 1.0,
    ) -> None:
        super().__init__(name="control-listener", daemon=True)
        self._consumer_factory = consumer_factory
        self._topic = topic
        self._state = state
        self._logger = logger
        self._poll_timeout_s = poll_timeout_s
        self._retry_delay_s = retry_delay_s
        self._stop_event = threading.Event()
        self.caught_up = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        delay = self._retry_delay_s
        while not self._stop_event.is_set():
            consumer = None
            try:
                consumer = self._consumer_factory()
                self._consume(consumer)
            except Exception:  # keep producing with the last known state, then reconnect
                self._logger.exception("control.listener_crashed", retry_in_s=delay)
                self.caught_up.set()  # never block the producer on a broken control topic
            finally:
                if consumer is not None:
                    consumer.close()
            if self._stop_event.wait(delay):
                break
            delay = min(delay * 2, 30.0)

    def _consume(self, consumer: Consumer) -> None:
        partition = TopicPartition(self._topic, 0, OFFSET_BEGINNING)
        consumer.assign([partition])
        low, high = consumer.get_watermark_offsets(TopicPartition(self._topic, 0), timeout=5.0)
        if high <= low:
            self.caught_up.set()  # nothing retained: the start-up defaults apply
        while not self._stop_event.is_set():
            message = consumer.poll(self._poll_timeout_s)
            if message is None:
                continue
            if message.error():
                self._logger.warning("control.consume_error", error=str(message.error()))
                continue
            command = parse_control_message(message.value(), self._logger)
            if command is not None:
                self._state.apply(command)
                paused, rate = self._state.snapshot()
                self._logger.info(
                    "control.applied", command=command.command, paused=paused, rate=rate
                )
            if message.offset() >= high - 1:
                self.caught_up.set()


def control_consumer_config(bootstrap_servers: str, group_id: str) -> dict[str, Any]:
    """Consumer config for reading the control topic without committing offsets."""
    return {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    }
