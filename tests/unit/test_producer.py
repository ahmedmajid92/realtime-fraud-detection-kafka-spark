"""Producer: rate limiting, control commands, replay loop, serialization (Kafka mocked)."""

from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from confluent_kafka import OFFSET_BEGINNING

from src.common.config import get_path_settings
from src.common.health import Heartbeat
from src.common.schemas import ControlCommand, Source, TransactionEvent
from src.producer import producer as producer_module
from src.producer.control import (
    ControlListener,
    ControlState,
    control_consumer_config,
    parse_control_message,
)
from src.producer.rate_limiter import RateLimiter
from src.producer.replay import (
    ReplayDataMissingError,
    ReplayDataset,
    TransactionReplayer,
    build_event,
    load_replay_dataset,
)
from tests.conftest import make_transactions
from tests.unit.fakes import FakeConsumer, FakeLogger, FakeMessage, FakeProducer

NOW = datetime(2026, 11, 14, 18, 0, tzinfo=UTC)


class FakeClock:
    def __init__(self, start: float = 100.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


def command(action: str, paused: bool, rate: float = 50.0) -> ControlCommand:
    return ControlCommand(command=action, paused=paused, rate=rate, issued_at=NOW)  # type: ignore[arg-type]


# --------------------------------------------------------------------------- rate limiter
def test_rate_limiter_spacing() -> None:
    clock = FakeClock()
    limiter = RateLimiter(10, clock=clock)
    assert limiter.seconds_until_next() == 0.0  # first send goes immediately
    limiter.record_send()
    assert limiter.seconds_until_next() == pytest.approx(0.1)
    clock.now += 0.1
    assert limiter.seconds_until_next() == pytest.approx(0.0, abs=1e-9)


def test_rate_limiter_catches_up_short_stalls() -> None:
    clock = FakeClock()
    limiter = RateLimiter(10, clock=clock)
    limiter.record_send()  # next slot 100.1
    clock.now = 100.35  # a 0.25 s hiccup
    limiter.record_send()  # booked at 100.1 → next slot 100.2, already due
    assert limiter.seconds_until_next() == 0.0


def test_rate_limiter_caps_burst_after_long_stall() -> None:
    clock = FakeClock()
    limiter = RateLimiter(10, clock=clock, max_lag_s=1.0)
    limiter.record_send()
    clock.now = 160.0  # a minute-long stall
    burst = 0
    while limiter.seconds_until_next() == 0.0 and burst < 1000:
        limiter.record_send()
        burst += 1
    assert 9 <= burst <= 11  # at most ~max_lag_s × rate catch-up messages


def test_rate_limiter_set_rate_and_reset() -> None:
    clock = FakeClock()
    limiter = RateLimiter(10, clock=clock)
    limiter.record_send()
    limiter.set_rate(100)
    assert limiter.rate == 100
    assert limiter.seconds_until_next() == 0.0  # schedule restarted
    limiter.record_send()
    limiter.reset()
    assert limiter.seconds_until_next() == 0.0


@pytest.mark.parametrize("rate", [0, -1, 1000.5])
def test_rate_limiter_rejects_invalid_rate(rate: float) -> None:
    with pytest.raises(ValueError):
        RateLimiter(rate)


# --------------------------------------------------------------------------- control
def test_control_state_adopts_full_state() -> None:
    state = ControlState(rate=50)
    assert state.snapshot() == (False, 50.0)
    state.apply(command("pause", True, 20))
    assert state.paused is True and state.rate == 20.0
    state.apply(command("resume", False, 20))
    assert state.snapshot() == (False, 20.0)


def test_parse_control_message() -> None:
    logger = FakeLogger()
    valid = command("set_rate", False, 75).to_json_bytes()
    assert parse_control_message(valid, logger).rate == 75  # type: ignore[union-attr]
    assert parse_control_message(b"{not json", logger) is None
    assert parse_control_message(b'{"command": "pause"}', logger) is None
    assert parse_control_message(None, logger) is None
    assert logger.events("warning") == ["control.invalid_message", "control.invalid_message"]


def test_control_consumer_config_never_commits() -> None:
    cfg = control_consumer_config("k:9092", "g1")
    assert cfg["enable.auto.commit"] is False
    assert cfg["group.id"] == "g1"


def _wait_until(predicate, timeout: float = 5.0) -> None:  # type: ignore[no-untyped-def]
    deadline = time.monotonic() + timeout
    while not predicate() and time.monotonic() < deadline:
        time.sleep(0.01)


def test_control_listener_applies_commands_from_beginning() -> None:
    consumer = FakeConsumer(
        messages=[
            FakeMessage(command("pause", True, 30).to_json_bytes()),
            FakeMessage(b"garbage"),
            FakeMessage(None, error="broker hiccup"),
        ]
    )
    state, logger = ControlState(rate=50), FakeLogger()
    listener = ControlListener(
        lambda: consumer, "producer-control", state, logger, poll_timeout_s=0.01
    )
    listener.start()
    _wait_until(lambda: not consumer.messages)
    listener.stop()
    listener.join(timeout=5)
    assert not listener.is_alive()
    assert state.snapshot() == (True, 30.0)
    partition = consumer.assigned[0]  # type: ignore[index]
    assert (partition.topic, partition.partition, partition.offset) == (
        "producer-control",
        0,
        OFFSET_BEGINNING,
    )
    assert consumer.closed
    assert {"control.applied", "control.invalid_message", "control.consume_error"} <= set(
        logger.events()
    )


def test_control_listener_reconnects_after_a_crash() -> None:
    class BrokenConsumer(FakeConsumer):
        def assign(self, partitions):  # type: ignore[no-untyped-def]
            raise RuntimeError("cannot assign")

    broken = BrokenConsumer()
    healthy = FakeConsumer(messages=[FakeMessage(command("pause", True, 10).to_json_bytes())])
    consumers = [broken, healthy]
    state, logger = ControlState(rate=50), FakeLogger()
    listener = ControlListener(
        lambda: consumers.pop(0),
        "producer-control",
        state,
        logger,
        poll_timeout_s=0.01,
        retry_delay_s=0.01,
    )
    listener.start()
    _wait_until(lambda: state.paused)
    listener.stop()
    listener.join(timeout=5)
    assert logger.events("exception") == ["control.listener_crashed"]
    assert broken.closed and healthy.closed
    assert state.snapshot() == (True, 10.0)  # control works again after reconnecting


def test_control_listener_signals_caught_up_after_retained_messages() -> None:
    retained = [
        FakeMessage(command("pause", True, 30).to_json_bytes(), offset=0),
        FakeMessage(command("set_rate", True, 40).to_json_bytes(), offset=1),
    ]
    consumer = FakeConsumer(messages=retained, watermarks={("producer-control", 0): (0, 2)})
    state = ControlState(rate=50)
    listener = ControlListener(
        lambda: consumer, "producer-control", state, FakeLogger(), poll_timeout_s=0.01
    )
    listener.start()
    assert listener.caught_up.wait(5)
    assert state.snapshot() == (True, 40.0)  # every retained command applied before replay starts
    listener.stop()
    listener.join(timeout=5)


def test_control_listener_caught_up_immediately_on_empty_topic() -> None:
    listener = ControlListener(
        lambda: FakeConsumer(),
        "producer-control",
        ControlState(50),
        FakeLogger(),
        poll_timeout_s=0.01,
    )
    listener.start()
    assert listener.caught_up.wait(5)
    listener.stop()
    listener.join(timeout=5)


# --------------------------------------------------------------------------- replay data
def test_replay_dataset_sorts_by_time() -> None:
    frame = make_transactions(5, 2).sample(frac=1.0, random_state=1)
    dataset = ReplayDataset.from_frame(frame)
    assert len(dataset) == 7
    assert np.all(np.diff(dataset.features[:, 0]) >= 0)  # Time column ascending
    assert dataset.labels.dtype == np.int64


def test_replay_dataset_validation() -> None:
    with pytest.raises(ValueError, match="missing columns"):
        ReplayDataset.from_frame(make_transactions(3, 1).drop(columns=["V5"]))
    broken = make_transactions(3, 1)
    broken.loc[0, "V1"] = np.nan
    with pytest.raises(ValueError, match="NaN"):
        ReplayDataset.from_frame(broken)


def test_load_replay_dataset(tmp_path: Path) -> None:
    with pytest.raises(ReplayDataMissingError, match=r"run.ps1 train"):
        load_replay_dataset(tmp_path / "missing.parquet")
    path = tmp_path / "test.parquet"
    make_transactions(8, 2).to_parquet(path, index=False)
    assert len(load_replay_dataset(path)) == 10


def test_build_event_from_row() -> None:
    dataset = ReplayDataset.from_frame(make_transactions(1, 1))
    event = build_event(dataset, 1, replay_pass=3, now=NOW)
    assert isinstance(event, TransactionEvent)
    assert event.source is Source.REPLAY
    assert event.replay_pass == 3
    assert event.actual_class == int(dataset.labels[1])
    assert event.Time == dataset.features[1, 0]
    assert json.loads(event.to_json_bytes())["event_time"] == "2026-11-14T18:00:00.000Z"


# --------------------------------------------------------------------------- replay loop
def make_replayer(
    rows: int = 5, *, loop: bool = False, paused: bool = False, rate: float = 1000.0, **kwargs
):  # type: ignore[no-untyped-def]
    dataset = ReplayDataset.from_frame(make_transactions(rows - 1, 1))
    sent: list[TransactionEvent] = []
    stop = threading.Event()
    state = ControlState(rate=rate, paused=paused)
    replayer = TransactionReplayer(
        dataset,
        sent.append,
        state,
        RateLimiter(1000.0),
        stop,
        loop=loop,
        sleep_slice_s=0.001,
        **kwargs,
    )
    return replayer, sent, stop, state


def test_single_pass_sends_every_row_once() -> None:
    replayer, sent, _, _ = make_replayer(rows=5)
    assert replayer.run() == 5
    assert len({e.transaction_id for e in sent}) == 5
    assert {e.replay_pass for e in sent} == {1}


def test_looping_replay_increments_pass_until_stopped() -> None:
    dataset = ReplayDataset.from_frame(make_transactions(4, 1))
    stop, sent = threading.Event(), []

    def send(event: TransactionEvent) -> None:
        sent.append(event)
        if len(sent) == 12:
            stop.set()

    replayer = TransactionReplayer(
        dataset, send, ControlState(1000), RateLimiter(1000), stop, loop=True, sleep_slice_s=0.001
    )
    assert replayer.run() == 12
    assert {e.replay_pass for e in sent} == {1, 2, 3}


def test_paused_replayer_sends_nothing_and_still_ticks() -> None:
    ticks = {"n": 0}
    holder: dict[str, threading.Event] = {}

    def on_tick() -> None:
        ticks["n"] += 1
        if ticks["n"] >= 5:
            holder["stop"].set()

    replayer, sent, stop, _ = make_replayer(paused=True, on_tick=on_tick)
    holder["stop"] = stop
    assert replayer.run() == 0
    assert sent == [] and ticks["n"] >= 5


def test_resume_and_rate_change_are_picked_up() -> None:
    ticks = {"n": 0}
    holder: dict[str, object] = {}

    def on_tick() -> None:
        ticks["n"] += 1
        if ticks["n"] == 3:
            holder["state"].apply(command("resume", False, 500))  # type: ignore[attr-defined]

    replayer, sent, _, state = make_replayer(rows=3, paused=True, on_tick=on_tick)
    holder["state"] = state
    assert replayer.run() == 3
    assert len(sent) == 3
    assert replayer._limiter.rate == 500


def test_replayer_rejects_empty_dataset() -> None:
    empty = ReplayDataset(features=np.empty((0, 30)), labels=np.empty(0, dtype=np.int64))
    with pytest.raises(ValueError):
        TransactionReplayer(
            empty, lambda e: None, ControlState(1), RateLimiter(1), threading.Event()
        )


# --------------------------------------------------------------------------- producer service
def test_make_sender_serialises_and_retries_on_full_queue() -> None:
    fake = FakeProducer(buffer_errors=1)
    stats = producer_module.DeliveryStats()
    send = producer_module.make_sender(fake, "transactions", stats)
    event = build_event(ReplayDataset.from_frame(make_transactions(1, 0)), 0, 1, NOW)
    send(event)
    topic, key, value = fake.produced[0]
    assert topic == "transactions"
    assert key == event.key
    assert TransactionEvent.model_validate_json(value) == event.model_copy(
        update={"event_time": NOW}
    )
    assert fake.polls >= 2  # polled while the queue was full, then after producing


def test_delivery_stats() -> None:
    stats = producer_module.DeliveryStats()
    stats(None, None)  # type: ignore[arg-type]
    stats("timeout", None)  # type: ignore[arg-type]
    assert (stats.delivered, stats.failed) == (1, 1)


def test_progress_reporter_beats_and_logs(tmp_path: Path) -> None:
    heartbeat = Heartbeat(tmp_path / "producer.heartbeat", min_interval_s=0)
    replayer, _, _, state = make_replayer(rows=2)
    reporter = producer_module.ProgressReporter(
        lambda: replayer, state, producer_module.DeliveryStats(), heartbeat, interval_s=0.0
    )
    reporter()
    assert heartbeat.path.is_file()


def test_main_replays_test_split_once(monkeypatch: pytest.MonkeyPatch) -> None:
    paths = get_path_settings()
    paths.test_split_path.parent.mkdir(parents=True)
    make_transactions(15, 5).to_parquet(paths.test_split_path, index=False)
    monkeypatch.setenv("PRODUCER_LOOP", "false")
    monkeypatch.setenv("PRODUCER_RATE", "1000")
    fake_producer = FakeProducer()
    monkeypatch.setattr(producer_module, "wait_for_kafka", lambda *a, **k: None)
    monkeypatch.setattr(producer_module, "Producer", lambda cfg: fake_producer)
    monkeypatch.setattr(producer_module, "Consumer", lambda cfg: FakeConsumer())
    monkeypatch.setattr(producer_module.signal, "signal", lambda *a: None)

    assert producer_module.main() == 0
    assert len(fake_producer.produced) == 20
    assert {topic for topic, _, _ in fake_producer.produced} == {"transactions"}
    assert fake_producer.flushes >= 1
    assert paths.heartbeat_path("producer").is_file()


def test_main_exits_cleanly_without_test_split(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(producer_module.signal, "signal", lambda *a: None)
    monkeypatch.setattr(
        producer_module, "wait_for_kafka", lambda *a, **k: pytest.fail("must not reach Kafka")
    )
    assert producer_module.main() == 2


def test_test_split_parquet_has_expected_columns() -> None:
    frame = make_transactions(2, 1)
    assert isinstance(frame, pd.DataFrame)
    assert ReplayDataset.from_frame(frame).features.shape == (3, 30)
