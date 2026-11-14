"""Loading the test split and turning its rows into paced :class:`TransactionEvent` messages."""

from __future__ import annotations

import threading
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

from src.common.schemas import (
    LABEL_COLUMN,
    RAW_INPUT_COLUMNS,
    TIME_COLUMN,
    Source,
    TransactionEvent,
    utc_now,
)
from src.producer.control import ControlState
from src.producer.rate_limiter import RateLimiter


class ReplayDataMissingError(FileNotFoundError):
    """The test split has not been produced by the trainer yet."""


@dataclass(frozen=True)
class ReplayDataset:
    """The test split as compact numpy arrays (about 10 MB instead of 42k Python dicts)."""

    features: np.ndarray  # shape (n, len(RAW_INPUT_COLUMNS)), float64
    labels: np.ndarray  # shape (n,), int

    def __len__(self) -> int:
        return int(self.labels.shape[0])

    @classmethod
    def from_frame(cls, frame: pd.DataFrame) -> ReplayDataset:
        missing = [c for c in (*RAW_INPUT_COLUMNS, LABEL_COLUMN) if c not in frame.columns]
        if missing:
            raise ValueError(f"test split is missing columns: {missing}")
        ordered = frame.sort_values(TIME_COLUMN, kind="stable")
        features = ordered[list(RAW_INPUT_COLUMNS)].to_numpy(dtype=np.float64)
        if not np.isfinite(features).all():
            raise ValueError("test split contains NaN or infinite feature values")
        return cls(features=features, labels=ordered[LABEL_COLUMN].to_numpy(dtype=np.int64))


def load_replay_dataset(path: Path) -> ReplayDataset:
    """Read ``test.parquet`` written by the trainer (sorted by ``Time``)."""
    if not path.is_file():
        raise ReplayDataMissingError(
            f"Test split not found at {path}. It is created by the trainer: .\\run.ps1 train"
        )
    return ReplayDataset.from_frame(
        pd.read_parquet(path, columns=[*RAW_INPUT_COLUMNS, LABEL_COLUMN])
    )


def build_event(
    dataset: ReplayDataset,
    index: int,
    replay_pass: int,
    now: datetime,
    transaction_id: uuid.UUID | None = None,
) -> TransactionEvent:
    """Turn row ``index`` of the test split into a fresh transaction event."""
    values = dict(zip(RAW_INPUT_COLUMNS, dataset.features[index].tolist(), strict=True))
    return TransactionEvent(
        transaction_id=transaction_id or uuid.uuid4(),
        event_time=now,
        source=Source.REPLAY,
        replay_pass=replay_pass,
        actual_class=int(dataset.labels[index]),
        **values,
    )


class TransactionReplayer:
    """Paced, pausable, optionally looping replay of a :class:`ReplayDataset`."""

    def __init__(
        self,
        dataset: ReplayDataset,
        send: Callable[[TransactionEvent], None],
        state: ControlState,
        limiter: RateLimiter,
        stop_event: threading.Event,
        *,
        loop: bool = True,
        now: Callable[[], datetime] = utc_now,
        on_tick: Callable[[], None] | None = None,
        sleep_slice_s: float = 0.2,
    ) -> None:
        if len(dataset) == 0:
            raise ValueError("cannot replay an empty dataset")
        self._dataset = dataset
        self._send = send
        self._state = state
        self._limiter = limiter
        self._stop = stop_event
        self._loop = loop
        self._now = now
        self._on_tick = on_tick
        self._slice = sleep_slice_s
        self._was_paused = False
        self.sent = 0
        self.replay_pass = 1

    def run(self) -> int:
        """Replay until stopped (or until one pass ends when ``loop`` is False)."""
        while not self._stop.is_set():
            for index in range(len(self._dataset)):
                if not self._wait_for_slot():
                    return self.sent
                self._send(build_event(self._dataset, index, self.replay_pass, self._now()))
                self._limiter.record_send()
                self.sent += 1
            if not self._loop:
                break
            self.replay_pass += 1
        return self.sent

    def _wait_for_slot(self) -> bool:
        """Block until the next send is allowed; ``False`` if a stop was requested."""
        while not self._stop.is_set():
            if self._on_tick is not None:
                self._on_tick()
            paused, rate = self._state.snapshot()
            if paused:
                self._was_paused = True
                self._stop.wait(self._slice)
                continue
            if self._was_paused:
                self._limiter.reset()
                self._was_paused = False
            if rate != self._limiter.rate:
                self._limiter.set_rate(rate)
            delay = self._limiter.seconds_until_next()
            if delay <= 0:
                return True
            self._stop.wait(min(delay, self._slice))
        return False
