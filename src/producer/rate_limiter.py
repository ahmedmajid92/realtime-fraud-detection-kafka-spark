"""Fixed-interval pacing for the producer, with an injectable clock for unit tests."""

from __future__ import annotations

import time
from collections.abc import Callable

from src.common.schemas import MAX_PRODUCER_RATE


class RateLimiter:
    """Spaces sends ``1 / rate`` seconds apart on a monotonic clock.

    Short stalls (e.g. a slow ``produce`` call) are caught up, so the long-run average
    rate stays on target. Catch-up is capped at ``max_lag_s`` of backlog, so a long pause
    never produces a burst.
    """

    def __init__(
        self,
        rate: float,
        clock: Callable[[], float] = time.monotonic,
        max_lag_s: float = 1.0,
    ) -> None:
        self._clock = clock
        self._max_lag_s = max_lag_s
        self._rate = 0.0
        self._interval = 0.0
        self._next_slot: float | None = None
        self.set_rate(rate)

    @property
    def rate(self) -> float:
        return self._rate

    def set_rate(self, rate: float) -> None:
        """Change the target rate (transactions/second) and restart the schedule."""
        if not 0 < rate <= MAX_PRODUCER_RATE:
            raise ValueError(f"rate must be in (0, {MAX_PRODUCER_RATE:g}], got {rate}")
        self._rate = float(rate)
        self._interval = 1.0 / self._rate
        self._next_slot = None

    def reset(self) -> None:
        """Forget the schedule (call after a pause so resuming does not burst)."""
        self._next_slot = None

    def seconds_until_next(self) -> float:
        """How long to wait before the next send is allowed (0 if it may go now)."""
        if self._next_slot is None:
            return 0.0
        return max(0.0, self._next_slot - self._clock())

    def record_send(self) -> None:
        """Book the slot just used and schedule the next one."""
        now = self._clock()
        base = now if self._next_slot is None else max(self._next_slot, now - self._max_lag_s)
        self._next_slot = base + self._interval
