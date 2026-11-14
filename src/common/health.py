"""Heartbeat files used by Docker healthchecks.

Long-running services touch a file regularly; ``scripts/healthcheck.py file <path> <age>``
reports *unhealthy* when the file is missing or older than ``age`` seconds. This tests
"the main loop is alive", which an open port alone cannot show.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from pathlib import Path


class Heartbeat:
    """Rate-limited heartbeat writer."""

    def __init__(
        self,
        path: Path,
        min_interval_s: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._path = path
        self._min_interval_s = min_interval_s
        self._clock = clock
        self._last_beat: float | None = None

    @property
    def path(self) -> Path:
        return self._path

    def beat(self, force: bool = False) -> bool:
        """Touch the heartbeat file; returns ``True`` if the file was written."""
        now = self._clock()
        recently_beaten = (
            self._last_beat is not None and now - self._last_beat < self._min_interval_s
        )
        if recently_beaten and not force:
            return False
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(f"{time.time():.3f}\n", encoding="utf-8")
        self._last_beat = now
        return True


def heartbeat_age_seconds(path: Path, now: float | None = None) -> float | None:
    """Seconds since the heartbeat file was last modified, or ``None`` if it is missing."""
    try:
        mtime = path.stat().st_mtime
    except FileNotFoundError:
        return None
    return (time.time() if now is None else now) - mtime
