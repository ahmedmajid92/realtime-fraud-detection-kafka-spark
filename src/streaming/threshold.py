"""Reads the live alert threshold (set by the dashboard slider) once per micro-batch."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import psycopg

from src.common.db import get_threshold, validate_threshold


class ThresholdProvider:
    """Returns the current threshold from Postgres, falling back to the last good value.

    A database hiccup must never stop scoring: on error the previous value (initially the
    model's tuned default) is used and the connection is re-opened on the next call.
    """

    def __init__(
        self,
        connect: Callable[[], psycopg.Connection[Any]],
        default: float,
        logger: Any | None = None,
    ) -> None:
        self._connect = connect
        self._last = validate_threshold(default)
        self._logger = logger
        self._conn: psycopg.Connection[Any] | None = None

    @property
    def last(self) -> float:
        return self._last

    def current(self) -> float:
        try:
            if self._conn is None or self._conn.closed:
                self._conn = self._connect()
            value = get_threshold(self._conn)
            if value is not None:
                if value != self._last and self._logger is not None:
                    self._logger.info("threshold.changed", old=self._last, new=value)
                self._last = value
        except psycopg.Error as exc:
            if self._logger is not None:
                self._logger.warning(
                    "threshold.read_failed", error=str(exc).strip(), using=self._last
                )
            self.close()
        return self._last

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            finally:
                self._conn = None
