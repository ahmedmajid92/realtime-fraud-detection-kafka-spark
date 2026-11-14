"""Small in-memory stand-ins for Kafka, PostgreSQL and loggers used by the unit tests."""

from __future__ import annotations

import time
from collections.abc import Iterable, Sequence
from types import SimpleNamespace
from typing import Any


class FakeLogger:
    """Records structlog-style calls: ``(level, event, fields)``."""

    def __init__(self) -> None:
        self.records: list[tuple[str, str, dict[str, Any]]] = []

    def _log(self, level: str, event: str, **fields: Any) -> None:
        self.records.append((level, event, fields))

    def debug(self, event: str, **fields: Any) -> None:
        self._log("debug", event, **fields)

    def info(self, event: str, **fields: Any) -> None:
        self._log("info", event, **fields)

    def warning(self, event: str, **fields: Any) -> None:
        self._log("warning", event, **fields)

    def error(self, event: str, **fields: Any) -> None:
        self._log("error", event, **fields)

    def exception(self, event: str, **fields: Any) -> None:
        self._log("exception", event, **fields)

    def events(self, level: str | None = None) -> list[str]:
        return [e for lvl, e, _ in self.records if level is None or lvl == level]


# --------------------------------------------------------------------------- PostgreSQL
class FakeCursor:
    """psycopg cursor stand-in returning pre-programmed rows."""

    def __init__(
        self, rows: Sequence[Any] | None = None, columns: Sequence[str] | None = None
    ) -> None:
        self.rows = list(rows or [])
        self.description = [SimpleNamespace(name=c) for c in columns] if columns else None
        self.executed: list[tuple[str, Any]] = []
        self.executemany_calls: list[tuple[str, list[Any]]] = []

    def execute(self, sql: str, params: Any = None) -> FakeCursor:
        self.executed.append((sql, params))
        return self

    def executemany(self, sql: str, rows: Iterable[Any]) -> None:
        self.executemany_calls.append((sql, list(rows)))

    def fetchone(self) -> Any:
        return self.rows[0] if self.rows else None

    def fetchall(self) -> list[Any]:
        return list(self.rows)

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *exc: Any) -> bool:
        return False


class FakeConnection:
    """psycopg connection stand-in with an in-memory ``settings`` table.

    ``cursor_results`` is a queue of ``(rows, columns)`` returned by successive
    ``cursor()`` calls (for the dashboard query helpers).
    """

    def __init__(
        self,
        settings: dict[str, str] | None = None,
        cursor_results: Sequence[tuple[Sequence[Any], Sequence[str] | None]] = (),
    ) -> None:
        self.settings = dict(settings or {})
        self.executed: list[tuple[str, Any]] = []
        self.cursors: list[FakeCursor] = []
        self._cursor_results = list(cursor_results)
        self.closed = False
        self.committed = False

    def execute(self, sql: str, params: Any = None) -> FakeCursor:
        self.executed.append((sql, params))
        if sql.startswith("SELECT value FROM settings"):
            value = self.settings.get(params[0])
            return FakeCursor(rows=[(value,)] if value is not None else [])
        if sql.startswith("INSERT INTO settings"):
            self.settings[params[0]] = params[1]
        return FakeCursor()

    def cursor(self, row_factory: Any = None) -> FakeCursor:
        rows, columns = self._cursor_results.pop(0) if self._cursor_results else ([], None)
        cursor = FakeCursor(rows=rows, columns=columns)
        self.cursors.append(cursor)
        return cursor

    def close(self) -> None:
        self.closed = True

    def __enter__(self) -> FakeConnection:
        return self

    def __exit__(self, exc_type: Any, *exc: Any) -> bool:
        self.committed = exc_type is None
        self.closed = True
        return False


# --------------------------------------------------------------------------- Kafka
class FakeMessage:
    def __init__(self, value: bytes | None, error: Any = None, offset: int = 0) -> None:
        self._value = value
        self._error = error
        self._offset = offset

    def offset(self) -> int:
        return self._offset

    def value(self) -> bytes | None:
        return self._value

    def error(self) -> Any:
        return self._error


class FakeTopicMetadata:
    def __init__(self, partitions: Iterable[int], error: Any = None) -> None:
        self.partitions = {p: object() for p in partitions}
        self.error = error


class FakeClusterMetadata:
    def __init__(self, topics: dict[str, Iterable[int]] | None) -> None:
        self.topics = {name: FakeTopicMetadata(parts) for name, parts in (topics or {}).items()}


class FakeConsumer:
    """confluent_kafka.Consumer stand-in."""

    def __init__(
        self,
        messages: Iterable[FakeMessage] = (),
        watermarks: dict[tuple[str, int], tuple[int, int]] | None = None,
        topics: dict[str, Iterable[int]] | None = None,
    ) -> None:
        self.messages = list(messages)
        self.watermarks = watermarks or {}
        self.topics = topics or {}
        self.assigned: list[Any] | None = None
        self.closed = False

    def assign(self, partitions: list[Any]) -> None:
        self.assigned = partitions

    def poll(self, timeout: float | None = None) -> FakeMessage | None:
        if self.messages:
            return self.messages.pop(0)
        time.sleep(min(timeout or 0.0, 0.01))  # behave like a real poll timeout, without spinning
        return None

    def get_watermark_offsets(
        self, partition: Any, timeout: float | None = None, cached: bool = False
    ) -> tuple[int, int]:
        return self.watermarks.get((partition.topic, partition.partition), (0, 0))

    def list_topics(self, topic: str | None = None, timeout: float | None = None) -> Any:
        return FakeClusterMetadata(self.topics)

    def close(self) -> None:
        self.closed = True


class FakeProducer:
    """confluent_kafka.Producer stand-in.

    ``buffer_errors``: how many ``produce`` calls raise ``BufferError`` first.
    ``undelivered``: value returned by ``flush`` (messages still queued).
    ``delivery_error``: passed to delivery callbacks on ``flush``.
    """

    def __init__(
        self, buffer_errors: int = 0, undelivered: int = 0, delivery_error: Any = None
    ) -> None:
        self.produced: list[tuple[str, bytes | None, bytes | None]] = []
        self.polls = 0
        self.flushes = 0
        self._buffer_errors = buffer_errors
        self._undelivered = undelivered
        self._delivery_error = delivery_error
        self._callbacks: list[Any] = []

    def produce(
        self,
        topic: str,
        value: bytes | None = None,
        key: bytes | None = None,
        on_delivery: Any = None,
    ) -> None:
        if self._buffer_errors > 0:
            self._buffer_errors -= 1
            raise BufferError("local queue full")
        self.produced.append((topic, key, value))
        if on_delivery is not None:
            self._callbacks.append(on_delivery)

    def poll(self, timeout: float = 0) -> int:
        self.polls += 1
        return 0

    def flush(self, timeout: float | None = None) -> int:
        self.flushes += 1
        for callback in self._callbacks:
            callback(self._delivery_error, None)
        self._callbacks.clear()
        return self._undelivered
