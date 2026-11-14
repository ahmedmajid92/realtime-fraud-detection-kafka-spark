"""Streaming query listener: heartbeat for the healthcheck + progress rows for the dashboard."""

from __future__ import annotations

import json
import math
from collections.abc import Callable
from typing import Any

import psycopg
from pyspark.sql.streaming import StreamingQueryListener

from src.common.db import insert_stream_progress, prune_predictions, prune_stream_progress
from src.common.health import Heartbeat

PRUNE_EVERY_EVENTS = 600


def _finite_or_none(value: Any) -> float | None:
    if value is None:
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def progress_to_record(progress: Any) -> dict[str, Any]:
    """Flatten a ``StreamingQueryProgress`` into a ``stream_progress`` row."""
    end_offsets: dict[str, Any] = {}
    for source in getattr(progress, "sources", None) or []:
        raw = getattr(source, "endOffset", None)
        if not raw:
            continue
        try:
            parsed = json.loads(raw) if isinstance(raw, str) else raw
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            end_offsets.update(parsed)
    duration = (getattr(progress, "durationMs", None) or {}).get("triggerExecution")
    return {
        "query_id": str(getattr(progress, "id", "")) or None,
        "run_id": str(getattr(progress, "runId", "")) or None,
        "batch_id": int(progress.batchId),
        "num_input_rows": int(getattr(progress, "numInputRows", 0) or 0),
        "input_rows_per_second": _finite_or_none(getattr(progress, "inputRowsPerSecond", None)),
        "processed_rows_per_second": _finite_or_none(
            getattr(progress, "processedRowsPerSecond", None)
        ),
        "batch_duration_ms": None if duration is None else int(duration),
        "end_offsets": end_offsets,
    }


class ProgressListener(StreamingQueryListener):
    """Touches the heartbeat on every event and stores progress for the System Health page."""

    def __init__(
        self,
        heartbeat: Heartbeat,
        connect: Callable[[], psycopg.Connection[Any]],
        logger: Any,
        predictions_retention_hours: float = 6.0,
    ) -> None:
        super().__init__()
        self._heartbeat = heartbeat
        self._connect = connect
        self._logger = logger
        self._conn: psycopg.Connection[Any] | None = None
        self._events = 0
        self._retention_hours = predictions_retention_hours

    def onQueryStarted(self, event: Any) -> None:
        self._heartbeat.beat(force=True)
        self._logger.info("query.started", query_id=str(event.id), run_id=str(event.runId))

    def onQueryProgress(self, event: Any) -> None:
        self._heartbeat.beat(force=True)
        try:
            record = progress_to_record(event.progress)
            if self._conn is None or self._conn.closed:
                self._conn = self._connect()
            insert_stream_progress(self._conn, record)
            self._events += 1
            if self._events % PRUNE_EVERY_EVENTS == 0:
                prune_stream_progress(self._conn)
                removed = prune_predictions(self._conn, self._retention_hours)
                if removed:
                    self._logger.info("retention.pruned", predictions=removed)
        except Exception as exc:  # never let monitoring break the query
            self._logger.warning("progress.store_failed", error=str(exc).strip())
            if self._conn is not None:
                self._conn.close()
            self._conn = None

    def onQueryIdle(self, event: Any) -> None:
        self._heartbeat.beat(force=True)

    def onQueryTerminated(self, event: Any) -> None:
        self._logger.warning(
            "query.terminated", query_id=str(event.id), exception=getattr(event, "exception", None)
        )
        if self._conn is not None:
            self._conn.close()
            self._conn = None
