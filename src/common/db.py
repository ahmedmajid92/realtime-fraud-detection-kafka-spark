"""PostgreSQL access shared by the stream processor and the dashboard (psycopg 3).

Schema: ``docker/postgres/init.sql``.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping, Sequence
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from src.common.config import PostgresSettings

THRESHOLD_KEY = "alert_threshold"
MODEL_RUN_KEY = "model_run_id"

PREDICTION_DB_COLUMNS: tuple[str, ...] = (
    "transaction_id",
    "event_time",
    "processed_at",
    "source",
    "replay_pass",
    "amount",
    "hour",
    "fraud_probability",
    "prediction",
    "threshold",
    "actual_class",
    "model_name",
    "latency_ms",
)

# transaction_id is cast explicitly: psycopg sends Python str as text, not uuid.
INSERT_PREDICTIONS_SQL = (
    f"INSERT INTO predictions ({', '.join(PREDICTION_DB_COLUMNS)}) "
    f"VALUES (%s::uuid, {', '.join(['%s'] * (len(PREDICTION_DB_COLUMNS) - 1))}) "
    "ON CONFLICT (transaction_id) DO NOTHING"
)

INSERT_PROGRESS_SQL = (
    "INSERT INTO stream_progress (query_id, run_id, batch_id, num_input_rows, "
    "input_rows_per_second, processed_rows_per_second, batch_duration_ms, end_offsets) "
    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
)

PRUNE_PROGRESS_SQL = "DELETE FROM stream_progress WHERE recorded_at < now() - %s::interval"

UPSERT_SETTING_SQL = (
    "INSERT INTO settings (key, value, updated_at) VALUES (%s, %s, now()) "
    "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = now()"
)


def connect(
    settings: PostgresSettings, *, autocommit: bool = True, connect_timeout: int = 5
) -> psycopg.Connection[Any]:
    """Open a new connection."""
    return psycopg.connect(
        **settings.connect_kwargs(), autocommit=autocommit, connect_timeout=connect_timeout
    )


def wait_for_postgres(
    settings: PostgresSettings,
    *,
    timeout_s: float = 120.0,
    logger: Any | None = None,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Block until PostgreSQL accepts connections and answers ``SELECT 1``."""
    deadline = clock() + timeout_s
    delay = 1.0
    while True:
        try:
            with connect(settings) as conn:
                conn.execute("SELECT 1")
            if logger is not None:
                logger.info("postgres.ready", host=settings.postgres_host)
            return
        except psycopg.OperationalError as exc:
            if clock() >= deadline:
                raise TimeoutError(f"PostgreSQL not ready after {timeout_s:.0f}s: {exc}") from exc
            if logger is not None:
                logger.info("postgres.waiting", reason=str(exc).strip(), retry_in_s=delay)
            sleep(delay)
            delay = min(delay * 2, 10.0)


# ---------------------------------------------------------------------------
# settings table (alert threshold shared by the dashboard slider and Spark)
# ---------------------------------------------------------------------------
def validate_threshold(value: float) -> float:
    """Return ``value`` as float if it is a valid probability threshold in (0, 1)."""
    value = float(value)
    if not math.isfinite(value) or not 0.0 < value < 1.0:
        raise ValueError(f"threshold must be in (0, 1), got {value}")
    return value


def get_setting(conn: psycopg.Connection[Any], key: str) -> str | None:
    row = conn.execute("SELECT value FROM settings WHERE key = %s", (key,)).fetchone()
    return None if row is None else str(row[0])


def set_setting(conn: psycopg.Connection[Any], key: str, value: str) -> None:
    conn.execute(UPSERT_SETTING_SQL, (key, value))


def get_threshold(conn: psycopg.Connection[Any]) -> float | None:
    """Current alert threshold, or ``None`` if unset or invalid."""
    raw = get_setting(conn, THRESHOLD_KEY)
    if raw is None:
        return None
    try:
        return validate_threshold(float(raw))
    except ValueError:
        return None


def set_threshold(conn: psycopg.Connection[Any], value: float) -> float:
    """Store a new alert threshold (validated) and return it."""
    value = validate_threshold(value)
    set_setting(conn, THRESHOLD_KEY, repr(value))
    return value


def sync_threshold_with_model(
    conn: psycopg.Connection[Any],
    *,
    model_run_id: str,
    default_threshold: float,
    override: float | None = None,
) -> float:
    """Decide the active alert threshold when the stream processor starts.

    * an explicit ``override`` (``ALERT_THRESHOLD_OVERRIDE``) always wins;
    * a newly trained model (different ``run_id``) resets it to that model's tuned default;
    * otherwise the value last chosen with the dashboard slider is kept.
    """
    if override is not None:
        value = set_threshold(conn, override)
    elif get_setting(conn, MODEL_RUN_KEY) != model_run_id or get_threshold(conn) is None:
        value = set_threshold(conn, default_threshold)
    else:
        stored = get_threshold(conn)
        value = default_threshold if stored is None else stored
    set_setting(conn, MODEL_RUN_KEY, model_run_id)
    return value


# ---------------------------------------------------------------------------
# predictions / stream_progress
# ---------------------------------------------------------------------------
def insert_predictions(conn: psycopg.Connection[Any], rows: Sequence[Sequence[Any]]) -> int:
    """Insert prediction rows (ordered as ``PREDICTION_DB_COLUMNS``); duplicates are ignored."""
    if not rows:
        return 0
    with conn.cursor() as cursor:
        cursor.executemany(INSERT_PREDICTIONS_SQL, rows)
    return len(rows)


def insert_stream_progress(conn: psycopg.Connection[Any], record: Mapping[str, Any]) -> None:
    """Insert one Spark progress record (see :func:`src.streaming.listener.progress_to_record`)."""
    conn.execute(
        INSERT_PROGRESS_SQL,
        (
            record.get("query_id"),
            record.get("run_id"),
            record["batch_id"],
            record.get("num_input_rows"),
            record.get("input_rows_per_second"),
            record.get("processed_rows_per_second"),
            record.get("batch_duration_ms"),
            Jsonb(record.get("end_offsets") or {}),
        ),
    )


def prune_stream_progress(conn: psycopg.Connection[Any], keep: str = "1 hour") -> None:
    """Delete progress rows older than ``keep`` (a Postgres interval string)."""
    conn.execute(PRUNE_PROGRESS_SQL, (keep,))
