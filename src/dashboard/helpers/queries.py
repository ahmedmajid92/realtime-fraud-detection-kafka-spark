"""SQL used by the dashboard, and thin helpers that run it (psycopg 3).

KPIs count **replay** traffic only. Manual tests and e2e probes appear in the feed and
alert tables but don't skew the live fraud rate or throughput.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pandas as pd
from psycopg.rows import dict_row

KPI_SUMMARY_SQL = """
SELECT
    count(*)                                                    AS total,
    count(*) FILTER (WHERE fraud_probability >= %(threshold)s)  AS flagged,
    count(*) FILTER (WHERE processed_at >= now() - make_interval(secs => %(window_s)s))
                                                                AS window_count,
    avg(latency_ms) FILTER (WHERE processed_at >= now() - make_interval(secs => %(window_s)s))
                                                                AS avg_latency_ms,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)
        FILTER (WHERE processed_at >= now() - make_interval(secs => %(window_s)s))
                                                                AS p95_latency_ms,
    EXTRACT(EPOCH FROM now() - min(processed_at))               AS uptime_s,
    EXTRACT(EPOCH FROM now() - max(processed_at))               AS idle_s
FROM predictions
WHERE source = 'replay'
"""

QUALITY_SQL = """
SELECT
    count(*) FILTER (WHERE fraud_probability >= %(threshold)s AND actual_class = 1) AS tp,
    count(*) FILTER (WHERE fraud_probability >= %(threshold)s AND actual_class = 0) AS fp,
    count(*) FILTER (WHERE fraud_probability <  %(threshold)s AND actual_class = 0) AS tn,
    count(*) FILTER (WHERE fraud_probability <  %(threshold)s AND actual_class = 1) AS fn
FROM predictions
WHERE source = 'replay' AND actual_class IS NOT NULL
"""

_FEED_COLUMNS = (
    "transaction_id, processed_at, source, amount, hour, fraud_probability, "
    "actual_class, latency_ms"
)

RECENT_FEED_SQL = f"""
SELECT {_FEED_COLUMNS}
FROM predictions
ORDER BY processed_at DESC
LIMIT %(limit)s
"""

RECENT_ALERTS_SQL = f"""
SELECT {_FEED_COLUMNS}
FROM predictions
WHERE fraud_probability >= %(threshold)s
ORDER BY processed_at DESC
LIMIT %(limit)s
"""

PER_MINUTE_SQL = """
SELECT
    date_trunc('minute', processed_at)                          AS minute,
    count(*)                                                    AS total,
    count(*) FILTER (WHERE fraud_probability >= %(threshold)s)  AS flagged,
    count(*) FILTER (WHERE actual_class = 1)                    AS actual_frauds
FROM predictions
WHERE source = 'replay' AND processed_at >= now() - make_interval(mins => %(minutes)s)
GROUP BY 1
ORDER BY 1
"""

RECENT_SCORES_SQL = """
SELECT fraud_probability, amount
FROM predictions
WHERE source = 'replay'
ORDER BY processed_at DESC
LIMIT %(limit)s
"""

PREDICTION_BY_ID_SQL = """
SELECT *,
       EXTRACT(EPOCH FROM stored_at - event_time) * 1000.0 AS end_to_end_ms
FROM predictions
WHERE transaction_id = %(transaction_id)s::uuid
"""

RECENT_MANUAL_SQL = f"""
SELECT {_FEED_COLUMNS}
FROM predictions
WHERE source = 'manual'
ORDER BY processed_at DESC
LIMIT %(limit)s
"""

LATEST_PROGRESS_SQL = """
SELECT batch_id, num_input_rows, input_rows_per_second, processed_rows_per_second,
       batch_duration_ms, end_offsets, recorded_at,
       EXTRACT(EPOCH FROM now() - recorded_at) AS age_s
FROM stream_progress
ORDER BY id DESC
LIMIT 1
"""

TABLE_COUNTS_SQL = """
SELECT
    (SELECT count(*) FROM predictions)                          AS predictions,
    (SELECT count(*) FROM predictions WHERE prediction = 1)     AS alerts,
    (SELECT count(*) FROM predictions WHERE source = 'manual')  AS manual,
    (SELECT count(*) FROM predictions WHERE source = 'e2e')     AS e2e,
    pg_database_size(current_database())                        AS db_bytes
"""


def fetch_one(
    conn: Any, sql: str, params: Mapping[str, Any] | None = None
) -> dict[str, Any] | None:
    """First row as a dict (or ``None``)."""
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(sql, params or {})
        return cursor.fetchone()


def fetch_frame(conn: Any, sql: str, params: Mapping[str, Any] | None = None) -> pd.DataFrame:
    """All rows as a DataFrame (empty DataFrame with the right columns if no rows)."""
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(sql, params or {})
        rows = cursor.fetchall()
        columns = [d.name for d in cursor.description] if cursor.description else []
    return pd.DataFrame(rows, columns=columns)


def live_snapshot(
    conn: Any,
    threshold: float,
    *,
    window_s: float = 60.0,
    feed_limit: int = 50,
    minutes: int = 60,
    scores_limit: int = 5000,
) -> dict[str, Any]:
    """Everything the Live Monitor page needs, in one call."""
    params = {"threshold": threshold, "window_s": window_s}
    return {
        "summary": fetch_one(conn, KPI_SUMMARY_SQL, params) or {},
        "quality": fetch_one(conn, QUALITY_SQL, params) or {},
        "feed": fetch_frame(conn, RECENT_FEED_SQL, {"limit": feed_limit}),
        "alerts": fetch_frame(
            conn, RECENT_ALERTS_SQL, {"threshold": threshold, "limit": feed_limit}
        ),
        "per_minute": fetch_frame(
            conn, PER_MINUTE_SQL, {"threshold": threshold, "minutes": minutes}
        ),
        "scores": fetch_frame(conn, RECENT_SCORES_SQL, {"limit": scores_limit}),
    }


def fetch_prediction(conn: Any, transaction_id: str) -> dict[str, Any] | None:
    return fetch_one(conn, PREDICTION_BY_ID_SQL, {"transaction_id": transaction_id})
