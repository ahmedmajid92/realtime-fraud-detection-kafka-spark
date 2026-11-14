"""KPI calculations for the Live Monitor and System Health pages (pure functions)."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import pandas as pd


def safe_ratio(numerator: float, denominator: float) -> float:
    """``numerator / denominator``, or 0.0 when the denominator is 0."""
    return float(numerator) / float(denominator) if denominator else 0.0


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    value = float(value)
    return value if math.isfinite(value) else None


def throughput(window_count: int, window_s: float, uptime_s: float | None) -> float:
    """Transactions per second over the last ``window_s`` seconds.

    While the stream has been running for less than ``window_s``, divide by the actual
    uptime instead (otherwise the first minute would under-report). The span is at
    least 1 s, so the very first rows don't give a huge rate.
    """
    span = window_s if uptime_s is None else min(window_s, max(uptime_s, 0.0))
    span = max(span, 1.0)
    return window_count / span


@dataclass(frozen=True)
class KpiSnapshot:
    total: int
    flagged: int
    fraud_rate: float
    throughput: float
    avg_latency_ms: float | None
    p95_latency_ms: float | None
    idle_s: float | None


def build_kpi_snapshot(summary: Mapping[str, Any], window_s: float) -> KpiSnapshot:
    """Turn the KPI summary query row into display-ready numbers."""
    total = int(summary.get("total") or 0)
    flagged = int(summary.get("flagged") or 0)
    return KpiSnapshot(
        total=total,
        flagged=flagged,
        fraud_rate=safe_ratio(flagged, total),
        throughput=throughput(
            int(summary.get("window_count") or 0),
            window_s,
            _optional_float(summary.get("uptime_s")),
        ),
        avg_latency_ms=_optional_float(summary.get("avg_latency_ms")),
        p95_latency_ms=_optional_float(summary.get("p95_latency_ms")),
        idle_s=_optional_float(summary.get("idle_s")),
    )


def quality_metrics(counts: Mapping[str, Any]) -> dict[str, float | int]:
    """Live precision / recall / F1 against ``actual_class`` (replay traffic only)."""
    tp, fp = int(counts.get("tp") or 0), int(counts.get("fp") or 0)
    tn, fn = int(counts.get("tn") or 0), int(counts.get("fn") or 0)
    precision = safe_ratio(tp, tp + fp)
    recall = safe_ratio(tp, tp + fn)
    f1 = safe_ratio(2 * precision * recall, precision + recall)
    return {
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def relabel(
    frame: pd.DataFrame,
    threshold: float,
    prob_col: str = "fraud_probability",
    out_col: str = "flagged",
) -> pd.DataFrame:
    """Copy of ``frame`` with a boolean ``flagged`` column at the current threshold."""
    result = frame.copy()
    if prob_col in result.columns:
        result[out_col] = result[prob_col].astype(float) >= threshold
    else:
        result[out_col] = pd.Series(dtype=bool)
    return result


def total_offset(offsets: Mapping[Any, int]) -> int:
    """Sum of per-partition offsets."""
    return int(sum(int(v) for v in offsets.values()))


def consumer_lag(
    topic_offsets: Mapping[int, int], processed: Mapping[str, Any] | None
) -> int | None:
    """Messages in the topic that Spark has not processed yet.

    ``processed`` is one topic's entry from Spark's ``endOffset`` JSON (partition keys are
    strings). Returns ``None`` if Spark has not reported progress for the topic yet.
    """
    if not processed:
        return None
    lag = 0
    for partition, end in topic_offsets.items():
        done = int(processed.get(str(partition), processed.get(partition, 0)) or 0)  # type: ignore[call-overload]
        lag += max(0, int(end) - done)
    return lag


def commanded_state(last: Mapping[str, Any] | None, default_rate: float) -> tuple[bool, float]:
    """``(paused, rate)`` from the last producer-control message, or the start-up defaults."""
    if not last:
        return False, float(default_rate)
    return bool(last.get("paused", False)), float(last.get("rate", default_rate))


def rate_between(previous: tuple[float, int] | None, current: tuple[float, int]) -> float | None:
    """Messages/second between two ``(monotonic_time, total_offset)`` samples."""
    if previous is None:
        return None
    elapsed = current[0] - previous[0]
    if elapsed <= 0:
        return None
    return max(0.0, (current[1] - previous[1]) / elapsed)
