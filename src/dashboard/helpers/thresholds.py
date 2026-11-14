"""Which alert threshold the dashboard shows and uses (pure functions, unit-tested).

The authoritative threshold is the one in Postgres, because that's what Spark labels each
micro-batch with. The dashboard always computes KPIs with it, and the sidebar slider
follows it when another session (or a retrain) changes it.
"""

from __future__ import annotations

from src.common.schemas import THRESHOLD_MAX, THRESHOLD_MIN

FALLBACK_THRESHOLD = 0.5


def clamp_threshold(value: float) -> float:
    """Force a value into the slider's range ``[THRESHOLD_MIN, THRESHOLD_MAX]``."""
    return min(THRESHOLD_MAX, max(THRESHOLD_MIN, float(value)))


def resolve_threshold(
    db_value: float | None, session_value: float | None, model_default: float | None
) -> float:
    """Threshold to compute with: database (what Spark uses) → session → model default → 0.5."""
    for candidate in (db_value, session_value, model_default):
        if candidate is not None:
            return clamp_threshold(candidate)
    return FALLBACK_THRESHOLD


def slider_update(db_value: float | None, last_synced: float | None) -> float | None:
    """New slider value if the shared threshold changed since this session last saw it.

    Returns ``None`` when the slider should keep its current value (database unreachable
    or unchanged), so a user's in-progress slider position is never overwritten by stale data.
    """
    if db_value is None:
        return None
    if last_synced is None or abs(db_value - last_synced) > 1e-12:
        return clamp_threshold(db_value)
    return None
