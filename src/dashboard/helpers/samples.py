"""Test-split samples for the "Test a Transaction" page (pure functions)."""

from __future__ import annotations

import math
import uuid
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.common.schemas import (
    AMOUNT_COLUMN,
    LABEL_COLUMN,
    RAW_INPUT_COLUMNS,
    TIME_COLUMN,
    V_COLUMNS,
    Source,
    TransactionEvent,
)

SECONDS_PER_DAY = 86_400.0
SECONDS_PER_HOUR = 3_600.0


def load_samples(path: Path) -> pd.DataFrame | None:
    """The test split (only the columns the page needs), or ``None`` if not trained yet."""
    if not path.is_file():
        return None
    return pd.read_parquet(path, columns=[*RAW_INPUT_COLUMNS, LABEL_COLUMN])


def pick_random(samples: pd.DataFrame, fraud: bool, rng: np.random.Generator) -> dict[str, Any]:
    """A random row of the requested class as a plain dict."""
    pool = samples[samples[LABEL_COLUMN] == (1 if fraud else 0)]
    if pool.empty:
        raise ValueError("no sample of the requested class")
    row = pool.iloc[int(rng.integers(len(pool)))]
    record = {column: float(row[column]) for column in RAW_INPUT_COLUMNS}
    record[LABEL_COLUMN] = int(row[LABEL_COLUMN])
    return record


def hour_of(time_seconds: float) -> int:
    """Hour of day (0-23) encoded by the dataset's ``Time`` column."""
    return int((time_seconds / SECONDS_PER_HOUR) % 24)


def with_hour(time_seconds: float, hour: int) -> float:
    """Move ``time_seconds`` to ``hour`` on the same day, keeping minutes and seconds."""
    if not 0 <= hour <= 23:
        raise ValueError("hour must be in 0..23")
    day_start = math.floor(time_seconds / SECONDS_PER_DAY) * SECONDS_PER_DAY
    within_hour = time_seconds % SECONDS_PER_HOUR
    return day_start + hour * SECONDS_PER_HOUR + within_hour


def apply_edits(
    sample: Mapping[str, Any],
    *,
    amount: float,
    hour: int,
    v_values: Mapping[str, float] | None = None,
) -> dict[str, float]:
    """Raw feature dict after the user's edits on the form."""
    edited = {column: float(sample[column]) for column in RAW_INPUT_COLUMNS}
    edited[AMOUNT_COLUMN] = float(amount)
    if hour != hour_of(edited[TIME_COLUMN]):
        edited[TIME_COLUMN] = with_hour(edited[TIME_COLUMN], hour)
    for column in V_COLUMNS:
        if v_values and column in v_values:
            edited[column] = float(v_values[column])
    return edited


def is_edited(original: Mapping[str, Any], edited: Mapping[str, Any], tol: float = 1e-6) -> bool:
    """True if any raw feature differs from the original sample."""
    return any(abs(float(original[c]) - float(edited[c])) > tol for c in RAW_INPUT_COLUMNS)


def build_manual_event(
    features: Mapping[str, float],
    actual_class: int | None,
    now: datetime,
    *,
    source: Source = Source.MANUAL,
    transaction_id: uuid.UUID | None = None,
) -> TransactionEvent:
    """A transaction event for the Test page (or the e2e test)."""
    return TransactionEvent(
        transaction_id=transaction_id or uuid.uuid4(),
        event_time=now,
        source=source,
        replay_pass=0,
        actual_class=actual_class,
        **{column: float(features[column]) for column in RAW_INPUT_COLUMNS},
    )
