"""Message schemas and column definitions shared by every service.

This module is the single source of truth for:

* dataset column names and the model's feature list;
* the JSON messages on the Kafka topics, as pydantic models
  (:class:`TransactionEvent`, :class:`PredictionEvent`, :class:`ControlCommand`);
* the same message fields described with Spark SQL type names
  (``TRANSACTION_FIELD_TYPES`` / ``PREDICTION_FIELD_TYPES``), from which
  :mod:`src.common.spark_schemas` builds the Spark ``StructType``. A unit test asserts that
  the pydantic and Spark views list exactly the same fields.
"""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_serializer, model_validator

# ---------------------------------------------------------------------------
# Dataset columns
# ---------------------------------------------------------------------------
V_COLUMNS: tuple[str, ...] = tuple(f"V{i}" for i in range(1, 29))
TIME_COLUMN = "Time"
AMOUNT_COLUMN = "Amount"
LABEL_COLUMN = "Class"
WEIGHT_COLUMN = "weight"

RAW_INPUT_COLUMNS: tuple[str, ...] = (TIME_COLUMN, *V_COLUMNS, AMOUNT_COLUMN)
"""Columns the saved PipelineModel consumes (raw features as they appear in the CSV)."""

CSV_COLUMNS: tuple[str, ...] = (*RAW_INPUT_COLUMNS, LABEL_COLUMN)
"""Exact header of ``creditcard.csv``."""

ENGINEERED_COLUMNS: tuple[str, ...] = ("log_amount", "hour_sin", "hour_cos")
FEATURE_COLUMNS: tuple[str, ...] = (*V_COLUMNS, *ENGINEERED_COLUMNS)
"""Columns assembled into the model's feature vector. The label is never a feature."""

MAX_PRODUCER_RATE = 1000.0
"""Upper bound (transactions/second) accepted by the producer and the dashboard."""

CONTROL_MESSAGE_KEY = b"producer"
"""Key of every message on the compacted ``producer-control`` topic."""


class Source(StrEnum):
    """Where a transaction came from."""

    REPLAY = "replay"  # producer replaying the held-out test split
    MANUAL = "manual"  # dashboard "Test a Transaction" page
    E2E = "e2e"  # automated end-to-end test


def utc_now() -> datetime:
    """Current time as a timezone-aware UTC datetime."""
    return datetime.now(UTC)


def format_utc(value: datetime) -> str:
    """Format an aware datetime as ISO-8601 UTC with millisecond precision and a ``Z`` suffix.

    Example: ``2026-11-14T18:00:00.123Z``. Spark parses this format natively.
    """
    if value.tzinfo is None:
        raise ValueError("naive datetimes are not allowed; use timezone-aware UTC values")
    return value.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


_STRICT = ConfigDict(extra="forbid", frozen=True, allow_inf_nan=False)


class TransactionEvent(BaseModel):
    """A card transaction published to the ``transactions`` topic."""

    model_config = _STRICT

    transaction_id: UUID
    event_time: AwareDatetime
    source: Source
    replay_pass: int = Field(ge=0, description="Replay loop number (0 for manual/e2e events)")
    Time: float = Field(ge=0)
    V1: float
    V2: float
    V3: float
    V4: float
    V5: float
    V6: float
    V7: float
    V8: float
    V9: float
    V10: float
    V11: float
    V12: float
    V13: float
    V14: float
    V15: float
    V16: float
    V17: float
    V18: float
    V19: float
    V20: float
    V21: float
    V22: float
    V23: float
    V24: float
    V25: float
    V26: float
    V27: float
    V28: float
    Amount: float = Field(ge=0)
    actual_class: Literal[0, 1] | None = Field(
        default=None,
        description="Ground truth for live monitoring only. Never used as a model feature.",
    )

    @field_serializer("event_time")
    def _serialize_event_time(self, value: datetime) -> str:
        return format_utc(value)

    @property
    def key(self) -> bytes:
        """Kafka message key (the transaction id)."""
        return str(self.transaction_id).encode("utf-8")

    def to_json_bytes(self) -> bytes:
        """Serialise to the UTF-8 JSON bytes sent to Kafka."""
        return self.model_dump_json().encode("utf-8")

    def raw_features(self) -> dict[str, float]:
        """The raw model inputs (``Time``, ``V1``..``V28``, ``Amount``)."""
        return {column: float(getattr(self, column)) for column in RAW_INPUT_COLUMNS}


class PredictionEvent(BaseModel):
    """A scored transaction on the ``predictions`` and ``fraud-alerts`` topics."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    transaction_id: UUID
    event_time: AwareDatetime
    processed_at: AwareDatetime
    latency_ms: float
    source: Source
    replay_pass: int
    amount: float
    hour: int = Field(ge=0, le=23)
    fraud_probability: float = Field(ge=0.0, le=1.0)
    prediction: Literal[0, 1]
    threshold: float = Field(gt=0.0, lt=1.0)
    actual_class: Literal[0, 1] | None = None
    model_name: str


ControlAction = Literal["pause", "resume", "set_rate"]


class ControlCommand(BaseModel):
    """A command for the producer on the compacted ``producer-control`` topic.

    Every message carries the **complete desired state** (``paused`` + ``rate``), not just
    a delta. Log compaction keeps only the latest message per key, so a lone "set_rate"
    must not forget an earlier "pause". ``command`` records which action produced the state.
    """

    model_config = _STRICT

    command: ControlAction
    paused: bool
    rate: float = Field(gt=0, le=MAX_PRODUCER_RATE)
    issued_at: AwareDatetime
    issued_by: str = Field(default="dashboard", min_length=1, max_length=64)

    @model_validator(mode="after")
    def _check_consistency(self) -> ControlCommand:
        if self.command == "pause" and not self.paused:
            raise ValueError("a 'pause' command must have paused=true")
        if self.command == "resume" and self.paused:
            raise ValueError("a 'resume' command must have paused=false")
        return self

    @classmethod
    def from_action(
        cls,
        action: ControlAction,
        *,
        current_paused: bool,
        current_rate: float,
        new_rate: float | None = None,
        issued_by: str = "dashboard",
        now: datetime | None = None,
    ) -> ControlCommand:
        """Build the next full-state command from the current state and a UI action."""
        if action == "set_rate" and new_rate is None:
            raise ValueError("new_rate is required for set_rate")
        paused = {"pause": True, "resume": False}.get(action, current_paused)
        rate = new_rate if action == "set_rate" and new_rate is not None else current_rate
        return cls(
            command=action,
            paused=paused,
            rate=rate,
            issued_at=now or utc_now(),
            issued_by=issued_by,
        )

    @field_serializer("issued_at")
    def _serialize_issued_at(self, value: datetime) -> str:
        return format_utc(value)

    def to_json_bytes(self) -> bytes:
        """Serialise to the UTF-8 JSON bytes sent to Kafka."""
        return self.model_dump_json().encode("utf-8")


# ---------------------------------------------------------------------------
# Spark-side view of the same messages (Spark SQL type names)
# ---------------------------------------------------------------------------
TRANSACTION_FIELD_TYPES: dict[str, str] = {
    "transaction_id": "string",
    # Parsed as a string, then converted with try_to_timestamp so a malformed value
    # becomes NULL (and the row is dropped) instead of failing the micro-batch.
    "event_time": "string",
    "source": "string",
    "replay_pass": "int",
    TIME_COLUMN: "double",
    **{column: "double" for column in V_COLUMNS},
    AMOUNT_COLUMN: "double",
    "actual_class": "int",
}

REQUIRED_TRANSACTION_FIELDS: tuple[str, ...] = tuple(
    name for name in TRANSACTION_FIELD_TYPES if name != "actual_class"
)

PREDICTION_FIELD_TYPES: dict[str, str] = {
    "transaction_id": "string",
    "event_time": "timestamp",
    "processed_at": "timestamp",
    "latency_ms": "double",
    "source": "string",
    "replay_pass": "int",
    "amount": "double",
    "hour": "int",
    "fraud_probability": "double",
    "prediction": "int",
    "threshold": "double",
    "actual_class": "int",
    "model_name": "string",
}

PREDICTION_FIELDS: tuple[str, ...] = tuple(PREDICTION_FIELD_TYPES)
"""Ordered fields of a prediction (Kafka JSON and the Postgres ``predictions`` table)."""
