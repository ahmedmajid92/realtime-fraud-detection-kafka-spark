"""Message schemas: pydantic models, field parity with the Spark schema, feature list."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from src.common import schemas
from src.common.schemas import (
    CSV_COLUMNS,
    FEATURE_COLUMNS,
    LABEL_COLUMN,
    PREDICTION_FIELD_TYPES,
    RAW_INPUT_COLUMNS,
    TRANSACTION_FIELD_TYPES,
    ControlCommand,
    PredictionEvent,
    Source,
    TransactionEvent,
)
from src.common.spark_schemas import (
    csv_schema,
    prediction_schema,
    struct_from_field_types,
    transaction_schema,
)

EVENT_TIME = datetime(2026, 11, 14, 18, 0, 0, 123456, tzinfo=UTC)


def make_event(**overrides: object) -> TransactionEvent:
    values: dict[str, object] = {
        "transaction_id": uuid.UUID("9b2f6c1e-4a7d-4f0e-9a51-3f1c2d7e8a90"),
        "event_time": EVENT_TIME,
        "source": Source.REPLAY,
        "replay_pass": 1,
        "Time": 3600.0,
        **{f"V{i}": float(i) / 10 for i in range(1, 29)},
        "Amount": 149.62,
        "actual_class": 0,
    }
    values.update(overrides)
    return TransactionEvent(**values)


# --------------------------------------------------------------------------- columns
def test_feature_columns_never_contain_the_label() -> None:
    assert LABEL_COLUMN not in FEATURE_COLUMNS
    assert "actual_class" not in FEATURE_COLUMNS
    assert len(FEATURE_COLUMNS) == 31  # V1..V28 + log_amount + hour_sin + hour_cos
    assert "Time" not in FEATURE_COLUMNS  # raw Time is replaced by hour-of-day encoding


def test_csv_columns_match_kaggle_header() -> None:
    assert CSV_COLUMNS[0] == "Time"
    assert CSV_COLUMNS[-2:] == ("Amount", "Class")
    assert len(CSV_COLUMNS) == 31
    assert CSV_COLUMNS[:-1] == RAW_INPUT_COLUMNS


# --------------------------------------------------------------------------- TransactionEvent
def test_transaction_json_round_trip() -> None:
    event = make_event()
    payload = json.loads(event.to_json_bytes())
    assert payload["event_time"] == "2026-11-14T18:00:00.123Z"
    assert payload["transaction_id"] == "9b2f6c1e-4a7d-4f0e-9a51-3f1c2d7e8a90"
    assert payload["source"] == "replay"
    assert TransactionEvent.model_validate_json(event.to_json_bytes()) == make_event(
        event_time=EVENT_TIME.replace(microsecond=123000)
    )


def test_transaction_key_and_raw_features() -> None:
    event = make_event()
    assert event.key == b"9b2f6c1e-4a7d-4f0e-9a51-3f1c2d7e8a90"
    features = event.raw_features()
    assert list(features) == list(RAW_INPUT_COLUMNS)
    assert features["Amount"] == 149.62


@pytest.mark.parametrize(
    "overrides",
    [
        {"Amount": -1.0},
        {"Time": -5.0},
        {"actual_class": 2},
        {"V1": float("nan")},
        {"V2": float("inf")},
        {"replay_pass": -1},
        {"source": "unknown"},
        {"event_time": datetime(2026, 1, 1, 0, 0)},  # naive datetime
    ],
)
def test_transaction_validation_rejects_bad_values(overrides: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        make_event(**overrides)


def test_transaction_rejects_unknown_fields() -> None:
    payload = json.loads(make_event().to_json_bytes())
    payload["Class"] = 1  # the label must never travel as a feature-like field
    with pytest.raises(ValidationError):
        TransactionEvent.model_validate(payload)


def test_actual_class_is_optional() -> None:
    assert make_event(actual_class=None).actual_class is None


# --------------------------------------------------------------------------- helpers
def test_format_utc_converts_and_rejects_naive() -> None:
    plus_three = datetime(2026, 11, 14, 21, 0, tzinfo=timezone(timedelta(hours=3)))
    assert schemas.format_utc(plus_three) == "2026-11-14T18:00:00.000Z"
    with pytest.raises(ValueError):
        schemas.format_utc(datetime(2026, 11, 14))


def test_utc_now_is_aware() -> None:
    assert schemas.utc_now().tzinfo is not None


# --------------------------------------------------------------------------- PredictionEvent
def test_prediction_event_parses_spark_json_without_null_fields() -> None:
    spark_json = (
        '{"transaction_id":"9b2f6c1e-4a7d-4f0e-9a51-3f1c2d7e8a90",'
        '"event_time":"2026-11-14T18:00:00.123Z","processed_at":"2026-11-14T18:00:01.402Z",'
        '"latency_ms":1279.0,"source":"manual","replay_pass":0,"amount":1.0,"hour":18,'
        '"fraud_probability":0.97,"prediction":1,"threshold":0.4,"model_name":"RF"}'
    )
    event = PredictionEvent.model_validate_json(spark_json)
    assert event.actual_class is None
    assert event.prediction == 1


@pytest.mark.parametrize(
    "field,value", [("hour", 24), ("fraud_probability", 1.5), ("threshold", 1.0)]
)
def test_prediction_event_bounds(field: str, value: object) -> None:
    payload = {
        "transaction_id": str(uuid.uuid4()),
        "event_time": "2026-11-14T18:00:00Z",
        "processed_at": "2026-11-14T18:00:01Z",
        "latency_ms": 1.0,
        "source": "replay",
        "replay_pass": 1,
        "amount": 1.0,
        "hour": 1,
        "fraud_probability": 0.5,
        "prediction": 0,
        "threshold": 0.5,
        "model_name": "m",
        field: value,
    }
    with pytest.raises(ValidationError):
        PredictionEvent.model_validate(payload)


# --------------------------------------------------------------------------- ControlCommand
@pytest.mark.parametrize(
    ("action", "paused", "rate", "new_rate", "expected"),
    [
        ("pause", False, 50.0, None, (True, 50.0)),
        ("resume", True, 50.0, None, (False, 50.0)),
        ("set_rate", True, 50.0, 120.0, (True, 120.0)),
        ("set_rate", False, 50.0, 5.0, (False, 5.0)),
    ],
)
def test_control_command_from_action_carries_full_state(
    action: str, paused: bool, rate: float, new_rate: float | None, expected: tuple[bool, float]
) -> None:
    command = ControlCommand.from_action(
        action,  # type: ignore[arg-type]
        current_paused=paused,
        current_rate=rate,
        new_rate=new_rate,
        now=EVENT_TIME,
    )
    assert (command.paused, command.rate) == expected
    assert json.loads(command.to_json_bytes())["issued_at"] == "2026-11-14T18:00:00.123Z"


def test_set_rate_requires_new_rate() -> None:
    with pytest.raises(ValueError):
        ControlCommand.from_action("set_rate", current_paused=False, current_rate=50.0)


@pytest.mark.parametrize(
    "payload",
    [
        {"command": "pause", "paused": False, "rate": 50.0},
        {"command": "resume", "paused": True, "rate": 50.0},
        {"command": "set_rate", "paused": False, "rate": 0.0},
        {"command": "set_rate", "paused": False, "rate": 5000.0},
        {"command": "explode", "paused": False, "rate": 5.0},
    ],
)
def test_control_command_validation(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ControlCommand(issued_at=EVENT_TIME, **payload)


# --------------------------------------------------------------------------- Spark parity
def test_transaction_fields_match_spark_schema() -> None:
    assert set(TRANSACTION_FIELD_TYPES) == set(TransactionEvent.model_fields)
    assert [f.name for f in transaction_schema().fields] == list(TRANSACTION_FIELD_TYPES)


def test_prediction_fields_match_spark_schema() -> None:
    assert set(PREDICTION_FIELD_TYPES) == set(PredictionEvent.model_fields)
    assert [f.name for f in prediction_schema().fields] == list(PREDICTION_FIELD_TYPES)


def test_csv_schema_types() -> None:
    schema = csv_schema()
    assert schema["Class"].dataType.simpleString() == "int"
    assert schema["Amount"].dataType.simpleString() == "double"
    assert [f.name for f in schema.fields] == list(CSV_COLUMNS)


def test_struct_from_field_types_rejects_unknown_type() -> None:
    with pytest.raises(ValueError, match="unsupported"):
        struct_from_field_types({"x": "decimal"})
