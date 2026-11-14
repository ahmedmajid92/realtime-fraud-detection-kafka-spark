"""Spark ``StructType`` schemas generated from :mod:`src.common.schemas`.

Kept separate from ``schemas.py`` because it imports PySpark, which only the
``fraud-spark`` image has.
"""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql.types import (
    DataType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.schemas import (
    CSV_COLUMNS,
    LABEL_COLUMN,
    PREDICTION_FIELD_TYPES,
    TRANSACTION_FIELD_TYPES,
)

_SPARK_TYPES: dict[str, DataType] = {
    "string": StringType(),
    "double": DoubleType(),
    "int": IntegerType(),
    "timestamp": TimestampType(),
}


def struct_from_field_types(field_types: Mapping[str, str]) -> StructType:
    """Build a nullable ``StructType`` from ``{field_name: spark_type_name}``."""
    try:
        return StructType(
            [
                StructField(name, _SPARK_TYPES[type_name], True)
                for name, type_name in field_types.items()
            ]
        )
    except KeyError as exc:
        raise ValueError(f"unsupported Spark type name: {exc.args[0]}") from exc


def transaction_schema() -> StructType:
    """Schema used to parse the JSON value of ``transactions`` messages."""
    return struct_from_field_types(TRANSACTION_FIELD_TYPES)


def prediction_schema() -> StructType:
    """Schema of a scored transaction (``predictions`` / ``fraud-alerts``)."""
    return struct_from_field_types(PREDICTION_FIELD_TYPES)


def csv_schema() -> StructType:
    """Explicit schema of ``creditcard.csv`` (all doubles, integer label)."""
    return StructType(
        [
            StructField(name, IntegerType() if name == LABEL_COLUMN else DoubleType(), True)
            for name in CSV_COLUMNS
        ]
    )
