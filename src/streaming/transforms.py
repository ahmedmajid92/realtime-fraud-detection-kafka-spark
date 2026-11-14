"""Pure DataFrame transformations of the stream processor.

None of these functions touch Kafka or Postgres, so unit tests run them on small static
DataFrames, and the same functions build the streaming plan::

    streaming plan (built once):  parse_transactions → observe input quality
                                  → filter(is_valid_transaction) → score → enrich
    per micro-batch:              apply_threshold → sinks          (foreachBatch)

Scoring is part of the streaming plan, not ``foreachBatch``. The PipelineModel is
therefore applied and its tree ensemble broadcast **once**, not re-planned for every
micro-batch (that cost about 0.5 s per batch).
"""

from __future__ import annotations

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from src.common.schemas import (
    AMOUNT_COLUMN,
    PREDICTION_FIELDS,
    RAW_INPUT_COLUMNS,
    REQUIRED_TRANSACTION_FIELDS,
    TIME_COLUMN,
    Source,
)
from src.common.spark_schemas import transaction_schema

FRAUD_PROBABILITY_COL = "fraud_probability"


def parse_transactions(raw: DataFrame) -> DataFrame:
    """Kafka records (binary ``value``) → one typed column per transaction field.

    Malformed JSON yields a row of NULLs (Spark's PERMISSIVE mode) and an unparsable
    ``event_time`` becomes NULL via ``try_to_timestamp``; both are removed by
    :func:`is_valid_transaction` instead of crashing the micro-batch.
    """
    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), transaction_schema()).alias("txn")
    ).select("txn.*")
    return parsed.withColumn("event_time", F.try_to_timestamp(F.col("event_time")))


UUID_PATTERN = r"^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$"
_INFINITY = float("inf")


def is_valid_transaction() -> Column:
    """Row filter: everything a row needs to be scored **and** stored without error.

    Required fields present, a well-formed UUID (the Postgres key is ``uuid``), a known
    source, a non-negative replay pass, finite non-negative inputs (Spark's JSON reader
    accepts ``NaN``/``Infinity`` tokens, and either would make ``VectorAssembler`` fail
    the whole micro-batch), and a binary or missing label. Rows that fail are dropped
    and counted, so one bad message can never crash-loop the query.
    """
    condition = F.lit(True)
    for name in REQUIRED_TRANSACTION_FIELDS:
        condition = condition & F.col(name).isNotNull()
    for name in RAW_INPUT_COLUMNS:
        column = F.col(name)
        condition = condition & ~F.isnan(column) & (F.abs(column) < F.lit(_INFINITY))
    condition = condition & F.col("transaction_id").rlike(UUID_PATTERN)
    condition = condition & F.col("source").isin([s.value for s in Source])
    condition = condition & (F.col("replay_pass") >= 0)
    condition = condition & (F.col(AMOUNT_COLUMN) >= 0) & (F.col(TIME_COLUMN) >= 0)
    condition = condition & (F.col("actual_class").isNull() | F.col("actual_class").isin(0, 1))
    return condition


INPUT_QUALITY_METRIC = "input_quality"


def observe_input_quality(parsed: DataFrame) -> DataFrame:
    """Attach observed metrics (``total``, ``invalid`` rows per micro-batch) to the stream.

    Spark collects them while the batch runs, with no extra job, and reports them in each
    ``StreamingQueryProgress.observedMetrics`` (logged by the listener).
    """
    valid = F.coalesce(is_valid_transaction(), F.lit(False))
    return parsed.observe(
        INPUT_QUALITY_METRIC,
        F.count(F.lit(1)).alias("total"),
        F.sum((~valid).cast("int")).alias("invalid"),
    )


def score_valid_transactions(parsed: DataFrame, model: PipelineModel, model_name: str) -> DataFrame:
    """Drop invalid rows, score with the model and add output fields (works on streams)."""
    return enrich(score(parsed.filter(is_valid_transaction()), model), model_name)


def score(transactions: DataFrame, model: PipelineModel) -> DataFrame:
    """Apply the saved PipelineModel and extract ``fraud_probability`` = P(class 1)."""
    return model.transform(transactions).withColumn(
        FRAUD_PROBABILITY_COL, vector_to_array(F.col("probability")).getItem(1)
    )


def enrich(scored: DataFrame, model_name: str) -> DataFrame:
    """Keep the output fields and add processing time, latency, hour and model name.

    ``latency_ms`` = time from the producer's ``event_time`` to the **start** of this
    micro-batch's execution, i.e. Kafka queueing plus trigger wait. ``current_timestamp()``
    is fixed once when the batch query starts, so it excludes model inference and the
    sink writes. The end-to-end figure (``stored_at - event_time`` in Postgres) covers
    those as well.
    """
    processed_at = F.current_timestamp()
    latency_ms = (F.unix_micros(processed_at) - F.unix_micros(F.col("event_time"))) / F.lit(1000.0)
    return scored.select(
        F.col("transaction_id"),
        F.col("event_time"),
        processed_at.alias("processed_at"),
        latency_ms.alias("latency_ms"),
        F.col("source"),
        F.col("replay_pass"),
        F.col(AMOUNT_COLUMN).alias("amount"),
        F.floor(F.pmod(F.col(TIME_COLUMN) / F.lit(3600.0), F.lit(24.0))).cast("int").alias("hour"),
        F.col(FRAUD_PROBABILITY_COL),
        F.col("actual_class"),
        F.lit(model_name).alias("model_name"),
    )


def apply_threshold(enriched: DataFrame, threshold: float) -> DataFrame:
    """Label each row: ``prediction = 1`` when ``fraud_probability >= threshold``."""
    if not 0.0 < threshold < 1.0:
        raise ValueError(f"threshold must be in (0, 1), got {threshold}")
    labelled = enriched.withColumn(
        "prediction", (F.col(FRAUD_PROBABILITY_COL) >= F.lit(float(threshold))).cast("int")
    ).withColumn("threshold", F.lit(float(threshold)))
    return labelled.select(*PREDICTION_FIELDS)


def to_kafka_records(predictions: DataFrame) -> DataFrame:
    """``key`` = transaction id, ``value`` = prediction JSON (null fields are omitted)."""
    return predictions.select(
        F.col("transaction_id").alias("key"),
        F.to_json(F.struct(*[F.col(name) for name in PREDICTION_FIELDS])).alias("value"),
    )
