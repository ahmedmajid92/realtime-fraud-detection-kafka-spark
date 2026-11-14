"""The ``foreachBatch`` function: score one micro-batch and fan it out to every sink."""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

from pyspark import StorageLevel
from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.streaming.sinks import write_kafka, write_postgres
from src.streaming.threshold import ThresholdProvider
from src.streaming.transforms import apply_threshold, enrich, is_valid_transaction, score


class BatchProcessor:
    """Callable passed to ``DataStreamWriter.foreachBatch``.

    For each micro-batch:

    1. drop invalid rows (logged);
    2. read the live alert threshold (dashboard slider) from Postgres;
    3. score with the PipelineModel, add latency/hour, label with the threshold;
    4. write every row to ``predictions`` and flagged rows to ``fraud-alerts`` (Kafka);
    5. upsert every row into Postgres (idempotent).

    The scored result is cached once, so the model runs a single time per batch even
    though three sinks read it.
    """

    def __init__(
        self,
        *,
        model: PipelineModel,
        model_name: str,
        bootstrap_servers: str,
        predictions_topic: str,
        alerts_topic: str,
        postgres_connect_kwargs: Mapping[str, Any],
        threshold_provider: ThresholdProvider,
        logger: Any,
    ) -> None:
        self._model = model
        self._model_name = model_name
        self._bootstrap = bootstrap_servers
        self._predictions_topic = predictions_topic
        self._alerts_topic = alerts_topic
        self._pg_kwargs = dict(postgres_connect_kwargs)
        self._thresholds = threshold_provider
        self._logger = logger

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        started = time.monotonic()
        batch = batch.persist(StorageLevel.MEMORY_AND_DISK)
        result: DataFrame | None = None
        try:
            counts = batch.agg(
                F.count(F.lit(1)).alias("total"),
                F.coalesce(F.sum(is_valid_transaction().cast("int")), F.lit(0)).alias("valid"),
            ).first()
            total, valid = int(counts["total"]), int(counts["valid"])
            if total > valid:
                self._logger.warning("batch.invalid_rows", batch_id=batch_id, dropped=total - valid)
            if valid == 0:
                return

            threshold = self._thresholds.current()
            result = apply_threshold(
                enrich(score(batch.filter(is_valid_transaction()), self._model), self._model_name),
                threshold,
            ).persist(StorageLevel.MEMORY_AND_DISK)

            write_kafka(result, self._bootstrap, self._predictions_topic)
            write_kafka(
                result.filter(F.col("prediction") == 1), self._bootstrap, self._alerts_topic
            )
            write_postgres(result, self._pg_kwargs)

            stats = result.agg(
                F.sum("prediction").alias("alerts"),
                F.avg("latency_ms").alias("avg_latency_ms"),
                F.max("latency_ms").alias("max_latency_ms"),
            ).first()
            self._logger.info(
                "batch.processed",
                batch_id=batch_id,
                rows=valid,
                alerts=int(stats["alerts"] or 0),
                threshold=threshold,
                avg_latency_ms=round(float(stats["avg_latency_ms"] or 0.0), 1),
                max_latency_ms=round(float(stats["max_latency_ms"] or 0.0), 1),
                duration_ms=round((time.monotonic() - started) * 1000, 1),
            )
        finally:
            if result is not None:
                result.unpersist()
            batch.unpersist()
