"""The ``foreachBatch`` function: label one scored micro-batch and fan it out to every sink."""

from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.streaming.sinks import write_kafka, write_postgres
from src.streaming.threshold import ThresholdProvider
from src.streaming.transforms import apply_threshold


class BatchProcessor:
    """Callable passed to ``DataStreamWriter.foreachBatch``.

    The incoming micro-batch is already validated, scored and enriched by the streaming
    plan (see :mod:`src.streaming.transforms`). For each batch:

    1. read the live alert threshold (dashboard slider) from Postgres;
    2. label rows with it (one partition: batches are small, so one task and one
       database connection beat three);
    3. compute batch statistics. This single aggregation also materializes the cached,
       scored result, so the model runs once; empty batches stop here;
    4. write every row to ``predictions`` and flagged rows to ``fraud-alerts`` (Kafka);
    5. upsert every row into Postgres (idempotent).

    (An ``Observation`` would avoid the aggregation job, but its metrics are never
    delivered when the first action is a Kafka *write command* over cached data, so
    ``Observation.get`` would block the query forever.)
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        predictions_topic: str,
        alerts_topic: str,
        postgres_connect_kwargs: Mapping[str, Any],
        threshold_provider: ThresholdProvider,
        logger: Any,
    ) -> None:
        self._bootstrap = bootstrap_servers
        self._predictions_topic = predictions_topic
        self._alerts_topic = alerts_topic
        self._pg_kwargs = dict(postgres_connect_kwargs)
        self._thresholds = threshold_provider
        self._logger = logger

    def __call__(self, batch: DataFrame, batch_id: int) -> None:
        started = time.monotonic()
        threshold = self._thresholds.current()
        result = apply_threshold(batch.coalesce(1), threshold).persist(StorageLevel.MEMORY_AND_DISK)
        try:
            stats = result.agg(
                F.count(F.lit(1)).alias("rows"),
                F.coalesce(F.sum("prediction"), F.lit(0)).alias("alerts"),
                F.avg("latency_ms").alias("avg_latency_ms"),
                F.max("latency_ms").alias("max_latency_ms"),
            ).first()
            if stats is None or not stats["rows"]:
                self._logger.debug("batch.empty", batch_id=batch_id)
                return
            write_kafka(result, self._bootstrap, self._predictions_topic)
            write_kafka(
                result.filter(F.col("prediction") == 1), self._bootstrap, self._alerts_topic
            )
            write_postgres(result, self._pg_kwargs)
            self._logger.info(
                "batch.processed",
                batch_id=batch_id,
                rows=int(stats["rows"]),
                alerts=int(stats["alerts"]),
                threshold=threshold,
                avg_latency_ms=round(float(stats["avg_latency_ms"] or 0.0), 1),
                max_latency_ms=round(float(stats["max_latency_ms"] or 0.0), 1),
                duration_ms=round((time.monotonic() - started) * 1000, 1),
            )
        finally:
            result.unpersist()
