"""Spark Structured Streaming job: Kafka ``transactions`` → model → Kafka + PostgreSQL.

Run inside the ``fraud-spark`` image::

    python -m src.streaming.stream_processor
"""

from __future__ import annotations

import json
import signal
import sys
import threading
from functools import partial
from pathlib import Path
from typing import Any

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession

from src.common import db
from src.common.config import (
    get_kafka_settings,
    get_logging_settings,
    get_path_settings,
    get_postgres_settings,
    get_streaming_settings,
)
from src.common.health import Heartbeat
from src.common.kafka_utils import wait_for_kafka
from src.common.logging_setup import configure_logging, get_logger
from src.common.spark import build_spark_session
from src.streaming.batch import BatchProcessor
from src.streaming.listener import ProgressListener
from src.streaming.threshold import ThresholdProvider
from src.streaming.transforms import (
    observe_input_quality,
    parse_transactions,
    score_valid_transactions,
)

SERVICE = "stream-processor"
QUERY_NAME = "fraud-scoring"

logger = get_logger(__name__)


class ModelNotTrainedError(FileNotFoundError):
    """The trained model or its metrics file is missing."""


def load_model_metadata(metrics_path: Path, model_path: Path) -> dict[str, Any]:
    """Read ``metrics.json`` and check the saved model exists."""
    if not metrics_path.is_file() or not (model_path / "metadata").exists():
        raise ModelNotTrainedError(
            f"No trained model found ({model_path}, {metrics_path}). "
            "Train it first: .\\run.ps1 train"
        )
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    best = metrics["best"]
    return {
        "run_id": metrics["run_id"],
        "model_name": best["display_name"],
        "default_threshold": float(best["threshold"]),
    }


def build_stream(spark: SparkSession, bootstrap: str, topic: str, settings: Any) -> Any:
    """Kafka source → parsed transactions (streaming DataFrame)."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", settings.starting_offsets)
        .option("maxOffsetsPerTrigger", settings.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )
    return parse_transactions(raw)


def main() -> int:
    configure_logging(SERVICE, get_logging_settings().log_level)
    kafka = get_kafka_settings()
    pg = get_postgres_settings()
    paths = get_path_settings()
    settings = get_streaming_settings()

    stop_requested = threading.Event()

    def _request_stop(signum: int, _frame: Any) -> None:
        logger.info("shutdown.requested", signal=signal.Signals(signum).name)
        stop_requested.set()

    signal.signal(signal.SIGTERM, _request_stop)
    signal.signal(signal.SIGINT, _request_stop)

    try:
        meta = load_model_metadata(paths.metrics_path, paths.model_path)
    except ModelNotTrainedError as exc:
        logger.error("model.missing", error=str(exc))
        return 2

    wait_for_kafka(
        kafka.kafka_bootstrap_servers,
        required_topics=(
            kafka.topic_transactions,
            kafka.topic_predictions,
            kafka.topic_fraud_alerts,
        ),
        logger=logger,
    )
    db.wait_for_postgres(pg, logger=logger)

    connect = partial(db.connect, pg)
    with connect() as conn:
        active_threshold = db.sync_threshold_with_model(
            conn,
            model_run_id=meta["run_id"],
            default_threshold=meta["default_threshold"],
            override=settings.alert_threshold_override,
        )
    logger.info(
        "model.selected",
        model=meta["model_name"],
        run_id=meta["run_id"],
        threshold=active_threshold,
    )

    spark = build_spark_session(
        "fraud-stream-processor",
        master=settings.spark_master,
        driver_memory=settings.stream_driver_memory,
        extra_conf={
            "spark.sql.shuffle.partitions": "4",
            "spark.sql.streaming.noDataProgressEventInterval": "10000",
        },
    )
    model = PipelineModel.load(str(paths.model_path))
    logger.info("model.loaded", path=str(paths.model_path))

    heartbeat = Heartbeat(paths.heartbeat_path(SERVICE))
    heartbeat.beat(force=True)
    spark.streams.addListener(
        ProgressListener(
            heartbeat,
            connect,
            logger,
            predictions_retention_hours=settings.predictions_retention_hours,
        )
    )

    thresholds = ThresholdProvider(connect, default=active_threshold, logger=logger)
    processor = BatchProcessor(
        bootstrap_servers=kafka.kafka_bootstrap_servers,
        predictions_topic=kafka.topic_predictions,
        alerts_topic=kafka.topic_fraud_alerts,
        postgres_connect_kwargs=pg.connect_kwargs(),
        threshold_provider=thresholds,
        logger=logger,
    )

    query = (
        score_valid_transactions(
            observe_input_quality(
                build_stream(
                    spark, kafka.kafka_bootstrap_servers, kafka.topic_transactions, settings
                )
            ),
            model,
            meta["model_name"],
        )
        .writeStream.queryName(QUERY_NAME)
        .foreachBatch(processor)
        .option("checkpointLocation", str(paths.stream_checkpoint_path))
        .trigger(processingTime=settings.trigger_interval)
        .start()
    )
    logger.info(
        "query.running",
        trigger=settings.trigger_interval,
        checkpoint=str(paths.stream_checkpoint_path),
        spark_ui="http://localhost:4040",
    )

    exit_code = 0
    try:
        while not stop_requested.is_set() and query.isActive:
            query.awaitTermination(1)
    finally:
        if query.isActive:
            logger.info("query.stopping")
            query.stop()
        error = query.exception()
        if error is not None:
            logger.error("query.failed", error=str(error))
            exit_code = 1
        thresholds.close()
        spark.stop()
        logger.info("shutdown.complete", exit_code=exit_code)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
