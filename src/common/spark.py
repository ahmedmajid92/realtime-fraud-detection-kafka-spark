"""SparkSession factory shared by the trainer, the stream processor and the unit tests."""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import SparkSession


def build_spark_session(
    app_name: str,
    *,
    master: str,
    driver_memory: str,
    extra_conf: Mapping[str, str] | None = None,
) -> SparkSession:
    """Create (or reuse) a SparkSession with project-wide defaults.

    * UTC everywhere (session time zone and JVM), so latencies and timestamps are consistent;
    * Arrow enabled for fast ``toPandas()``;
    * a small shuffle partition count suited to a laptop.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.executor.extraJavaOptions", "-Duser.timezone=UTC")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
    )
    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
