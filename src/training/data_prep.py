"""Loading, validation, de-duplication, stratified splitting and class-imbalance handling."""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.schemas import (
    AMOUNT_COLUMN,
    CSV_COLUMNS,
    LABEL_COLUMN,
    TIME_COLUMN,
    V_COLUMNS,
    WEIGHT_COLUMN,
)
from src.common.spark_schemas import csv_schema

KAGGLE_DATASET_URL = "https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud"


class DatasetMissingError(FileNotFoundError):
    """``creditcard.csv`` is not where the trainer expects it."""


class DatasetValidationError(ValueError):
    """The CSV exists but does not look like the Kaggle credit card fraud dataset."""


class Strategy(StrEnum):
    """Class-imbalance handling strategies compared during training."""

    CLASS_WEIGHTS = "class_weights"
    UNDERSAMPLING = "undersampling"


@dataclass(frozen=True)
class DatasetStats:
    rows: int
    frauds: int

    @property
    def fraud_rate(self) -> float:
        return self.frauds / self.rows if self.rows else 0.0

    def to_dict(self) -> dict[str, float | int]:
        return {"rows": self.rows, "frauds": self.frauds, "fraud_rate": round(self.fraud_rate, 6)}


@dataclass(frozen=True)
class Splits:
    train: DataFrame
    val: DataFrame
    test: DataFrame
    assigned: DataFrame | None = None  # all rows with the ``_split`` column (for diagnostics)


def missing_dataset_message(path: Path) -> str:
    return (
        f"Dataset not found at {path}.\n"
        f"  1. Download it from {KAGGLE_DATASET_URL} (free Kaggle account required).\n"
        "  2. Extract creditcard.csv from archive.zip into the project's data\\raw\\ folder,\n"
        "     so the file is data\\raw\\creditcard.csv.\n"
        "  Or, with a Kaggle API token in %USERPROFILE%\\.kaggle: .\\run.ps1 download-data"
    )


def ensure_dataset_exists(path: Path) -> None:
    """Raise :class:`DatasetMissingError` with download instructions if ``path`` is missing."""
    if not path.is_file():
        raise DatasetMissingError(missing_dataset_message(path))


def read_csv_header(path: Path) -> tuple[str, ...]:
    """Column names from the first line of a CSV file (quotes and whitespace stripped)."""
    with path.open(encoding="utf-8-sig") as handle:
        first_line = handle.readline()
    return tuple(name.strip().strip('"') for name in first_line.strip().split(","))


def load_raw_dataset(spark: SparkSession, path: Path) -> DataFrame:
    """Read ``creditcard.csv`` with an explicit schema after checking its header.

    The header is checked explicitly: Spark's own check (``enforceSchema=False``) only
    covers the columns a query reads, so a renamed column could slip through validation
    and fail much later. ``FAILFAST`` aborts on malformed rows instead of nulling them.
    """
    ensure_dataset_exists(path)
    header = read_csv_header(path)
    if header != CSV_COLUMNS:
        unexpected = sorted(set(header) - set(CSV_COLUMNS))
        missing = sorted(set(CSV_COLUMNS) - set(header))
        raise DatasetValidationError(
            f"{path.name} does not look like the Kaggle creditcard.csv: "
            f"missing columns {missing}, unexpected columns {unexpected}"
        )
    return spark.read.csv(
        str(path), header=True, schema=csv_schema(), enforceSchema=False, mode="FAILFAST"
    )


def compute_stats(df: DataFrame, label_col: str = LABEL_COLUMN) -> DatasetStats:
    """Row and fraud counts in a single pass."""
    row = df.agg(
        F.count(F.lit(1)).alias("rows"), F.coalesce(F.sum(label_col), F.lit(0)).alias("frauds")
    ).first()
    assert row is not None
    return DatasetStats(rows=int(row["rows"]), frauds=int(row["frauds"]))


def validate_dataset(df: DataFrame, label_col: str = LABEL_COLUMN) -> DatasetStats:
    """Check the label is binary and both classes are present; return dataset stats."""
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.coalesce(F.sum(label_col), F.lit(0)).alias("frauds"),
        F.sum((F.col(label_col).isNull() | ~F.col(label_col).isin(0, 1)).cast("int")).alias("bad"),
    ).first()
    assert row is not None
    stats = DatasetStats(rows=int(row["rows"]), frauds=int(row["frauds"]))
    if stats.rows == 0:
        raise DatasetValidationError("the dataset is empty")
    if row["bad"]:
        raise DatasetValidationError(
            f"{row['bad']} rows have a '{label_col}' value outside {{0, 1}}"
        )
    if stats.frauds == 0 or stats.frauds == stats.rows:
        raise DatasetValidationError("the dataset must contain both fraud and legitimate rows")
    return stats


def drop_exact_duplicates(df: DataFrame) -> tuple[DataFrame, int]:
    """Remove exact duplicate rows *before* splitting.

    Identical rows landing in both train and test would leak information and inflate test
    scores. Returns the de-duplicated DataFrame and the number of rows removed.
    """
    before = df.count()
    deduplicated = df.dropDuplicates()
    removed = before - deduplicated.count()
    return deduplicated, removed


SPLIT_COLUMN = "_split"
SPLIT_NAMES = ("train", "val", "test")
GROUP_COLUMNS: tuple[str, ...] = (*V_COLUMNS, AMOUNT_COLUMN)
"""Model inputs except ``Time``. Rows equal on all of these are "feature twins"."""


def assign_splits(
    df: DataFrame,
    weights: tuple[float, float, float] = (0.70, 0.15, 0.15),
    seed: int = 42,
    label_col: str = LABEL_COLUMN,
    group_cols: tuple[str, ...] = GROUP_COLUMNS,
) -> DataFrame:
    """Add a ``_split`` column (``train``/``val``/``test``): stratified and group-aware.

    * **Group-aware:** the key is ``xxhash64(seed, V1..V28, Amount)``, so it ignores
      ``Time``. Rows that differ only in ``Time`` (about 12k "feature twins" in this
      dataset) share a key, get the same ``rank()`` and therefore always land in the same
      split. Otherwise an identical feature vector could be trained on and then tested.
    * **Stratified and exact:** ranks are computed within each class and cut at the
      cumulative weights, so every split has the same fraud ratio (exact up to the size
      of a twin group at a boundary). The result doesn't depend on row order.
    """
    if len(weights) != 3 or any(w <= 0 for w in weights):
        raise ValueError("weights must be three positive numbers")
    if abs(sum(weights) - 1.0) > 1e-9:
        raise ValueError("weights must sum to 1")

    key = F.xxhash64(F.lit(seed), *[F.col(c) for c in group_cols])
    by_class = Window.partitionBy(label_col)
    ordered = by_class.orderBy("_split_key", *group_cols)
    position = (F.rank().over(ordered) - 1) / F.count(F.lit(1)).over(by_class)
    train_end, val_end = weights[0], weights[0] + weights[1]
    return (
        df.withColumn("_split_key", key)
        .withColumn("_split_pos", position)
        .withColumn(
            SPLIT_COLUMN,
            F.when(F.col("_split_pos") < train_end, F.lit("train"))
            .when(F.col("_split_pos") < val_end, F.lit("val"))
            .otherwise(F.lit("test")),
        )
        .drop("_split_key", "_split_pos")
    )


def stratified_split(
    df: DataFrame,
    weights: tuple[float, float, float] = (0.70, 0.15, 0.15),
    seed: int = 42,
    label_col: str = LABEL_COLUMN,
) -> Splits:
    """Disjoint, group-aware train / validation / test splits with the same class ratio.

    The window in :func:`assign_splits` leaves the data in one partition per class. Each
    split is repartitioned so training uses every core.
    """
    assigned = assign_splits(df, weights, seed, label_col)
    partitions = int(df.sparkSession.conf.get("spark.sql.shuffle.partitions", "8"))
    train, val, test = (
        assigned.filter(F.col(SPLIT_COLUMN) == name).drop(SPLIT_COLUMN).repartition(partitions)
        for name in SPLIT_NAMES
    )
    return Splits(train=train, val=val, test=test, assigned=assigned)


def feature_twin_stats(
    assigned: DataFrame, group_cols: tuple[str, ...] = GROUP_COLUMNS
) -> dict[str, int]:
    """Count feature-twin groups and how many span more than one split (should be 0)."""
    groups = assigned.groupBy(*group_cols).agg(
        F.count(F.lit(1)).alias("n"), F.countDistinct(SPLIT_COLUMN).alias("splits")
    )
    row = (
        groups.filter(F.col("n") > 1)
        .agg(
            F.count(F.lit(1)).alias("groups"),
            F.coalesce(F.sum("n"), F.lit(0)).alias("rows"),
            F.coalesce(F.sum((F.col("splits") > 1).cast("int")), F.lit(0)).alias("spanning"),
        )
        .first()
    )
    assert row is not None
    return {
        "twin_groups": int(row["groups"]),
        "twin_rows": int(row["rows"]),
        "groups_spanning_splits": int(row["spanning"]),
    }


def class_weight_values(n_negative: int, n_positive: int) -> tuple[float, float]:
    """Balanced class weights ``n / (2 * n_class)`` for (legitimate, fraud)."""
    if n_negative <= 0 or n_positive <= 0:
        raise ValueError("both classes need at least one row")
    total = n_negative + n_positive
    return total / (2.0 * n_negative), total / (2.0 * n_positive)


def add_class_weights(
    df: DataFrame, label_col: str = LABEL_COLUMN, weight_col: str = WEIGHT_COLUMN
) -> DataFrame:
    """Add a balanced per-row weight column (frauds get a much larger weight)."""
    stats = compute_stats(df, label_col)
    w_negative, w_positive = class_weight_values(stats.rows - stats.frauds, stats.frauds)
    return df.withColumn(
        weight_col,
        F.when(F.col(label_col) == 1, F.lit(w_positive)).otherwise(F.lit(w_negative)),
    )


def add_unit_weights(df: DataFrame, weight_col: str = WEIGHT_COLUMN) -> DataFrame:
    """Add a constant weight of 1.0 (used with undersampling)."""
    return df.withColumn(weight_col, F.lit(1.0))


def undersample_majority(
    df: DataFrame, ratio: float = 10.0, seed: int = 42, label_col: str = LABEL_COLUMN
) -> DataFrame:
    """Keep every fraud and randomly sample legitimate rows to about ``ratio`` per fraud.

    Applied to the training split only. Validation and test keep the real class balance.
    """
    if ratio <= 0:
        raise ValueError("ratio must be positive")
    stats = compute_stats(df, label_col)
    n_negative = stats.rows - stats.frauds
    if stats.frauds == 0 or n_negative == 0:
        raise ValueError("both classes need at least one row")
    fraction = min(1.0, ratio * stats.frauds / n_negative)
    frauds = df.filter(F.col(label_col) == 1)
    legit = df.filter(F.col(label_col) == 0).sample(
        withReplacement=False, fraction=fraction, seed=seed
    )
    return frauds.unionByName(legit)


def prepare_training_frame(
    train: DataFrame, strategy: Strategy, *, undersample_ratio: float, seed: int
) -> DataFrame:
    """Training DataFrame for an imbalance strategy, always with a ``weight`` column."""
    if strategy is Strategy.CLASS_WEIGHTS:
        return add_class_weights(train)
    if strategy is Strategy.UNDERSAMPLING:
        return add_unit_weights(undersample_majority(train, undersample_ratio, seed))
    raise ValueError(f"unknown strategy: {strategy}")


def save_split(df: DataFrame, path: Path, sort_by: str | None = TIME_COLUMN) -> int:
    """Write a split as one Parquet file (via pandas/Arrow) and return its row count.

    Written atomically (temp file + rename) so readers never see a half-written file. A
    single Parquet file is easier for the producer and dashboard to read than Spark's
    part-file directories, and avoids Hadoop rename semantics on Windows bind mounts.
    """
    pdf = df.toPandas()
    if sort_by is not None:
        pdf = pdf.sort_values(sort_by, kind="stable").reset_index(drop=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, suffix=".parquet.tmp")
    os.close(fd)
    try:
        pdf.to_parquet(tmp_name, index=False)
        os.replace(tmp_name, path)
    finally:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
    return len(pdf)
