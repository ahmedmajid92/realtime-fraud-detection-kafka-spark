"""Data loading, validation, de-duplication, exact stratified split and imbalance handling."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import functions as F

from src.common.schemas import LABEL_COLUMN, WEIGHT_COLUMN
from src.training import data_prep
from src.training.data_prep import Strategy
from tests.conftest import make_transactions

pytestmark = pytest.mark.spark


def write_csv(frame: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path


def test_missing_dataset_message_has_download_instructions(tmp_path: Path) -> None:
    with pytest.raises(data_prep.DatasetMissingError) as excinfo:
        data_prep.ensure_dataset_exists(tmp_path / "creditcard.csv")
    message = str(excinfo.value)
    assert data_prep.KAGGLE_DATASET_URL in message
    assert "data\\raw\\creditcard.csv" in message


def test_load_and_validate_dataset(spark, tmp_path: Path) -> None:
    path = write_csv(make_transactions(50, 5), tmp_path / "creditcard.csv")
    frame = data_prep.load_raw_dataset(spark, path)
    stats = data_prep.validate_dataset(frame)
    assert (stats.rows, stats.frauds) == (55, 5)
    assert stats.fraud_rate == pytest.approx(5 / 55)
    assert stats.to_dict()["rows"] == 55


def test_load_rejects_wrong_header(spark, tmp_path: Path) -> None:
    bad = make_transactions(10, 2).rename(columns={"Amount": "amount_usd"})
    path = write_csv(bad, tmp_path / "creditcard.csv")
    with pytest.raises(data_prep.DatasetValidationError, match="amount_usd"):
        data_prep.load_raw_dataset(spark, path)


def test_read_csv_header_strips_quotes_and_bom(tmp_path: Path) -> None:
    path = tmp_path / "quoted.csv"
    path.write_text('﻿"Time","V1" , "Class"\n1,2,0\n', encoding="utf-8")
    assert data_prep.read_csv_header(path) == ("Time", "V1", "Class")


@pytest.mark.parametrize(
    ("labels", "message"),
    [([], "empty"), ([0, 0, 0], "both"), ([1, 1], "both"), ([0, 1, 3], "outside")],
)
def test_validate_dataset_rejects_bad_labels(spark, labels: list[int], message: str) -> None:
    frame = spark.createDataFrame(
        [(float(i), label) for i, label in enumerate(labels)], "Amount double, Class int"
    )
    with pytest.raises(data_prep.DatasetValidationError, match=message):
        data_prep.validate_dataset(frame)


def test_drop_exact_duplicates(spark) -> None:
    pdf = make_transactions(30, 3)
    with_dupes = pd.concat([pdf, pdf.iloc[[0, 1, 31]]], ignore_index=True)
    deduplicated, removed = data_prep.drop_exact_duplicates(spark.createDataFrame(with_dupes))
    assert removed == 3
    assert deduplicated.count() == 33


def _split_ids(splits: data_prep.Splits) -> dict[str, set[float]]:
    return {
        name: {row["Time"] for row in getattr(splits, name).select("Time").collect()}
        for name in ("train", "val", "test")
    }


def test_stratified_split_is_exact_disjoint_and_complete(spark) -> None:
    pdf = make_transactions(1000, 100)
    pdf["Time"] = range(len(pdf))  # unique id per row
    splits = data_prep.stratified_split(spark.createDataFrame(pdf), seed=42)
    counts = {
        name: getattr(splits, name)
        .groupBy(LABEL_COLUMN)
        .count()
        .toPandas()
        .set_index(LABEL_COLUMN)["count"]
        .to_dict()
        for name in ("train", "val", "test")
    }
    assert counts["train"] == {0: 700, 1: 70}
    assert counts["val"] == {0: 150, 1: 15}
    assert counts["test"] == {0: 150, 1: 15}
    ids = _split_ids(splits)
    assert not ids["train"] & ids["val"]
    assert not ids["train"] & ids["test"]
    assert not ids["val"] & ids["test"]
    assert len(ids["train"] | ids["val"] | ids["test"]) == len(pdf)
    assert splits.train.columns == list(pdf.columns)  # helper columns are dropped


def test_stratified_split_is_reproducible_and_order_independent(spark) -> None:
    pdf = make_transactions(300, 30)
    pdf["Time"] = range(len(pdf))
    first = _split_ids(data_prep.stratified_split(spark.createDataFrame(pdf), seed=7))
    shuffled = pdf.sample(frac=1.0, random_state=3)
    second = _split_ids(
        data_prep.stratified_split(spark.createDataFrame(shuffled).repartition(3), seed=7)
    )
    assert first == second
    other_seed = _split_ids(data_prep.stratified_split(spark.createDataFrame(pdf), seed=8))
    assert other_seed["test"] != first["test"]


@pytest.mark.parametrize("weights", [(0.5, 0.5), (0.7, 0.2, 0.2), (0.8, 0.3, -0.1)])
def test_stratified_split_rejects_bad_weights(spark, weights: tuple[float, ...]) -> None:
    frame = spark.createDataFrame(make_transactions(10, 2))
    with pytest.raises(ValueError):
        data_prep.stratified_split(frame, weights)  # type: ignore[arg-type]


def test_class_weight_values() -> None:
    w_legit, w_fraud = data_prep.class_weight_values(n_negative=990, n_positive=10)
    assert w_legit == pytest.approx(1000 / 1980)
    assert w_fraud == pytest.approx(50.0)
    with pytest.raises(ValueError):
        data_prep.class_weight_values(0, 5)


def test_add_class_weights(spark) -> None:
    weighted = data_prep.add_class_weights(spark.createDataFrame(make_transactions(90, 10)))
    by_class = {
        r[LABEL_COLUMN]: r["w"]
        for r in weighted.groupBy(LABEL_COLUMN).agg(F.first(WEIGHT_COLUMN).alias("w")).collect()
    }
    assert by_class[1] == pytest.approx(100 / 20)
    assert by_class[0] == pytest.approx(100 / 180)


def test_undersample_majority_ratio(spark) -> None:
    frame = spark.createDataFrame(make_transactions(2000, 20))
    sampled = data_prep.undersample_majority(frame, ratio=10, seed=1)
    stats = data_prep.compute_stats(sampled)
    assert stats.frauds == 20  # every fraud kept
    assert 120 <= stats.rows - stats.frauds <= 280  # ~200 legit, binomial noise
    with pytest.raises(ValueError):
        data_prep.undersample_majority(frame, ratio=0)


def test_undersample_requires_both_classes(spark) -> None:
    frame = spark.createDataFrame(make_transactions(20, 0))
    with pytest.raises(ValueError):
        data_prep.undersample_majority(frame)


@pytest.mark.parametrize("strategy", list(Strategy))
def test_prepare_training_frame_always_has_weights(spark, strategy: Strategy) -> None:
    frame = spark.createDataFrame(make_transactions(200, 20))
    prepared = data_prep.prepare_training_frame(frame, strategy, undersample_ratio=5, seed=1)
    assert WEIGHT_COLUMN in prepared.columns
    assert prepared.filter(F.col(WEIGHT_COLUMN).isNull()).count() == 0


def test_prepare_training_frame_rejects_unknown_strategy(spark) -> None:
    frame = spark.createDataFrame(make_transactions(10, 2))
    with pytest.raises(ValueError):
        data_prep.prepare_training_frame(frame, "smote", undersample_ratio=5, seed=1)  # type: ignore[arg-type]


def test_save_split_writes_sorted_parquet(spark, tmp_path: Path) -> None:
    frame = spark.createDataFrame(make_transactions(40, 4))
    target = tmp_path / "processed" / "test.parquet"
    assert data_prep.save_split(frame, target) == 44
    written = pd.read_parquet(target)
    assert written["Time"].is_monotonic_increasing
    assert not list(target.parent.glob("*.tmp"))  # temp file cleaned up
