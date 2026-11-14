"""Shared fixtures: isolated environment, synthetic transactions, a local SparkSession,
a tiny trained PipelineModel and a realistic ``metrics.json`` structure."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import numpy as np
import pandas as pd
import pytest

from src.common import config
from src.common.schemas import LABEL_COLUMN, TIME_COLUMN, V_COLUMNS

BASE_ENV = {
    "KAFKA_BOOTSTRAP_SERVERS": "kafka-test:9092",
    "POSTGRES_HOST": "pg-test",
    "POSTGRES_PORT": "5432",
    "POSTGRES_DB": "fraud",
    "POSTGRES_USER": "fraud",
    "POSTGRES_PASSWORD": "s3cret-test",
    "LOG_LEVEL": "INFO",
}


def clear_settings_cache() -> None:
    for name in dir(config):
        accessor = getattr(config, name)
        if name.startswith("get_") and hasattr(accessor, "cache_clear"):
            accessor.cache_clear()


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> Iterator[dict[str, str]]:
    """Every test gets fresh settings pointing at its own temporary directories."""
    env = {
        **BASE_ENV,
        "DATA_DIR": str(tmp_path / "data"),
        "MODEL_DIR": str(tmp_path / "models"),
        "DOCS_DIR": str(tmp_path / "docs"),
        "CHECKPOINT_DIR": str(tmp_path / "checkpoints"),
        "HEARTBEAT_DIR": str(tmp_path / "heartbeats"),
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    clear_settings_cache()
    yield env
    clear_settings_cache()


def make_transactions(n_legit: int = 400, n_fraud: int = 40, seed: int = 7) -> pd.DataFrame:
    """Synthetic data shaped like creditcard.csv; frauds are shifted so models can learn."""
    rng = np.random.default_rng(seed)
    legit = rng.normal(0.0, 1.0, size=(n_legit, len(V_COLUMNS)))
    fraud = rng.normal(2.5, 1.0, size=(n_fraud, len(V_COLUMNS)))
    frame = pd.DataFrame(np.vstack([legit, fraud]), columns=list(V_COLUMNS))
    n = n_legit + n_fraud
    frame.insert(0, TIME_COLUMN, rng.uniform(0, 172_800, n).round(0))
    frame["Amount"] = rng.exponential(80.0, n).round(2)
    frame[LABEL_COLUMN] = [0] * n_legit + [1] * n_fraud
    return frame


@pytest.fixture
def transactions_pdf() -> pd.DataFrame:
    return make_transactions()


@pytest.fixture(scope="session")
def spark() -> Iterator[Any]:
    from src.common.spark import build_spark_session

    session = build_spark_session(
        "unit-tests",
        master="local[2]",
        driver_memory="1g",
        extra_conf={
            "spark.ui.enabled": "false",
            "spark.sql.shuffle.partitions": "2",
            "spark.default.parallelism": "2",
        },
    )
    yield session
    session.stop()


@pytest.fixture(scope="session")
def tiny_model(spark: Any) -> Any:
    """Feature pipeline + logistic regression trained on synthetic data."""
    from pyspark.ml import Pipeline
    from pyspark.ml.classification import LogisticRegression
    from pyspark.sql import functions as F

    from src.common.schemas import WEIGHT_COLUMN
    from src.training.features import FEATURES_COL, build_feature_pipeline

    frame = spark.createDataFrame(make_transactions()).withColumn(WEIGHT_COLUMN, F.lit(1.0))
    classifier = LogisticRegression(
        featuresCol=FEATURES_COL, labelCol=LABEL_COLUMN, weightCol=WEIGHT_COLUMN, maxIter=50
    )
    return Pipeline(stages=[*build_feature_pipeline().getStages(), classifier]).fit(frame)


def make_fake_metrics() -> dict[str, Any]:
    """A complete metrics.json-shaped dict with 4 models × 2 strategies."""
    models = (
        ("lr", "Logistic Regression"),
        ("rf", "Random Forest"),
        ("gbt", "Gradient-Boosted Trees"),
        ("svm", "Linear SVM"),
    )
    comparison, curves, candidates = [], {}, []
    for i, (key, name) in enumerate(models):
        for j, (strategy, label) in enumerate(
            (("class_weights", "class weights"), ("undersampling", "undersampling"))
        ):
            display = f"{name} · {label}"
            auc = round(0.5 + 0.05 * i + 0.01 * j, 4)
            comparison.append(
                {
                    "model_key": key,
                    "model": name,
                    "strategy": strategy,
                    "display_name": display,
                    "params": {"regParam": 0.01},
                    "train_seconds": 1.5,
                    "threshold": 0.5,
                    "val": {
                        "auc_pr": auc,
                        "roc_auc": 0.95,
                        "precision": 0.7,
                        "recall": 0.8,
                        "f1": 0.75,
                    },
                    "test": {
                        "auc_pr": auc - 0.02,
                        "roc_auc": 0.94,
                        "average_precision": auc - 0.03,
                        "precision": 0.7,
                        "recall": 0.75,
                        "f1": 0.72,
                        "confusion": {"tp": 50, "fp": 10, "tn": 42000, "fn": 20},
                    },
                }
            )
            curves[display] = {
                "pr": {"recall": [1.0, 0.5, 0.0], "precision": [0.01, 0.8, 1.0]},
                "roc": {"fpr": [0.0, 0.1, 1.0], "tpr": [0.0, 0.9, 1.0]},
            }
            candidates.append(
                {
                    "model_key": key,
                    "model": name,
                    "strategy": strategy,
                    "display_name": display,
                    "params": {"regParam": 0.01},
                    "val_auc_pr": auc,
                    "val_roc_auc": 0.95,
                    "train_seconds": 1.5,
                }
            )
    best = max(comparison, key=lambda row: row["val"]["auc_pr"])
    return {
        "run_id": "20261114T000000Z-test00",
        "created_at": "2026-11-14T00:00:00Z",
        "spark_version": "4.1.4",
        "seed": 42,
        "split_weights": [0.7, 0.15, 0.15],
        "undersample_ratio": 10.0,
        "feature_columns": [],
        "selection_metric": "val_auc_pr",
        "threshold_rule": "max F1 on validation",
        "dataset": {
            "rows_raw": 1000,
            "frauds_raw": 20,
            "duplicates_removed": 5,
            "rows": 995,
            "frauds": 19,
            "fraud_rate": 0.019,
        },
        "splits": {
            "train": {"rows": 696, "frauds": 13, "fraud_rate": 0.0187},
            "val": {"rows": 149, "frauds": 3, "fraud_rate": 0.0201},
            "test": {"rows": 150, "frauds": 3, "fraud_rate": 0.02},
        },
        "candidates": candidates,
        "comparison": comparison,
        "best": best,
        "curves": curves,
        "threshold_sweep": [
            {"threshold": t, "precision": t, "recall": 1 - t, "f1": 0.5} for t in (0.1, 0.5, 0.9)
        ],
        "training_seconds": 60.0,
    }


@pytest.fixture
def fake_metrics() -> dict[str, Any]:
    return make_fake_metrics()
