"""End-to-end training job on a small synthetic dataset (reduced model grid for speed)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from src.common.config import get_path_settings, get_training_settings
from src.training import models, train_model
from tests.conftest import make_transactions

pytestmark = pytest.mark.spark


@pytest.fixture
def small_grid(monkeypatch: pytest.MonkeyPatch) -> None:
    """LR + calibrated SVM, one config each: covers both scoring paths, 4 fits total."""
    reduced = tuple(
        models.ModelSpec(
            key=spec.key,
            name=spec.name,
            param_grid=spec.param_grid[:1],
            builder=spec.builder,
            needs_calibration=spec.needs_calibration,
        )
        for spec in models.MODEL_SPECS
        if spec.key in {"lr", "svm"}
    )
    monkeypatch.setattr(train_model, "MODEL_SPECS", reduced)


@pytest.fixture
def raw_dataset() -> Path:
    paths = get_path_settings()
    frame = make_transactions(1500, 60, seed=3)
    frame = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)  # one exact duplicate
    paths.raw_dataset_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(paths.raw_dataset_path, index=False)
    return paths.raw_dataset_path


def test_run_training_writes_all_artifacts(spark, small_grid: None, raw_dataset: Path) -> None:
    paths = get_path_settings()
    metrics = train_model.run_training(spark, paths, get_training_settings())

    assert metrics["dataset"]["duplicates_removed"] == 1
    assert metrics["dataset"]["rows"] == 1560
    assert [metrics["splits"][s]["frauds"] for s in ("train", "val", "test")] == [42, 9, 9]
    assert len(metrics["candidates"]) == 4
    assert len(metrics["comparison"]) == 4
    assert metrics["best"]["display_name"] in metrics["curves"]
    assert 0 < metrics["best"]["threshold"] < 1
    assert len(metrics["threshold_sweep"]) == 99

    on_disk = json.loads(paths.metrics_path.read_text(encoding="utf-8"))
    assert on_disk["run_id"] == metrics["run_id"]
    assert train_model.artifacts_exist(paths)
    assert (paths.model_path / "metadata").exists()

    demo = json.loads(paths.demo_samples_path.read_text(encoding="utf-8"))
    assert demo["frauds"] and demo["legit"]
    assert all(sample["Class"] == 1 for sample in demo["frauds"])
    assert demo["frauds"][0]["fraud_probability"] >= demo["frauds"][-1]["fraud_probability"]

    test_split = pd.read_parquet(paths.test_split_path)
    assert len(test_split) == metrics["splits"]["test"]["rows"]
    assert test_split["Time"].is_monotonic_increasing
    assert paths.results_markdown_path.is_file()
    assert len(list(paths.figures_dir.glob("*.png"))) == 5


def test_main_skips_when_artifacts_exist(monkeypatch: pytest.MonkeyPatch) -> None:
    paths = get_path_settings()
    (paths.model_path / "metadata").mkdir(parents=True)
    paths.metrics_path.write_text("{}", encoding="utf-8")
    paths.test_split_path.parent.mkdir(parents=True)
    paths.test_split_path.write_bytes(b"")

    def must_not_start_spark(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("Spark must not start when the model already exists")

    monkeypatch.setattr(train_model, "build_spark_session", must_not_start_spark)
    assert train_model.main([]) == 0


def test_main_reports_missing_dataset(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        train_model, "build_spark_session", lambda *a, **k: pytest.fail("no Spark without data")
    )
    assert train_model.main(["--force"]) == 2
    assert "kaggle.com/datasets/mlg-ulb/creditcardfraud" in capsys.readouterr().err


def test_parse_args() -> None:
    assert train_model.parse_args(["--force"]).force is True
    assert train_model.parse_args([]).force is False
