"""Evaluation metrics (numpy/sklearn helpers and MLlib AUCs)."""

from __future__ import annotations

import numpy as np
import pytest
from pyspark.ml.linalg import Vectors

from src.training import evaluate

Y = np.array([0, 0, 0, 0, 1, 1, 0, 1])
P = np.array([0.1, 0.2, 0.3, 0.6, 0.7, 0.8, 0.85, 0.9])


def test_confusion_at_threshold() -> None:
    confusion = evaluate.confusion_at(Y, P, 0.65)
    assert confusion.to_dict() == {"tp": 3, "fp": 1, "tn": 4, "fn": 0}
    # p >= threshold counts as fraud (inclusive boundary, same rule as streaming)
    assert evaluate.confusion_at(Y, P, 0.7).tp == 3


def test_precision_recall_f1_handles_zero_division() -> None:
    assert evaluate.precision_recall_f1(evaluate.Confusion(0, 0, 10, 0)) == {
        "precision": 0.0,
        "recall": 0.0,
        "f1": 0.0,
    }
    scores = evaluate.precision_recall_f1(evaluate.Confusion(tp=3, fp=1, tn=4, fn=0))
    assert scores["precision"] == pytest.approx(0.75)
    assert scores["recall"] == pytest.approx(1.0)
    assert scores["f1"] == pytest.approx(2 * 0.75 / 1.75)


def test_threshold_metrics_combines_both() -> None:
    metrics = evaluate.threshold_metrics(Y, P, 0.65)
    assert metrics["threshold"] == 0.65
    assert metrics["tp"] == 3 and metrics["recall"] == 1.0


def test_best_f1_threshold_on_separable_data() -> None:
    y = np.array([0, 0, 0, 1, 1])
    p = np.array([0.1, 0.2, 0.3, 0.8, 0.9])
    threshold, f1 = evaluate.best_f1_threshold(y, p)
    assert f1 == pytest.approx(1.0)
    assert 0.3 < threshold <= 0.8
    assert evaluate.threshold_metrics(y, p, threshold)["f1"] == pytest.approx(1.0)


def test_best_f1_threshold_is_never_exactly_zero_or_one() -> None:
    threshold, _ = evaluate.best_f1_threshold(np.array([0, 1, 1]), np.array([0.0, 1.0, 1.0]))
    assert 0.0 < threshold < 1.0


def test_threshold_sweep_grid() -> None:
    sweep = evaluate.threshold_sweep(Y, P)
    assert len(sweep) == 99
    assert sweep[0]["threshold"] == 0.01 and sweep[-1]["threshold"] == 0.99
    recalls = [row["recall"] for row in sweep]
    assert recalls == sorted(recalls, reverse=True)  # recall never increases with the threshold
    custom = evaluate.threshold_sweep(Y, P, np.array([0.5]))
    assert custom == [
        {
            "threshold": 0.5,
            **{
                k: round(v, 6)
                for k, v in evaluate.precision_recall_f1(evaluate.confusion_at(Y, P, 0.5)).items()
            },
        }
    ]


def test_downsample_keeps_endpoints() -> None:
    values = np.arange(1000)
    sampled = evaluate.downsample(values, 10)
    assert len(sampled) <= 10 and sampled[0] == 0 and sampled[-1] == 999
    assert np.array_equal(evaluate.downsample(values[:5], 10), values[:5])


def test_curve_points_shape_and_bounds() -> None:
    rng = np.random.default_rng(0)
    y = np.r_[np.zeros(5000, int), np.ones(20, int)]
    p = np.r_[rng.uniform(0, 0.6, 5000), rng.uniform(0.4, 1, 20)]
    curves = evaluate.curve_points(y, p, max_points=600)
    assert set(curves) == {"pr", "roc"}
    for kind, (a, b) in {"pr": ("recall", "precision"), "roc": ("fpr", "tpr")}.items():
        xs, ys = curves[kind][a], curves[kind][b]
        assert len(xs) == len(ys) <= 600
        assert all(0 <= v <= 1 for v in xs + ys)
    # only corner points are kept: far fewer than one point per transaction
    assert len(curves["roc"]["fpr"]) < 200


def test_average_precision_perfect_ranking() -> None:
    assert evaluate.average_precision(np.array([0, 0, 1, 1]), np.array([0.1, 0.2, 0.8, 0.9])) == 1.0


@pytest.mark.spark
def test_spark_auc_metrics_and_collect_scores(spark) -> None:
    rows = [(float(label), Vectors.dense([1 - p, p])) for label, p in zip(Y, P, strict=True)]
    scored = spark.createDataFrame(rows, ["Class", "probability"])
    aucs = evaluate.spark_auc_metrics(scored)
    assert set(aucs) == {"auc_pr", "roc_auc"}
    assert aucs["roc_auc"] == pytest.approx(0.8667, abs=1e-3)  # 13 of 15 pairs ranked correctly
    y, p = evaluate.collect_scores(scored)
    assert y.tolist() == Y.tolist()
    assert np.allclose(p, P)


@pytest.mark.spark
def test_spark_auc_pr_is_not_binned(spark) -> None:
    """With only a few positives, MLlib's default 1000-bin down-sampling distorts AUC-PR.

    The evaluator must use numBins=0, so MLlib agrees with an exact computation.
    """
    rng = np.random.default_rng(1)
    n_neg, n_pos = 20000, 15
    p = np.r_[rng.uniform(0, 0.9, n_neg), rng.uniform(0.85, 1.0, n_pos)]
    y = np.r_[np.zeros(n_neg), np.ones(n_pos)]
    scored = spark.createDataFrame(
        [(float(label), Vectors.dense([1 - prob, prob])) for label, prob in zip(y, p, strict=True)],
        ["Class", "probability"],
    )
    mllib = evaluate.spark_auc_metrics(scored)["auc_pr"]
    exact = evaluate.average_precision(y.astype(int), p)
    assert mllib == pytest.approx(exact, abs=0.03)


def test_bootstrap_ci_on_perfect_ranking() -> None:
    y = np.r_[np.zeros(300, int), np.ones(30, int)]
    p = np.r_[np.linspace(0, 0.4, 300), np.linspace(0.6, 1.0, 30)]
    ci = evaluate.bootstrap_ci(y, p, 0.5, n_resamples=200)
    assert set(ci) == {"average_precision", "precision", "recall", "f1"}
    assert ci["recall"] == [1.0, 1.0] and ci["average_precision"] == [1.0, 1.0]
    assert evaluate.bootstrap_ci(y, p, 0.5, n_resamples=200) == ci  # seeded, reproducible


def test_bootstrap_ci_widens_with_noise() -> None:
    rng = np.random.default_rng(3)
    y = np.r_[np.zeros(2000, int), np.ones(20, int)]
    p = np.r_[rng.uniform(0, 0.7, 2000), rng.uniform(0.3, 1.0, 20)]
    low, high = evaluate.bootstrap_ci(y, p, 0.5, n_resamples=300)["average_precision"]
    assert 0 <= low < high <= 1 and high - low > 0.05


def test_bootstrap_ci_skips_resamples_without_frauds() -> None:
    y = np.r_[np.zeros(500, int), 1]
    p = np.r_[np.zeros(500), 1.0]
    ci = evaluate.bootstrap_ci(y, p, 0.5, n_resamples=50)
    assert ci["recall"][0] <= ci["recall"][1]


def test_tuned_threshold_stays_in_slider_range() -> None:
    y = np.array([0, 0, 1, 1])
    p = np.array([0.1, 0.2, 0.99995, 0.99999])
    threshold, _ = evaluate.best_f1_threshold(y, p)
    assert threshold == 0.999
