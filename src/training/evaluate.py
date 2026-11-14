"""Evaluation metrics: Spark MLlib AUCs plus threshold-dependent metrics computed with numpy.

* **AUC-PR** (primary) and **ROC-AUC** come from MLlib's ``BinaryClassificationEvaluator``.
* Precision, recall, F1 and the confusion matrix at a threshold, the PR/ROC curve points
  and the threshold sweep are computed with numpy/scikit-learn on the collected
  ``(label, probability)`` pairs. That is only about 42k rows per split.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import numpy as np
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_curve

from src.common.schemas import LABEL_COLUMN, THRESHOLD_MAX, THRESHOLD_MIN

PROBABILITY_COL = "probability"
BOOTSTRAP_RESAMPLES = 1000


@dataclass(frozen=True)
class Confusion:
    tp: int
    fp: int
    tn: int
    fn: int

    def to_dict(self) -> dict[str, int]:
        return asdict(self)


def fraud_probability_column(probability_col: str = PROBABILITY_COL) -> Column:
    """P(fraud) = element 1 of MLlib's probability vector."""
    return vector_to_array(F.col(probability_col)).getItem(1)


def spark_auc_metrics(scored: DataFrame, label_col: str = LABEL_COLUMN) -> dict[str, float]:
    """AUC-PR and ROC-AUC with MLlib's ``BinaryClassificationEvaluator``.

    ``numBins=0`` disables MLlib's default down-sampling of the curves into 1,000 bins.
    With about 42k scored rows but only about 70 frauds, each default bin (about 42 rows)
    blurs exactly the high-score region that AUC-PR depends on. In this project the binned
    value under-reported GBT's AUC-PR by about 0.19.
    """
    evaluator = BinaryClassificationEvaluator(
        rawPredictionCol=PROBABILITY_COL, labelCol=label_col, numBins=0
    )
    return {
        "auc_pr": float(evaluator.evaluate(scored, {evaluator.metricName: "areaUnderPR"})),
        "roc_auc": float(evaluator.evaluate(scored, {evaluator.metricName: "areaUnderROC"})),
    }


def collect_scores(
    scored: DataFrame, label_col: str = LABEL_COLUMN
) -> tuple[np.ndarray, np.ndarray]:
    """Collect ``(y_true, p_fraud)`` arrays to the driver."""
    pdf = scored.select(
        F.col(label_col).cast("int").alias("y"), fraud_probability_column().alias("p")
    ).toPandas()
    return pdf["y"].to_numpy(dtype=np.int64), pdf["p"].to_numpy(dtype=np.float64)


def confusion_at(y_true: np.ndarray, p_fraud: np.ndarray, threshold: float) -> Confusion:
    """Confusion counts when predicting fraud for ``p >= threshold``."""
    y_true = np.asarray(y_true).astype(bool)
    y_pred = np.asarray(p_fraud) >= threshold
    return Confusion(
        tp=int(np.sum(y_pred & y_true)),
        fp=int(np.sum(y_pred & ~y_true)),
        tn=int(np.sum(~y_pred & ~y_true)),
        fn=int(np.sum(~y_pred & y_true)),
    )


def precision_recall_f1(confusion: Confusion) -> dict[str, float]:
    """Precision, recall and F1 from confusion counts (0.0 when undefined)."""
    predicted_positive = confusion.tp + confusion.fp
    actual_positive = confusion.tp + confusion.fn
    precision = confusion.tp / predicted_positive if predicted_positive else 0.0
    recall = confusion.tp / actual_positive if actual_positive else 0.0
    f1 = 2 * precision * recall / (precision + recall) if precision + recall else 0.0
    return {"precision": precision, "recall": recall, "f1": f1}


def threshold_metrics(y_true: np.ndarray, p_fraud: np.ndarray, threshold: float) -> dict[str, Any]:
    """Precision/recall/F1 and confusion counts at ``threshold``."""
    confusion = confusion_at(y_true, p_fraud, threshold)
    return {"threshold": float(threshold), **precision_recall_f1(confusion), **confusion.to_dict()}


def best_f1_threshold(y_true: np.ndarray, p_fraud: np.ndarray) -> tuple[float, float]:
    """Threshold that maximises F1 (``p >= threshold`` means fraud) and that F1 value."""
    precision, recall, thresholds = precision_recall_curve(y_true, p_fraud)
    # precision/recall have one more element than thresholds; drop the final (recall=0) point.
    precision, recall = precision[:-1], recall[:-1]
    denominator = precision + recall
    f1 = np.divide(
        2 * precision * recall, denominator, out=np.zeros_like(denominator), where=denominator > 0
    )
    best = int(np.argmax(f1))
    # Keep the tuned value inside the range every component accepts (dashboard slider,
    # ALERT_THRESHOLD_OVERRIDE), so it can always be displayed and adjusted.
    threshold = float(np.clip(thresholds[best], THRESHOLD_MIN, THRESHOLD_MAX))
    return threshold, float(f1[best])


def threshold_sweep(
    y_true: np.ndarray, p_fraud: np.ndarray, thresholds: np.ndarray | None = None
) -> list[dict[str, float]]:
    """Precision/recall/F1 over a grid of thresholds (default 0.01..0.99)."""
    grid = np.round(np.arange(0.01, 1.0, 0.01), 2) if thresholds is None else thresholds
    sweep = []
    for threshold in grid:
        scores = precision_recall_f1(confusion_at(y_true, p_fraud, float(threshold)))
        sweep.append({"threshold": float(threshold), **{k: round(v, 6) for k, v in scores.items()}})
    return sweep


def downsample(values: np.ndarray, max_points: int) -> np.ndarray:
    """Evenly spaced subset of ``values`` (always keeps first and last)."""
    if len(values) <= max_points:
        return values
    index = np.unique(np.linspace(0, len(values) - 1, max_points).round().astype(int))
    return values[index]


def curve_points(
    y_true: np.ndarray, p_fraud: np.ndarray, max_points: int = 600
) -> dict[str, dict[str, list[float]]]:
    """PR and ROC curve points for plotting.

    ``drop_intermediate=True`` keeps only the corner points of each curve. With ~70 frauds
    that's a few hundred points at most, all in the informative region. (Index-based
    down-sampling of the full ~42k-point curve would keep mostly the near-zero-precision
    tail and draw misleading straight lines across the rest.)
    """
    precision, recall, _ = precision_recall_curve(y_true, p_fraud, drop_intermediate=True)
    fpr, tpr, _ = roc_curve(y_true, p_fraud, drop_intermediate=True)
    pr = np.column_stack([recall, precision])
    roc = np.column_stack([fpr, tpr])
    pr, roc = downsample(pr, max_points), downsample(roc, max_points)
    return {
        "pr": {"recall": _round_list(pr[:, 0]), "precision": _round_list(pr[:, 1])},
        "roc": {"fpr": _round_list(roc[:, 0]), "tpr": _round_list(roc[:, 1])},
    }


def average_precision(y_true: np.ndarray, p_fraud: np.ndarray) -> float:
    """scikit-learn's average precision (step-wise AUC-PR), reported as a secondary metric."""
    return float(average_precision_score(y_true, p_fraud))


def bootstrap_ci(
    y_true: np.ndarray,
    p_fraud: np.ndarray,
    threshold: float,
    *,
    n_resamples: int = BOOTSTRAP_RESAMPLES,
    confidence: float = 0.95,
    seed: int = 42,
) -> dict[str, list[float]]:
    """Percentile-bootstrap confidence intervals over transactions.

    The model and threshold are fixed; the test transactions are resampled with
    replacement. This shows how much of a difference between models is just the
    sampling noise of ~70 frauds. Returns ``{metric: [low, high]}`` for average precision,
    precision, recall and F1. Resamples that contain no fraud are skipped.
    """
    y_true, p_fraud = np.asarray(y_true), np.asarray(p_fraud)
    rng = np.random.default_rng(seed)
    samples: dict[str, list[float]] = {
        "average_precision": [],
        "precision": [],
        "recall": [],
        "f1": [],
    }
    n = len(y_true)
    for _ in range(n_resamples):
        index = rng.integers(0, n, n)
        y_b, p_b = y_true[index], p_fraud[index]
        if not y_b.any():
            continue
        samples["average_precision"].append(average_precision_score(y_b, p_b))
        for name, value in precision_recall_f1(confusion_at(y_b, p_b, threshold)).items():
            samples[name].append(value)
    alpha = (1.0 - confidence) / 2.0
    return {
        name: [
            round(float(np.quantile(values, alpha)), 4),
            round(float(np.quantile(values, 1 - alpha)), 4),
        ]
        for name, values in samples.items()
        if values
    }


def _round_list(values: np.ndarray, digits: int = 5) -> list[float]:
    return [round(float(v), digits) for v in values]
