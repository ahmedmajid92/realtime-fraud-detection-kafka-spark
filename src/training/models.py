"""Candidate classifiers, their hyper-parameter grids, and Platt calibration for the SVM."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any

from pyspark.ml import PipelineModel, Transformer
from pyspark.ml.classification import (
    GBTClassifier,
    LinearSVC,
    LogisticRegression,
    LogisticRegressionModel,
    RandomForestClassifier,
)
from pyspark.ml.feature import VectorSlicer
from pyspark.sql import DataFrame

from src.common.schemas import LABEL_COLUMN, WEIGHT_COLUMN
from src.training.data_prep import Strategy
from src.training.features import FEATURES_COL

SVM_RAW_COL = "svm_raw"
SVM_PREDICTION_COL = "svm_prediction"
SVM_MARGIN_COL = "svm_margin"

STRATEGY_LABELS: dict[Strategy, str] = {
    Strategy.CLASS_WEIGHTS: "class weights",
    Strategy.UNDERSAMPLING: "undersampling",
}


@dataclass(frozen=True)
class ModelSpec:
    """A model family: how to build it and which hyper-parameters to try."""

    key: str
    name: str
    param_grid: tuple[dict[str, Any], ...]
    builder: Callable[[dict[str, Any], int], Any]
    needs_calibration: bool = False


def _logistic_regression(params: dict[str, Any], seed: int) -> LogisticRegression:
    del seed  # deterministic solver
    return LogisticRegression(
        featuresCol=FEATURES_COL,
        labelCol=LABEL_COLUMN,
        weightCol=WEIGHT_COLUMN,
        maxIter=100,
        elasticNetParam=0.0,
        **params,
    )


def _random_forest(params: dict[str, Any], seed: int) -> RandomForestClassifier:
    return RandomForestClassifier(
        featuresCol=FEATURES_COL,
        labelCol=LABEL_COLUMN,
        weightCol=WEIGHT_COLUMN,
        numTrees=100,
        featureSubsetStrategy="sqrt",
        subsamplingRate=1.0,
        seed=seed,
        **params,
    )


def _gradient_boosted_trees(params: dict[str, Any], seed: int) -> GBTClassifier:
    return GBTClassifier(
        featuresCol=FEATURES_COL,
        labelCol=LABEL_COLUMN,
        weightCol=WEIGHT_COLUMN,
        maxDepth=5,
        stepSize=0.1,
        seed=seed,
        **params,
    )


def _linear_svm(params: dict[str, Any], seed: int) -> LinearSVC:
    del seed
    return LinearSVC(
        featuresCol=FEATURES_COL,
        labelCol=LABEL_COLUMN,
        weightCol=WEIGHT_COLUMN,
        maxIter=100,
        rawPredictionCol=SVM_RAW_COL,
        predictionCol=SVM_PREDICTION_COL,
        **params,
    )


MODEL_SPECS: tuple[ModelSpec, ...] = (
    ModelSpec(
        key="lr",
        name="Logistic Regression",
        param_grid=({"regParam": 0.001}, {"regParam": 0.01}),
        builder=_logistic_regression,
    ),
    ModelSpec(
        key="rf",
        name="Random Forest",
        param_grid=({"maxDepth": 8}, {"maxDepth": 12}),
        builder=_random_forest,
    ),
    ModelSpec(
        key="gbt",
        name="Gradient-Boosted Trees",
        param_grid=({"maxIter": 50}, {"maxIter": 100}),
        builder=_gradient_boosted_trees,
    ),
    ModelSpec(
        key="svm",
        name="Linear SVM",
        param_grid=({"regParam": 0.001}, {"regParam": 0.01}),
        builder=_linear_svm,
        needs_calibration=True,
    ),
)


def display_name(spec: ModelSpec, strategy: Strategy) -> str:
    """Human-readable name, e.g. ``Gradient-Boosted Trees · class weights``."""
    return f"{spec.name} · {STRATEGY_LABELS[strategy]}"


def fit_platt_scaler(svm_scored: DataFrame) -> tuple[VectorSlicer, LogisticRegressionModel]:
    """Fit Platt scaling: a 1-D logistic regression on the SVM margin.

    LinearSVC has no probability output. Calibrating its margin on the validation split
    gives a ``probability`` column like every other model, so the alert threshold always
    means "fraud probability". The transform is monotonic, so AUC-PR/ROC-AUC are unchanged.
    """
    slicer = VectorSlicer(inputCol=SVM_RAW_COL, outputCol=SVM_MARGIN_COL, indices=[1])
    platt = LogisticRegression(
        featuresCol=SVM_MARGIN_COL, labelCol=LABEL_COLUMN, maxIter=100, regParam=0.0
    ).fit(slicer.transform(svm_scored))
    return slicer, platt


@dataclass
class Candidate:
    """One fitted (model family, imbalance strategy, hyper-parameters) combination."""

    spec: ModelSpec
    strategy: Strategy
    params: dict[str, Any]
    val_auc_pr: float
    val_roc_auc: float
    train_seconds: float
    pipeline_model: PipelineModel = field(repr=False)

    @property
    def display_name(self) -> str:
        return display_name(self.spec, self.strategy)

    def summary(self) -> dict[str, Any]:
        """JSON-serialisable description (without the model object)."""
        return {
            "model_key": self.spec.key,
            "model": self.spec.name,
            "strategy": self.strategy.value,
            "display_name": self.display_name,
            "params": self.params,
            "val_auc_pr": round(self.val_auc_pr, 6),
            "val_roc_auc": round(self.val_roc_auc, 6),
            "train_seconds": round(self.train_seconds, 2),
        }


def assemble_pipeline_model(
    feature_model: PipelineModel, classifier: Transformer, extra_stages: Sequence[Transformer] = ()
) -> PipelineModel:
    """Combine fitted feature stages, the classifier and optional calibration stages."""
    return PipelineModel(stages=[*feature_model.stages, classifier, *extra_stages])
