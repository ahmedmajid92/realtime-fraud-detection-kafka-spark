"""Feature pipeline, model specs, Platt calibration and model save/load/predict."""

from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pytest
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import SQLTransformer
from pyspark.ml.functions import vector_to_array
from pyspark.sql import functions as F

from src.common.schemas import FEATURE_COLUMNS, LABEL_COLUMN, WEIGHT_COLUMN
from src.training import features, models
from src.training.data_prep import Strategy
from src.training.train_model import save_pipeline_model
from tests.conftest import make_transactions

pytestmark = pytest.mark.spark


def test_feature_sql_engineered_values(spark) -> None:
    rows = [
        (0.0, 0.0),
        (6 * 3600.0, math.e - 1),
        (30 * 3600.0, 99.0),
    ]  # 00:00, 06:00, 06:00 next day
    frame = spark.createDataFrame(rows, "Time double, Amount double")
    sql = features.feature_engineering_sql()
    out = SQLTransformer(statement=sql).transform(frame).collect()
    assert out[0]["hour_sin"] == pytest.approx(0.0, abs=1e-9)
    assert out[0]["hour_cos"] == pytest.approx(1.0)
    assert out[1]["hour_sin"] == pytest.approx(1.0)
    assert out[1]["log_amount"] == pytest.approx(1.0)
    assert out[2]["hour_sin"] == pytest.approx(out[1]["hour_sin"])  # same hour, different day


def test_feature_pipeline_produces_standardised_vector(spark) -> None:
    frame = spark.createDataFrame(make_transactions(200, 20))
    model = features.build_feature_pipeline().fit(frame)
    vectors = np.array(
        model.transform(frame)
        .select(vector_to_array(features.FEATURES_COL).alias("v"))
        .toPandas()["v"]
        .tolist()
    )
    assert vectors.shape == (220, len(FEATURE_COLUMNS))
    assert np.allclose(vectors.mean(axis=0), 0.0, atol=1e-6)
    assert np.allclose(vectors.std(axis=0, ddof=1), 1.0, atol=1e-6)


def test_model_specs_cover_four_families_with_small_grids() -> None:
    assert [spec.key for spec in models.MODEL_SPECS] == ["lr", "rf", "gbt", "svm"]
    for spec in models.MODEL_SPECS:
        assert len(spec.param_grid) == 2
        estimator = spec.builder(dict(spec.param_grid[0]), 42)
        assert estimator.getWeightCol() == WEIGHT_COLUMN
        assert estimator.getLabelCol() == LABEL_COLUMN
        assert estimator.getFeaturesCol() == features.FEATURES_COL
    assert [s.key for s in models.MODEL_SPECS if s.needs_calibration] == ["svm"]


def test_display_name() -> None:
    spec = models.MODEL_SPECS[1]
    assert models.display_name(spec, Strategy.UNDERSAMPLING) == "Random Forest · undersampling"


def _fit_svm_pipeline(spark) -> tuple[PipelineModel, object]:
    frame = spark.createDataFrame(make_transactions(300, 30)).withColumn(WEIGHT_COLUMN, F.lit(1.0))
    feature_model = features.build_feature_pipeline().fit(frame)
    transformed = feature_model.transform(frame)
    svm_spec = next(s for s in models.MODEL_SPECS if s.key == "svm")
    svm = svm_spec.builder({"regParam": 0.01}, 42).fit(transformed)
    slicer, platt = models.fit_platt_scaler(svm.transform(transformed))
    return models.assemble_pipeline_model(feature_model, svm, (slicer, platt)), frame


def test_platt_scaled_svm_outputs_probabilities(spark) -> None:
    pipeline, frame = _fit_svm_pipeline(spark)
    scored = (
        pipeline.transform(frame)
        .select(LABEL_COLUMN, vector_to_array("probability").getItem(1).alias("p"))
        .toPandas()
    )
    assert scored["p"].between(0, 1).all()
    # calibrated scores still separate the (well-separated) synthetic classes
    assert (
        scored.loc[scored[LABEL_COLUMN] == 1, "p"].mean()
        > scored.loc[scored[LABEL_COLUMN] == 0, "p"].mean()
    )


def test_model_save_load_and_predict_on_sample_rows(spark, tiny_model, tmp_path: Path) -> None:
    destination = tmp_path / "models" / "best_model"
    save_pipeline_model(tiny_model, destination)
    save_pipeline_model(tiny_model, destination)  # overwriting an existing model works
    assert (destination / "metadata").exists()
    assert not destination.with_name("best_model.incoming").exists()

    loaded = PipelineModel.load(str(destination))
    sample = spark.createDataFrame(make_transactions(5, 5, seed=11))
    before = (
        tiny_model.transform(sample)
        .select(vector_to_array("probability").getItem(1).alias("p"))
        .toPandas()["p"]
    )
    after = (
        loaded.transform(sample)
        .select(vector_to_array("probability").getItem(1).alias("p"), LABEL_COLUMN)
        .toPandas()
    )
    assert np.allclose(before.to_numpy(), after["p"].to_numpy())
    assert after.loc[after[LABEL_COLUMN] == 1, "p"].min() > 0.5  # frauds are recognised
    assert after.loc[after[LABEL_COLUMN] == 0, "p"].max() < 0.5


def test_candidate_summary_is_json_serialisable(tiny_model) -> None:
    candidate = models.Candidate(
        spec=models.MODEL_SPECS[0],
        strategy=Strategy.CLASS_WEIGHTS,
        params={"regParam": 0.01},
        val_auc_pr=0.812345678,
        val_roc_auc=0.97,
        train_seconds=3.14159,
        pipeline_model=tiny_model,
    )
    summary = json.loads(json.dumps(candidate.summary()))
    assert summary["display_name"] == "Logistic Regression · class weights"
    assert summary["val_auc_pr"] == 0.812346
    assert summary["train_seconds"] == 3.14
    assert "pipeline_model" not in summary


def test_pipeline_stage_types(tiny_model) -> None:
    stage_names = [type(stage).__name__ for stage in tiny_model.stages]
    assert stage_names[:3] == ["SQLTransformer", "VectorAssembler", "StandardScalerModel"]
    assert isinstance(features.build_feature_pipeline(), Pipeline)
