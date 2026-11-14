"""Offline training job: compare 4 models × 2 imbalance strategies and save the best one.

Run inside the ``fraud-spark`` image::

    python -m src.training.train_model            # skip if artifacts already exist
    python -m src.training.train_model --force    # always retrain

Outputs (paths from :class:`src.common.config.PathSettings`):

* ``data/processed/{train,val,test}.parquet``: the stratified splits (test is replayed)
* ``models/best_model/``: the selected Spark ``PipelineModel``
* ``models/metrics.json``: everything the dashboard's Model Performance page shows
* ``models/demo_samples.json``: test frauds the model catches (used by the e2e test)
* ``docs/figures/*.png`` and ``docs/RESULTS.md``: report assets
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark import StorageLevel
from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.config import (
    PathSettings,
    TrainingSettings,
    get_logging_settings,
    get_path_settings,
    get_training_settings,
)
from src.common.logging_setup import configure_logging, get_logger
from src.common.schemas import FEATURE_COLUMNS, LABEL_COLUMN, RAW_INPUT_COLUMNS, WEIGHT_COLUMN
from src.common.spark import build_spark_session
from src.training import data_prep, evaluate, report
from src.training.data_prep import Strategy
from src.training.features import FEATURES_COL, build_feature_pipeline
from src.training.models import MODEL_SPECS, Candidate, assemble_pipeline_model, fit_platt_scaler

SPLIT_WEIGHTS = (0.70, 0.15, 0.15)
DEMO_SAMPLES_PER_CLASS = 5

logger = get_logger(__name__)


@dataclass(frozen=True)
class SplitStats:
    rows: int
    frauds: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "rows": self.rows,
            "frauds": self.frauds,
            "fraud_rate": round(self.frauds / self.rows, 6),
        }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train and compare fraud detection models.")
    parser.add_argument(
        "--force", action="store_true", help="retrain even if model artifacts already exist"
    )
    return parser.parse_args(argv)


def artifacts_exist(paths: PathSettings) -> bool:
    """True when every artifact the streaming stack needs is already present."""
    return (
        paths.metrics_path.is_file()
        and (paths.model_path / "metadata").exists()
        and paths.test_split_path.is_file()
    )


def _round_metrics(values: dict[str, Any], digits: int = 6) -> dict[str, Any]:
    return {k: round(v, digits) if isinstance(v, float) else v for k, v in values.items()}


def _write_json_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, suffix=".json.tmp")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)
    os.replace(tmp_name, path)


def save_pipeline_model(model: PipelineModel, destination: Path) -> None:
    """Save via a container-local staging dir, then copy into place.

    Spark/Hadoop writes ``.crc`` side files and renames directories, which can misbehave
    on Windows bind mounts. Writing to ``/tmp`` first and copying the finished tree
    sidesteps that, and readers never see a half-written model.
    """
    staging_root = Path(tempfile.mkdtemp(prefix="model-staging-"))
    staged = staging_root / "best_model"
    try:
        model.write().overwrite().save(str(staged))
        incoming = destination.with_name(destination.name + ".incoming")
        if incoming.exists():
            shutil.rmtree(incoming)
        shutil.copytree(staged, incoming)
        if destination.exists():
            shutil.rmtree(destination)
        os.replace(incoming, destination)
    finally:
        shutil.rmtree(staging_root, ignore_errors=True)


def _split_stats(df: DataFrame) -> SplitStats:
    stats = data_prep.compute_stats(df)
    return SplitStats(rows=stats.rows, frauds=stats.frauds)


def train_candidates(
    train: DataFrame, val: DataFrame, cfg: TrainingSettings
) -> tuple[list[Candidate], dict[tuple[str, Strategy], Candidate]]:
    """Fit every (model, strategy, params) combination; score each on validation AUC-PR.

    The feature pipeline is fitted once per strategy on that strategy's training frame,
    and the transformed features are cached, so each classifier fit reuses them.
    """
    candidates: list[Candidate] = []
    best_per_combo: dict[tuple[str, Strategy], Candidate] = {}
    for strategy in Strategy:
        train_s = data_prep.prepare_training_frame(
            train, strategy, undersample_ratio=cfg.undersample_ratio, seed=cfg.random_seed
        )
        feature_model = build_feature_pipeline().fit(train_s)
        train_features = (
            feature_model.transform(train_s)
            .select(FEATURES_COL, LABEL_COLUMN, WEIGHT_COLUMN)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        val_features = feature_model.transform(val).persist(StorageLevel.MEMORY_AND_DISK)
        n_train = train_features.count()
        val_features.count()
        logger.info("training.strategy_start", strategy=strategy.value, train_rows=n_train)

        for spec in MODEL_SPECS:
            for params in spec.param_grid:
                started = time.monotonic()
                classifier = spec.builder(dict(params), cfg.random_seed).fit(train_features)
                scored_val = classifier.transform(val_features)
                extra_stages: tuple[Any, ...] = ()
                if spec.needs_calibration:
                    slicer, platt = fit_platt_scaler(scored_val)
                    scored_val = platt.transform(slicer.transform(scored_val))
                    extra_stages = (slicer, platt)
                aucs = evaluate.spark_auc_metrics(scored_val)
                candidate = Candidate(
                    spec=spec,
                    strategy=strategy,
                    params=dict(params),
                    val_auc_pr=aucs["auc_pr"],
                    val_roc_auc=aucs["roc_auc"],
                    train_seconds=time.monotonic() - started,
                    pipeline_model=assemble_pipeline_model(feature_model, classifier, extra_stages),
                )
                candidates.append(candidate)
                logger.info("training.candidate", **candidate.summary())
                key = (spec.key, strategy)
                if (
                    key not in best_per_combo
                    or candidate.val_auc_pr > best_per_combo[key].val_auc_pr
                ):
                    best_per_combo[key] = candidate

        train_features.unpersist()
        val_features.unpersist()
    return candidates, best_per_combo


def evaluate_combination(
    candidate: Candidate, val: DataFrame, test: DataFrame
) -> tuple[dict[str, Any], dict[str, Any], DataFrame]:
    """Tune the threshold on validation, then measure everything on test.

    Returns the comparison-table row, the test PR/ROC curve points, and the scored test
    DataFrame (cached by the caller if it needs it again).
    """
    model = candidate.pipeline_model
    y_val, p_val = evaluate.collect_scores(model.transform(val))
    threshold, _ = evaluate.best_f1_threshold(y_val, p_val)

    scored_test = model.transform(test)
    test_aucs = evaluate.spark_auc_metrics(scored_test)
    y_test, p_test = evaluate.collect_scores(scored_test)
    test_at_t = evaluate.threshold_metrics(y_test, p_test, threshold)
    val_at_t = evaluate.threshold_metrics(y_val, p_val, threshold)

    row = {
        **{k: v for k, v in candidate.summary().items() if k not in {"val_auc_pr", "val_roc_auc"}},
        "threshold": round(threshold, 6),
        "val": _round_metrics(
            {
                "auc_pr": candidate.val_auc_pr,
                "roc_auc": candidate.val_roc_auc,
                "precision": val_at_t["precision"],
                "recall": val_at_t["recall"],
                "f1": val_at_t["f1"],
            }
        ),
        "test": {
            **_round_metrics(
                {
                    "auc_pr": test_aucs["auc_pr"],
                    "roc_auc": test_aucs["roc_auc"],
                    "average_precision": evaluate.average_precision(y_test, p_test),
                    "precision": test_at_t["precision"],
                    "recall": test_at_t["recall"],
                    "f1": test_at_t["f1"],
                }
            ),
            "confusion": {k: test_at_t[k] for k in ("tp", "fp", "tn", "fn")},
            "ci95": evaluate.bootstrap_ci(y_test, p_test, threshold),
        },
    }
    return row, evaluate.curve_points(y_test, p_test), scored_test


def pick_demo_samples(scored_test: DataFrame, threshold: float) -> dict[str, list[dict[str, Any]]]:
    """A few test frauds the model flags (highest probability first) and confident legit rows."""
    scored = scored_test.withColumn("fraud_probability", evaluate.fraud_probability_column())
    columns = [*RAW_INPUT_COLUMNS, LABEL_COLUMN, "fraud_probability"]
    frauds = (
        scored.filter(F.col(LABEL_COLUMN) == 1)
        .orderBy(F.col("fraud_probability").desc())
        .select(*columns)
        .limit(DEMO_SAMPLES_PER_CLASS)
        .toPandas()
    )
    legit = (
        scored.filter(F.col(LABEL_COLUMN) == 0)
        .orderBy(F.col("fraud_probability").asc())
        .select(*columns)
        .limit(DEMO_SAMPLES_PER_CLASS)
        .toPandas()
    )

    def records(pdf: Any) -> list[dict[str, Any]]:
        out = []
        for rec in pdf.to_dict(orient="records"):
            rec[LABEL_COLUMN] = int(rec[LABEL_COLUMN])
            rec["fraud_probability"] = round(float(rec["fraud_probability"]), 6)
            rec["above_threshold"] = rec["fraud_probability"] >= threshold
            out.append(rec)
        return out

    return {"threshold": threshold, "frauds": records(frauds), "legit": records(legit)}


def run_training(spark: SparkSession, paths: PathSettings, cfg: TrainingSettings) -> dict[str, Any]:
    """The full training pipeline. Returns the metrics dictionary (also written to disk)."""
    started = time.monotonic()
    run_id = f"{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"
    logger.info("training.start", run_id=run_id, dataset=str(paths.raw_dataset_path))

    raw = data_prep.load_raw_dataset(spark, paths.raw_dataset_path)
    raw_stats = data_prep.validate_dataset(raw)
    deduplicated, duplicates = data_prep.drop_exact_duplicates(raw)
    deduplicated = deduplicated.persist(StorageLevel.MEMORY_AND_DISK)
    clean_stats = data_prep.compute_stats(deduplicated)
    logger.info(
        "training.dataset",
        rows_raw=raw_stats.rows,
        frauds_raw=raw_stats.frauds,
        duplicates_removed=duplicates,
        rows=clean_stats.rows,
        frauds=clean_stats.frauds,
    )

    splits = data_prep.stratified_split(deduplicated, SPLIT_WEIGHTS, seed=cfg.random_seed)
    assert splits.assigned is not None
    twins = data_prep.feature_twin_stats(splits.assigned)
    if twins["groups_spanning_splits"]:
        raise RuntimeError(f"feature twins straddle splits: {twins}")  # would be leakage
    logger.info("training.feature_twins", **twins)
    train = splits.train.persist(StorageLevel.MEMORY_AND_DISK)
    val = splits.val.persist(StorageLevel.MEMORY_AND_DISK)
    test = splits.test.persist(StorageLevel.MEMORY_AND_DISK)
    split_stats = {
        "train": _split_stats(train),
        "val": _split_stats(val),
        "test": _split_stats(test),
    }
    for name, frame in (("train", train), ("val", val), ("test", test)):
        data_prep.save_split(frame, paths.split_path(name))
    logger.info("training.splits_saved", **{k: v.to_dict() for k, v in split_stats.items()})

    candidates, best_per_combo = train_candidates(train, val, cfg)

    comparison: list[dict[str, Any]] = []
    curves: dict[str, Any] = {}
    for spec in MODEL_SPECS:
        for strategy in Strategy:
            candidate = best_per_combo[(spec.key, strategy)]
            row, curve, _ = evaluate_combination(candidate, val, test)
            comparison.append(row)
            curves[candidate.display_name] = curve
            logger.info(
                "training.evaluated",
                model=candidate.display_name,
                val_auc_pr=row["val"]["auc_pr"],
                test_auc_pr=row["test"]["auc_pr"],
                threshold=row["threshold"],
            )

    best_row = max(comparison, key=lambda r: r["val"]["auc_pr"])
    best_candidate = next(
        c for c in best_per_combo.values() if c.display_name == best_row["display_name"]
    )
    best_model = best_candidate.pipeline_model
    y_val, p_val = evaluate.collect_scores(best_model.transform(val))
    scored_test = best_model.transform(test).persist(StorageLevel.MEMORY_AND_DISK)
    demo_samples = pick_demo_samples(scored_test, best_row["threshold"])
    scored_test.unpersist()

    paths.model_dir.mkdir(parents=True, exist_ok=True)
    save_pipeline_model(best_model, paths.model_path)

    metrics: dict[str, Any] = {
        "run_id": run_id,
        "created_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "spark_version": spark.version,
        "seed": cfg.random_seed,
        "split_weights": list(SPLIT_WEIGHTS),
        "undersample_ratio": cfg.undersample_ratio,
        "feature_columns": list(FEATURE_COLUMNS),
        "selection_metric": "val_auc_pr",
        "threshold_rule": "max F1 on validation",
        "dataset": {
            "rows_raw": raw_stats.rows,
            "frauds_raw": raw_stats.frauds,
            "duplicates_removed": duplicates,
            **clean_stats.to_dict(),
            **twins,
        },
        "splits": {k: v.to_dict() for k, v in split_stats.items()},
        "candidates": [c.summary() for c in candidates],
        "comparison": comparison,
        "best": best_row,
        "curves": curves,
        "threshold_sweep": evaluate.threshold_sweep(y_val, p_val),
        "training_seconds": round(time.monotonic() - started, 1),
    }

    _write_json_atomic(paths.demo_samples_path, demo_samples)
    report.write_figures(metrics, paths.figures_dir)
    report.write_results_markdown(metrics, paths.results_markdown_path)
    _write_json_atomic(paths.metrics_path, metrics)  # last: its presence marks a complete run

    for frame in (train, val, test, deduplicated):
        frame.unpersist()
    logger.info(
        "training.done",
        run_id=run_id,
        best=best_row["display_name"],
        threshold=best_row["threshold"],
        test_auc_pr=best_row["test"]["auc_pr"],
        minutes=round(metrics["training_seconds"] / 60, 1),
    )
    return metrics


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging("trainer", get_logging_settings().log_level)
    paths = get_path_settings()
    cfg = get_training_settings()

    if not args.force and artifacts_exist(paths):
        logger.info(
            "training.skipped",
            reason="model artifacts already exist",
            hint="run '.\\run.ps1 train' (or add --force) to retrain",
        )
        return 0

    try:
        data_prep.ensure_dataset_exists(paths.raw_dataset_path)
    except data_prep.DatasetMissingError as exc:
        logger.error("training.dataset_missing", path=str(paths.raw_dataset_path))
        print(f"\nERROR: {exc}\n", file=sys.stderr, flush=True)
        return 2

    spark = build_spark_session(
        "fraud-trainer", master=cfg.spark_master, driver_memory=cfg.trainer_driver_memory
    )
    try:
        run_training(spark, paths, cfg)
        return 0
    except data_prep.DatasetValidationError as exc:
        logger.error("training.invalid_dataset", error=str(exc))
        return 2
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
