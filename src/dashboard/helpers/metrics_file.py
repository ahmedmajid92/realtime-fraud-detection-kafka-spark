"""Reading ``models/metrics.json`` and shaping it into tables (pure functions)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from src.dashboard.helpers.i18n import t


def load_metrics(path: Path) -> dict[str, Any] | None:
    """Parsed ``metrics.json``, or ``None`` if the model has not been trained yet."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def model_label(row: Mapping[str, Any], lang: str) -> str:
    return t(f"model.{row['model_key']}", lang)


def strategy_label(strategy: str, lang: str) -> str:
    return t(f"strategy.{strategy}", lang)


def row_label(row: Mapping[str, Any], lang: str) -> str:
    """Localised "<model> · <strategy>" label."""
    return f"{model_label(row, lang)} · {strategy_label(row['strategy'], lang)}"


def _params(params: Mapping[str, Any]) -> str:
    return ", ".join(f"{k}={v}" for k, v in params.items())


def comparison_frame(metrics: Mapping[str, Any], lang: str) -> pd.DataFrame:
    """Model comparison table (one row per model × imbalance strategy)."""
    rows = []
    for row in metrics.get("comparison", []):
        test = row["test"]
        rows.append(
            {
                t("perf.col.model", lang): model_label(row, lang),
                t("perf.col.strategy", lang): strategy_label(row["strategy"], lang),
                t("perf.col.params", lang): _params(row["params"]),
                t("perf.col.val_auc_pr", lang): row["val"]["auc_pr"],
                t("perf.col.test_auc_pr", lang): test["auc_pr"],
                t("perf.col.test_roc_auc", lang): test["roc_auc"],
                t("perf.col.precision", lang): test["precision"],
                t("perf.col.recall", lang): test["recall"],
                t("perf.col.f1", lang): test["f1"],
                t("perf.col.threshold", lang): row["threshold"],
            }
        )
    return pd.DataFrame(rows)


def candidates_frame(metrics: Mapping[str, Any], lang: str) -> pd.DataFrame:
    """Every grid candidate with its validation scores and fit time."""
    rows = [
        {
            t("perf.col.model", lang): model_label(c, lang),
            t("perf.col.strategy", lang): strategy_label(c["strategy"], lang),
            t("perf.col.params", lang): _params(c["params"]),
            t("perf.col.val_auc_pr", lang): c["val_auc_pr"],
            t("perf.col.val_roc_auc", lang): c["val_roc_auc"],
            t("perf.col.fit_seconds", lang): c["train_seconds"],
        }
        for c in metrics.get("candidates", [])
    ]
    return pd.DataFrame(rows)


def dataset_frame(metrics: Mapping[str, Any], lang: str) -> pd.DataFrame:
    """Dataset and split sizes."""
    ds, splits = metrics["dataset"], metrics["splits"]
    entries = [
        (t("perf.ds.raw", lang), ds["rows_raw"], ds["frauds_raw"]),
        (t("perf.ds.dedup", lang), ds["rows"], ds["frauds"]),
        (t("perf.ds.train", lang), splits["train"]["rows"], splits["train"]["frauds"]),
        (t("perf.ds.val", lang), splits["val"]["rows"], splits["val"]["frauds"]),
        (t("perf.ds.test", lang), splits["test"]["rows"], splits["test"]["frauds"]),
    ]
    return pd.DataFrame(
        [
            {
                t("perf.ds.set", lang): name,
                t("perf.ds.rows", lang): f"{rows:,}",
                t("perf.ds.frauds", lang): f"{frauds:,}",
                t("perf.ds.rate", lang): f"{frauds / rows:.4%}" if rows else "—",
            }
            for name, rows, frauds in entries
        ]
    )
