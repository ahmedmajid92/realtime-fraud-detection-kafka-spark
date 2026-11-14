"""Plotly chart builders: structure, palette roles and threshold markers."""

from __future__ import annotations

from datetime import UTC
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import pytest

from src.dashboard.helpers import charts

LIGHT, DARK = charts.LIGHT, charts.DARK
LABELS: dict[str, str] = {
    k: k
    for k in [
        "x",
        "y",
        "threshold",
        "flagged",
        "actual",
        "legit",
        "tn",
        "fp",
        "fn",
        "tp",
        "pred_legit",
        "pred_fraud",
        "act_legit",
        "act_fraud",
        "recall",
        "precision",
        "fpr",
        "tpr",
        "random",
        "f1",
        "tuned",
        "current",
        "title",
    ]
}


def test_theme_selection_and_model_colours() -> None:
    assert charts.theme_for(False) is LIGHT and charts.theme_for(True) is DARK
    assert charts.model_color("rf", LIGHT) == LIGHT.series[1]
    assert charts.model_color("unknown", LIGHT) == LIGHT.muted
    assert LIGHT.series != DARK.series  # dark mode has its own validated steps


def test_probability_histogram_has_threshold_line() -> None:
    fig = charts.probability_histogram([0.1, 0.2, 0.9], 0.4, LIGHT, LABELS)
    assert isinstance(fig, go.Figure)
    assert fig.data[0].type == "histogram"
    assert fig.data[0].marker.color == LIGHT.series[0]
    assert any(shape.x0 == 0.4 for shape in fig.layout.shapes)
    assert fig.layout.yaxis.type == "log"


def test_frauds_over_time_two_series() -> None:
    per_minute = pd.DataFrame(
        {
            "minute": pd.date_range("2026-11-14", periods=3, freq="min", tz=UTC),
            "flagged": [1, 0, 2],
            "actual_frauds": [1, 1, 2],
        }
    )
    fig = charts.frauds_over_time(per_minute, LIGHT, LABELS)
    assert [trace.name for trace in fig.data] == ["flagged", "actual"]
    assert fig.data[0].line.color == LIGHT.series[1]  # flagged = slot 2
    assert fig.data[1].line.color == LIGHT.series[2]  # actual fraud = slot 3
    empty = charts.frauds_over_time(pd.DataFrame(), LIGHT, LABELS)
    assert len(empty.data) == 2


def test_amount_distribution_splits_by_threshold() -> None:
    scores = pd.DataFrame({"fraud_probability": [0.1, 0.2, 0.9], "amount": [5.0, 0.0, 999.0]})
    fig = charts.amount_distribution(scores, 0.5, LIGHT, LABELS)
    assert [trace.name for trace in fig.data] == ["legit (2)", "flagged (1)"]
    assert min(fig.data[0].y) >= 1.0  # +1 so the log axis can show zero amounts
    assert len(charts.amount_distribution(pd.DataFrame(), 0.5, LIGHT, LABELS).data) == 0


def test_confusion_heatmap_annotations() -> None:
    fig = charts.confusion_heatmap({"tp": 56, "fp": 17, "tn": 42470, "fn": 14}, DARK, LABELS)
    text = fig.data[0].text
    assert text[0][0].startswith("42,470") and text[1][1].startswith("56")
    assert fig.layout.paper_bgcolor == DARK.surface


def test_curves_highlight_selected_model(fake_metrics: dict[str, Any]) -> None:
    row_labels = {row["display_name"]: row["display_name"] for row in fake_metrics["comparison"]}
    pr = charts.pr_curves(fake_metrics, LIGHT, LABELS, row_labels)
    roc = charts.roc_curves(fake_metrics, LIGHT, LABELS, row_labels)
    assert len(pr.data) == 8 and len(roc.data) == 9  # + random-classifier diagonal
    best = [trace for trace in pr.data if trace.name.endswith("★")]
    assert len(best) == 1 and best[0].line.width == 3
    dashes = {trace.line.dash for trace in pr.data}
    assert dashes == {"solid", "dash"}  # strategy encoded by line style, not only colour


@pytest.mark.parametrize(("current", "lines"), [(0.5, 1), (0.8, 2), (None, 1)])
def test_threshold_sweep_chart_marks(
    fake_metrics: dict[str, Any], current: float | None, lines: int
) -> None:
    fig = charts.threshold_sweep_chart(fake_metrics["threshold_sweep"], 0.5, current, LIGHT, LABELS)
    assert len(fig.data) == 3
    assert len([s for s in fig.layout.shapes if s.type == "line"]) == lines


def test_comparison_bars(fake_metrics: dict[str, Any]) -> None:
    models = {k: k.upper() for k in charts.MODEL_ORDER}
    strategies = {"class_weights": "cw", "undersampling": "us"}
    fig = charts.comparison_bars(fake_metrics, LIGHT, LABELS, models, strategies)
    bars = [trace for trace in fig.data if trace.showlegend is False]
    legend = [trace for trace in fig.data if trace.showlegend is not False]
    assert [trace.name for trace in legend] == ["cw", "us"]
    assert {trace.marker.color for trace in legend} == {LIGHT.muted}  # neutral swatches
    assert list(bars[0].x) == ["LR", "RF", "GBT", "SVM"]
    assert list(bars[0].marker.color) == list(LIGHT.series)  # colour = model, fixed order
    assert bars[1].marker.pattern.shape == "/"


@pytest.mark.parametrize(("probability", "colour_attr"), [(0.9, "critical"), (0.1, "series")])
def test_probability_gauge_colour(probability: float, colour_attr: str) -> None:
    fig = charts.probability_gauge(probability, 0.5, LIGHT, LABELS)
    bar_colour = fig.data[0].gauge.bar.color
    expected = LIGHT.critical if colour_attr == "critical" else LIGHT.series[0]
    assert bar_colour == expected
    assert fig.data[0].gauge.threshold.value == 0.5


def test_all_figures_use_theme_surface() -> None:
    fig = charts.probability_histogram([], 0.5, DARK, LABELS)
    assert fig.layout.plot_bgcolor == DARK.surface
