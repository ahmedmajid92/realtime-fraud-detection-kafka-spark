"""Plotly figure builders for the dashboard (pure functions, no Streamlit).

Colour roles follow the project's validated categorical palette (checked with the
dataviz palette validator, light and dark):

* slot 1 (blue)   → legitimate / the single series of a one-series chart
* slot 2 (orange) → flagged by the model (predicted fraud)
* slot 3 (aqua)   → actual (ground-truth) fraud
* model families take slots 1-4 in a fixed order; strategy = line dash / bar pattern

Status "critical" red is used only where a value *means* "alert" (gauge above threshold),
always next to an icon + text label. Every chart has a legend for ≥ 2 series and a table
twin elsewhere on its page.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
import plotly.graph_objects as go

FONT_FAMILY = 'system-ui, -apple-system, "Segoe UI", "Noto Sans Arabic", sans-serif'
MODEL_ORDER = ("lr", "rf", "gbt", "svm")
STRATEGY_DASH = {"class_weights": "solid", "undersampling": "dash"}
STRATEGY_PATTERN = {"class_weights": "", "undersampling": "/"}


@dataclass(frozen=True)
class ChartTheme:
    surface: str
    ink: str
    ink_secondary: str
    muted: str
    grid: str
    axis: str
    series: tuple[str, str, str, str]
    critical: str
    sequential: tuple[str, ...]


LIGHT = ChartTheme(
    surface="#fcfcfb",
    ink="#0b0b0b",
    ink_secondary="#52514e",
    muted="#898781",
    grid="#e1e0d9",
    axis="#c3c2b7",
    series=("#2a78d6", "#eb6834", "#1baf7a", "#eda100"),
    critical="#d03b3b",
    sequential=("#cde2fb", "#86b6ef", "#3987e5", "#256abf", "#104281"),
)

DARK = ChartTheme(
    surface="#1a1a19",
    ink="#f0efec",
    ink_secondary="#c3c2b7",
    muted="#898781",
    grid="#2c2c2a",
    axis="#383835",
    series=("#3987e5", "#d95926", "#199e70", "#c98500"),
    critical="#d03b3b",
    sequential=("#184f95", "#256abf", "#3987e5", "#6da7ec", "#b7d3f6"),
)


def theme_for(dark: bool) -> ChartTheme:
    return DARK if dark else LIGHT


def model_color(model_key: str, theme: ChartTheme) -> str:
    try:
        return theme.series[MODEL_ORDER.index(model_key)]
    except ValueError:
        return theme.muted


def _apply_layout(
    fig: go.Figure, theme: ChartTheme, *, height: int = 300, title: str | None = None
) -> go.Figure:
    fig.update_layout(
        template="none",
        height=height,
        title={"text": title, "x": 0, "font": {"size": 14, "color": theme.ink}} if title else None,
        paper_bgcolor=theme.surface,
        plot_bgcolor=theme.surface,
        font={"family": FONT_FAMILY, "color": theme.ink_secondary, "size": 12},
        margin={"l": 8, "r": 8, "t": 40 if title else 16, "b": 8},
        legend={
            "orientation": "h",
            "x": 0,
            "y": 1.02,
            "yanchor": "bottom",
            "font": {"color": theme.ink_secondary},
        },
        hoverlabel={"font": {"family": FONT_FAMILY}},
        bargap=0.15,
        bargroupgap=0.06,
        barcornerradius=4,
    )
    axis_style = {
        "gridcolor": theme.grid,
        "gridwidth": 1,
        "linecolor": theme.axis,
        "zeroline": False,
        "tickfont": {"color": theme.muted},
        "title": {"font": {"color": theme.ink_secondary}},
        "automargin": True,
    }
    fig.update_xaxes(**axis_style)
    fig.update_yaxes(**axis_style)
    return fig


def _reference_line(fig: go.Figure, x: float, label: str, theme: ChartTheme) -> None:
    fig.add_vline(x=x, line={"color": theme.ink_secondary, "width": 1.5})
    fig.add_annotation(
        x=x,
        y=1,
        yref="paper",
        text=label,
        showarrow=False,
        xanchor="left",
        yanchor="top",
        xshift=4,
        font={"color": theme.ink_secondary, "size": 11},
    )


def probability_histogram(
    probabilities: Sequence[float], threshold: float, theme: ChartTheme, labels: Mapping[str, str]
) -> go.Figure:
    """Distribution of fraud probabilities (log count axis) with the alert threshold."""
    fig = go.Figure(
        go.Histogram(
            x=list(probabilities),
            xbins={"start": 0.0, "end": 1.0, "size": 0.02},
            marker={"color": theme.series[0], "line": {"color": theme.surface, "width": 1}},
            hovertemplate=f"{labels['x']}: %{{x}}<br>{labels['y']}: %{{y:,}}<extra></extra>",
        )
    )
    _reference_line(fig, threshold, labels["threshold"], theme)
    fig.update_xaxes(title_text=labels["x"], range=[0, 1])
    fig.update_yaxes(title_text=labels["y"], type="log")
    return _apply_layout(fig, theme)


def frauds_over_time(
    per_minute: pd.DataFrame, theme: ChartTheme, labels: Mapping[str, str]
) -> go.Figure:
    """Flagged vs actual frauds per minute (two series of the same unit, one axis)."""
    fig = go.Figure()
    for column, colour, name in (
        ("flagged", theme.series[1], labels["flagged"]),
        ("actual_frauds", theme.series[2], labels["actual"]),
    ):
        fig.add_trace(
            go.Scatter(
                x=per_minute.get("minute", []),
                y=per_minute.get(column, []),
                name=name,
                mode="lines+markers",
                line={"color": colour, "width": 2},
                marker={"size": 8, "color": colour, "line": {"color": theme.surface, "width": 2}},
                hovertemplate=f"{name}: %{{y:,}}<extra></extra>",
            )
        )
    fig.update_layout(hovermode="x unified")
    fig.update_xaxes(title_text=labels["x"])
    fig.update_yaxes(title_text=labels["y"], rangemode="tozero")
    return _apply_layout(fig, theme)


def amount_distribution(
    scores: pd.DataFrame, threshold: float, theme: ChartTheme, labels: Mapping[str, str]
) -> go.Figure:
    """Transaction amounts of legitimate-looking vs flagged transactions (log scale)."""
    fig = go.Figure()
    if not scores.empty:
        flagged = scores["fraud_probability"].astype(float) >= threshold
        for mask, colour, name in (
            (~flagged, theme.series[0], labels["legit"]),
            (flagged, theme.series[1], labels["flagged"]),
        ):
            amounts = scores.loc[mask, "amount"].astype(float) + 1.0  # log axis needs > 0
            fig.add_trace(
                go.Box(
                    y=amounts,
                    name=f"{name} ({int(mask.sum()):,})",
                    marker={"color": colour},
                    line={"color": colour, "width": 1.5},
                    boxpoints=False,
                    hovertemplate=f"{name}<extra></extra>",
                )
            )
    fig.update_yaxes(title_text=labels["y"], type="log")
    fig.update_layout(showlegend=True)
    return _apply_layout(fig, theme)


def confusion_heatmap(
    confusion: Mapping[str, int], theme: ChartTheme, labels: Mapping[str, str]
) -> go.Figure:
    """2x2 confusion matrix; colour on a log scale because TN dwarfs the other cells."""
    matrix = np.array([[confusion["tn"], confusion["fp"]], [confusion["fn"], confusion["tp"]]])
    text = [
        [f"{matrix[0, 0]:,}<br>{labels['tn']}", f"{matrix[0, 1]:,}<br>{labels['fp']}"],
        [f"{matrix[1, 0]:,}<br>{labels['fn']}", f"{matrix[1, 1]:,}<br>{labels['tp']}"],
    ]
    steps = theme.sequential
    colourscale = [[i / (len(steps) - 1), c] for i, c in enumerate(steps)]
    fig = go.Figure(
        go.Heatmap(
            z=np.log10(matrix + 1),
            x=[labels["pred_legit"], labels["pred_fraud"]],
            y=[labels["act_legit"], labels["act_fraud"]],
            text=text,
            texttemplate="%{text}",
            textfont={"size": 14},
            colorscale=colourscale,
            showscale=False,
            xgap=2,
            ygap=2,
            hovertemplate="%{y} / %{x}: %{text}<extra></extra>",
        )
    )
    fig.update_yaxes(autorange="reversed", showgrid=False)
    fig.update_xaxes(side="top", showgrid=False)
    return _apply_layout(fig, theme, height=320)


def _curve_traces(
    metrics: Mapping[str, Any], kind: str, theme: ChartTheme, row_labels: Mapping[str, str]
) -> list[go.Scatter]:
    best = metrics["best"]["display_name"]
    traces = []
    for row in metrics["comparison"]:
        curve = metrics["curves"][row["display_name"]][kind]
        x, y = (
            (curve["recall"], curve["precision"]) if kind == "pr" else (curve["fpr"], curve["tpr"])
        )
        is_best = row["display_name"] == best
        name = row_labels[row["display_name"]] + (" ★" if is_best else "")
        traces.append(
            go.Scatter(
                x=x,
                y=y,
                name=name,
                mode="lines",
                line={
                    "color": model_color(row["model_key"], theme),
                    "dash": STRATEGY_DASH.get(row["strategy"], "solid"),
                    "width": 3 if is_best else 1.5,
                },
                opacity=1.0 if is_best else 0.8,
                hovertemplate=f"{name}<br>%{{x:.3f}}, %{{y:.3f}}<extra></extra>",
            )
        )
    return traces


def pr_curves(
    metrics: Mapping[str, Any],
    theme: ChartTheme,
    labels: Mapping[str, str],
    row_labels: Mapping[str, str],
) -> go.Figure:
    fig = go.Figure(_curve_traces(metrics, "pr", theme, row_labels))
    baseline = metrics["splits"]["test"]["fraud_rate"]
    fig.add_hline(y=baseline, line={"color": theme.muted, "width": 1})
    fig.update_xaxes(title_text=labels["recall"], range=[0, 1])
    fig.update_yaxes(title_text=labels["precision"], range=[0, 1.02])
    fig.update_layout(legend={"orientation": "v", "x": 1.02, "y": 1, "yanchor": "top"})
    return _apply_layout(fig, theme, height=420)


def roc_curves(
    metrics: Mapping[str, Any],
    theme: ChartTheme,
    labels: Mapping[str, str],
    row_labels: Mapping[str, str],
) -> go.Figure:
    fig = go.Figure(_curve_traces(metrics, "roc", theme, row_labels))
    fig.add_trace(
        go.Scatter(
            x=[0, 1],
            y=[0, 1],
            mode="lines",
            name=labels["random"],
            line={"color": theme.muted, "width": 1},
            hoverinfo="skip",
        )
    )
    fig.update_xaxes(title_text=labels["fpr"], range=[0, 1])
    fig.update_yaxes(title_text=labels["tpr"], range=[0, 1.02])
    fig.update_layout(legend={"orientation": "v", "x": 1.02, "y": 1, "yanchor": "top"})
    return _apply_layout(fig, theme, height=420)


def threshold_sweep_chart(
    sweep: Sequence[Mapping[str, float]],
    tuned: float,
    current: float | None,
    theme: ChartTheme,
    labels: Mapping[str, str],
) -> go.Figure:
    """Precision / recall / F1 against the threshold (validation split)."""
    frame = pd.DataFrame(sweep)
    fig = go.Figure()
    for metric, colour in zip(("precision", "recall", "f1"), theme.series[:3], strict=True):
        fig.add_trace(
            go.Scatter(
                x=frame["threshold"],
                y=frame[metric],
                name=labels[metric],
                mode="lines",
                line={"color": colour, "width": 2},
                hovertemplate=f"{labels[metric]}: %{{y:.3f}}<extra></extra>",
            )
        )
    _reference_line(fig, tuned, labels["tuned"], theme)
    if current is not None and abs(current - tuned) > 1e-3:
        fig.add_vline(x=current, line={"color": theme.muted, "width": 1.5})
        fig.add_annotation(
            x=current,
            y=0.08,
            yref="paper",
            text=labels["current"],
            showarrow=False,
            xanchor="left",
            xshift=4,
            font={"color": theme.muted, "size": 11},
        )
    fig.update_layout(hovermode="x unified")
    fig.update_xaxes(title_text=labels["x"], range=[0, 1])
    fig.update_yaxes(title_text=labels["y"], range=[0, 1.02])
    return _apply_layout(fig, theme, height=360)


def comparison_bars(
    metrics: Mapping[str, Any],
    theme: ChartTheme,
    labels: Mapping[str, str],
    model_labels: Mapping[str, str],
    strategy_labels: Mapping[str, str],
) -> go.Figure:
    """Test AUC-PR: models on the x-axis, one bar per strategy (pattern = strategy)."""
    rows = metrics["comparison"]
    models = [key for key in MODEL_ORDER if any(r["model_key"] == key for r in rows)]
    fig = go.Figure()
    for strategy in ("class_weights", "undersampling"):
        values = []
        for key in models:
            match = [r for r in rows if r["model_key"] == key and r["strategy"] == strategy]
            values.append(match[0]["test"]["auc_pr"] if match else None)
        fig.add_trace(
            go.Bar(
                x=[model_labels[k] for k in models],
                y=values,
                name=strategy_labels[strategy],
                marker={
                    "color": [model_color(k, theme) for k in models],
                    "pattern": {
                        "shape": STRATEGY_PATTERN[strategy],
                        "fgcolor": theme.surface,
                        "solidity": 0.35,
                    },
                    "line": {"width": 0},
                },
                text=[f"{v:.3f}" if v is not None else "" for v in values],
                textposition="outside",
                textfont={"color": theme.ink_secondary},
                hovertemplate=f"%{{x}} · {strategy_labels[strategy]}: %{{y:.4f}}<extra></extra>",
                showlegend=False,
            )
        )
    # Colour encodes the model (the x-axis labels say which). The legend explains only the
    # pattern, with neutral swatches, so it doesn't borrow one model's colour.
    for strategy in ("class_weights", "undersampling"):
        fig.add_trace(
            go.Bar(
                x=[None],
                y=[None],
                name=strategy_labels[strategy],
                marker={
                    "color": theme.muted,
                    "pattern": {
                        "shape": STRATEGY_PATTERN[strategy],
                        "fgcolor": theme.surface,
                        "solidity": 0.35,
                    },
                },
                hoverinfo="skip",
            )
        )
    fig.update_layout(barmode="group")
    fig.update_yaxes(title_text=labels["y"], range=[0, 1.1])
    return _apply_layout(fig, theme, height=360)


def probability_gauge(
    probability: float, threshold: float, theme: ChartTheme, labels: Mapping[str, str]
) -> go.Figure:
    """Gauge of one transaction's fraud probability with the threshold marked."""
    flagged = probability >= threshold
    fig = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=probability,
            number={"valueformat": ".3f", "font": {"color": theme.ink}},
            title={"text": labels["title"], "font": {"color": theme.ink_secondary, "size": 13}},
            gauge={
                "axis": {
                    "range": [0, 1],
                    "tickcolor": theme.muted,
                    "tickfont": {"color": theme.muted},
                },
                "bar": {"color": theme.critical if flagged else theme.series[0], "thickness": 0.3},
                "bgcolor": theme.surface,
                "borderwidth": 0,
                "steps": [{"range": [threshold, 1], "color": theme.grid}],
                "threshold": {
                    "line": {"color": theme.ink, "width": 2},
                    "thickness": 0.85,
                    "value": threshold,
                },
            },
        )
    )
    return _apply_layout(fig, theme, height=260)
