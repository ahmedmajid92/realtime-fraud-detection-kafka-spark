"""Report assets for the course report: PNG figures and ``docs/RESULTS.md``.

Colours follow the project's validated categorical palette (light mode, because the
figures are pasted into a document): models take slots 1-4 in a fixed order, and the
imbalance strategy is encoded by line style / hatching. Identity therefore never
depends on colour alone.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")  # headless rendering inside the container

import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Patch

# Validated categorical slots (light mode) and chart chrome.
SERIES = ("#2a78d6", "#eb6834", "#1baf7a", "#eda100")
MODEL_ORDER = ("lr", "rf", "gbt", "svm")
MODEL_COLORS = dict(zip(MODEL_ORDER, SERIES, strict=True))
STRATEGY_LINESTYLE = {"class_weights": "-", "undersampling": "--"}
INK_PRIMARY = "#0b0b0b"
INK_SECONDARY = "#52514e"
INK_MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
SURFACE = "#fcfcfb"
SEQUENTIAL_BLUE = ("#cde2fb", "#86b6ef", "#3987e5", "#256abf", "#104281")

FIGURE_FILES = {
    "pr": "pr_curves.png",
    "roc": "roc_curves.png",
    "confusion": "confusion_matrix.png",
    "sweep": "threshold_sweep.png",
    "comparison": "model_comparison.png",
}


def _style_axes(ax: Any, title: str, xlabel: str, ylabel: str) -> None:
    ax.set_facecolor(SURFACE)
    ax.set_title(title, color=INK_PRIMARY, fontsize=12, loc="left", pad=10)
    ax.set_xlabel(xlabel, color=INK_SECONDARY)
    ax.set_ylabel(ylabel, color=INK_SECONDARY)
    ax.tick_params(colors=INK_MUTED, labelcolor=INK_SECONDARY)
    ax.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ("top", "right"):
        ax.spines[side].set_visible(False)
    for side in ("left", "bottom"):
        ax.spines[side].set_color(AXIS)


def _save(fig: Any, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.patch.set_facecolor(SURFACE)
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=SURFACE)
    plt.close(fig)
    return path


def _row_style(row: Mapping[str, Any], best_name: str) -> dict[str, Any]:
    is_best = row["display_name"] == best_name
    return {
        "color": MODEL_COLORS.get(row["model_key"], INK_MUTED),
        "linestyle": STRATEGY_LINESTYLE.get(row["strategy"], "-"),
        "linewidth": 2.6 if is_best else 1.4,
        "alpha": 1.0 if is_best else 0.75,
        "label": f"{row['display_name']}{'  (selected)' if is_best else ''}",
        "zorder": 3 if is_best else 2,
    }


def plot_pr_curves(metrics: Mapping[str, Any], path: Path) -> Path:
    best = metrics["best"]["display_name"]
    fig, ax = plt.subplots(figsize=(7.5, 5.5))
    for row in metrics["comparison"]:
        curve = metrics["curves"][row["display_name"]]["pr"]
        ax.plot(curve["recall"], curve["precision"], **_row_style(row, best))
    baseline = metrics["splits"]["test"]["fraud_rate"]
    ax.axhline(baseline, color=INK_MUTED, linewidth=1, label=f"No-skill baseline ({baseline:.4f})")
    ax.set_xlim(0, 1.0)
    ax.set_ylim(0, 1.02)
    _style_axes(ax, "Precision-Recall curves (test split)", "Recall", "Precision")
    ax.legend(fontsize=7.5, loc="lower left", frameon=False)
    return _save(fig, path)


def plot_roc_curves(metrics: Mapping[str, Any], path: Path) -> Path:
    best = metrics["best"]["display_name"]
    fig, ax = plt.subplots(figsize=(7.5, 5.5))
    for row in metrics["comparison"]:
        curve = metrics["curves"][row["display_name"]]["roc"]
        ax.plot(curve["fpr"], curve["tpr"], **_row_style(row, best))
    ax.plot([0, 1], [0, 1], color=INK_MUTED, linewidth=1, label="Random classifier")
    ax.set_xlim(0, 1.0)
    ax.set_ylim(0, 1.02)
    _style_axes(ax, "ROC curves (test split)", "False positive rate", "True positive rate")
    ax.legend(fontsize=7.5, loc="lower right", frameon=False)
    return _save(fig, path)


def plot_confusion_matrix(confusion: Mapping[str, int], title: str, path: Path) -> Path:
    """2x2 confusion matrix, coloured on a log scale (TN dwarfs the other cells)."""
    matrix = np.array([[confusion["tn"], confusion["fp"]], [confusion["fn"], confusion["tp"]]])
    colour_values = np.log10(matrix + 1)
    cmap = LinearSegmentedColormap.from_list("seq_blue", SEQUENTIAL_BLUE)
    fig, ax = plt.subplots(figsize=(5.2, 4.6))
    ax.imshow(colour_values, cmap=cmap, vmin=0, vmax=max(colour_values.max(), 1))
    names = (("True negative", "False positive"), ("False negative", "True positive"))
    for i in range(2):
        for j in range(2):
            dark = colour_values[i, j] > 0.6 * colour_values.max()
            ax.text(
                j,
                i,
                f"{matrix[i, j]:,}\n{names[i][j]}",
                ha="center",
                va="center",
                color="#ffffff" if dark else INK_PRIMARY,
                fontsize=11,
            )
    ax.set_xticks([0, 1], ["Legitimate", "Fraud"])
    ax.set_yticks([0, 1], ["Legitimate", "Fraud"])
    ax.set_xlabel("Predicted", color=INK_SECONDARY)
    ax.set_ylabel("Actual", color=INK_SECONDARY)
    ax.set_title(title, color=INK_PRIMARY, fontsize=11, loc="left", pad=10)
    ax.tick_params(colors=INK_MUTED, labelcolor=INK_SECONDARY, length=0)
    for spine in ax.spines.values():
        spine.set_visible(False)
    return _save(fig, path)


def plot_threshold_sweep(
    sweep: Sequence[Mapping[str, float]], chosen: float, path: Path, model_name: str
) -> Path:
    thresholds = [row["threshold"] for row in sweep]
    fig, ax = plt.subplots(figsize=(7.5, 4.8))
    for metric, colour in zip(("precision", "recall", "f1"), SERIES[:3], strict=True):
        ax.plot(
            thresholds,
            [row[metric] for row in sweep],
            color=colour,
            linewidth=2,
            label=metric.capitalize() if metric != "f1" else "F1",
        )
    ax.axvline(chosen, color=INK_SECONDARY, linewidth=1.2)
    ax.annotate(
        f"chosen = {chosen:.3f}",
        xy=(chosen, 0.6),
        xytext=(6, 0),
        textcoords="offset points",
        color=INK_SECONDARY,
        fontsize=9,
    )
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1.02)
    _style_axes(ax, f"Threshold sweep on validation · {model_name}", "Alert threshold", "Score")
    ax.legend(fontsize=9, loc="lower center", bbox_to_anchor=(0.6, 0.02), frameon=False, ncol=3)
    return _save(fig, path)


def plot_model_comparison(metrics: Mapping[str, Any], path: Path) -> Path:
    """Grouped bars: test AUC-PR per model, one bar per imbalance strategy (hatch = strategy)."""
    rows = metrics["comparison"]
    models = [key for key in MODEL_ORDER if any(r["model_key"] == key for r in rows)]
    names = {r["model_key"]: r["model"] for r in rows}
    strategies = ("class_weights", "undersampling")
    width = 0.36
    fig, ax = plt.subplots(figsize=(7.5, 4.8))
    x = np.arange(len(models))
    for offset, strategy in zip((-width / 2, width / 2), strategies, strict=True):
        values = []
        for key in models:
            match = [r for r in rows if r["model_key"] == key and r["strategy"] == strategy]
            values.append(match[0]["test"]["auc_pr"] if match else 0.0)
        bars = ax.bar(
            x + offset,
            values,
            width * 0.94,
            color=[MODEL_COLORS[k] for k in models],
            hatch=None if strategy == "class_weights" else "//",
            edgecolor=SURFACE,
            linewidth=0,
        )
        ax.bar_label(
            bars, labels=[f"{v:.3f}" for v in values], fontsize=8, color=INK_SECONDARY, padding=2
        )
    ax.set_xticks(x, [names[k] for k in models])
    ax.set_ylim(0, 1.05)
    _style_axes(ax, "Test AUC-PR by model and imbalance strategy", "", "AUC-PR (test)")
    # Colour = model (the x-axis labels say which); the legend explains only the pattern,
    # so its swatches are neutral grey rather than borrowing one model's colour.
    strategy_handles = [
        Patch(facecolor=INK_MUTED, label="class weights"),
        Patch(facecolor=INK_MUTED, hatch="//", edgecolor=SURFACE, label="undersampling (hatched)"),
    ]
    ax.legend(handles=strategy_handles, fontsize=9, frameon=False, loc="upper left")
    return _save(fig, path)


def write_figures(metrics: Mapping[str, Any], figures_dir: Path) -> dict[str, Path]:
    """Render every figure and return ``{figure_key: path}``."""
    best = metrics["best"]
    return {
        "pr": plot_pr_curves(metrics, figures_dir / FIGURE_FILES["pr"]),
        "roc": plot_roc_curves(metrics, figures_dir / FIGURE_FILES["roc"]),
        "confusion": plot_confusion_matrix(
            best["test"]["confusion"],
            f"Confusion matrix (test) · {best['display_name']} @ {best['threshold']:.3f}",
            figures_dir / FIGURE_FILES["confusion"],
        ),
        "sweep": plot_threshold_sweep(
            metrics["threshold_sweep"],
            best["threshold"],
            figures_dir / FIGURE_FILES["sweep"],
            best["display_name"],
        ),
        "comparison": plot_model_comparison(metrics, figures_dir / FIGURE_FILES["comparison"]),
    }


def _fmt(value: float, digits: int = 4) -> str:
    return f"{value:.{digits}f}"


def render_results_markdown(metrics: Mapping[str, Any]) -> str:
    """Markdown summary of the training run (dataset, comparison table, best model)."""
    ds, splits, best = metrics["dataset"], metrics["splits"], metrics["best"]
    lines = [
        "# Model Results",
        "",
        f"Generated by the trainer on {metrics['created_at']} "
        f"(run `{metrics['run_id']}`, Spark {metrics['spark_version']}, seed {metrics['seed']}, "
        f"training time {metrics['training_seconds'] / 60:.1f} min).",
        "",
        "## Dataset",
        "",
        "| Set | Rows | Frauds | Fraud rate |",
        "|---|---:|---:|---:|",
        f"| Raw CSV | {ds['rows_raw']:,} | {ds['frauds_raw']:,} | {ds['frauds_raw'] / ds['rows_raw']:.4%} |",
        f"| After removing {ds['duplicates_removed']:,} exact duplicates | {ds['rows']:,} | {ds['frauds']:,} | {ds['fraud_rate']:.4%} |",
    ]
    for name in ("train", "val", "test"):
        split = splits[name]
        lines.append(
            f"| {name.capitalize()} split | {split['rows']:,} | {split['frauds']:,} | {split['fraud_rate']:.4%} |"
        )
    if "twin_rows" in ds:
        lines += [
            "",
            f"**Feature twins:** {ds['twin_rows']:,} rows in {ds['twin_groups']:,} groups have identical "
            "V1–V28 and Amount and differ only in `Time`. They are kept, but each group is assigned to a "
            f"single split ({ds['groups_spanning_splits']} groups span more than one split), so no "
            "feature vector is trained on and then tested.",
        ]
    lines += [
        "",
        "## Model comparison",
        "",
        "Each row is the best hyper-parameter configuration of a model × imbalance strategy, "
        "selected by **validation AUC-PR**. The alert threshold of each row maximises F1 on the "
        "validation split; precision, recall and F1 are measured on the **test split** at that threshold.",
        "",
        "| Model | Imbalance strategy | Params | Val AUC-PR | Test AUC-PR | Test ROC-AUC | Precision | Recall | F1 | Threshold |",
        "|---|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in metrics["comparison"]:
        mark = "**" if row["display_name"] == best["display_name"] else ""
        params = ", ".join(f"{k}={v}" for k, v in row["params"].items())
        test = row["test"]
        lines.append(
            f"| {mark}{row['model']}{mark} | {row['strategy'].replace('_', ' ')} | {params} | "
            f"{_fmt(row['val']['auc_pr'])} | {mark}{_fmt(test['auc_pr'])}{mark} | {_fmt(test['roc_auc'])} | "
            f"{_fmt(test['precision'])} | {_fmt(test['recall'])} | {_fmt(test['f1'])} | {_fmt(row['threshold'], 3)} |"
        )
    if all("ci95" in row["test"] for row in metrics["comparison"]):
        lines += [
            "",
            "### Uncertainty: 95% bootstrap confidence intervals (test split)",
            "",
            "The test transactions are resampled 1,000 times with the model and threshold fixed. With "
            "only ~70 frauds, intervals are wide; models whose intervals overlap cannot be "
            "reliably ranked.",
            "",
            "| Model | Imbalance strategy | Average precision | Precision | Recall | F1 |",
            "|---|---|---|---|---|---|",
        ]
        for row in metrics["comparison"]:
            ci = row["test"]["ci95"]

            def _ci(name: str, ci: dict[str, list[float]] = ci) -> str:
                low, high = ci.get(name, [float("nan"), float("nan")])
                return f"{low:.3f} – {high:.3f}"

            lines.append(
                f"| {row['model']} | {row['strategy'].replace('_', ' ')} | {_ci('average_precision')} | "
                f"{_ci('precision')} | {_ci('recall')} | {_ci('f1')} |"
            )
    conf = best["test"]["confusion"]
    lines += [
        "",
        f"## Selected model: {best['display_name']}",
        "",
        f"* Hyper-parameters: `{best['params']}`",
        f"* Alert threshold (max F1 on validation): **{best['threshold']:.4f}**",
        f"* Test AUC-PR **{_fmt(best['test']['auc_pr'])}**, ROC-AUC {_fmt(best['test']['roc_auc'])}, "
        f"average precision {_fmt(best['test']['average_precision'])}",
        f"* Test precision {_fmt(best['test']['precision'])}, recall {_fmt(best['test']['recall'])}, "
        f"F1 {_fmt(best['test']['f1'])}",
        "",
        "| | Predicted legitimate | Predicted fraud |",
        "|---|---:|---:|",
        f"| **Actual legitimate** | {conf['tn']:,} (TN) | {conf['fp']:,} (FP) |",
        f"| **Actual fraud** | {conf['fn']:,} (FN) | {conf['tp']:,} (TP) |",
        "",
        "## All grid candidates (validation)",
        "",
        "| Model | Strategy | Params | Val AUC-PR | Val ROC-AUC | Fit time (s) |",
        "|---|---|---|---:|---:|---:|",
    ]
    for cand in metrics["candidates"]:
        params = ", ".join(f"{k}={v}" for k, v in cand["params"].items())
        lines.append(
            f"| {cand['model']} | {cand['strategy'].replace('_', ' ')} | {params} | "
            f"{_fmt(cand['val_auc_pr'])} | {_fmt(cand['val_roc_auc'])} | {cand['train_seconds']:.1f} |"
        )
    lines += [
        "",
        "## Methodology notes",
        "",
        "* Exact duplicate rows are removed **before** the split. Rows that differ only in `Time` "
        "are kept, but grouped, so no identical feature vector appears in both training and test data.",
        "* Group-aware, exact stratified 70/15/15 split (rows ranked within each class by a seeded hash "
        "of the model inputs). The split is random, not time-ordered. The test split is **never used "
        "for model selection or threshold tuning**, and it is the only data replayed by the producer.",
        "* Imbalance: *class weights* = balanced weights n/(2·n_class) on the full training "
        f"split; *undersampling* = all frauds + legitimate rows sampled at 1:{metrics['undersample_ratio']:g}.",
        "* **Selection noise:** the model, its hyper-parameters and the threshold are all chosen on a "
        "single validation split with ~71 frauds. Where the confidence intervals above overlap, the "
        "ranking is not statistically conclusive. k-fold cross-validation would be more robust; it was "
        "not used, to keep the agreed train/validation/test design and a training time of a few minutes.",
        "* **Calibration:** only the Linear SVM is calibrated (Platt scaling fitted on the validation "
        "split, which makes its tuned threshold slightly optimistic). The other models' outputs are "
        "shifted by class weights or undersampling. The dashboard therefore shows a *fraud score*: it "
        "ranks transactions correctly, but 0.39 does not mean a 39% chance of fraud, and thresholds "
        "are not comparable across models.",
        "* AUC-PR is MLlib's `areaUnderPR` computed exactly (`numBins=0`; the default 1,000-bin "
        "down-sampling distorts AUC-PR when there are only ~70 frauds among ~42k rows). "
        "Average precision is scikit-learn's step-wise estimate, reported as a cross-check.",
        "",
        "## Figures",
        "",
        "![Precision-Recall curves](figures/pr_curves.png)",
        "",
        "![ROC curves](figures/roc_curves.png)",
        "",
        "![Model comparison](figures/model_comparison.png)",
        "",
        "![Threshold sweep](figures/threshold_sweep.png)",
        "",
        "![Confusion matrix](figures/confusion_matrix.png)",
        "",
    ]
    return "\n".join(lines)


def write_results_markdown(metrics: Mapping[str, Any], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_results_markdown(metrics), encoding="utf-8")
    return path
