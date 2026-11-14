"""Report assets: Markdown results and PNG figures."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.training import report


def test_results_markdown_contains_all_rows(fake_metrics: dict[str, Any]) -> None:
    markdown = report.render_results_markdown(fake_metrics)
    assert markdown.startswith("# Model Results")
    for row in fake_metrics["comparison"]:
        assert row["model"] in markdown
    best = fake_metrics["best"]
    assert f"## Selected model: {best['display_name']}" in markdown
    assert f"**{best['model']}**" in markdown  # best row is bold
    assert "| **Actual fraud** | 20 (FN) | 50 (TP) |" in markdown
    # once in the comparison table, once in the all-candidates table
    assert markdown.count("| Logistic Regression | class weights") == 2
    assert "![Precision-Recall curves](figures/pr_curves.png)" in markdown
    assert "1:10" in markdown


def test_write_results_markdown(fake_metrics: dict[str, Any], tmp_path: Path) -> None:
    path = report.write_results_markdown(fake_metrics, tmp_path / "docs" / "RESULTS.md")
    assert path.read_text(encoding="utf-8").startswith("# Model Results")


def test_write_figures_creates_pngs(fake_metrics: dict[str, Any], tmp_path: Path) -> None:
    written = report.write_figures(fake_metrics, tmp_path / "figures")
    assert set(written) == set(report.FIGURE_FILES)
    for path in written.values():
        assert path.is_file()
        assert path.read_bytes()[:8] == b"\x89PNG\r\n\x1a\n"
        assert path.stat().st_size > 5_000


def test_model_colours_follow_fixed_slot_order() -> None:
    assert report.MODEL_COLORS == {
        "lr": "#2a78d6",
        "rf": "#eb6834",
        "gbt": "#1baf7a",
        "svm": "#eda100",
    }
