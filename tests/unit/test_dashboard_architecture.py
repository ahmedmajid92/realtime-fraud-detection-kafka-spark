"""Architecture diagram (Graphviz DOT) used by the About page and docs/figures."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from src.dashboard.helpers import architecture, i18n


def test_every_label_key_exists_in_both_languages() -> None:
    keys = {k for _, _, title, sub in architecture.NODES.values() for k in (title, sub)}
    keys |= {label for _, _, label, _ in architecture.EDGES if label}
    assert keys <= set(i18n.EN) and keys <= set(i18n.AR)


def test_dot_contains_every_node_and_edge() -> None:
    dot = architecture.architecture_dot("en")
    assert dot.startswith("digraph architecture {") and dot.rstrip().endswith("}")
    for node_id in architecture.NODES:
        assert re.search(rf"^  {node_id} \[shape=", dot, re.MULTILINE), node_id
    for source, target, _, _ in architecture.EDGES:
        assert f"  {source} -> {target} [" in dot
    for row in architecture.ROWS:
        assert f"{{ rank=same; {'; '.join(row)}; }}" in dot
    assert 'bgcolor="transparent"' in dot  # blends into light and dark Streamlit themes


def test_control_edges_are_dashed_and_do_not_affect_layout() -> None:
    dot = architecture.architecture_dot("en")
    control = [e for e in architecture.EDGES if e[3] == "control"]
    assert control
    for source, target, _, _ in control:
        line = next(
            line
            for line in dot.splitlines()
            if line.startswith(f"  {source} -> {target} [") and "invis" not in line
        )
        assert "style=dashed" in line and "constraint=false" in line


def test_labels_follow_the_language() -> None:
    english = architecture.architecture_dot("en")
    arabic = architecture.architecture_dot("ar")
    assert "Prepare data" in english and "تجهيز البيانات" in arabic
    assert "Prepare data" not in arabic
    assert english.count("<B>") == len(architecture.NODES)


def test_label_text_is_html_escaped(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(i18n.EN, "arch.csv", "a <b> & c")
    dot = architecture.architecture_dot("en")
    assert "a &lt;b&gt; &amp; c" in dot


def test_render_script_writes_dot_and_images(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from scripts import render_architecture

    calls: list[list[str]] = []
    monkeypatch.setattr(render_architecture, "OUT_DIR", tmp_path)
    monkeypatch.setattr(render_architecture.subprocess, "run", lambda cmd, check: calls.append(cmd))
    assert render_architecture.main() == 0
    assert (tmp_path / "architecture.dot").is_file() and (
        tmp_path / "architecture-ar.dot"
    ).is_file()
    assert 'bgcolor="white"' in (tmp_path / "architecture.dot").read_text(encoding="utf-8")
    assert sorted(cmd[1] for cmd in calls) == ["-Tpng", "-Tpng", "-Tsvg", "-Tsvg"]
    assert all(cmd[0] == "dot" for cmd in calls)
