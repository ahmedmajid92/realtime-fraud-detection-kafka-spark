"""System architecture as a Graphviz DOT diagram (bilingual, pure function).

Used by the About page (``st.graphviz_chart`` renders it as vector graphics in the browser,
so it stays sharp at any size) and by ``scripts/render_architecture.py``, which produces
``docs/figures/architecture.{svg,png}`` for the README and the report.

Layout: five narrow rows, so the diagram stays legible at page width instead of being
one long strip. A ``|`` in a subtitle starts a new line.

    row 1  dataset → prepare → train & select              (offline, Spark MLlib)
    row 2  test split · saved model
    row 3  producer → transactions → Spark streaming
    row 4  producer-control · predictions · fraud-alerts · PostgreSQL
    row 5  Kafka UI · dashboard
"""

from __future__ import annotations

from html import escape
from itertools import pairwise

from src.dashboard.helpers.i18n import t

# Layer colours: light tints of the validated categorical palette (fill, border).
LAYERS: dict[str, tuple[str, str]] = {
    "offline": ("#e7f0fc", "#2a78d6"),  # blue: offline training (Spark MLlib)
    "kafka": ("#fdeee6", "#eb6834"),  # orange: Kafka topics
    "spark": ("#e2f5ee", "#1baf7a"),  # aqua: Spark Structured Streaming
    "store": ("#f3f2ef", "#898781"),  # neutral: files and databases
    "ui": ("#fdf4dc", "#c98500"),  # yellow: user interfaces
}
INK = "#0b0b0b"
INK_SECONDARY = "#52514e"
MUTED = "#898781"  # edges and edge labels: readable on light and dark backgrounds

# node id → (layer, shape, title key, subtitle key)
NODES: dict[str, tuple[str, str, str, str]] = {
    "csv": ("store", "cylinder", "arch.csv", "arch.csv.sub"),
    "prep": ("offline", "box", "arch.prep", "arch.prep.sub"),
    "train": ("offline", "box", "arch.train", "arch.train.sub"),
    "model": ("store", "cylinder", "arch.model", "arch.model.sub"),
    "test": ("store", "cylinder", "arch.test", "arch.test.sub"),
    "producer": ("spark", "box", "arch.producer", "arch.producer.sub"),
    "tx": ("kafka", "box", "arch.tx", "arch.topic.sub3"),
    "spark": ("spark", "box", "arch.spark", "arch.spark.sub"),
    "ctrl": ("kafka", "box", "arch.ctrl", "arch.ctrl.sub"),
    "pred": ("kafka", "box", "arch.pred", "arch.topic.sub3"),
    "alert": ("kafka", "box", "arch.alert", "arch.topic.sub1"),
    "pg": ("store", "cylinder", "arch.pg", "arch.pg.sub"),
    "kui": ("ui", "box", "arch.kui", "arch.kui.sub"),
    "dash": ("ui", "box", "arch.dash", "arch.dash.sub"),
}

ROWS: tuple[tuple[str, ...], ...] = (
    ("csv", "prep", "train"),
    ("test", "model"),
    ("producer", "tx", "spark"),
    ("ctrl", "pred", "alert", "pg"),
    ("kui", "dash"),
)

# (source, target, label key or "", style) — style: "data" | "control" | "layout"
EDGES: tuple[tuple[str, str, str, str], ...] = (
    ("csv", "prep", "", "data"),
    ("prep", "train", "arch.e.train", "data"),
    ("train", "model", "arch.e.best", "data"),
    ("prep", "test", "arch.e.held_out", "data"),
    ("test", "producer", "", "data"),
    ("producer", "tx", "arch.e.json", "data"),
    ("tx", "spark", "", "data"),
    ("model", "spark", "arch.e.load", "data"),
    ("spark", "pred", "arch.e.all_rows", "data"),
    ("spark", "alert", "arch.e.flagged", "data"),
    ("spark", "pg", "arch.e.upsert", "data"),
    ("pg", "dash", "arch.e.queries", "data"),
    ("dash", "pg", "arch.e.slider", "control"),
    ("pg", "spark", "arch.e.threshold", "control"),
    ("dash", "ctrl", "arch.e.commands", "control"),
    ("ctrl", "producer", "", "control"),
    ("dash", "tx", "arch.e.manual", "control"),
    ("kui", "pred", "arch.e.browse", "control"),
)


def _label(node_id: str, lang: str) -> str:
    _, _, title_key, subtitle_key = NODES[node_id]
    title = escape(t(title_key, lang))
    subtitle_lines = [escape(part.strip()) for part in t(subtitle_key, lang).split("|")]
    # One table row per line: the cell padding keeps tall Arabic glyphs from touching the
    # line above (plain <BR/> line breaks are too tight for Arabic script).
    rows = [f'<TR><TD><FONT POINT-SIZE="16" COLOR="{INK}"><B>{title}</B></FONT></TD></TR>']
    rows += [
        f'<TR><TD><FONT POINT-SIZE="12" COLOR="{INK_SECONDARY}">{line}</FONT></TD></TR>'
        for line in subtitle_lines
    ]
    return (
        '<<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="2">'
        + "".join(rows)
        + "</TABLE>>"
    )


def architecture_dot(lang: str = "en") -> str:
    """The architecture diagram as a DOT string, with labels in ``lang``."""
    lines = [
        "digraph architecture {",
        '  graph [rankdir=TB, bgcolor="transparent", nodesep=0.45, ranksep=0.65, '
        'pad=0.25, newrank=true, fontname="Helvetica"];',
        f'  node [fontname="Helvetica", margin="0.18,0.10", penwidth=1.6, color="{MUTED}"];',
        f'  edge [fontname="Helvetica", fontsize=11, color="{MUTED}", '
        f'fontcolor="{MUTED}", arrowsize=0.8, penwidth=1.4];',
    ]
    for node_id, (layer, shape, _, _) in NODES.items():
        fill, border = LAYERS[layer]
        style = '"rounded,filled"' if shape == "box" else "filled"
        lines.append(
            f'  {node_id} [shape={shape}, style={style}, fillcolor="{fill}", '
            f'color="{border}", label={_label(node_id, lang)}];'
        )
    for row in ROWS:
        lines.append(f"  {{ rank=same; {'; '.join(row)}; }}")
        # Invisible edges keep each row in left-to-right order.
        for left, right in pairwise(row):
            lines.append(f"  {left} -> {right} [style=invis, weight=4];")
    for source, target, label_key, kind in EDGES:
        attrs = []
        if label_key:
            attrs.append(f'label="  {escape(t(label_key, lang), quote=True)}  "')
        if kind == "control":
            attrs += ["style=dashed", "constraint=false"]
        lines.append(f"  {source} -> {target} [{', '.join(attrs)}];")
    lines.append("}")
    return "\n".join(lines)
