"""Render the architecture diagram to docs/figures (run with ``.\\run.ps1 diagrams``).

Uses the same DOT source as the dashboard's About page
(:func:`src.dashboard.helpers.architecture.architecture_dot`), so the README, the report
and the dashboard always show the same diagram. Needs the Graphviz ``dot`` binary; the
``diagrams`` Compose service provides it.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from src.dashboard.helpers.architecture import architecture_dot

OUT_DIR = Path(os.environ.get("DIAGRAM_DIR", "docs/figures"))


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    for lang in ("en", "ar"):
        suffix = "" if lang == "en" else "-ar"
        source = OUT_DIR / f"architecture{suffix}.dot"
        # The PNG/SVG get a white background so they read well in documents.
        dot = architecture_dot(lang).replace('bgcolor="transparent"', 'bgcolor="white"')
        source.write_text(dot, encoding="utf-8")
        for fmt, extra in (("svg", []), ("png", ["-Gdpi=220"])):
            target = OUT_DIR / f"architecture{suffix}.{fmt}"
            subprocess.run(["dot", f"-T{fmt}", *extra, str(source), "-o", str(target)], check=True)
            print(f"saved {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
