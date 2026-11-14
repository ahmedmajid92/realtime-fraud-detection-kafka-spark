"""Number formatting and table preparation for the dashboard (pure functions)."""

from __future__ import annotations

import math
from typing import Any

import pandas as pd

from src.dashboard.helpers.i18n import t

DASH = "—"
ALERT_ICON = "🚨"
OK_ICON = "✓"
# Status "critical" (#d03b3b) as a translucent wash: readable on light and dark themes.
ALERT_ROW_CSS = "background-color: rgba(208, 59, 59, 0.16); font-weight: 600;"


def _missing(value: Any) -> bool:
    return value is None or (isinstance(value, float) and not math.isfinite(value))


def fmt_int(value: Any) -> str:
    return DASH if _missing(value) else f"{int(value):,}"


def fmt_pct(value: Any, digits: int = 3) -> str:
    return DASH if _missing(value) else f"{float(value) * 100:.{digits}f}%"


def fmt_ms(value: Any) -> str:
    return DASH if _missing(value) else f"{float(value):,.0f} ms"


def fmt_rate(value: Any) -> str:
    return DASH if _missing(value) else f"{float(value):,.1f} tx/s"


def fmt_prob(value: Any) -> str:
    return DASH if _missing(value) else f"{float(value):.3f}"


def fmt_seconds_ago(value: Any, lang: str) -> str:
    if _missing(value):
        return DASH
    return t("common.seconds_ago", lang, n=f"{float(value):,.0f}")


def fmt_bytes(value: Any) -> str:
    if _missing(value):
        return DASH
    size = float(value)
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024 or unit == "GB":
            return f"{size:,.1f} {unit}"
        size /= 1024
    return DASH  # pragma: no cover


def short_id(value: Any) -> str:
    """First 8 characters of a UUID (enough to tell rows apart on screen)."""
    return DASH if value is None else str(value)[:8]


def class_label(actual_class: Any, lang: str) -> str:
    if actual_class is None or (isinstance(actual_class, float) and math.isnan(actual_class)):
        return DASH
    return t("class.fraud", lang) if int(actual_class) == 1 else t("class.legit", lang)


def decision_label(flagged: bool, lang: str) -> str:
    """Icon + text, so the decision never relies on colour alone."""
    return (
        f"{ALERT_ICON} {t('decision.fraud', lang)}"
        if flagged
        else f"{OK_ICON} {t('decision.ok', lang)}"
    )


def source_label(source: Any, lang: str) -> str:
    return t(f"source.{source}", lang) if source in {"replay", "manual", "e2e"} else str(source)


def prediction_table(frame: pd.DataFrame, threshold: float, lang: str) -> pd.DataFrame:
    """Display-ready predictions table (localised headers, decision at the live threshold)."""
    columns = [
        t("table.time", lang),
        t("table.id", lang),
        t("table.decision", lang),
        t("table.probability", lang),
        t("table.amount", lang),
        t("table.hour", lang),
        t("table.actual", lang),
        t("table.source", lang),
        t("table.latency", lang),
    ]
    if frame.empty:
        return pd.DataFrame(columns=columns)
    flagged = frame["fraud_probability"].astype(float) >= threshold
    processed = pd.to_datetime(frame["processed_at"], utc=True)
    table = pd.DataFrame(
        {
            columns[0]: processed.dt.strftime("%H:%M:%S"),
            columns[1]: frame["transaction_id"].map(short_id),
            columns[2]: [decision_label(f, lang) for f in flagged],
            columns[3]: frame["fraud_probability"].map(fmt_prob),
            columns[4]: frame["amount"].map(lambda v: f"{float(v):,.2f}"),
            columns[5]: frame["hour"].map(lambda v: f"{int(v):02d}:00"),
            columns[6]: frame["actual_class"].map(lambda v: class_label(v, lang)),
            columns[7]: frame["source"].map(lambda v: source_label(v, lang)),
            columns[8]: frame["latency_ms"].map(fmt_ms),
        }
    )
    return table.reset_index(drop=True)


def highlight_flagged(table: pd.DataFrame, lang: str) -> Any:
    """pandas Styler that tints rows whose decision is fraud."""
    decision_col = t("table.decision", lang)
    fraud_text = t("decision.fraud", lang)

    def _row_style(row: pd.Series) -> list[str]:
        is_alert = fraud_text in str(row.get(decision_col, ""))
        return [ALERT_ROW_CSS if is_alert else "" for _ in row]

    return table.style.apply(_row_style, axis=1)
