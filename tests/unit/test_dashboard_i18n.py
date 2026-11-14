"""Translations: both languages complete, placeholders consistent, every used key defined."""

from __future__ import annotations

import re
import string
from pathlib import Path

import pytest

from src.dashboard.helpers import i18n

DASHBOARD_DIR = Path(__file__).resolve().parents[2] / "src" / "dashboard"
KEY_PATTERN = re.compile(r"""\bt\(\s*["']([a-z][a-z0-9_.]*)["']""")
DYNAMIC_KEYS = (
    [f"model.{k}" for k in ("lr", "rf", "gbt", "svm")]
    + [f"strategy.{s}" for s in ("class_weights", "undersampling")]
    + [f"source.{s}" for s in ("replay", "manual", "e2e")]
    + [f"sidebar.action.{a}" for a in ("pause", "resume", "set_rate")]
    + [
        f"about.comp.{c}.{part}"
        for c in ("trainer", "producer", "kafka", "spark", "postgres", "dashboard", "kafka_ui")
        for part in ("name", "desc")
    ]
)


def _placeholders(text: str) -> set[str]:
    return {field for _, field, _, _ in string.Formatter().parse(text) if field}


def test_both_languages_define_the_same_keys() -> None:
    assert set(i18n.EN) == set(i18n.AR)


@pytest.mark.parametrize("key", sorted(i18n.EN))
def test_placeholders_match_between_languages(key: str) -> None:
    assert _placeholders(i18n.EN[key]) == _placeholders(i18n.AR[key]), key


def test_every_key_used_in_code_is_defined() -> None:
    used: set[str] = set()
    for path in DASHBOARD_DIR.rglob("*.py"):
        used |= set(KEY_PATTERN.findall(path.read_text(encoding="utf-8")))
    used |= set(DYNAMIC_KEYS)
    assert len(used) > 150  # sanity check that the scan found the call sites
    missing = sorted(key for key in used if key not in i18n.EN)
    assert missing == []


def test_arabic_strings_are_really_translated() -> None:
    arabic = re.compile(r"[؀-ۿ]")
    untranslated = [k for k, v in i18n.AR.items() if not arabic.search(v) and v == i18n.EN[k]]
    # only technical labels that are identical in both languages may stay in Latin script
    assert set(untranslated) <= {
        "metric.f1",
        "metric.auc_pr",
        "metric.roc_auc",
        "perf.tab.roc",
        "perf.col.f1",
        "health.spark",
        "about.comp.spark.name",
        "about.comp.postgres.name",
    }


def test_translate_with_fallbacks_and_params() -> None:
    assert i18n.t("nav.live", "en") == "Live Monitor"
    assert i18n.t("nav.live", "ar") == "المراقبة المباشرة"
    assert i18n.t("nav.live", "fr") == "Live Monitor"  # unknown language → English
    assert i18n.t("no.such.key", "ar") == "no.such.key"
    assert i18n.t("common.seconds_ago", "en", n="5") == "5 s ago"


def test_rtl_helpers() -> None:
    assert i18n.is_rtl("ar") and not i18n.is_rtl("en")
    assert "direction: rtl" in i18n.RTL_CSS
    assert set(i18n.LANGUAGES) == {"en", "ar"}
