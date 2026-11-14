"""Dashboard pure helpers: KPIs, formatting, test-page samples, metrics.json tables."""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest

from src.common.schemas import RAW_INPUT_COLUMNS, Source
from src.dashboard.helpers import formatting, kpis, metrics_file, samples, thresholds
from tests.conftest import make_transactions


# --------------------------------------------------------------------------- KPIs
def test_safe_ratio() -> None:
    assert kpis.safe_ratio(1, 4) == 0.25
    assert kpis.safe_ratio(3, 0) == 0.0


@pytest.mark.parametrize(
    ("count", "window", "uptime", "expected"),
    [
        (300, 60, 600, 5.0),  # steady state: count / window
        (100, 60, 20, 5.0),  # just started: count / uptime
        (3, 60, 0.2, 3.0),  # first second: span is at least 1 s
        (120, 60, None, 2.0),  # no uptime known
    ],
)
def test_throughput(count: int, window: float, uptime: float | None, expected: float) -> None:
    assert kpis.throughput(count, window, uptime) == pytest.approx(expected)


def test_build_kpi_snapshot() -> None:
    snapshot = kpis.build_kpi_snapshot(
        {
            "total": 1000,
            "flagged": 2,
            "window_count": 600,
            "avg_latency_ms": 812.5,
            "p95_latency_ms": None,
            "uptime_s": 3600.0,
            "idle_s": 0.4,
        },
        window_s=60,
    )
    assert snapshot.fraud_rate == 0.002
    assert snapshot.throughput == 10.0
    assert snapshot.avg_latency_ms == 812.5
    assert snapshot.p95_latency_ms is None
    empty = kpis.build_kpi_snapshot({}, 60)
    assert (empty.total, empty.flagged, empty.fraud_rate, empty.throughput) == (0, 0, 0.0, 0.0)
    assert kpis.build_kpi_snapshot({"avg_latency_ms": float("nan")}, 60).avg_latency_ms is None


def test_quality_metrics() -> None:
    quality = kpis.quality_metrics({"tp": 8, "fp": 2, "tn": 980, "fn": 2})
    assert quality["precision"] == pytest.approx(0.8)
    assert quality["recall"] == pytest.approx(0.8)
    assert quality["f1"] == pytest.approx(0.8)
    assert kpis.quality_metrics({})["f1"] == 0.0


def test_relabel() -> None:
    frame = pd.DataFrame({"fraud_probability": [0.1, 0.5, 0.9]})
    assert kpis.relabel(frame, 0.5)["flagged"].tolist() == [False, True, True]
    assert "flagged" not in frame.columns  # input untouched
    assert kpis.relabel(pd.DataFrame({"x": [1]}), 0.5)["flagged"].isna().all()


def test_offsets_and_lag() -> None:
    assert kpis.total_offset({0: 10, 1: 5}) == 15
    assert kpis.consumer_lag({0: 10, 1: 5}, {"0": 7, "1": 5}) == 3
    assert kpis.consumer_lag({0: 10}, {"0": 12}) == 0  # never negative
    assert kpis.consumer_lag({0: 10, 1: 4}, {"0": 10}) == 4  # unreported partition
    assert kpis.consumer_lag({0: 10}, None) is None


def test_rate_between() -> None:
    assert kpis.rate_between(None, (1.0, 100)) is None
    assert kpis.rate_between((0.0, 100), (2.0, 200)) == 50.0
    assert kpis.rate_between((2.0, 100), (2.0, 200)) is None
    assert kpis.rate_between((0.0, 200), (1.0, 100)) == 0.0  # topic recreated


def test_commanded_state() -> None:
    assert kpis.commanded_state(None, 50) == (False, 50.0)
    assert kpis.commanded_state({"paused": True, "rate": 20}, 50) == (True, 20.0)


# --------------------------------------------------------------------------- formatting
@pytest.mark.parametrize(
    ("func", "value", "expected"),
    [
        (formatting.fmt_int, 12345, "12,345"),
        (formatting.fmt_int, None, "—"),
        (formatting.fmt_pct, 0.00172, "0.172%"),
        (formatting.fmt_pct, float("nan"), "—"),
        (formatting.fmt_ms, 1234.4, "1,234 ms"),
        (formatting.fmt_rate, 49.96, "50.0 tx/s"),
        (formatting.fmt_prob, 0.97314, "0.973"),
        (formatting.fmt_bytes, 512, "512.0 B"),
        (formatting.fmt_bytes, 5 * 1024 * 1024, "5.0 MB"),
        (formatting.fmt_bytes, None, "—"),
        (formatting.short_id, "9b2f6c1e-4a7d", "9b2f6c1e"),
        (formatting.short_id, None, "—"),
    ],
)
def test_formatters(func: Any, value: Any, expected: str) -> None:
    assert func(value) == expected


def test_language_dependent_labels() -> None:
    assert formatting.fmt_seconds_ago(3.2, "en") == "3 s ago"
    assert formatting.fmt_seconds_ago(None, "en") == "—"
    assert formatting.class_label(1, "en") == "Fraud"
    assert formatting.class_label(0, "ar") == "سليمة"
    assert formatting.class_label(math.nan, "en") == "—"
    assert formatting.decision_label(True, "en") == "🚨 FRAUD"
    assert formatting.decision_label(False, "en") == "✓ OK"
    assert formatting.source_label("manual", "en") == "Manual test"
    assert formatting.source_label("other", "en") == "other"


def _feed() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "transaction_id": ["aaaaaaaa-1", "bbbbbbbb-2"],
            "processed_at": [datetime(2026, 11, 14, 18, 0, 1, tzinfo=UTC)] * 2,
            "source": ["replay", "manual"],
            "amount": [12.5, 999.0],
            "hour": [3, 23],
            "fraud_probability": [0.05, 0.95],
            "actual_class": [0, None],
            "latency_ms": [800.0, 1200.0],
        }
    )


def test_prediction_table_english() -> None:
    table = formatting.prediction_table(_feed(), threshold=0.5, lang="en")
    assert list(table.columns)[:3] == ["Time", "ID", "Decision"]
    assert table["Decision"].tolist() == ["✓ OK", "🚨 FRAUD"]
    assert table["Hour"].tolist() == ["03:00", "23:00"]
    assert table["Actual"].tolist() == ["Legitimate", "—"]
    assert table["Time"].tolist() == ["18:00:01", "18:00:01"]


def test_prediction_table_arabic_and_empty() -> None:
    table = formatting.prediction_table(_feed(), threshold=0.99, lang="ar")
    assert "القرار" in table.columns
    assert all("احتيال" not in value for value in table["القرار"])  # nothing above 0.99
    empty = formatting.prediction_table(_feed().iloc[0:0], 0.5, "en")
    assert empty.empty and "Decision" in empty.columns


def test_highlight_flagged_marks_only_fraud_rows() -> None:
    html = formatting.highlight_flagged(
        formatting.prediction_table(_feed(), 0.5, "en"), "en"
    ).to_html()
    assert html.count("rgba(208, 59, 59, 0.16)") >= 1
    clean = formatting.highlight_flagged(
        formatting.prediction_table(_feed(), 0.99, "en"), "en"
    ).to_html()
    assert "rgba(208, 59, 59" not in clean


# --------------------------------------------------------------------------- samples
def test_load_samples(tmp_path: Path) -> None:
    assert samples.load_samples(tmp_path / "missing.parquet") is None
    path = tmp_path / "test.parquet"
    make_transactions(10, 2).to_parquet(path, index=False)
    loaded = samples.load_samples(path)
    assert loaded is not None and len(loaded) == 12 and "Class" in loaded.columns


def test_pick_random_respects_class() -> None:
    frame = make_transactions(20, 3)
    rng = np.random.default_rng(0)
    fraud = samples.pick_random(frame, True, rng)
    legit = samples.pick_random(frame, False, rng)
    assert fraud["Class"] == 1 and legit["Class"] == 0
    assert set(RAW_INPUT_COLUMNS) <= set(fraud)
    with pytest.raises(ValueError):
        samples.pick_random(make_transactions(5, 0), True, rng)


def test_hour_helpers() -> None:
    time_s = 86_400 + 5 * 3600 + 125.0  # day 2, 05:02:05
    assert samples.hour_of(time_s) == 5
    moved = samples.with_hour(time_s, 23)
    assert samples.hour_of(moved) == 23
    assert moved // 86_400 == time_s // 86_400  # same day
    assert moved % 3600 == time_s % 3600  # same minutes/seconds
    with pytest.raises(ValueError):
        samples.with_hour(time_s, 24)


def test_apply_edits_and_detection() -> None:
    original = samples.pick_random(make_transactions(5, 2), True, np.random.default_rng(1))
    unchanged = samples.apply_edits(
        original, amount=original["Amount"], hour=samples.hour_of(original["Time"])
    )
    assert not samples.is_edited(original, unchanged)
    edited = samples.apply_edits(
        original,
        amount=1.0,
        hour=(samples.hour_of(original["Time"]) + 1) % 24,
        v_values={"V3": 9.0},
    )
    assert edited["Amount"] == 1.0 and edited["V3"] == 9.0
    assert edited["Time"] != original["Time"]
    assert samples.is_edited(original, edited)


def test_build_manual_event() -> None:
    features = {column: 1.0 for column in RAW_INPUT_COLUMNS}
    now = datetime(2026, 11, 14, tzinfo=UTC)
    event = samples.build_manual_event(features, None, now)
    assert event.source is Source.MANUAL and event.replay_pass == 0 and event.actual_class is None
    e2e = samples.build_manual_event(features, 1, now, source=Source.E2E)
    assert e2e.source is Source.E2E and e2e.actual_class == 1


# --------------------------------------------------------------------------- metrics.json tables
def test_load_metrics(tmp_path: Path, fake_metrics: dict[str, Any]) -> None:
    assert metrics_file.load_metrics(tmp_path / "missing.json") is None
    broken = tmp_path / "broken.json"
    broken.write_text("{", encoding="utf-8")
    assert metrics_file.load_metrics(broken) is None
    good = tmp_path / "metrics.json"
    good.write_text(json.dumps(fake_metrics), encoding="utf-8")
    assert metrics_file.load_metrics(good)["run_id"] == fake_metrics["run_id"]  # type: ignore[index]


def test_comparison_and_candidate_frames(fake_metrics: dict[str, Any]) -> None:
    table = metrics_file.comparison_frame(fake_metrics, "en")
    assert len(table) == 8
    assert table.columns[0] == "Model"
    assert table["Imbalance strategy"].tolist()[:2] == ["class weights", "undersampling"]
    arabic = metrics_file.comparison_frame(fake_metrics, "ar")
    assert "النموذج" in arabic.columns
    assert arabic.iloc[0, 0] == "الانحدار اللوجستي"
    candidates = metrics_file.candidates_frame(fake_metrics, "en")
    assert len(candidates) == 8 and "Fit time (s)" in candidates.columns


def test_dataset_frame_and_labels(fake_metrics: dict[str, Any]) -> None:
    frame = metrics_file.dataset_frame(fake_metrics, "en")
    assert frame["Set"].tolist()[0] == "Raw CSV"
    assert frame["Rows"].tolist()[0] == "1,000"
    assert frame["Fraud rate"].tolist()[0] == "2.0000%"
    assert metrics_file.row_label(fake_metrics["best"], "en") == "Linear SVM · undersampling"


# --------------------------------------------------------------------------- thresholds (review R7)
@pytest.mark.parametrize(
    ("value", "expected"), [(0.0, 0.001), (0.5, 0.5), (0.9995, 0.999), (2.0, 0.999)]
)
def test_clamp_threshold(value: float, expected: float) -> None:
    assert thresholds.clamp_threshold(value) == expected


def test_resolve_threshold_prefers_the_value_spark_uses() -> None:
    assert thresholds.resolve_threshold(0.3863, 0.5, 0.4) == 0.3863  # unrounded DB value wins
    assert thresholds.resolve_threshold(None, 0.5, 0.4) == 0.5
    assert thresholds.resolve_threshold(None, None, 0.4) == 0.4
    assert thresholds.resolve_threshold(None, None, None) == thresholds.FALLBACK_THRESHOLD
    assert thresholds.resolve_threshold(0.9999, None, None) == 0.999  # never breaks the slider


def test_slider_update_only_when_shared_value_changed() -> None:
    assert thresholds.slider_update(None, 0.4) is None  # database down: keep the slider
    assert thresholds.slider_update(0.4, 0.4) is None  # unchanged: keep the user's position
    assert thresholds.slider_update(0.6, 0.4) == 0.6  # changed elsewhere: follow it
    assert thresholds.slider_update(0.6, None) == 0.6  # first sync


def test_control_state_unknown_is_not_treated_as_defaults() -> None:
    assert kpis.control_known({}) and kpis.control_known(None)
    assert not kpis.control_known(kpis.CONTROL_UNKNOWN)
    assert kpis.commanded_state(kpis.CONTROL_UNKNOWN, 50) == (False, 50.0)


def test_comparison_frame_shows_confidence_interval(fake_metrics: dict[str, Any]) -> None:
    table = metrics_file.comparison_frame(fake_metrics, "en")
    assert table["Test AP 95% CI"].iloc[0] == "0.400–0.550"
    fake_metrics["comparison"][0]["test"].pop("ci95")
    assert metrics_file.comparison_frame(fake_metrics, "en")["Test AP 95% CI"].iloc[0] == "—"
