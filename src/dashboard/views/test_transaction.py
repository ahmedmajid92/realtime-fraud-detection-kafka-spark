"""Test a Transaction: send a (possibly edited) test-split sample through Kafka, show the result."""

from __future__ import annotations

import time
from typing import Any

import numpy as np
import streamlit as st

from src.common.schemas import AMOUNT_COLUMN, LABEL_COLUMN, TIME_COLUMN, V_COLUMNS, utc_now
from src.dashboard import resources as res
from src.dashboard.helpers import charts, kafka_io, queries, samples
from src.dashboard.helpers.formatting import (
    class_label,
    decision_label,
    fmt_ms,
    highlight_flagged,
    prediction_table,
)
from src.dashboard.helpers.i18n import t

SAMPLE_KEY = "test_sample"
NONCE_KEY = "test_nonce"
RESULT_KEY = "test_result"
POLL_INTERVAL_S = 0.5


def _pick(fraud: bool) -> None:
    frame = res.test_samples()
    if frame is None:
        return
    rng = np.random.default_rng()
    st.session_state[SAMPLE_KEY] = samples.pick_random(frame, fraud, rng)
    st.session_state[NONCE_KEY] = st.session_state.get(NONCE_KEY, 0) + 1  # fresh widget keys
    st.session_state.pop(RESULT_KEY, None)


def _wait_for_prediction(transaction_id: str, timeout_s: float) -> dict[str, Any] | None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        row = res.with_db(lambda conn: queries.fetch_prediction(conn, transaction_id))
        if row is not None:
            return row
        time.sleep(POLL_INTERVAL_S)
    return None


def _send(sample: dict[str, Any], edited: dict[str, float], lang: str) -> None:
    settings = res.app_settings()
    changed = samples.is_edited(sample, edited)
    event = samples.build_manual_event(
        edited, None if changed else int(sample[LABEL_COLUMN]), utc_now()
    )
    try:
        kafka_io.publish_transaction(res.kafka_producer(), settings.kafka.topic_transactions, event)
    except Exception as exc:
        st.error(t("test.publish_failed", lang, error=str(exc)[:200]), icon="⚠️")
        return
    timeout = settings.dashboard.manual_test_timeout_seconds
    with st.spinner(t("test.waiting", lang, seconds=f"{timeout:g}")):
        row = _wait_for_prediction(str(event.transaction_id), timeout)
    st.session_state[RESULT_KEY] = {
        "transaction_id": str(event.transaction_id),
        "edited": changed,
        "row": row,
    }


def _render_result(lang: str) -> None:
    result = st.session_state.get(RESULT_KEY)
    if not result:
        return
    row = result["row"]
    st.divider()
    if row is None:
        st.warning(t("test.timeout", lang, id=result["transaction_id"][:8]), icon="⏱")
        return
    probability = float(row["fraud_probability"])
    threshold = float(row["threshold"])
    flagged = bool(row["prediction"])
    gauge_col, info_col = st.columns([1, 1], gap="large")
    with gauge_col:
        st.plotly_chart(
            charts.probability_gauge(
                probability, threshold, res.chart_theme(), {"title": t("test.gauge", lang)}
            ),
            width="stretch",
            config={"displayModeBar": False},
        )
    with info_col:
        (st.error if flagged else st.success)(decision_label(flagged, lang))
        st.markdown(
            "\n".join(
                [
                    f"- **{t('test.result.probability', lang)}:** {probability:.4f}",
                    f"- **{t('test.result.threshold', lang)}:** {threshold:.4f}",
                    f"- **{t('test.result.actual', lang)}:** "
                    + (
                        t("test.result.edited", lang)
                        if result["edited"]
                        else class_label(row["actual_class"], lang)
                    ),
                    f"- **{t('test.result.processing', lang)}:** {fmt_ms(row['latency_ms'])}",
                    f"- **{t('test.result.e2e', lang)}:** {fmt_ms(row['end_to_end_ms'])}",
                    f"- **{t('test.result.model', lang)}:** {row['model_name']}",
                    f"- **{t('test.result.id', lang)}:** `{row['transaction_id']}`",
                ]
            )
        )
        st.caption(t("common.score_note", lang))


def render() -> None:
    lang = res.current_lang()
    st.title(t("test.title", lang))
    st.caption(t("test.caption", lang))
    frame = res.test_samples()
    if frame is None:
        st.info(t("common.no_model", lang), icon="ℹ️")
        return

    b1, b2, _ = st.columns([1, 1, 3])
    b1.button("🎲 " + t("test.random_fraud", lang), on_click=_pick, args=(True,), width="stretch")
    b2.button("🎲 " + t("test.random_legit", lang), on_click=_pick, args=(False,), width="stretch")

    sample = st.session_state.get(SAMPLE_KEY)
    if sample is None:
        st.info(t("test.pick_first", lang), icon="👆")
        return

    nonce = st.session_state.get(NONCE_KEY, 0)
    st.markdown(
        t(
            "test.sample_summary",
            lang,
            actual=class_label(sample[LABEL_COLUMN], lang),
            amount=f"{sample[AMOUNT_COLUMN]:,.2f}",
            hour=f"{samples.hour_of(sample[TIME_COLUMN]):02d}:00",
        )
    )
    with st.form(f"test_form_{nonce}"):
        c1, c2 = st.columns(2)
        amount = c1.number_input(
            t("test.amount", lang),
            min_value=0.0,
            value=float(sample[AMOUNT_COLUMN]),
            step=1.0,
            format="%.2f",
            key=f"amount_{nonce}",
        )
        hour = c2.slider(
            t("test.hour", lang),
            min_value=0,
            max_value=23,
            value=samples.hour_of(sample[TIME_COLUMN]),
            key=f"hour_{nonce}",
        )
        v_values: dict[str, float] = {}
        with st.expander(t("test.advanced", lang)):
            grid = st.columns(4)
            for i, column in enumerate(V_COLUMNS):
                v_values[column] = grid[i % 4].number_input(
                    column, value=float(sample[column]), format="%.4f", key=f"{column}_{nonce}"
                )
        submitted = st.form_submit_button("📨 " + t("test.send", lang), type="primary")

    if submitted:
        edited = samples.apply_edits(sample, amount=amount, hour=int(hour), v_values=v_values)
        _send(sample, edited, lang)
    _render_result(lang)

    st.subheader(t("test.recent", lang))
    recent = res.with_db(
        lambda conn: queries.fetch_frame(conn, queries.RECENT_MANUAL_SQL, {"limit": 10})
    )
    if recent is None:
        st.warning(t("common.db_unavailable", lang), icon="⚠️")
    elif recent.empty:
        st.caption(t("test.no_recent", lang))
    else:
        table = prediction_table(recent, res.current_threshold(), lang)
        st.dataframe(highlight_flagged(table, lang), hide_index=True, width="stretch")
