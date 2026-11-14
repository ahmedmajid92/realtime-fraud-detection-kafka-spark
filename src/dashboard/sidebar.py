"""Sidebar: language toggle, live alert-threshold slider and producer controls.

Decision logic lives in :mod:`src.dashboard.helpers.thresholds` and
:mod:`src.dashboard.helpers.kpis` (unit-tested). This module only wires it to widgets.
"""

from __future__ import annotations

import streamlit as st

from src.common import db
from src.common.schemas import MAX_PRODUCER_RATE, THRESHOLD_MAX, THRESHOLD_MIN, ControlCommand
from src.dashboard import resources as res
from src.dashboard.helpers import kafka_io, thresholds
from src.dashboard.helpers.formatting import fmt_rate
from src.dashboard.helpers.i18n import LANGUAGES, t
from src.dashboard.helpers.kpis import commanded_state, control_known

_SAVED_FLAG = "_threshold_saved"
_DB_SYNCED = "_threshold_db_synced"
_CONTROL_FEEDBACK = "_control_feedback"
_RATE_INPUT = "rate_input"
MIN_RATE_INPUT = 0.1


def render_language_selector() -> str:
    st.sidebar.radio(
        "Language · اللغة",
        options=list(LANGUAGES),
        format_func=lambda code: LANGUAGES[code],
        key=res.LANG_KEY,
        horizontal=True,
    )
    return res.current_lang()


# ---------------------------------------------------------------------------
# Alert threshold (shared with Spark through the Postgres settings table)
# ---------------------------------------------------------------------------
def _sync_threshold_from_db() -> None:
    """Before the slider renders: adopt the shared value if it changed elsewhere."""
    db_value = res.db_threshold()
    if res.THRESHOLD_KEY not in st.session_state:
        st.session_state[res.THRESHOLD_KEY] = thresholds.resolve_threshold(
            db_value, None, res.model_default_threshold()
        )
    else:
        update = thresholds.slider_update(db_value, st.session_state.get(_DB_SYNCED))
        if update is not None:
            st.session_state[res.THRESHOLD_KEY] = update
    if db_value is not None:
        st.session_state[_DB_SYNCED] = db_value


def _save_threshold() -> None:
    value = thresholds.clamp_threshold(st.session_state[res.THRESHOLD_KEY])
    saved = res.with_db(lambda conn: db.set_threshold(conn, value))
    if saved is not None:
        st.session_state[_DB_SYNCED] = saved
        res.db_threshold.clear()  # live panels pick up the new value immediately
    st.session_state[_SAVED_FLAG] = "ok" if saved is not None else "error"


def _reset_threshold() -> None:
    default = res.model_default_threshold()
    if default is not None:
        st.session_state[res.THRESHOLD_KEY] = thresholds.clamp_threshold(default)
        _save_threshold()


def render_threshold_controls(lang: str) -> None:
    _sync_threshold_from_db()
    sidebar = st.sidebar
    sidebar.subheader(t("sidebar.threshold_title", lang))
    sidebar.slider(
        t("sidebar.threshold", lang),
        min_value=THRESHOLD_MIN,
        max_value=THRESHOLD_MAX,
        step=0.001,
        format="%.4f",
        key=res.THRESHOLD_KEY,
        on_change=_save_threshold,
        help=t("sidebar.threshold_help", lang),
    )
    default = res.model_default_threshold()
    if default is not None:
        sidebar.caption(t("sidebar.threshold_default", lang, value=f"{default:.4f}"))
        sidebar.button(
            t("sidebar.threshold_reset", lang), on_click=_reset_threshold, width="stretch"
        )
    status = st.session_state.pop(_SAVED_FLAG, None)
    if status == "ok":
        st.toast(t("sidebar.threshold_saved", lang), icon="✅")
    elif status == "error":
        st.toast(t("common.db_unavailable", lang), icon="⚠️")


# ---------------------------------------------------------------------------
# Producer control (compacted producer-control topic)
# ---------------------------------------------------------------------------
def _send_control(action: str) -> None:
    settings = res.app_settings()
    res.last_control_command.clear()  # decide on fresh state, not a 3 s old cache
    last = res.last_control_command()
    if not control_known(last):
        # Building a full-state command from defaults could silently resume a paused producer.
        st.session_state[_CONTROL_FEEDBACK] = (
            "error",
            "producer state unknown (Kafka unreachable)",
        )
        return
    paused, rate = commanded_state(last, settings.producer.producer_rate)
    try:
        command = ControlCommand.from_action(
            action,  # type: ignore[arg-type]
            current_paused=paused,
            current_rate=rate,
            new_rate=float(st.session_state[_RATE_INPUT]) if action == "set_rate" else None,
        )
        kafka_io.publish_control(
            res.kafka_producer(), settings.kafka.topic_producer_control, command
        )
        res.last_control_command.clear()
        st.session_state[_CONTROL_FEEDBACK] = ("ok", action)
    except Exception as exc:
        st.session_state[_CONTROL_FEEDBACK] = ("error", str(exc))


def render_producer_controls(lang: str) -> None:
    settings = res.app_settings()
    sidebar = st.sidebar
    sidebar.subheader(t("sidebar.producer_title", lang))
    last = res.last_control_command()
    known = control_known(last)
    paused, rate = commanded_state(last, settings.producer.producer_rate)
    if known:
        state_text = t("sidebar.state_paused", lang) if paused else t("sidebar.state_running", lang)
        sidebar.markdown(f"**{state_text}** · {fmt_rate(rate)}")
    else:
        sidebar.warning(t("sidebar.state_unknown", lang), icon="⚠️")

    left, right = sidebar.columns(2)
    left.button(
        "⏸ " + t("sidebar.pause", lang),
        on_click=_send_control,
        args=("pause",),
        disabled=not known or paused,
        width="stretch",
    )
    right.button(
        "▶ " + t("sidebar.resume", lang),
        on_click=_send_control,
        args=("resume",),
        disabled=not known or not paused,
        width="stretch",
    )
    if _RATE_INPUT not in st.session_state:
        st.session_state[_RATE_INPUT] = min(MAX_PRODUCER_RATE, max(MIN_RATE_INPUT, float(rate)))
    sidebar.number_input(
        t("sidebar.rate", lang),
        min_value=MIN_RATE_INPUT,
        max_value=MAX_PRODUCER_RATE,
        step=10.0,
        key=_RATE_INPUT,
        format="%.1f",
    )
    sidebar.button(
        t("sidebar.apply_rate", lang),
        on_click=_send_control,
        args=("set_rate",),
        disabled=not known,
        width="stretch",
    )
    feedback = st.session_state.pop(_CONTROL_FEEDBACK, None)
    if feedback is not None:
        kind, detail = feedback
        if kind == "ok":
            st.toast(
                t("sidebar.control_sent", lang, action=t(f"sidebar.action.{detail}", lang)),
                icon="✅",
            )
        else:
            st.toast(t("sidebar.control_failed", lang, error=detail[:120]), icon="⚠️")
