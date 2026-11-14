"""Sidebar: language toggle, live alert-threshold slider and producer controls."""

from __future__ import annotations

import streamlit as st

from src.common import db
from src.common.schemas import MAX_PRODUCER_RATE, ControlCommand
from src.dashboard import resources as res
from src.dashboard.helpers import kafka_io
from src.dashboard.helpers.formatting import fmt_rate
from src.dashboard.helpers.i18n import LANGUAGES, t
from src.dashboard.helpers.kpis import commanded_state

_SAVED_FLAG = "_threshold_saved"
_CONTROL_FEEDBACK = "_control_feedback"
_RATE_INPUT = "rate_input"


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
def _init_threshold() -> None:
    if res.THRESHOLD_KEY in st.session_state:
        return
    stored = res.with_db(db.get_threshold)
    value = stored or res.model_default_threshold() or 0.5
    st.session_state[res.THRESHOLD_KEY] = round(float(value), 3)


def _save_threshold() -> None:
    value = float(st.session_state[res.THRESHOLD_KEY])
    saved = res.with_db(lambda conn: db.set_threshold(conn, value))
    st.session_state[_SAVED_FLAG] = "ok" if saved is not None else "error"


def _reset_threshold() -> None:
    default = res.model_default_threshold()
    if default is not None:
        st.session_state[res.THRESHOLD_KEY] = round(default, 3)
        _save_threshold()


def render_threshold_controls(lang: str) -> None:
    _init_threshold()
    sidebar = st.sidebar
    sidebar.subheader(t("sidebar.threshold_title", lang))
    sidebar.slider(
        t("sidebar.threshold", lang),
        min_value=0.01,
        max_value=0.99,
        step=0.001,
        format="%.3f",
        key=res.THRESHOLD_KEY,
        on_change=_save_threshold,
        help=t("sidebar.threshold_help", lang),
    )
    default = res.model_default_threshold()
    if default is not None:
        sidebar.caption(t("sidebar.threshold_default", lang, value=f"{default:.3f}"))
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
    paused, rate = commanded_state(res.last_control_command(), settings.producer.producer_rate)
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
    paused, rate = commanded_state(last, settings.producer.producer_rate)
    state_text = t("sidebar.state_paused", lang) if paused else t("sidebar.state_running", lang)
    sidebar.markdown(f"**{state_text}** · {fmt_rate(rate)}")

    left, right = sidebar.columns(2)
    left.button(
        "⏸ " + t("sidebar.pause", lang),
        on_click=_send_control,
        args=("pause",),
        disabled=paused,
        width="stretch",
    )
    right.button(
        "▶ " + t("sidebar.resume", lang),
        on_click=_send_control,
        args=("resume",),
        disabled=not paused,
        width="stretch",
    )
    if _RATE_INPUT not in st.session_state:
        st.session_state[_RATE_INPUT] = float(rate)
    sidebar.number_input(
        t("sidebar.rate", lang),
        min_value=1.0,
        max_value=MAX_PRODUCER_RATE,
        step=10.0,
        key=_RATE_INPUT,
        format="%.0f",
    )
    sidebar.button(
        t("sidebar.apply_rate", lang), on_click=_send_control, args=("set_rate",), width="stretch"
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
