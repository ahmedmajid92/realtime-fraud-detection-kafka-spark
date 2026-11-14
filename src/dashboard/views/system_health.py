"""System Health: Kafka topics, Spark progress and lag, producer state, Postgres."""

from __future__ import annotations

import time
from typing import Any

import pandas as pd
import streamlit as st

from src.common import db
from src.dashboard import resources as res
from src.dashboard.helpers import kafka_io, queries
from src.dashboard.helpers.formatting import (
    fmt_bytes,
    fmt_int,
    fmt_ms,
    fmt_rate,
    fmt_seconds_ago,
)
from src.dashboard.helpers.i18n import t
from src.dashboard.helpers.kpis import commanded_state, consumer_lag, rate_between, total_offset

REFRESH_S = 5.0
SPARK_STALE_S = 60.0
_OFFSET_SAMPLE_KEY = "_tx_offset_sample"


def _status(ok: bool, label: str, detail: str) -> None:
    (st.success if ok else st.error)(f"{'✅' if ok else '❌'} **{label}** · {detail}")


@st.fragment(run_every=REFRESH_S)
def _health_panel() -> None:
    lang = res.current_lang()
    settings = res.app_settings()
    kafka = settings.kafka

    try:
        offsets: dict[str, dict[int, int]] | None = kafka_io.topic_overview(
            kafka.kafka_bootstrap_servers, kafka.all_topics, timeout_s=3.0
        )
    except Exception:
        offsets = None
    progress = res.with_db(lambda conn: queries.fetch_one(conn, queries.LATEST_PROGRESS_SQL))
    counts = res.with_db(lambda conn: queries.fetch_one(conn, queries.TABLE_COUNTS_SQL))
    db_threshold = res.with_db(db.get_threshold)
    last_control = res.last_control_command()

    # Observed producer rate = growth of the transactions topic between refreshes.
    observed_rate = None
    if offsets is not None:
        sample = (time.monotonic(), total_offset(offsets.get(kafka.topic_transactions, {})))
        observed_rate = rate_between(st.session_state.get(_OFFSET_SAMPLE_KEY), sample)
        st.session_state[_OFFSET_SAMPLE_KEY] = sample

    spark_age = float(progress["age_s"]) if progress else None
    spark_ok = spark_age is not None and spark_age < SPARK_STALE_S
    paused, rate = commanded_state(last_control, settings.producer.producer_rate)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _status(
            offsets is not None,
            "Kafka",
            t("health.reachable", lang) if offsets is not None else t("health.unreachable", lang),
        )
    with c2:
        _status(
            counts is not None,
            "PostgreSQL",
            t("health.reachable", lang) if counts is not None else t("health.unreachable", lang),
        )
    with c3:
        _status(
            spark_ok, "Spark", t("health.last_progress", lang, ago=fmt_seconds_ago(spark_age, lang))
        )
    with c4:
        producer_detail = t("sidebar.state_paused", lang) if paused else fmt_rate(observed_rate)
        _status(paused or bool(observed_rate), t("health.producer", lang), producer_detail)

    st.subheader(t("health.topics", lang))
    if offsets is None:
        st.warning(t("health.unreachable", lang), icon="⚠️")
    else:
        rows = [
            {
                t("health.col.topic", lang): topic,
                t("health.col.partitions", lang): len(parts),
                t("health.col.messages", lang): fmt_int(total_offset(parts)),
                t("health.col.offsets", lang): ", ".join(f"p{p}={o:,}" for p, o in parts.items())
                or "—",
            }
            for topic, parts in offsets.items()
        ]
        st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")

    st.subheader(t("health.spark", lang))
    if not progress:
        st.info(t("health.no_progress", lang), icon="⏳")
    else:
        spark_offsets: dict[str, Any] = (progress.get("end_offsets") or {}).get(
            kafka.topic_transactions, {}
        )
        lag = (
            consumer_lag(offsets.get(kafka.topic_transactions, {}), spark_offsets)
            if offsets
            else None
        )
        s = st.columns(5)
        s[0].metric(t("health.batch", lang), fmt_int(progress["batch_id"]))
        s[1].metric(t("health.input_rate", lang), fmt_rate(progress["input_rows_per_second"]))
        s[2].metric(
            t("health.processing_rate", lang), fmt_rate(progress["processed_rows_per_second"])
        )
        s[3].metric(t("health.batch_duration", lang), fmt_ms(progress["batch_duration_ms"]))
        s[4].metric(t("health.lag", lang), fmt_int(lag))
        st.caption(t("health.spark_note", lang))

    st.subheader(t("health.producer", lang))
    p = st.columns(3)
    p[0].metric(
        t("health.commanded", lang),
        t("sidebar.state_paused", lang) if paused else t("sidebar.state_running", lang),
    )
    p[1].metric(t("health.target_rate", lang), fmt_rate(rate))
    p[2].metric(t("health.observed_rate", lang), fmt_rate(observed_rate))

    st.subheader("PostgreSQL")
    if counts is None:
        st.warning(t("common.db_unavailable", lang), icon="⚠️")
    else:
        d = st.columns(5)
        d[0].metric(t("health.rows", lang), fmt_int(counts["predictions"]))
        d[1].metric(t("health.alert_rows", lang), fmt_int(counts["alerts"]))
        d[2].metric(t("health.manual_rows", lang), fmt_int(counts["manual"]))
        d[3].metric(t("health.db_size", lang), fmt_bytes(counts["db_bytes"]))
        d[4].metric(
            t("health.db_threshold", lang), "—" if db_threshold is None else f"{db_threshold:.3f}"
        )


def render() -> None:
    lang = res.current_lang()
    st.title(t("health.title", lang))
    st.caption(t("health.caption", lang, seconds=f"{REFRESH_S:g}"))
    _health_panel()
