"""Live Monitor: KPIs, live feed, fraud alerts, charts and live model quality."""

from __future__ import annotations

import streamlit as st

from src.common.config import get_dashboard_settings
from src.dashboard import resources as res
from src.dashboard.helpers import charts, queries
from src.dashboard.helpers.formatting import (
    fmt_int,
    fmt_ms,
    fmt_pct,
    fmt_rate,
    highlight_flagged,
    prediction_table,
)
from src.dashboard.helpers.i18n import t
from src.dashboard.helpers.kpis import build_kpi_snapshot, quality_metrics

WINDOW_S = 60.0
REFRESH_S = get_dashboard_settings().dashboard_refresh_seconds


@st.fragment(run_every=REFRESH_S)
def _live_panel() -> None:
    lang = res.current_lang()
    threshold = res.current_threshold()
    data = res.with_db(lambda conn: queries.live_snapshot(conn, threshold, window_s=WINDOW_S))
    if data is None:
        st.warning(t("common.db_unavailable", lang), icon="⚠️")
        return

    kpi = build_kpi_snapshot(data["summary"], WINDOW_S)
    cols = st.columns(6)
    cols[0].metric(t("live.kpi.total", lang), fmt_int(kpi.total))
    cols[1].metric(t("live.kpi.flagged", lang), fmt_int(kpi.flagged))
    cols[2].metric(t("live.kpi.rate", lang), fmt_pct(kpi.fraud_rate))
    cols[3].metric(t("live.kpi.throughput", lang), fmt_rate(kpi.throughput))
    cols[4].metric(t("live.kpi.avg_latency", lang), fmt_ms(kpi.avg_latency_ms))
    cols[5].metric(t("live.kpi.p95_latency", lang), fmt_ms(kpi.p95_latency_ms))

    if kpi.total == 0:
        st.info(t("live.waiting", lang), icon="⏳")
    elif kpi.idle_s is not None and kpi.idle_s > 30:
        st.warning(t("live.stalled", lang, seconds=f"{kpi.idle_s:,.0f}"), icon="⏸")

    feed_col, alerts_col = st.columns([3, 2], gap="large")
    with feed_col:
        st.subheader(t("live.feed", lang))
        feed = prediction_table(data["feed"], threshold, lang)
        st.dataframe(highlight_flagged(feed, lang), hide_index=True, height=360, width="stretch")
    with alerts_col:
        st.subheader("🚨 " + t("live.alerts", lang))
        alerts = prediction_table(data["alerts"], threshold, lang)
        if alerts.empty:
            st.caption(t("live.no_alerts", lang))
        else:
            st.dataframe(
                highlight_flagged(alerts, lang), hide_index=True, height=360, width="stretch"
            )

    theme = res.chart_theme()
    c1, c2, c3 = st.columns(3, gap="medium")
    with c1:
        st.markdown(f"**{t('live.chart.prob', lang)}**")
        probabilities = (
            data["scores"]["fraud_probability"].tolist() if not data["scores"].empty else []
        )
        st.plotly_chart(
            charts.probability_histogram(
                probabilities,
                threshold,
                theme,
                {
                    "x": t("chart.probability", lang),
                    "y": t("chart.count_log", lang),
                    "threshold": t("chart.threshold_at", lang, value=f"{threshold:.3f}"),
                },
            ),
            width="stretch",
            config={"displayModeBar": False},
        )
    with c2:
        st.markdown(f"**{t('live.chart.over_time', lang)}**")
        st.plotly_chart(
            charts.frauds_over_time(
                data["per_minute"],
                theme,
                {
                    "flagged": t("chart.flagged", lang),
                    "actual": t("chart.actual_fraud", lang),
                    "x": t("chart.minute", lang),
                    "y": t("chart.transactions", lang),
                },
            ),
            width="stretch",
            config={"displayModeBar": False},
        )
    with c3:
        st.markdown(f"**{t('live.chart.amount', lang)}**")
        st.plotly_chart(
            charts.amount_distribution(
                data["scores"],
                threshold,
                theme,
                {
                    "legit": t("chart.legit", lang),
                    "flagged": t("chart.flagged", lang),
                    "y": t("chart.amount_log", lang),
                },
            ),
            width="stretch",
            config={"displayModeBar": False},
        )

    st.subheader(t("live.quality", lang))
    st.caption(t("live.quality_caption", lang))
    quality = quality_metrics(data["quality"])
    q = st.columns(7)
    q[0].metric(t("metric.precision", lang), fmt_pct(quality["precision"], 1))
    q[1].metric(t("metric.recall", lang), fmt_pct(quality["recall"], 1))
    q[2].metric(t("metric.f1", lang), f"{quality['f1']:.3f}")
    q[3].metric(t("metric.tp", lang), fmt_int(quality["tp"]))
    q[4].metric(t("metric.fp", lang), fmt_int(quality["fp"]))
    q[5].metric(t("metric.fn", lang), fmt_int(quality["fn"]))
    q[6].metric(t("metric.tn", lang), fmt_int(quality["tn"]))


def render() -> None:
    lang = res.current_lang()
    st.title(t("live.title", lang))
    st.caption(
        t(
            "live.caption",
            lang,
            threshold=f"{res.current_threshold():.3f}",
            seconds=f"{REFRESH_S:g}",
        )
    )
    _live_panel()
