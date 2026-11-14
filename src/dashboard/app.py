"""Streamlit entry point: navigation, language toggle (EN/AR) and sidebar controls.

Run: ``streamlit run src/dashboard/app.py``
"""

from __future__ import annotations

import streamlit as st

from src.dashboard import sidebar
from src.dashboard.helpers.i18n import RTL_CSS, t
from src.dashboard.views import (
    about,
    live_monitor,
    model_performance,
    system_health,
    test_transaction,
)

st.set_page_config(page_title="Fraud Detection · Kafka + Spark", page_icon="🛡️", layout="wide")

lang = sidebar.render_language_selector()
if lang == "ar":
    st.markdown(RTL_CSS, unsafe_allow_html=True)

pages = [
    st.Page(
        live_monitor.render,
        title=t("nav.live", lang),
        icon=":material/monitoring:",
        url_path="live",
        default=True,
    ),
    st.Page(
        model_performance.render,
        title=t("nav.performance", lang),
        icon=":material/insights:",
        url_path="model-performance",
    ),
    st.Page(
        test_transaction.render,
        title=t("nav.test", lang),
        icon=":material/science:",
        url_path="test-transaction",
    ),
    st.Page(
        system_health.render,
        title=t("nav.health", lang),
        icon=":material/monitor_heart:",
        url_path="system-health",
    ),
    st.Page(about.render, title=t("nav.about", lang), icon=":material/info:", url_path="about"),
]
navigation = st.navigation(pages)

sidebar.render_threshold_controls(lang)
sidebar.render_producer_controls(lang)

navigation.run()
