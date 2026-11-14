"""Architecture / About: diagram, components, dataset and tech stack."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from src.dashboard import resources as res
from src.dashboard.helpers.i18n import t

TECH_STACK = (
    ("Apache Kafka", "4.1.2 (KRaft)"),
    ("Apache Spark / MLlib", "4.1.4 (Structured Streaming)"),
    ("spark-sql-kafka-0-10_2.13", "4.1.4"),
    ("PostgreSQL", "17.11"),
    ("Streamlit / Plotly", "1.66.1 / 6.9.0"),
    ("Python / Java", "3.12 / Temurin 17"),
    ("Kafka UI (kafbat)", "v1.5.0"),
)

COMPONENT_KEYS = ("trainer", "producer", "kafka", "spark", "postgres", "dashboard", "kafka_ui")


def render() -> None:
    lang = res.current_lang()
    st.title(t("about.title", lang))
    st.markdown(t("about.intro", lang))

    st.subheader(t("about.architecture", lang))
    diagram = res.app_settings().paths.figures_dir / "architecture.png"
    if diagram.is_file():
        st.image(str(diagram), width="stretch")
    else:
        st.info(t("about.no_diagram", lang), icon="ℹ️")

    st.subheader(t("about.components", lang))
    for key in COMPONENT_KEYS:
        st.markdown(
            f"- **{t(f'about.comp.{key}.name', lang)}**: {t(f'about.comp.{key}.desc', lang)}"
        )

    st.subheader(t("about.dataset", lang))
    metrics = res.model_metrics()
    if metrics is not None:
        ds = metrics["dataset"]
        st.markdown(
            t(
                "about.dataset_text",
                lang,
                rows=f"{ds['rows_raw']:,}",
                frauds=f"{ds['frauds_raw']:,}",
                rate=f"{ds['frauds_raw'] / ds['rows_raw']:.3%}",
                dups=f"{ds['duplicates_removed']:,}",
                test=f"{metrics['splits']['test']['rows']:,}",
            )
        )
    else:
        st.markdown(t("about.dataset_generic", lang))

    st.subheader(t("about.stack", lang))
    st.dataframe(
        pd.DataFrame(
            TECH_STACK, columns=[t("about.col.component", lang), t("about.col.version", lang)]
        ),
        hide_index=True,
        width="stretch",
    )
