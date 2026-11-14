"""Model Performance: offline evaluation results from ``models/metrics.json``."""

from __future__ import annotations

import streamlit as st

from src.dashboard import resources as res
from src.dashboard.helpers import charts, metrics_file
from src.dashboard.helpers.i18n import t


def render() -> None:
    lang = res.current_lang()
    st.title(t("perf.title", lang))
    metrics = res.model_metrics()
    if metrics is None:
        st.info(t("common.no_model", lang), icon="ℹ️")
        return

    best = metrics["best"]
    st.caption(
        t(
            "perf.caption",
            lang,
            run_id=metrics["run_id"],
            created=metrics["created_at"],
            spark=metrics["spark_version"],
            minutes=f"{metrics['training_seconds'] / 60:.1f}",
        )
    )

    st.subheader(t("perf.best", lang, name=metrics_file.row_label(best, lang)))
    cols = st.columns(6)
    test = best["test"]
    cols[0].metric(t("metric.auc_pr", lang), f"{test['auc_pr']:.4f}")
    cols[1].metric(t("metric.roc_auc", lang), f"{test['roc_auc']:.4f}")
    cols[2].metric(t("metric.precision", lang), f"{test['precision']:.4f}")
    cols[3].metric(t("metric.recall", lang), f"{test['recall']:.4f}")
    cols[4].metric(t("metric.f1", lang), f"{test['f1']:.4f}")
    cols[5].metric(t("metric.threshold", lang), f"{best['threshold']:.4f}")
    st.caption(t("perf.selection_note", lang))
    st.caption(t("common.score_note", lang))

    st.subheader(t("perf.comparison", lang))
    table = metrics_file.comparison_frame(metrics, lang)
    best_index = next(
        i
        for i, row in enumerate(metrics["comparison"])
        if row["display_name"] == best["display_name"]
    )
    numeric = [c for c in table.columns if table[c].dtype.kind == "f"]
    styled = table.style.format({c: "{:.4f}" for c in numeric}).apply(
        lambda row: ["font-weight: 700" if row.name == best_index else "" for _ in row], axis=1
    )
    st.dataframe(styled, hide_index=True, width="stretch")

    theme = res.chart_theme()
    row_labels = {
        row["display_name"]: metrics_file.row_label(row, lang) for row in metrics["comparison"]
    }
    model_labels = {key: t(f"model.{key}", lang) for key in charts.MODEL_ORDER}
    strategy_labels = {s: t(f"strategy.{s}", lang) for s in ("class_weights", "undersampling")}
    st.plotly_chart(
        charts.comparison_bars(
            metrics, theme, {"y": t("perf.chart.auc_pr_test", lang)}, model_labels, strategy_labels
        ),
        width="stretch",
        config={"displayModeBar": False},
    )

    tab_pr, tab_roc, tab_cm, tab_sweep = st.tabs(
        [
            t("perf.tab.pr", lang),
            t("perf.tab.roc", lang),
            t("perf.tab.cm", lang),
            t("perf.tab.sweep", lang),
        ]
    )
    with tab_pr:
        st.plotly_chart(
            charts.pr_curves(
                metrics,
                theme,
                {"recall": t("metric.recall", lang), "precision": t("metric.precision", lang)},
                row_labels,
            ),
            width="stretch",
        )
        st.caption(
            t("perf.pr_note", lang, baseline=f"{metrics['splits']['test']['fraud_rate']:.4f}")
        )
    with tab_roc:
        st.plotly_chart(
            charts.roc_curves(
                metrics,
                theme,
                {
                    "fpr": t("chart.fpr", lang),
                    "tpr": t("chart.tpr", lang),
                    "random": t("chart.random", lang),
                },
                row_labels,
            ),
            width="stretch",
        )
    with tab_cm:
        left, _ = st.columns([2, 1])
        with left:
            st.plotly_chart(
                charts.confusion_heatmap(
                    test["confusion"],
                    theme,
                    {
                        "tn": t("metric.tn", lang),
                        "fp": t("metric.fp", lang),
                        "fn": t("metric.fn", lang),
                        "tp": t("metric.tp", lang),
                        "pred_legit": t("chart.pred_legit", lang),
                        "pred_fraud": t("chart.pred_fraud", lang),
                        "act_legit": t("chart.act_legit", lang),
                        "act_fraud": t("chart.act_fraud", lang),
                    },
                ),
                width="stretch",
                config={"displayModeBar": False},
            )
        st.caption(t("perf.cm_note", lang, threshold=f"{best['threshold']:.4f}"))
    with tab_sweep:
        st.plotly_chart(
            charts.threshold_sweep_chart(
                metrics["threshold_sweep"],
                best["threshold"],
                res.current_threshold(),
                theme,
                {
                    "precision": t("metric.precision", lang),
                    "recall": t("metric.recall", lang),
                    "f1": t("metric.f1", lang),
                    "tuned": t("chart.tuned", lang, value=f"{best['threshold']:.3f}"),
                    "current": t("chart.current", lang),
                    "x": t("chart.threshold", lang),
                    "y": t("chart.score", lang),
                },
            ),
            width="stretch",
        )
        st.caption(t("perf.sweep_note", lang))

    with st.expander(t("perf.dataset", lang)):
        st.dataframe(metrics_file.dataset_frame(metrics, lang), hide_index=True, width="stretch")
        st.caption(t("perf.dataset_note", lang, ratio=f"{metrics['undersample_ratio']:g}"))
    with st.expander(t("perf.candidates", lang)):
        candidates = metrics_file.candidates_frame(metrics, lang)
        numeric = [c for c in candidates.columns if candidates[c].dtype.kind == "f"]
        st.dataframe(
            candidates.style.format({c: "{:.4f}" for c in numeric}),
            hide_index=True,
            width="stretch",
        )
