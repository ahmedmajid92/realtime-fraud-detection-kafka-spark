"""Streamlit-cached resources shared by every page: settings, DB pool, Kafka producer, files."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import psycopg
import streamlit as st
from psycopg_pool import ConnectionPool, PoolTimeout

from src.common.config import (
    DashboardSettings,
    KafkaSettings,
    PathSettings,
    PostgresSettings,
    ProducerSettings,
    get_dashboard_settings,
    get_kafka_settings,
    get_logging_settings,
    get_path_settings,
    get_postgres_settings,
    get_producer_settings,
)
from src.common.logging_setup import configure_logging, get_logger
from src.dashboard.helpers import charts, kafka_io, metrics_file, samples

LANG_KEY = "lang"
THRESHOLD_KEY = "threshold"

logger = get_logger(__name__)


@dataclass(frozen=True)
class AppSettings:
    kafka: KafkaSettings
    postgres: PostgresSettings
    paths: PathSettings
    dashboard: DashboardSettings
    producer: ProducerSettings


@st.cache_resource(show_spinner=False)
def app_settings() -> AppSettings:
    configure_logging("dashboard", get_logging_settings().log_level)
    return AppSettings(
        kafka=get_kafka_settings(),
        postgres=get_postgres_settings(),
        paths=get_path_settings(),
        dashboard=get_dashboard_settings(),
        producer=get_producer_settings(),
    )


@st.cache_resource(show_spinner=False)
def db_pool() -> ConnectionPool:
    kwargs = {**app_settings().postgres.connect_kwargs(), "autocommit": True, "connect_timeout": 5}
    pool = ConnectionPool(
        kwargs=kwargs, min_size=1, max_size=4, timeout=5, name="dashboard", open=False
    )
    pool.open(wait=False)
    return pool


def with_db[T](fn: Callable[[Any], T]) -> T | None:
    """Run ``fn(connection)``; returns ``None`` (and logs) if the database is unavailable."""
    try:
        with db_pool().connection(timeout=5) as conn:
            return fn(conn)
    except (psycopg.Error, PoolTimeout) as exc:
        logger.warning("db.unavailable", error=str(exc).strip()[:200])
        return None


@st.cache_resource(show_spinner=False)
def kafka_producer() -> Any:
    return kafka_io.make_producer(app_settings().kafka.kafka_bootstrap_servers)


@st.cache_data(ttl=3, show_spinner=False)
def last_control_command() -> dict[str, Any] | None:
    """Latest producer-control message as a dict (cached briefly; ``None`` if unavailable)."""
    kafka = app_settings().kafka
    try:
        command = kafka_io.read_last_control(
            kafka.kafka_bootstrap_servers, kafka.topic_producer_control, timeout_s=2.0
        )
    except Exception as exc:  # broker down → show "unknown" instead of crashing the page
        logger.warning("control.read_failed", error=str(exc)[:200])
        return None
    return None if command is None else command.model_dump(mode="json")


def _mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except FileNotFoundError:
        return None


@st.cache_data(show_spinner=False)
def _metrics_cached(path: str, mtime: float | None) -> dict[str, Any] | None:
    del mtime  # cache key only: reload when the file changes
    return metrics_file.load_metrics(Path(path))


def model_metrics() -> dict[str, Any] | None:
    path = app_settings().paths.metrics_path
    return _metrics_cached(str(path), _mtime(path))


def model_default_threshold() -> float | None:
    metrics = model_metrics()
    return None if metrics is None else float(metrics["best"]["threshold"])


@st.cache_data(show_spinner=False)
def _samples_cached(path: str, mtime: float | None) -> pd.DataFrame | None:
    del mtime
    return samples.load_samples(Path(path))


def test_samples() -> pd.DataFrame | None:
    path = app_settings().paths.test_split_path
    return _samples_cached(str(path), _mtime(path))


def current_lang() -> str:
    return str(st.session_state.get(LANG_KEY, "en"))


def current_threshold() -> float:
    value = st.session_state.get(THRESHOLD_KEY)
    if value is None:
        value = model_default_threshold() or 0.5
    return float(value)


def chart_theme() -> charts.ChartTheme:
    """Light or dark chart palette, following the viewer's Streamlit theme."""
    try:
        dark = st.context.theme.type == "dark"
    except AttributeError:
        dark = False
    return charts.theme_for(dark)
