"""Structured JSON logging (structlog) for every service.

Each log line is one JSON object on stdout, e.g.::

    {"event": "batch.processed", "rows": 52, "level": "info", "service": "stream-processor",
     "timestamp": "2026-11-14T18:00:01.402Z", "module": "src.streaming.batch"}
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

_service_name = "unknown"


def _add_service(_: Any, __: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    # A module-level value (not a contextvar) so background threads get it too.
    event_dict.setdefault("service", _service_name)
    return event_dict


def configure_logging(service: str, level: str = "INFO") -> None:
    """Configure structlog and the stdlib root logger to emit JSON lines to stdout."""
    global _service_name
    _service_name = service
    numeric_level = logging.getLevelNamesMapping().get(level.upper(), logging.INFO)

    logging.basicConfig(level=numeric_level, stream=sys.stdout, format="%(message)s", force=True)
    for noisy in ("py4j", "py4j.clientserver", "urllib3", "matplotlib", "PIL"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            _add_service,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(ensure_ascii=False),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(numeric_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stdout),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str) -> Any:
    """Return a lazy structlog logger with ``module=name``.

    The proxy binds on first use, so module-level loggers created at import time (before
    :func:`configure_logging` runs) still pick up the JSON configuration. (``logger`` is a
    reserved argument of ``structlog.get_logger``, hence the ``module`` key.)
    """
    return structlog.get_logger(module=name)
