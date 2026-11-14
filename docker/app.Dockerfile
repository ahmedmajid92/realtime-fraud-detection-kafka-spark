# syntax=docker/dockerfile:1
# -----------------------------------------------------------------------------
# fraud-app: lightweight Python image (no Java/Spark) for the producer and dashboard.
# -----------------------------------------------------------------------------
ARG PYTHON_IMAGE=python:3.12.15-slim-bookworm

FROM ${PYTHON_IMAGE}

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=UTC

WORKDIR /app

COPY requirements/app.txt requirements/constraints.txt /tmp/requirements/
RUN pip install -r /tmp/requirements/app.txt -c /tmp/requirements/constraints.txt

RUN useradd --create-home --uid 1000 --shell /bin/bash app \
 && mkdir -p /app/data /app/models /app/docs \
 && chown -R app:app /app

COPY --chown=app:app .streamlit/ /app/.streamlit/
COPY --chown=app:app src/ /app/src/
COPY --chown=app:app scripts/ /app/scripts/
RUN find /app/scripts -type f \( -name '*.sh' -o -name '*.py' \) -exec sed -i 's/\r$//' {} +

USER app
EXPOSE 8501
CMD ["python", "-m", "src.producer.producer"]
