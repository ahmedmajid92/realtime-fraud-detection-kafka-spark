# syntax=docker/dockerfile:1
# -----------------------------------------------------------------------------
# fraud-spark: Python 3.12 + Temurin 17 JRE + PySpark 4.1.4 + Kafka connector.
#   target "runtime" → trainer, stream-processor
#   target "test"    → unit tests and e2e tests (adds app + dev dependencies)
# The Kafka connector jars are downloaded and SHA-1 verified at BUILD time, so the
# running containers never download anything.
# -----------------------------------------------------------------------------
ARG PYTHON_IMAGE=python:3.12.15-slim-bookworm
ARG JRE_IMAGE=eclipse-temurin:17.0.21_9-jre-noble

FROM ${JRE_IMAGE} AS jre

FROM ${PYTHON_IMAGE} AS runtime

ARG SPARK_VERSION=4.1.4
ARG SCALA_BINARY_VERSION=2.13
ARG KAFKA_CLIENTS_VERSION=3.9.1
ARG COMMONS_POOL2_VERSION=2.12.1
ARG SCALA_PARALLEL_COLLECTIONS_VERSION=1.2.0
ARG MAVEN_REPO=https://repo1.maven.org/maven2

ENV JAVA_HOME=/opt/java/openjdk \
    PATH=/opt/java/openjdk/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app \
    PYSPARK_PYTHON=python3 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=UTC

COPY --from=jre /opt/java/openjdk /opt/java/openjdk

RUN apt-get update \
 && apt-get install -y --no-install-recommends procps curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements/spark.txt requirements/constraints.txt /tmp/requirements/
RUN pip install -r /tmp/requirements/spark.txt -c /tmp/requirements/constraints.txt

# Spark–Kafka connector + its runtime dependencies, matched to Spark 4.1.4 / Scala 2.13.
# (lz4, snappy, zstd and slf4j already ship with PySpark.)
RUN set -eux; \
    jars_dir="$(python -c 'import os, pyspark; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"; \
    fetch() { \
      url="${MAVEN_REPO}/$1"; target="${jars_dir}/$(basename "$1")"; \
      curl -fsSL --retry 3 -o "$target" "$url"; \
      expected="$(curl -fsSL --retry 3 "${url}.sha1" | cut -c1-40)"; \
      echo "${expected}  ${target}" | sha1sum -c -; \
    }; \
    fetch "org/apache/spark/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_VERSION}/spark-sql-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar"; \
    fetch "org/apache/spark/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}/${SPARK_VERSION}/spark-token-provider-kafka-0-10_${SCALA_BINARY_VERSION}-${SPARK_VERSION}.jar"; \
    fetch "org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar"; \
    fetch "org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    if ! ls "$jars_dir" | grep -q '^scala-parallel-collections'; then \
      fetch "org/scala-lang/modules/scala-parallel-collections_${SCALA_BINARY_VERSION}/${SCALA_PARALLEL_COLLECTIONS_VERSION}/scala-parallel-collections_${SCALA_BINARY_VERSION}-${SCALA_PARALLEL_COLLECTIONS_VERSION}.jar"; \
    fi; \
    ls "$jars_dir" | grep -E 'kafka|commons-pool2|parallel-collections'

RUN useradd --create-home --uid 1000 --shell /bin/bash app \
 && mkdir -p /app/data /app/models /app/docs /checkpoints \
 && chown -R app:app /app /checkpoints

COPY --chown=app:app src/ /app/src/
COPY --chown=app:app scripts/ /app/scripts/
# Belt and braces for Windows checkouts: strip CR from scripts.
RUN find /app/scripts -type f \( -name '*.sh' -o -name '*.py' \) -exec sed -i 's/\r$//' {} +

USER app
CMD ["python", "-c", "print('fraud-spark image: run a module, e.g. python -m src.training.train_model')"]


FROM runtime AS test
USER root
COPY requirements/app.txt requirements/dev.txt /tmp/requirements/
RUN pip install -r /tmp/requirements/app.txt -r /tmp/requirements/dev.txt -c /tmp/requirements/constraints.txt
COPY --chown=app:app pyproject.toml /app/pyproject.toml
COPY --chown=app:app tests/ /app/tests/
USER app
CMD ["pytest"]
