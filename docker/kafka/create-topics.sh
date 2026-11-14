#!/usr/bin/env bash
# Creates the project's Kafka topics (idempotent), then lists them.
# Runs once in the "kafka-init" container after the broker is healthy.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:?KAFKA_BOOTSTRAP_SERVERS is not set}"
TOPICS_CMD=/opt/kafka/bin/kafka-topics.sh
# The CLI tools are JVM programs; keep their heap small inside this 256 MB container.
export KAFKA_HEAP_OPTS="-Xms32m -Xmx128m"

ONE_DAY_MS=86400000
SEVEN_DAYS_MS=604800000

echo "Waiting for Kafka at ${BOOTSTRAP}..."
for attempt in $(seq 1 30); do
  if "$TOPICS_CMD" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    break
  fi
  if [ "$attempt" -eq 30 ]; then
    echo "Kafka not reachable after 30 attempts" >&2
    exit 1
  fi
  sleep 2
done

create_topic() {
  local name="$1" partitions="$2"
  shift 2
  "$TOPICS_CMD" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 1 "$@"
}

create_topic "${TOPIC_TRANSACTIONS:?}"     3 --config "retention.ms=${ONE_DAY_MS}"
create_topic "${TOPIC_PREDICTIONS:?}"      3 --config "retention.ms=${ONE_DAY_MS}"
create_topic "${TOPIC_FRAUD_ALERTS:?}"     1 --config "retention.ms=${SEVEN_DAYS_MS}"
create_topic "${TOPIC_PRODUCER_CONTROL:?}" 1 --config "cleanup.policy=compact"

echo "Topics:"
"$TOPICS_CMD" --bootstrap-server "$BOOTSTRAP" --list
