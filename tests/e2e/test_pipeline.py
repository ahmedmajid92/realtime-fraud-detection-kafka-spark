"""End-to-end tests: Kafka → Spark Structured Streaming → Kafka/PostgreSQL → dashboard.

Requires the full stack (``.\\run.ps1 up``). Tests run in file order. The last one
measures latency and throughput and writes ``docs/E2E_REPORT.md``.
"""

from __future__ import annotations

import time
import urllib.request
from datetime import UTC, datetime
from typing import Any

import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, ConfigResource
from psycopg.rows import dict_row

from src.common import db
from src.common.kafka_utils import topic_end_offsets
from src.common.schemas import CONTROL_MESSAGE_KEY, ControlCommand, PredictionEvent
from src.dashboard.helpers.kafka_io import read_last_control
from src.dashboard.helpers.kpis import total_offset
from tests.e2e.conftest import make_event, publish, wait_for_message, wait_for_row

pytestmark = pytest.mark.e2e

PREDICTION_TIMEOUT_S = 60.0
REPORT_WINDOW_S = 60


def _http_get(url: str, timeout_s: float = 10.0) -> tuple[int, str]:
    with urllib.request.urlopen(url, timeout=timeout_s) as response:
        return response.status, response.read().decode("utf-8", errors="replace")


def _transactions_total(kafka: Any) -> int:
    consumer = Consumer(
        {"bootstrap.servers": kafka.kafka_bootstrap_servers, "group.id": "e2e-offsets"}
    )
    try:
        return total_offset(topic_end_offsets(consumer, kafka.topic_transactions))
    finally:
        consumer.close()


# --------------------------------------------------------------------------- infrastructure
def test_topics_exist_with_expected_layout(kafka: Any) -> None:
    admin = AdminClient({"bootstrap.servers": kafka.kafka_bootstrap_servers})
    metadata = admin.list_topics(timeout=10)
    expected = {
        kafka.topic_transactions: 3,
        kafka.topic_predictions: 3,
        kafka.topic_fraud_alerts: 1,
        kafka.topic_producer_control: 1,
    }
    for topic, partitions in expected.items():
        assert topic in metadata.topics, f"topic {topic} missing"
        assert len(metadata.topics[topic].partitions) == partitions
    resource = ConfigResource(ConfigResource.Type.TOPIC, kafka.topic_producer_control)
    configs = admin.describe_configs([resource])[resource].result(timeout=10)
    assert configs["cleanup.policy"].value == "compact"


def test_producer_is_publishing(kafka: Any) -> None:
    before = _transactions_total(kafka)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        time.sleep(2)
        if _transactions_total(kafka) > before:
            return
    pytest.fail("no new messages on the transactions topic within 20 s")


def test_spark_reports_progress(conn: Any) -> None:
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            "SELECT count(*) AS n, EXTRACT(EPOCH FROM now() - max(recorded_at)) AS age_s "
            "FROM stream_progress"
        )
        row = cursor.fetchone()
    assert row["n"] > 0, "no streaming progress recorded"
    assert row["age_s"] < 60, f"last Spark progress is {row['age_s']:.0f}s old"


# --------------------------------------------------------------------------- data flow
def test_predictions_flow_to_kafka_and_postgres(
    kafka: Any, conn: Any, consumer_at_end: Any
) -> None:
    consumer = consumer_at_end(kafka.topic_predictions)
    received: list[PredictionEvent] = []
    deadline = time.monotonic() + PREDICTION_TIMEOUT_S
    while len(received) < 10 and time.monotonic() < deadline:
        message = consumer.poll(0.5)
        if message is None or message.error():
            continue
        received.append(PredictionEvent.model_validate_json(message.value()))  # schema check
    assert len(received) >= 10, f"only {len(received)} predictions within {PREDICTION_TIMEOUT_S}s"
    assert {event.source.value for event in received} >= {"replay"}
    stored = wait_for_row(conn, str(received[0].transaction_id), timeout_s=15)
    assert stored is not None, "prediction published to Kafka but not stored in Postgres"
    assert stored["fraud_probability"] == pytest.approx(received[0].fraud_probability)


def test_known_fraud_produces_an_alert(
    kafka: Any,
    conn: Any,
    producer: Any,
    consumer_at_end: Any,
    metrics: dict[str, Any],
    demo_samples: dict[str, Any],
) -> None:
    db.set_threshold(conn, metrics["best"]["threshold"])  # model default, as after a reset
    fraud = next(s for s in demo_samples["frauds"] if s["above_threshold"])
    alerts = consumer_at_end(kafka.topic_fraud_alerts)
    event = make_event(fraud)
    publish(producer, kafka.topic_transactions, event)

    alert = wait_for_message(
        alerts, lambda m: m["transaction_id"] == str(event.transaction_id), PREDICTION_TIMEOUT_S
    )
    assert alert is not None, "known fraud did not reach the fraud-alerts topic"
    assert alert["prediction"] == 1 and alert["actual_class"] == 1 and alert["source"] == "e2e"
    assert alert["fraud_probability"] == pytest.approx(fraud["fraud_probability"], abs=1e-5)
    row = wait_for_row(conn, str(event.transaction_id), timeout_s=15)
    assert row is not None and row["prediction"] == 1


def test_known_legitimate_transaction_is_not_flagged(
    kafka: Any, conn: Any, producer: Any, metrics: dict[str, Any], demo_samples: dict[str, Any]
) -> None:
    db.set_threshold(conn, metrics["best"]["threshold"])
    event = make_event(demo_samples["legit"][0])
    publish(producer, kafka.topic_transactions, event)
    row = wait_for_row(conn, str(event.transaction_id), PREDICTION_TIMEOUT_S)
    assert row is not None, "legitimate sample was never scored"
    assert row["prediction"] == 0 and row["actual_class"] == 0


def test_threshold_change_reaches_spark(
    kafka: Any, conn: Any, producer: Any, metrics: dict[str, Any], demo_samples: dict[str, Any]
) -> None:
    """What the dashboard slider does: write the shared threshold, Spark uses it next batch."""
    default = metrics["best"]["threshold"]
    try:
        db.set_threshold(conn, 0.777)
        time.sleep(3)  # the threshold is read once per micro-batch (1 s trigger)
        event = make_event(demo_samples["legit"][1])
        publish(producer, kafka.topic_transactions, event)
        row = wait_for_row(conn, str(event.transaction_id), PREDICTION_TIMEOUT_S)
        assert row is not None
        assert row["threshold"] == pytest.approx(0.777)
    finally:
        db.set_threshold(conn, default)


# --------------------------------------------------------------------------- user interfaces
def test_dashboard_responds(urls: dict[str, str]) -> None:
    status, body = _http_get(f"{urls['dashboard']}/_stcore/health")
    assert status == 200 and body.strip() == "ok"
    status, body = _http_get(urls["dashboard"])
    assert status == 200 and "<html" in body.lower()


def test_kafka_ui_responds(urls: dict[str, str]) -> None:
    status, _ = _http_get(f"{urls['kafka_ui']}/actuator/health")
    assert status == 200


# --------------------------------------------------------------------------- performance report
def test_latency_and_throughput_report(conn: Any, paths: Any) -> None:
    """Measure the replay stream over the last minute and write docs/E2E_REPORT.md."""
    with conn.cursor(row_factory=dict_row) as cursor:
        cursor.execute(
            """
            SELECT count(*)                                                  AS rows,
                   EXTRACT(EPOCH FROM max(processed_at) - min(processed_at)) AS span_s,
                   avg(latency_ms)                                           AS proc_avg,
                   percentile_cont(0.5)  WITHIN GROUP (ORDER BY latency_ms)  AS proc_p50,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)  AS proc_p95,
                   percentile_cont(0.5)  WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM stored_at - event_time) * 1000) AS e2e_p50,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM stored_at - event_time) * 1000) AS e2e_p95,
                   max(EXTRACT(EPOCH FROM stored_at - event_time) * 1000)    AS e2e_max,
                   count(*) FILTER (WHERE prediction = 1)                    AS alerts
            FROM predictions
            WHERE source = 'replay' AND processed_at >= now() - make_interval(secs => %s)
            """,
            (REPORT_WINDOW_S,),
        )
        stats = cursor.fetchone()
        cursor.execute(
            "SELECT avg(batch_duration_ms) AS batch_avg, avg(processed_rows_per_second) AS proc_rate "
            "FROM stream_progress WHERE recorded_at >= now() - make_interval(secs => %s) AND num_input_rows > 0",
            (REPORT_WINDOW_S,),
        )
        spark = cursor.fetchone()

    assert stats["rows"] > 0, "no replay predictions in the last minute"
    throughput = stats["rows"] / max(float(stats["span_s"] or 0.0), 1.0)
    assert throughput > 1.0
    assert stats["e2e_p95"] < 10_000, "95% of transactions should be scored and stored within 10 s"

    report = f"""# End-to-end test report

Generated {datetime.now(UTC):%Y-%m-%d %H:%M:%S} UTC by `tests/e2e/test_pipeline.py`, measured over
the last {REPORT_WINDOW_S} s of replay traffic on the running Docker Compose stack.

| Metric | Value |
|---|---:|
| Transactions scored and stored | {stats["rows"]:,} |
| Throughput (end to end) | {throughput:,.1f} tx/s |
| Alerts in the window | {stats["alerts"]:,} |
| Processing latency, avg (event → batch start) | {stats["proc_avg"]:,.0f} ms |
| Processing latency, p50 / p95 | {stats["proc_p50"]:,.0f} / {stats["proc_p95"]:,.0f} ms |
| **End-to-end latency, p50 / p95** (event → stored in PostgreSQL) | **{stats["e2e_p50"]:,.0f} / {stats["e2e_p95"]:,.0f} ms** |
| End-to-end latency, max | {stats["e2e_max"]:,.0f} ms |
| Spark micro-batch duration, avg | {float(spark["batch_avg"] or 0):,.0f} ms |
| Spark processing rate, avg (rows/s while busy) | {float(spark["proc_rate"] or 0):,.1f} |

Processing latency covers Kafka queueing and the 1 s trigger wait. End-to-end latency also
includes model inference and the writes to Kafka and PostgreSQL.
"""
    paths.docs_dir.mkdir(parents=True, exist_ok=True)
    (paths.docs_dir / "E2E_REPORT.md").write_text(report, encoding="utf-8")
    print("\n" + report)


# --------------------------------------------------------------------------- producer control (last: it pauses the stream)
def test_producer_pause_and_resume(kafka: Any, producer: Any) -> None:
    last = read_last_control(kafka.kafka_bootstrap_servers, kafka.topic_producer_control)
    paused_before = bool(last.paused) if last else False
    rate_before = float(last.rate) if last else 50.0
    assert not paused_before, "the producer was paused before the test started"

    def send(action: str, paused: bool) -> None:
        command = ControlCommand.from_action(
            action,
            current_paused=paused,
            current_rate=rate_before,
            issued_by="e2e",  # type: ignore[arg-type]
        )
        producer.produce(
            kafka.topic_producer_control, key=CONTROL_MESSAGE_KEY, value=command.to_json_bytes()
        )
        assert producer.flush(10) == 0

    try:
        send("pause", False)
        time.sleep(4)  # let in-flight messages land
        paused_start = _transactions_total(kafka)
        time.sleep(5)
        assert _transactions_total(kafka) == paused_start, "producer kept publishing while paused"
    finally:
        send("resume", True)
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        time.sleep(2)
        if _transactions_total(kafka) > paused_start:
            return
    pytest.fail("producer did not resume")
