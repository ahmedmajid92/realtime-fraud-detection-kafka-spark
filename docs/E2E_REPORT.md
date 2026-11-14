# End-to-end test report

Generated 2026-11-14 18:57:05 UTC by `tests/e2e/test_pipeline.py`, measured over
the last 60 s of replay traffic on the running Docker Compose stack.

| Metric | Value |
|---|---:|
| Transactions scored and stored | 2,959 |
| Throughput (end to end) | 51.0 tx/s |
| Alerts in the window | 3 |
| Processing latency, avg (event → batch start) | 598 ms |
| Processing latency, p50 / p95 | 578 / 1,198 ms |
| **End-to-end latency, p50 / p95** (event → stored in PostgreSQL) | **1,673 / 2,557 ms** |
| End-to-end latency, max | 3,369 ms |
| Spark micro-batch duration, avg | 1,137 ms |
| Spark processing rate, avg (rows/s while busy) | 50.5 |

Processing latency covers Kafka queueing and the 1 s trigger wait. End-to-end latency also
includes model inference and the writes to Kafka and PostgreSQL.
