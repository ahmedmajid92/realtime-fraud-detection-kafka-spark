# PROGRESS

| Phase | Status | Date |
|---|---|---|
| 1. Planning | ✅ Done, plan approved | 2026-11-14 |
| 2. Implementation | ✅ Done, model trained | 2026-11-14 |
| 3. Unit testing | ✅ Done: 457 passing, 95.9% coverage | 2026-11-14 |
| 4. Independent review (Claude Fable 5.2) | ⏳ Next | |
| 5. Apply review findings | ⏳ Not started | |
| 6. End-to-end Docker test | ⏳ Not started (needs your go-ahead) | |
| 7. Delivery | ⏳ Not started | |

---

## Phase 3: Unit testing

Run with `.\run.ps1 test` (pytest in the `fraud-spark` test image, with a local `SparkSession` fixture). There are **190 test functions and 457 test cases** (209 of the cases are per-key translation-placeholder checks). The run takes about 3 min. Kafka and PostgreSQL are replaced by in-memory fakes ([tests/unit/fakes.py](tests/unit/fakes.py)).

| Area (from the brief) | Test module | What is covered |
|---|---|---|
| Config loading | `test_config.py` | env parsing, required endpoints fail fast, secret masking, range checks, blank override → `None` |
| Message schemas | `test_schemas.py` | JSON round-trip and `Z` timestamps, rejection of bad values and unknown fields (incl. `Class`), pydantic ↔ Spark field parity, label not in features, full-state control commands |
| Preprocessing | `test_data_prep.py` | header check, label validation, dedup count, **exact, disjoint, order-independent stratified split**, class weights, undersampling ratio, sorted Parquet output |
| Feature pipeline and model | `test_features_models.py`, `test_evaluate.py`, `test_report.py`, `test_train_model.py` | hour sin/cos and log-amount values, standardised 31-dim vector, model specs, Platt-calibrated SVM, **model save → load → predict on sample rows**, metric maths, **unbinned AUC-PR regression test**, figures and RESULTS.md, a full training run on synthetic data |
| Producer (Kafka mocked) | `test_producer.py` | rate limiter (spacing, catch-up, burst cap), control listener thread, pause/resume/rate pick-up, looping passes, serialization, full-queue retry, `main()` end to end |
| Stream processor (static DataFrames) | `test_streaming.py` | parsing, 8 kinds of malformed input filtered, scoring, latency/hour enrichment, inclusive threshold, Kafka JSON ↔ schema, Postgres batching, threshold fallback, listener, `foreachBatch` fan-out, Kafka source on the classpath |
| Dashboard helpers | `test_dashboard_*.py` | KPIs (throughput, lag, quality), formatting and row highlighting, test-page samples and edits, SQL wrappers, Kafka publish/read, chart structure and palette roles, EN/AR key completeness |

**Coverage: 95.9%** of core logic (statements + branches; Streamlit page glue is excluded as planned). The one module below target is `stream_processor.py` at 41%: its `main()` needs a live Kafka and Postgres and is exercised by the Phase 6 e2e test. Everything it calls is unit-tested.

**Bugs found by the tests and fixed:** a CSV with a renamed column passed validation, because Spark only checks the header for columns a query actually reads. The loader now checks the header explicitly and raises a clear `DatasetValidationError`.

---

## Phase 2: Implementation

### What was built

| Component | Files |
|---|---|
| Shared config, schemas, logging, Kafka/DB helpers | [src/common/](src/common/) |
| Training job (Spark MLlib, 4 models × 2 strategies, 16 grid fits) | [src/training/](src/training/) |
| Producer (paced replay of the test split, live control) | [src/producer/](src/producer/) |
| Stream processor (Structured Streaming, `foreachBatch` → Kafka + Postgres) | [src/streaming/](src/streaming/) |
| Dashboard (Streamlit, 5 pages, EN/AR) | [src/dashboard/](src/dashboard/) |
| Docker images, Compose stack, topic and DB init | [docker/](docker/), [docker-compose.yml](docker-compose.yml) |
| Windows entry point | [run.ps1](run.ps1) (`setup`, `download-data`, `train`, `up`, `down`, `reset`, `test`, `e2e`, `logs`, `status`, `screenshots`, `diagrams`) |
| Report assets | [docs/RESULTS.md](docs/RESULTS.md), [docs/figures/](docs/figures/) (architecture SVG/PNG, PR, ROC, confusion matrix, threshold sweep, model comparison) |
| Dependency pins | [requirements/](requirements/): direct pins per image, plus `constraints.txt` with all 67 transitive versions |

### Verified in this phase

- Both images build. The Kafka connector JARs (`spark-sql-kafka-0-10_2.13:4.1.4`, token provider, `kafka-clients 3.9.1`, `commons-pool2 2.12.1`) are downloaded at build time and SHA-1 checked.
- Runtime versions inside the images: PySpark 4.1.4, Python 3.12.15, Java 17.0.21.
- `ruff check` and `ruff format --check` pass on all code. Every module imports in its image. Every Streamlit 1.66 argument used was checked against the installed API.
- **Streaming smoke test** (static DataFrame, no Kafka): 2 malformed messages were dropped; streaming probabilities match offline training within 2.8e-7; every Kafka JSON record validates against the shared `PredictionEvent` schema.
- Training on the full dataset takes **5.6 min** with 4 CPUs.

### Model results (test split, 70 frauds out of 42,557 transactions)

Selection uses validation AUC-PR. Each row's threshold maximises F1 on validation.

| Model | Imbalance | Val AUC-PR | **Test AUC-PR** | Test ROC-AUC | Precision | Recall | F1 | Threshold | TP/FP/FN |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Logistic Regression | class weights | 0.7603 | 0.6928 | 0.9909 | 0.676 | 0.686 | 0.681 | 0.945 | 48/23/22 |
| Logistic Regression | undersampling | 0.7804 | 0.6985 | 0.9913 | 0.709 | 0.800 | 0.752 | 0.947 | 56/23/14 |
| **Random Forest ★** | **class weights** | **0.7985** | **0.8408** | 0.9801 | 0.767 | 0.800 | 0.783 | 0.386 | 56/17/14 |
| Random Forest | undersampling | 0.7423 | 0.7050 | 0.9873 | 0.800 | 0.800 | 0.800 | 0.760 | 56/14/14 |
| Gradient-Boosted Trees | class weights | 0.4776 | 0.7679 | 0.9756 | 0.823 | 0.729 | 0.773 | 0.926 | 51/11/19 |
| Gradient-Boosted Trees | undersampling | 0.3553 | 0.4210 | 0.9863 | 0.679 | 0.786 | 0.728 | 0.988 | 55/26/15 |
| Linear SVM (Platt) | class weights | 0.7596 | 0.6722 | 0.9931 | 0.641 | 0.714 | 0.676 | 0.153 | 50/28/20 |
| Linear SVM (Platt) | undersampling | 0.7869 | 0.7295 | 0.9913 | 0.812 | 0.800 | 0.806 | 0.201 | 56/13/14 |

**Deployed model:** Random Forest with class weights (`maxDepth=12`, 100 trees), alert threshold **0.386**. It catches 56 of the 70 test frauds with 17 false alarms among 42,487 legitimate transactions.

Observations for the report:
- ROC-AUC barely separates the models (0.976–0.993), while AUC-PR ranges from 0.42 to 0.84. That's the practical case for AUC-PR on data with 0.17% fraud.
- Class weights vs undersampling depends on the model. Undersampling slightly helps the linear models (LR, SVM) and clearly hurts the tree ensembles, which lose 99% of the legitimate examples.
- GBT is unstable between validation and test (0.48 vs 0.77 AUC-PR). Its probabilities saturate near 1, which creates many tied scores, and each split has only about 70 frauds. Small-sample variance also explains why the selected RF scores higher on test (0.84) than on validation (0.80).

### Problems found and fixed during Phase 2

1. **Compose image-tag collision.** Two services built the same `fraud-app:1.0.0` tag in parallel and the build failed. Fixed by letting Compose name each service's image; shared layers are still cached.
2. **Logging bug.** Module-level loggers were bound before `configure_logging()` ran, so the training logs came out as console text instead of JSON. Fixed with lazy loggers.
3. **The stratified split wasn't exact.** Per-class `randomSplit` gave 321/89/63 frauds. Replaced with an exact, order-independent split (rank by seeded `xxhash64` within each class): now **332/71/70**, with an identical fraud rate in every split.
4. **AUC-PR was distorted by MLlib's default binning.** `BinaryClassificationEvaluator` down-samples curves into 1,000 bins, about 42 rows each when there are only about 70 frauds. GBT read 0.504 instead of the true 0.694. Fixed with `numBins=0`; MLlib AUC-PR now agrees with scikit-learn's average precision to within about 0.002 for most rows. Model selection now uses the exact metric.
5. **PR/ROC curve artifacts.** Down-sampling the curve points by index drew misleading straight lines. Fixed with `drop_intermediate=True`, which keeps only the corner points.
6. **Producer-control design bug.** The topic is compacted, so a lone `set_rate` message would erase an earlier `pause` after compaction. Each control message now carries the full desired state (`paused` + `rate`).
7. mermaid-cli options (`-p` puppeteer config, `--size`) and figure legend/annotation overlaps.

Deviations from PLAN.md (all recorded there): dashboard pages live in `views/` (a folder named `pages/` would trigger Streamlit's legacy multipage mode); the Kaggle download is `.\run.ps1 download-data`; there is no separate `serializer.py`; the stream processor is split into `batch.py` and `threshold.py`.

### Not yet verified (Phase 6)

The full running stack: Kafka/Postgres sinks under load, healthchecks (including the Kafka UI `/actuator/health` endpoint), the dashboard pages in a browser, live threshold/producer control, latency and throughput, and screenshots.

---

## Phase 1: Planning

**Environment detected:** Windows 11 Pro (26300), 31.7 GB RAM, 20 logical CPUs, Docker Desktop 28.5.1 with WSL2 (Ubuntu) and about 15.5 GB available to Docker, Compose v2.40, Python 3.13.2, Java 24, Git 2.49.

**Done:** clarifying questions in 9 groups (answers in PLAN.md §2); versions checked on Docker Hub, PyPI and Maven Central; PLAN.md written (architecture, components, topics and schemas, versions, Compose layout, test strategy, risks) and approved.
