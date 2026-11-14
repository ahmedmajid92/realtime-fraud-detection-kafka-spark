# PROGRESS

| Phase | Status | Date |
|---|---|---|
| 1. Planning | ✅ Done, plan approved | 2026-11-14 |
| 2. Implementation | ✅ Done, model trained | 2026-11-14 |
| 3. Unit testing | ✅ Done: 457 passing, 95.9% coverage | 2026-11-14 |
| 4. Independent review (Claude Fable 5.2) | ✅ Done: 14 findings (0 Critical, 2 High, 7 Medium, 5 Low) | 2026-11-14 |
| 5. Apply review findings | ✅ Done: 12 fixed, 1 partly fixed, 1 not reproduced; 487 tests pass | 2026-11-14 |
| 6. End-to-end Docker test | ✅ Done: 11/11 e2e pass; e2e latency p50 1.7 s / p95 2.6 s at 51 tx/s | 2026-11-14 |
| 7. Delivery | ✅ Done: README, demo script, URLs verified | 2026-11-14 |

---

## After delivery

- **Architecture diagram, sharper:** the About page used to draw the diagram in the browser (small text, broken Arabic layout). It now shows a high-resolution PNG rendered server-side by Graphviz from one bilingual DOT source (`src/dashboard/helpers/architecture.py`, `scripts/render_architecture.py`, `.un.ps1 diagrams`): five compact rows, two-line labels, and proper Arabic shaping. The same files (`docs/figures/architecture{,-ar}.{png,svg}`) are used by the README and the report. The Mermaid source was retired.
- **[docs/SYSTEM_OVERVIEW.md](docs/SYSTEM_OVERVIEW.md):** a short explanation of the whole system (theory, how it works, experiments).
- Unit tests: **538 pass**, coverage 96.0%, `ruff` clean.

---

## Phase 7: Delivery

- **[README.md](README.md)** (complete): overview and architecture diagram, Windows prerequisites, dataset setup, step-by-step PowerShell commands, train/start/stop/reset, unit and e2e tests, a manual UI test checklist, model results, streaming performance, configuration, project structure, and Windows/Docker troubleshooting.
- **[docs/DEMO_SCRIPT.md](docs/DEMO_SCRIPT.md)**: a 10-minute presentation script with talking points, likely questions, and a fallback plan.
- **URLs verified from the Windows host (HTTP 200):** dashboard http://localhost:8501 (Arabic: `/?lang=ar`), Kafka UI http://localhost:8081, Spark UI http://localhost:4040. All six long-running containers are healthy.

---

## Phase 6: End-to-end Docker test

After your go-ahead: `docker compose up -d --build` (via `.
un.ps1 up`), health check of every container, the automated e2e suite ([tests/e2e/test_pipeline.py](tests/e2e/test_pipeline.py), run with `.
un.ps1 e2e`), dashboard screenshots, and a latency/throughput report.

**Containers:** kafka, postgres, kafka-ui, producer, stream-processor and dashboard are all *running (healthy)*; kafka-init and trainer exited 0 (topics created; training skipped because the model exists). Memory: about **2.6 GB** for the whole stack (stream processor 1.5 / 2 GB, Kafka 0.5 / 1 GB).

**E2E tests: 11/11 pass.**

| # | Test | Checks |
|---|---|---|
| 1 | topics exist | 4 topics, partitions 3/3/1/1, `producer-control` is compacted |
| 2 | producer publishing | `transactions` end offsets grow |
| 3 | Spark progress | `stream_progress` rows less than 60 s old |
| 4 | predictions flow | 10 messages on `predictions` validate against `PredictionEvent` and are stored in Postgres |
| 5 | **known fraud → alert** | a test-split fraud sent with `source=e2e` arrives on `fraud-alerts` with `prediction=1` |
| 6 | known legit → no alert | stored with `prediction=0` |
| 7 | threshold reaches Spark | writing 0.777 to `settings` (what the slider does) labels the next transaction with 0.777 |
| 8 | dashboard HTTP | `/_stcore/health` = `ok`, `/` returns HTML |
| 9 | Kafka UI HTTP | `/actuator/health` = 200 |
| 10 | performance report | writes [docs/E2E_REPORT.md](docs/E2E_REPORT.md) |
| 11 | producer pause / resume | no new messages while paused, and publishing resumes |

**Performance** (last 60 s of replay traffic, quiet machine):

| Metric | Before optimization | **After** |
|---|---:|---:|
| Throughput | 51.1 tx/s | **51.0 tx/s** (target 50) |
| Micro-batch duration (~50–120 rows) | ~2.2 s | **~0.8 s** (steady state) |
| Processing latency p50 / p95 | 2,516 / 4,037 ms | **578 / 1,198 ms** |
| **End-to-end latency p50 / p95** (event → stored in Postgres) | 3,322 / 4,904 ms | **1,673 / 2,557 ms** |

**Problems found and fixed in this phase**

1. **Port conflict:** a native PostgreSQL on this machine already listens on 5433. The stack now publishes Postgres on **15432** (`POSTGRES_HOST_PORT`, configurable).
2. **Latency:** per-step timing showed that the 2.2 s batches were fixed overhead, mostly rebuilding the model pipeline in every `foreachBatch`. Scoring moved into the streaming plan (built once); invalid-row counts became observed metrics; batches are coalesced to one task; separate count/stats jobs were merged. Batches got about 2.7× faster and processing latency about 4× lower.
3. **Deadlock avoided:** a first attempt used `Observation` for batch statistics. Its metrics are never delivered when the first action is a Kafka write over cached data, so `Observation.get` blocked the query. This was caught live in Docker (the unit test's fake sink didn't trigger it), replaced by one aggregation, and documented in the code.
4. Test-only: the fixture model kept its training summary, which isn't serializable once an `Observation` exists. The fixture now saves and reloads the model, as production does.
5. Screenshots: the Playwright image lacks the Python package (installed, pinned 1.64.1); the default page lives at `/`; a **`?lang=ar` URL parameter** opens the dashboard in Arabic.
6. UI polish seen in the screenshots: neutral legend swatches on the comparison chart; numbers with units stay left-to-right in Arabic mode.

**Screenshots:** [docs/screenshots/](docs/screenshots/) has 5 pages × English/Arabic, including a real flagged fraud on the Test page.

Unit tests after this phase: **491 pass**, coverage 95.9%, `ruff` clean.

---

## Phase 4: Independent review

A `code-reviewer` subagent ([.claude/agents/code-reviewer.md](.claude/agents/code-reviewer.md), model **`claude-fable-5-2`**, read-only tools plus `Write` for `REVIEW.md` only) reviewed the whole repository against the brief's checklist. Agent definitions are loaded when a session starts, so this session ran a Fable 5.2 agent with the same instructions; in later sessions you can call `code-reviewer` directly. Findings: **0 Critical, 2 High, 7 Medium, 5 Low**. No direct label leakage was found, and model selection and threshold tuning never read the test split. Full report: [REVIEW.md](REVIEW.md).

## Phase 5: Apply review findings

Every finding was checked against the code. The four the reviewer could not quantify were measured first (R2, R3 on the real data; R7, R13 with Streamlit's `AppTest`). The outcome of each is recorded in [REVIEW.md](REVIEW.md#resolution-phase-5):

- **Fixed (12):** R1 poison-pill validation (UUID, ±Infinity, `replay_pass`); R3 group-aware split (4.2% of test rows had a feature twin in train/val, now 0); R4 retrain recreates producer and stream processor; R6 "fraud score" labels and calibration note; R7 dashboard uses Spark's threshold, unrounded, safe slider range (an out-of-range value used to crash every page); R8 retention (6 h), topic size caps, lz4; R9 new tests; R10 latency definition; R11 unknown control state, wait for retained state, listener reconnect; R12 least-privilege env and `.kaggle/` ignored; R13 rate input range; R14 doc corrections.
- **Partly fixed (1):** R5. Bootstrap 95% confidence intervals added everywhere; k-fold CV deferred, because it conflicts with the 70/15/15 design you chose and would take about 5× longer to train.
- **Not reproduced (1):** R2. Measured: no saturated scores; AUC-PR is identical on `probability` and `rawPrediction`. Found along the way and fixed: the exact split had left training data in 2 Spark partitions (nearly single-threaded training).

**Verification:** `ruff` clean · **487 unit tests pass** · coverage **95.9%** · model retrained (7.6 min).

### Current model results (test split: 42,557 transactions, 70 frauds)

| Model | Imbalance | Val AUC-PR | **Test AUC-PR** | Test AP 95% CI | Precision | Recall | F1 | Threshold | TP/FP/FN |
|---|---|---:|---:|:---:|---:|---:|---:|---:|---:|
| Logistic Regression | class weights | 0.7637 | 0.7151 | 0.608–0.828 | 0.754 | 0.743 | 0.748 | 0.958 | 52/17/18 |
| Logistic Regression | undersampling | 0.8077 | 0.7375 | 0.625–0.846 | 0.825 | 0.743 | 0.782 | 0.982 | 52/11/18 |
| **Random Forest ★** | **class weights** | **0.8780** | **0.8283** | **0.750–0.906** | 0.964 | 0.757 | 0.848 | 0.480 | 53/2/17 |
| Random Forest | undersampling | 0.8328 | 0.7603 | 0.646–0.869 | 0.900 | 0.643 | 0.750 | 0.910 | 45/5/25 |
| Gradient-Boosted Trees | class weights | 0.7903 | 0.6941 | 0.584–0.803 | 0.906 | 0.686 | 0.780 | 0.972 | 48/5/22 |
| Gradient-Boosted Trees | undersampling | 0.5886 | 0.5442 | 0.446–0.702 | 0.732 | 0.743 | 0.738 | 0.977 | 52/19/18 |
| Linear SVM (Platt) | class weights | 0.7035 | 0.6779 | 0.567–0.795 | 0.804 | 0.586 | 0.678 | 0.169 | 41/10/29 |
| Linear SVM (Platt) | undersampling | 0.8519 | 0.7553 | 0.647–0.863 | 0.897 | 0.743 | 0.812 | 0.144 | 52/6/18 |

**Deployed:** Random Forest with class weights (`maxDepth=12`), threshold **0.4797**. It catches 53 of 70 test frauds with **2 false alarms** among 42,487 legitimate transactions. The next-best models' intervals overlap RF's, so the ranking among the top three is not statistically conclusive; RESULTS.md says so.

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

> **Superseded** by the Phase 5 retrain (group-aware split). See *Phase 5* above for the current numbers.

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
- GBT is unstable between validation and test (0.48 vs 0.77 AUC-PR). *(Corrected in Phase 5: an earlier version of this note blamed probability saturation and tied scores. Measured on the real splits, no score equals 1.0, and AUC-PR is identical on `probability` and `rawPrediction`. The instability is GBT's sensitivity to the data partitioning used when it bins features, combined with only about 70 frauds per split.)* Small-sample variance also explains why the selected RF scores higher on test (0.84) than on validation (0.80).

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
