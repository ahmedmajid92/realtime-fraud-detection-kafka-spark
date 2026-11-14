# Demo script (about 10 minutes)

A step-by-step script for presenting the project live. Times are approximate; the **bold** lines are what to say.

## Before the presentation (10 minutes ahead)

1. Start Docker Desktop, then in PowerShell, in the project folder:
   ```powershell
   .\run.ps1 up        # waits until every service is healthy and prints the URLs
   ```
2. Open four browser tabs:
   - Dashboard: http://localhost:8501
   - Dashboard in Arabic: http://localhost:8501/?lang=ar
   - Kafka UI: http://localhost:8081
   - Spark UI: http://localhost:4040
3. In the dashboard sidebar, check: threshold **0.4797** (click *Reset to model default* if not), stream **Running · 50 tx/s**.
4. Keep `docs/figures/architecture.png` and `docs/RESULTS.md` open as a backup in case the network or Docker misbehaves.

---

## 1. The problem (≈1 min)

**"Card fraud detection must happen in real time, and the data is extremely imbalanced: in the Kaggle dataset, 492 of 284,807 transactions (0.17%) are fraud. I built a complete streaming system on the Hadoop ecosystem, with Kafka for the stream, Spark MLlib for training and Spark Structured Streaming for scoring, plus a dashboard."**

## 2. Architecture (≈1.5 min): *Architecture / About* page

Walk the diagram from left to right:
- **"Offline, Spark MLlib trains and compares models and saves a PipelineModel."**
- **"A producer replays only the held-out test split into Kafka, 50 transactions per second. The model never saw these transactions during training."**
- **"Spark Structured Streaming scores each micro-batch, writes every result to `predictions`, the alerts to `fraud-alerts`, and everything to PostgreSQL. It uses checkpointing, and the database writes are idempotent."**
- **"Everything runs in Docker with one command, with Kafka in KRaft mode, so there's no ZooKeeper."**

## 3. The model (≈2 min): *Model Performance* page

- Point at the comparison table: **"Four algorithms (Logistic Regression, Random Forest, Gradient-Boosted Trees and a linear SVM), each with two imbalance strategies: class weights and undersampling."**
- Open the **ROC** tab, then the **Precision–Recall** tab: **"ROC-AUC is about 0.95–0.98 for every model, so it can't tell them apart. AUC-PR ranges from 0.54 to 0.83, which is why AUC-PR is my selection metric."**
- **"The model is selected on the validation split, and the test split is used once, for these numbers. Random Forest with class weights has test AUC-PR 0.83. The 95% bootstrap interval is 0.75 to 0.91: with only 70 test frauds there's real uncertainty, and I report it."**
- Open **Threshold sweep**: **"The alert threshold, 0.48, maximises F1 on validation. At that threshold the model catches 53 of 70 frauds with only 2 false alarms out of 42,000 legitimate transactions."**
- Leakage (likely question): **"I removed duplicates before splitting, kept transactions that are identical apart from the timestamp in the same split, and the label is never a feature."**

## 4. The live stream (≈2 min)

- *Live Monitor*: **"These numbers refresh every 2 seconds: about 50 transactions per second, latency under a second from event to scoring, and a fraud rate around 0.15%."** Point at the red rows in **Fraud alerts**, then **Live model quality**: **"Because the replayed transactions carry their true label for monitoring only, I can show live precision and recall. They match the offline test results."**
- **Kafka UI** tab: Topics → `fraud-alerts` → Messages → open one JSON message. **"This is a real alert on the Kafka topic."**
- **Spark UI** tab → *Structured Streaming*: **"The streaming query: input rate and processing rate are balanced, so Spark keeps up."**

## 5. Interactivity (≈2 min)

1. *Test a Transaction*: click **🎲 Random fraud**, then **📨 Send via Kafka**. **"This goes through the real pipeline (Kafka, then Spark, then Postgres), not a shortcut. Here's the score, the decision and the end-to-end latency."** (If this fraud is missed, say: **"This one is a false negative: recall is about 76%, so some frauds are missed,"** and send another.)
2. Sidebar **Alert threshold**: drag it to about 0.9. **"The threshold is shared with Spark through the database: the next micro-batch uses it, and the dashboard re-labels immediately. Fewer alerts, higher precision, lower recall."** Then **Reset to model default**.
3. Sidebar **Pause**, then point at *System Health* (Producer: Paused; the topic stops growing), then **Resume**. **"The producer is controlled live through a compacted Kafka topic."**

## 6. Engineering quality (≈1 min)

**"The project has 491 unit tests at 96% coverage and 11 end-to-end tests that run against the live Docker stack, including one that injects a known fraud and checks that the alert appears on Kafka. An independent AI code review found 14 issues; I fixed or answered every one, for example making the stream processor reject malformed messages and closing a small train/test overlap. I also measured and halved the end-to-end latency: the median is now 1.7 seconds."**

## 7. Arabic interface (≈30 s)

Switch the sidebar language to **العربية** (or use the `?lang=ar` tab): **"The whole interface is bilingual, with right-to-left layout."**

---

## Likely questions, and short answers

| Question | Answer |
|---|---|
| Why Kafka and Spark instead of a simple script? | They decouple producers and consumers, keep the data durable and replayable, scale horizontally (partitions, executors), and the checkpointing gives fault tolerance. The same code runs on a cluster. |
| Why not SMOTE? | Spark MLlib has no SMOTE. I compared the two standard alternatives, class weights and undersampling, and reported both. |
| Is the score a probability? | It ranks transactions correctly, but only the SVM is calibrated. That's why the UI calls it a *fraud score*. |
| How do you avoid data leakage? | Dedup before the split, feature twins kept in one split, the label never a feature, the test split used only for the final metrics and for the stream. |
| What happens if Spark crashes? | Docker restarts it, and the checkpoint resumes from the last committed Kafka offsets. Postgres writes are idempotent; Kafka outputs are at-least-once. |
| Why is GBT worse? | With extreme class weights and few frauds it overfits and is unstable between splits; the confidence intervals show it. |
| How would this scale? | More Kafka partitions, a Spark cluster instead of `local[*]`, and a larger trigger interval or `maxOffsetsPerTrigger`. The code doesn't change. |

## If something goes wrong during the demo

- The dashboard says *Database unavailable* or the stream looks stalled: run `.\run.ps1 status`, then `.\run.ps1 up` (it restarts unhealthy services).
- There's no time to fix it: show `docs/screenshots/` (every page, English and Arabic) and `docs/E2E_REPORT.md`.
