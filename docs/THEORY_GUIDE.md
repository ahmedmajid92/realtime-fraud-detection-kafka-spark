# Theory Guide: The System Explained from Zero

This guide is for a student who has never heard of Kafka, Spark or machine learning. It explains every idea the system uses, in plain words. A word in **bold** is explained where it first appears.

---

## 1. The big picture

A bank receives thousands of card payments every minute, and a few of them are **fraud**: someone using a stolen card. We want a program that looks at every payment **the moment it happens** and raises an alarm if the payment looks suspicious.

To build this, we need four things:

1. A way to **learn what fraud looks like** from past payments. This is **machine learning**.
2. A way to **move payments quickly and safely** from where they are created to where they are checked. This is **Apache Kafka**.
3. A way to **check many payments very fast**. This is **Apache Spark**.
4. A place to **store the results** and a **screen to show them**. These are a **database** and a **dashboard**.

```
past payments ──► learn (machine learning) ──► model
                                                 │ (used for checking)
new payments ──► Kafka ──► Spark checks each one ──► results ──► database ──► dashboard
```

## 2. Basic words

| Word | Meaning | Example in this system |
|---|---|---|
| **Data set** | A table of collected examples | 284,807 real card payments |
| **Record** (row) | One example in the table | One payment |
| **Feature** (column) | A measured property of a record | Amount, time of day |
| **Label** | The correct answer for a record | Fraud (1) or legitimate (0) |
| **Model** | A program that learned from examples and outputs an answer for new records | Takes a payment, outputs a fraud score |
| **Score** | The model's output: a number from 0 to 1 | Near 1 means "looks like fraud" |

**Big data** means data that is too large, or arrives too fast, for one ordinary program to handle. The solution is to **split the work** across many processors or computers. Kafka and Spark are two famous big data tools that do exactly this.

**Batch vs. stream processing:**
- **Batch processing** handles a fixed set of data all at once, like washing all the week's dishes on Sunday. The system learns its model this way.
- **Stream processing** handles data continuously, piece by piece as it arrives, like washing each dish right after using it. The system checks payments this way.

## 3. Apache Kafka: the post office between programs

**What is it?** Kafka is free software that **delivers messages between programs** and **keeps a copy of every message** in order. Think of it as a post office that also keeps a photocopy of every letter.

**Why do we need it?** The program that creates payments and the program that checks them run at different speeds, and either one may crash and restart. Kafka sits between them. The sender drops messages into Kafka, and the reader picks them up whenever it is ready. Nothing is lost, and neither side has to wait for the other. This is called **decoupling**.

**Key terms:**
- **Message:** one small piece of data, here one payment. It is written as **JSON**, a simple text format such as `{"Amount": 25.0, "Time": 3600}`.
- **Broker:** the Kafka server program that stores the messages. Several brokers working together form a **cluster**; this project uses one.
- **Topic:** a named channel for one kind of message, like a labelled mailbox. This system has four:
  - `transactions`: the payments;
  - `predictions`: every result;
  - `fraud-alerts`: only the suspicious results;
  - `producer-control`: commands such as "pause".
- **Producer:** a program that **writes** messages to a topic.
- **Consumer:** a program that **reads** messages from a topic.
- **Log:** a topic is stored as a list where new messages are only **added at the end**, never changed, like lines in a diary.
- **Offset:** the position number of a message in that list, like a line number. Each consumer remembers its offset, like a bookmark. It can stop, continue later from the same place, or go back and re-read (**replay**).
- **Partition:** a topic can be split into several parts so that several readers can work **at the same time** (**in parallel**). Each message has a **key** (here, the payment's ID), and messages with the same key always go to the same partition, so their order is kept.
- **Retention:** old messages are deleted after a time or size limit, so the disk doesn't fill up.
- **Log compaction:** a special rule that keeps only the **newest message for each key**, like a whiteboard where you erase the old value before writing the new one. The `producer-control` topic uses it, so it always holds the current command.
- **Reliable delivery:**
  - Kafka confirms a message only after it is safely saved.
  - If a message is sent twice because of a network problem, Kafka stores it only once.
  - Messages are **compressed** (made smaller) to save space.
- **KRaft:** Kafka's built-in method for keeping track of its own settings, such as which topics exist. Older versions needed a separate program called ZooKeeper for this.

## 4. Apache Spark: the team of workers

**What is it?** Spark is free software that **processes large amounts of data fast** by splitting the work among many workers. Imagine grading one million exam papers: one teacher would take months, but if a manager splits the papers into piles and gives one pile to each of 100 teachers, it takes a day.

**Key terms:**
- **CPU core:** one processing unit inside a computer's processor. A laptop has about 4–16 cores, and each can work on a different pile at the same time.
- **Driver and tasks:** the **driver** is the manager program. It splits the job into small **tasks** (the piles) and sends them to the cores. `local[*]` means "use all the cores of this one computer".
- **DataFrame:** Spark's table, like a spreadsheet. Its rows are spread across the workers.
- **Lazy evaluation:** Spark first writes a **plan** (a to-do list) of all the steps. It looks for the fastest way to do them, and only runs them when a result is actually needed.

### Spark MLlib: machine learning in Spark
**MLlib** is Spark's machine-learning library. Its key idea is the **pipeline**: a fixed recipe of preparation steps followed by the model. Once trained, the whole recipe is saved as one **PipelineModel**. Live payments go through **exactly the same recipe** as the training data. If they were prepared even slightly differently, the model would give wrong answers; this mistake is called **train/serve skew**.

### Spark Structured Streaming: Spark for never-ending data
- **Micro-batches:** every 1 second (the **trigger**), Spark takes the new messages that arrived in Kafka, checks them as one small batch, and writes the results.
- **Checkpoint:** after each batch, Spark saves its bookmarks (the Kafka offsets) to disk, like saving a video game. If it crashes, it continues from the last save, so no payment is skipped.
- **`foreachBatch`:** lets Spark send each batch's results to **several destinations**: two Kafka topics and the database.
- **Delivery guarantees:** what happens to a message if something fails.
  - *At-most-once:* a message may be lost.
  - *At-least-once:* never lost, but maybe written twice.
  - *Exactly-once:* written exactly one time.

  After a crash, Spark redoes the last batch, so its writes are *at-least-once*. The database write is **idempotent**, meaning that doing it twice has the same effect as doing it once, like pressing a lift button twice. It uses `INSERT … ON CONFLICT DO NOTHING`, which means "if this payment ID is already stored, skip it". So each payment is stored **exactly once**.

## 5. The other tools

- **PostgreSQL:** a **database**, a program that stores tables permanently on disk. It answers questions written in **SQL**, the standard language for asking a database for data. Here it stores every result and the current alarm setting.
- **Streamlit:** a Python library for building web pages with charts and buttons. The **dashboard**, the screen that shows live results, is built with it.
- **Docker:** packs each program together with everything it needs into a **container**, like a shipping container, so it runs the same on any computer. **Docker Compose** starts all the containers with one command.

## 6. Machine learning basics

**Machine learning** means the computer **learns rules from examples** instead of a person writing the rules by hand.

- **Training:** the model makes guesses on examples whose answers are known. A number called the **loss** measures how wrong the guesses are. The model adjusts its internal numbers (**weights**) step by step to make the loss smaller.
- **Overfitting:** the model memorises the training examples instead of learning the general pattern, like a student who memorises last year's answers and then fails a new exam. The opposite, doing well on new data, is called **generalization**.
- **Train / validation / test split (70% / 15% / 15%):**
  - **Training set:** the practice exercises. The model learns from these.
  - **Validation set:** the mock exam, used to choose the best model and its settings.
  - **Test set:** the final exam, used **once** at the very end to get an honest score.
- **Stratified split:** each of the three parts gets the same share of fraud cases, so no part has too few frauds by bad luck.
- **Data leakage:** when information from the test set accidentally reaches training, so the score looks better than it really is, like seeing exam questions in advance. Two protections are used:
  - **Exact duplicate rows are removed.**
  - A **group-aware split** keeps rows that are identical except for their time in the same part. It does this with a **hash**, a fixed "fingerprint" number computed from a row's values: identical rows get the same fingerprint, so they land in the same part.

## 7. Preparing the features

- **PCA (Principal Component Analysis):** a mathematical method that turns many related columns into new columns that each carry different information. The bank used it to **hide the original card details** for privacy, so the data set has 28 anonymous columns, `V1`–`V28`.
- **Log of the amount:** most payments are small and a few are huge. Taking `log(1 + amount)` squeezes the large values closer together, which makes them easier for a model to use.
- **Hour on a circle:** as plain numbers, 23:00 and 00:00 look far apart (23 and 0), although they are one hour apart. Placing the hour on a circle with `sin` and `cos`, like the hands of a clock, makes them neighbours.
- **Standardization:** rescales each column to an average (**mean**) of 0 and a typical spread (**standard deviation**) of 1, using `z = (x − mean) / std`. This stops large-valued columns from dominating. It matters for the "straight line" models in section 9.

## 8. When fraud is rare: imbalanced data

Only about 2 payments in 1,000 are fraud. A lazy model that always answers "legitimate" is **99.8% correct (accurate)** but catches **zero** fraud. So accuracy is useless here, and the model must be pushed to pay attention to the rare class. Two ways are compared:

- **Class weights:** each fraud example counts **hundreds of times more** in the loss than a legitimate one, so missing a fraud is "expensive" for the model. All the data is kept.
- **Undersampling:** keep all frauds, but only a random sample of the legitimate payments (here 10 for each fraud). Training is faster, but most legitimate examples are thrown away.

## 9. The four models

- **Logistic Regression:** gives each feature a weight, adds them up, and passes the total through the **sigmoid**, an S-shaped curve that turns any number into a value between 0 and 1. Simple and fast, but it can only separate the classes with a **straight line** (a flat boundary).
- **Decision tree:** asks a series of yes/no questions, like the game "20 questions": "Is V14 below −3? Is the amount above 100?". Each answer leads to the next question, and the last one gives the result.
- **Random Forest:** one tree makes many mistakes, so we train **many trees** (here 100), each on a random sample of the data and with random features to choose from. Then the trees **vote**. Asking many different people gives a better answer than asking one, which is the "wisdom of the crowd". *(Training on random samples is called **bagging**.)*
- **Gradient-Boosted Trees (GBT):** trees are built **one after another**, and each new tree focuses on the **mistakes** of the trees before it, like a team where each new member fixes what the others got wrong. This is very powerful, but sensitive to its settings.
- **Linear SVM (Support Vector Machine):** draws the straight boundary that leaves the **widest possible gap** (the **margin**) between the two classes, like building the widest road between two villages. Its output is a distance from the boundary, not a score from 0 to 1. **Platt scaling** fits a small sigmoid curve to turn that distance into a 0–1 score.

**Hyper-parameters** are settings chosen **before** training rather than learned, such as how deep a tree may grow. **Regularization** is a setting that punishes over-complicated models, to prevent overfitting. **Grid search** trains the model once with each combination of settings and keeps the combination that scores best on the **validation** set.

## 10. Measuring how good the model is

The model gives a **score**. A **threshold** (cut-off) turns the score into a decision, like the pass mark of an exam: if the score ≥ threshold, the payment is flagged as fraud.

**Confusion matrix:** the four possible outcomes.

| | Model says "legitimate" | Model says "fraud" |
|---|---|---|
| **Really legitimate** | True Negative (TN): correct | False Positive (FP): **false alarm** |
| **Really fraud** | False Negative (FN): **missed fraud** | True Positive (TP): correct |

Think of a fire alarm:
- **Precision** = TP / (TP + FP). *When the alarm rings, is there really a fire?* Low precision means many false alarms.
- **Recall** = TP / (TP + FN). *When there is a fire, does the alarm ring?* Low recall means missed frauds.
- **F1** combines both into one number. It is high only when both are high.

**The trade-off:** a **lower** threshold catches more fraud (higher recall) but rings more false alarms (lower precision). A **higher** threshold does the opposite. The system picks the threshold with the **best F1 on the validation set**, and the dashboard lets you move it live.

**Comparing models across all thresholds:**
- The **ROC curve** and its area, **ROC-AUC**, are common, but misleading when fraud is rare. Take 1,000 false alarms among 280,000 legitimate payments: that is only 0.4% of them, so ROC looks excellent. But if only 400 of the alarms are real fraud, precision is just 29%.
- The **Precision–Recall curve** and its area, **AUC-PR**, focus on the fraud cases, so they show this problem clearly. A model that guesses randomly scores only about 0.002, while a perfect one scores 1. **This is the main metric the system uses to choose the model.**

**Fraud score, not probability:** class weights and undersampling distort the scores. A score of 0.3 does **not** mean a 30% chance of fraud. Scores whose values do match real chances are called **calibrated**. The system's scores still **rank** payments correctly, from most to least suspicious.

## 11. How sure are we? The bootstrap

The test set has only about 70 frauds, so one lucky or unlucky case can change the results noticeably. The **bootstrap** measures this uncertainty:

1. Make a new test set of the same size by **drawing payments at random, with replacement**, so some appear twice and some not at all.
2. Measure the model on it.
3. Repeat 1,000 times.

The middle 95% of the 1,000 results is the **95% confidence interval**, the range where the true value most likely lies. If two models' intervals **overlap** a lot, we can't be sure which one is really better.

## 12. Measuring speed

- **Throughput:** how many payments are processed per second.
- **Latency:** how long one payment takes, from creation until its result is stored.
- **Percentiles:** the **p50** (median) means half the payments are faster than this value. The **p95** means 95% are faster, so it shows the slow cases that an average would hide.

---

## Summary

| Idea | What it does in the system |
|---|---|
| Kafka (topics, offsets, partitions) | Carries payments and results between programs safely, in order, without losing any |
| Log compaction | Keeps only the latest pause/rate command |
| Spark | Processes data fast by splitting the work among CPU cores |
| Structured Streaming + checkpoint | Checks new payments every second and survives crashes |
| Idempotent database writes | Each result is stored exactly once |
| MLlib pipeline | The same preparation recipe in training and live checking |
| Train / validation / test, stratified and group-aware | A fair, honest evaluation without leakage |
| Log amount, hour on a circle, standardization | Turns raw columns into features the models can use well |
| Class weights / undersampling | Makes the model care about the rare fraud cases |
| Four models + grid search | Compares simple and complex models and keeps the best |
| Precision, recall, F1, AUC-PR | Measures quality in a way that works when fraud is rare |
| Threshold | The adjustable cut-off between "OK" and "fraud" |
| Bootstrap | Shows how much the results could change by chance |
| PostgreSQL, Streamlit, Docker | Store the results, show them, and run everything with one command |

*How these pieces fit together, with the actual results: [SYSTEM_OVERVIEW.md](SYSTEM_OVERVIEW.md).*
