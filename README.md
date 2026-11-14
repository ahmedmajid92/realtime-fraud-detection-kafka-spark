# Real-Time Credit Card Fraud Detection

Apache Kafka · Spark Structured Streaming · Spark MLlib · PostgreSQL · Streamlit

> The full README (Windows prerequisites, UI checklist, troubleshooting, demo script) is
> written in Phase 7. See [PLAN.md](PLAN.md) for the design and [PROGRESS.md](PROGRESS.md)
> for the status.

## Quick start (Windows PowerShell)

```powershell
# 1. Put the Kaggle dataset at data\raw\creditcard.csv
#    https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud
# 2. Check Docker, create .env, build the images
.\run.ps1 setup
# 3. Start everything (the first start trains the model, ~10-15 min)
.\run.ps1 up
```

Dashboard: http://localhost:8501 · Kafka UI: http://localhost:8081 · Spark UI: http://localhost:4040

If PowerShell blocks the script: `powershell -ExecutionPolicy Bypass -File .\run.ps1 setup`
