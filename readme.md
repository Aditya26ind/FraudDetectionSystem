# Fraud Detection (Kafka + FastAPI + ML)

Lightweight real‑time fraud demo using Kafka events, a Python worker with a small ML model, and FastAPI for APIs.

## What it does
- API publishes transactions to Kafka.
- Worker consumes, scores with a logistic regression model (trained from `data/transactions.csv`), and writes decision + score to Redis/Postgres/S3.
- API lets you check the decision.

## Core components
- **Kafka**: event bus (`transactions` topic).
- **Worker**: `streaming/fraud_detector.py` (async Kafka consumer, model inference, persistence).
- **API**: `api/` (status + stats endpoints).
- **Storage**: Redis (latest results), Postgres (history), MinIO/S3 (JSON files).

## Quick start
```bash
docker compose up -d --build
```

Send a test transaction:
```bash
docker compose exec kafka bash -lc "echo '{\"transaction_id\":\"demo1\",\"step\":1,\"type\":\"TRANSFER\",\"amount\":250000,\"nameOrig\":\"C1\",\"oldbalanceOrg\":0,\"newbalanceOrig\":0,\"nameDest\":\"C2\",\"oldbalanceDest\":0,\"newbalanceDest\":0,\"isFraud\":0,\"isFlaggedFraud\":0}' \
 | kafka-console-producer --bootstrap-server kafka:9092 --topic transactions"
```

Check status (from host):
```bash
curl "http://localhost:8000/transaction/demo1/status?wait_seconds=5"
```

## Key files
- `streaming/fraud_detector.py` — consumer + model + decision logic.
- `api/fraud.py` — REST to fetch decisions/stats.
- `common/` — config, Redis/Postgres/S3 clients.
- `tests/latency_probe.py` — quick latency measurement.

## Model
- Trained on first run from `data/transactions.csv`.
- Simple features: amount, balances, flag, type (one‑hot).
- Thresholds: `prob>=0.8 → BLOCK`, `>=0.5 → FLAG`, else APPROVE; upstream `isFlaggedFraud=1` always BLOCK.
- Used GradientBoostingClassifier for training as it have non linear approach.

## Daily report
`python batch/daily_reports.py` — counts APPROVE/FLAG/BLOCK from Postgres for today.

## For optimization
- Used redis caching , asyncio for parallel processing of ML detection with s3 ingestion and database ingestion.
- scripts for daily reports locally runs using command but in production will be using MWAA for airflow and connect dag file with it to run periodically.

## Notes
- Keep messages valid JSON (double quotes) or they’ll be skipped.
- API inside compose is `http://api:8000`; from host use `http://localhost:8000`.
