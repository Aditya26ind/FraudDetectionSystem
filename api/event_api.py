import json
import uuid
from pathlib import Path
import pandas as pd
from datetime import datetime

from fastapi import APIRouter, HTTPException
from aiokafka import AIOKafkaProducer

from common.config import get_settings
import asyncio

settings = get_settings()
router = APIRouter(tags=["events"])

producer: AIOKafkaProducer | None = None


async def startup_event():
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap)
    # Retry to allow Kafka time to come up
    for attempt in range(10):
        try:
            await producer.start()
            return
        except Exception:
            await asyncio.sleep(3)
    raise RuntimeError("Kafka producer failed to start after retries")


async def shutdown_event():
    if producer:
        await producer.stop()


def _load_row(csv_row_number: int | None):
    path = Path("data/transactions.csv")
    if not path.exists():
        raise HTTPException(status_code=404, detail="transactions.csv not found")
    df = pd.read_csv(path)
    if csv_row_number is not None:
        if csv_row_number >= len(df):
            raise HTTPException(status_code=404, detail="Row out of range")
        return df.iloc[csv_row_number]
    return df.sample(1).iloc[0]


@router.post("/transaction/create")
async def create_transaction(csv_row_number: int = None):
    """Create transaction event from Kaggle CSV and send to Kafka"""
    if producer is None:
        raise HTTPException(status_code=500, detail="Kafka not ready")

    row = _load_row(csv_row_number)

    transaction_id = str(uuid.uuid4())

    transaction = {
        "transaction_id": transaction_id,
        "step": int(row["step"]),
        "type": str(row["type"]),
        "amount": float(row["amount"]),
        "nameOrig": str(row["nameOrig"]),
        "oldbalanceOrg": float(row["oldbalanceOrg"]),
        "newbalanceOrig": float(row["newbalanceOrig"]),
        "nameDest": str(row["nameDest"]),
        "oldbalanceDest": float(row["oldbalanceDest"]),
        "newbalanceDest": float(row["newbalanceDest"]),
        "isFraud": int(row["isFraud"]),
        "isFlaggedFraud": int(row["isFlaggedFraud"]),
        "timestamp": datetime.utcnow().isoformat(),
    }

    await producer.send_and_wait(
        settings.kafka_fraud_topic,
        json.dumps(transaction).encode()
    )

    return {
        "status": "success",
        "message": "Transaction sent to fraud detection system",
        "transaction_id": transaction_id,
        "type": transaction["type"],
        "amount": transaction["amount"],
        "origin": transaction["nameOrig"],
        "destination": transaction["nameDest"],
        "note": f"Check status at: GET /transaction/{transaction_id}/status"
    }
