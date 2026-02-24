import json
import uuid
from pathlib import Path
import pandas as pd
from datetime import datetime

from common.s3_client import get_s3_client
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
    
    s3_client = get_s3_client()
    path = pd.read_csv(f"s3://{settings.s3_bucket}/transactions.csv")
    
    #if no file named as transactions.csv is found , save it from local to s3
    if path is None:
        local_path = Path(__file__).resolve().parent.parent.parent / "data" / "transactions.csv"
        if not local_path.exists():
            raise HTTPException(status_code=404, detail="Local transactions.csv not found")
        s3_client.upload_file(str(local_path), settings.s3_bucket, "transactions.csv")
        path = pd.read_csv(f"s3://{settings.s3_bucket}/transactions.csv")
    # path = Path("data/transactions.csv")
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
