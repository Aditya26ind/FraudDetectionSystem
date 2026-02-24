import json
import asyncio
from datetime import datetime
from pathlib import Path

import joblib
import pandas as pd
from aiokafka import AIOKafkaConsumer
from sklearn.pipeline import Pipeline

from common.config import get_settings
from common.redis_client import get_redis_client
from common.s3_client import get_s3_client, ensure_bucket_exists
from common.db_client import get_db_conn, ensure_transactions_table, insert_transaction
from utils.ml_helper import build_feature_row, decision_from_score,get_or_train_model

settings = get_settings()

MODEL_PATH = Path(__file__).resolve().parent / "models" / "fraud_model.pkl"
MODEL_FEATURES = ["amount", "oldbalanceOrg", "newbalanceDest", "isFlaggedFraud", "type"]
_model: Pipeline | None = None


def detect_fraud(transaction: dict, redis_client) -> dict:
    """
    Core fraud detection function.
    Returns decision with score and reasons.
    """
    try:
        model = get_or_train_model()
    except Exception as exc:
        print(f"Error loading model: {exc}")
        return {
            "decision": "APPROVE",
            "fraud_score": 0.0,
            "reasons": ["model_load_error"]
        }

    payload = build_feature_row(transaction)

    try:
        proba = float(model.predict_proba(payload)[0, 1])
    except Exception as exc:  # fallback to approve on model issues
        print(f"Error in fraud model inference: {exc}")
        return {
            "decision": "APPROVE",
            "fraud_score": 0.0,
            "reasons": ["model_error"]
        }

    decision, reasons = decision_from_score(proba, transaction)

    return {
        "decision": decision,
        "fraud_score": round(proba * 100, 2),
        "reasons": reasons,
    }


async def fraud_detector():
    """
    Background worker that reads from Kafka and detects fraud.
    This runs 24/7.
    """
    get_or_train_model()  # ensure model is ready before consuming
    redis_client = get_redis_client()
    s3_client = get_s3_client()
    ensure_bucket_exists(s3_client)
    db_conn = get_db_conn()
    ensure_transactions_table(db_conn)

    while True:
        consumer = AIOKafkaConsumer(
            settings.kafka_fraud_topic,
            bootstrap_servers=settings.kafka_bootstrap,
            auto_offset_reset='latest',
            group_id="fraud-detector",
        )

        # Retry connecting to Kafka (broker can take a few seconds)
        for attempt in range(40):
            try:
                await consumer.start()
                break
            except Exception:
                await asyncio.sleep(2)
        else:
            await asyncio.sleep(5)
            continue

        print(" Fraud Detector Started - Listening for transactions...")

        try:
            async for msg in consumer:
                try:
                    transaction = json.loads(msg.value.decode("utf-8"))
                except Exception as exc:
                    print(f"Skipping bad message at offset {msg.offset}: {exc}")
                    continue

                # FRAUD DETECTION 
                fraud_result = detect_fraud(transaction, redis_client)

                # Combine transaction with fraud result
                processed_at = datetime.utcnow()
                result = {
                    **transaction,
                    **fraud_result,
                    "processed_at": processed_at.isoformat()
                }

                # Save to Redis (for API to read)
                redis_client.setex(
                    f"fraud:{transaction['transaction_id']}", 
                    3600,  # 1 hour
                    json.dumps(result)
                )

                # Save to S3 and Postgres off the hot path
                asyncio.create_task(_persist(result, s3_client, db_conn))

                
                redis_client.incr("stats:total")
                redis_client.incr(f"stats:{fraud_result['decision'].lower()}")

        except Exception:
            await asyncio.sleep(2)
        finally:
            try:
                await consumer.stop()
            except Exception:
                pass


async def _persist(result: dict, s3_client, db_conn):
    """Persist to S3 and Postgres without blocking the main consumer loop."""
    
    await asyncio.gather(
        asyncio.to_thread(
            s3_client.put_object,
            Bucket=settings.s3_bucket,
            Key=f"decisions/{result['transaction_id']}.json",
            Body=json.dumps(result),
        ),
        
        asyncio.to_thread(
            insert_transaction,
            db_conn,
            {
                **result,
                "processed_at": datetime.fromisoformat(result["processed_at"]),
            },
        ),
    )


_worker_task: asyncio.Task | None = None


async def start_fraud_worker():
    """Idempotently start the background fraud detector as an asyncio task."""
    global _worker_task
    if _worker_task is None or _worker_task.done():
        _worker_task = asyncio.create_task(fraud_detector())


async def stop_fraud_worker():
    """Cancel the background fraud detector task if running."""
    global _worker_task
    if _worker_task and not _worker_task.done():
        _worker_task.cancel()
        try:
            await _worker_task
        except asyncio.CancelledError:
            pass
    _worker_task = None


if __name__ == "__main__":
    asyncio.run(fraud_detector())


