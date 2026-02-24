import json
import time
from fastapi import APIRouter, HTTPException

from common.redis_client import get_redis_client

router = APIRouter(tags=["fraud"])


@router.get("/transaction/{transaction_id}/status")
async def get_fraud_alert(transaction_id: str, wait_seconds: int = 5):
    """
    Get fraud detection result for a transaction.
    Waits up to {wait_seconds} for result to be available.
    """
    
    redis_client = get_redis_client()
    
    
    max_attempts = wait_seconds * 10  # Check every 100ms
    
    for attempt in range(max_attempts):
        result = redis_client.get(f"fraud:{transaction_id}")
        
        if result:
            fraud_data = json.loads(result)
            return {
                "status": "processed",
                "transaction_id": transaction_id,
                "decision": fraud_data["decision"],
                "fraud_score": fraud_data["fraud_score"],
                "reasons": fraud_data.get("reasons", []),
                "amount": fraud_data["amount"],
                "type": fraud_data.get("type"),
                "origin": fraud_data.get("nameOrig"),
                "destination": fraud_data.get("nameDest"),
                "processed_at": fraud_data["processed_at"],
                "alert": get_alert_message(fraud_data["decision"])
            }
        
        # Wait 100ms before checking again
        time.sleep(0.1)
    
    # If not processed after waiting
    raise HTTPException(
        status_code=202,
        detail={
            "status": "processing",
            "message": f"Transaction is still being processed. Try again in a few seconds.",
            "transaction_id": transaction_id
        }
    )


def get_alert_message(decision: str) -> str:
    """Generate fraud alert message"""
    
    alerts = {
        "APPROVE": "Transaction APPROVED - No fraud detected",
        "FLAG": "Transaction FLAGGED - Manual review required",
        "BLOCK": "Transaction BLOCKED - Fraud detected!"
    }
    
    return alerts.get(decision, "Unknown status")


@router.get("/stats")
async def get_fraud_stats():
    """Get overall fraud statistics"""
    
    redis_client = get_redis_client()
    
    total = int(redis_client.get("stats:total") or 0)
    approved = int(redis_client.get("stats:approved") or 0)
    flagged = int(redis_client.get("stats:flagged") or 0)
    blocked = int(redis_client.get("stats:blocked") or 0)
    
    fraud_rate = 0.0
    if total > 0:
        fraud_rate = ((flagged + blocked) / total) * 100
    
    return {
        "total_transactions": total,
        "approved": approved,
        "flagged": flagged,
        "blocked": blocked,
        "fraud_rate": round(fraud_rate, 2)
    }
