"""
Temporary helper to measure latency:
1) Local model inference time.
2) End-to-end latency from publishing a transaction to Kafka until the API
   returns a processed result, plus the API response latency itself.

Assumptions:
- docker compose stack is up (`docker compose up -d`).
- API reachable at http://localhost:8000.
- Kafka service name is `kafka` and topic is `transactions`.
- Requires only the Python standard library.
"""

from __future__ import annotations

import json
import subprocess
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict
from urllib import request, error
import sys
from pathlib import Path
import asyncio
import os

# Ensure project root is on sys.path when run as a script
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from streaming.fraud_detector import detect_fraud

# --- Local model timing (uses FakeRedis so it doesn't touch prod Redis) ---


class _FakeRedis:
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value):
        self.store[key] = value

    def setex(self, key, ttl, value):
        self.store[key] = value

    def incr(self, key):
        self.store[key] = int(self.store.get(key, 0)) + 1


def measure_model_inference(txn: Dict[str, Any]) -> float:
    """Return inference time in milliseconds."""
    

    fake_redis = _FakeRedis()
    t0 = time.perf_counter()
    detect_fraud(txn, fake_redis)
    t1 = time.perf_counter()
    return (t1 - t0) * 1000


# --- Kafka publish + API poll timing ---


@dataclass
class ApiTimingResult:
    end_to_end_ms: float
    api_response_ms: float
    final_response: Dict[str, Any]


def publish_to_kafka(txn_json: str) -> None:
    """
    Publish JSON to Kafka.
    - Prefers docker compose exec (when running on host).
    - Falls back to aiokafka producer when docker binary is unavailable (e.g. inside container).
    """
    cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "kafka",
        "bash",
        "-lc",
        f"cat <<'EOF' | kafka-console-producer --bootstrap-server kafka:9092 --topic transactions\n{txn_json}\nEOF",
    ]
    try:
        subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return
    except FileNotFoundError:
        # docker not available (likely running inside a container)
        pass

    # Fallback: use aiokafka directly
    try:
        from aiokafka import AIOKafkaProducer
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("aiokafka not available; install dependencies") from exc

    async def _send():
        producer = AIOKafkaProducer(bootstrap_servers="kafka:9092")
        await producer.start()
        try:
            await producer.send_and_wait("transactions", txn_json.encode("utf-8"))
        finally:
            await producer.stop()

    asyncio.run(_send())


def poll_api(transaction_id: str, wait_seconds: int = 20) -> ApiTimingResult:
    """Poll the API until processed; return timing details."""
    api_base = os.environ.get("API_BASE", "http://localhost:8000")
    url = f"{api_base}/transaction/{transaction_id}/status?wait_seconds=1"

    start = time.perf_counter()
    deadline = start + wait_seconds
    last_latency = 0.0
    final_body: Dict[str, Any] | None = None

    while time.perf_counter() < deadline:
        req_start = time.perf_counter()
        try:
            with request.urlopen(url, timeout=2) as resp:
                body = resp.read().decode("utf-8")
                last_latency = (time.perf_counter() - req_start) * 1000
                payload = json.loads(body)
                final_body = payload
                if payload.get("status") == "processed":
                    break
        except error.HTTPError as exc:
            last_latency = (time.perf_counter() - req_start) * 1000
            try:
                payload = json.loads(exc.read().decode("utf-8"))
            except Exception:
                payload = {}
            if payload.get("status") == "processed":
                final_body = payload
                break
        time.sleep(0.2)

    end_to_end_ms = (time.perf_counter() - start) * 1000

    return ApiTimingResult(
        end_to_end_ms=end_to_end_ms,
        api_response_ms=last_latency,
        final_response=final_body or {},
    )


def main():
    txn_id = f"latency_{uuid.uuid4().hex[:8]}"
    txn = {
        "transaction_id": txn_id,
        "step": 1,
        "type": "TRANSFER",
        "amount": 12345.0,
        "nameOrig": "Ctemp",
        "oldbalanceOrg": 0.0,
        "newbalanceOrig": 0.0,
        "nameDest": "Cdest",
        "oldbalanceDest": 0.0,
        "newbalanceDest": 0.0,
        "isFraud": 0,
        "isFlaggedFraud": 0,
    }

    print(f"Transaction ID: {txn_id}")

    # 1) local model inference
    model_ms = measure_model_inference(txn)
    print(f"Local model inference: {model_ms:.2f} ms")

    # 2) publish to Kafka and poll API
    txn_json = json.dumps(txn)
    publish_to_kafka(txn_json)
    api_timing = poll_api(txn_id)
    print(f"End-to-end (publish -> API processed): {api_timing.end_to_end_ms:.2f} ms")
    print(f"API response latency (last call): {api_timing.api_response_ms:.2f} ms")
    print(f"Final API payload: {api_timing.final_response}")


if __name__ == "__main__":
    main()
