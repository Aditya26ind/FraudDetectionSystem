# Data Flow (short)

1) **API → Kafka**  
   - FastAPI (or any producer) publishes JSON to `transactions`.

2) **Kafka → Worker**  
   - `streaming/fraud_detector.py` consumes events, loads/ trains model, scores, and decides APPROVE/FLAG/BLOCK.

3) **Persist**  
   - Redis: latest decision per transaction (read by API).  
   - Postgres: full history.  
   - MinIO/S3: JSON copy for audit.

4) **API read path** 
   - `POST /transaction/create create events simulation using row number and data.
   - `GET /transaction/{id}/status` polls Redis for the decision.  
   - `GET /stats` aggregates counters from Redis.

Latency target: 1–2 seconds end-to-end in the demo stack (depends on Kafka polling).  
Daily summary: `batch/daily_reports.py` queries Postgres for today’s counts.
