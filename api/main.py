from fastapi import FastAPI
from api.event_api import router as event_router, startup_event, shutdown_event
from api.fraud import router as fraud_router
from streaming.fraud_detector import start_fraud_worker, stop_fraud_worker

app = FastAPI(title="Fraud Detection System", version="1.0.0")

app.include_router(event_router)
app.include_router(fraud_router)


@app.on_event("startup")
async def startup():
    await startup_event()
    await start_fraud_worker()


@app.on_event("shutdown")
async def shutdown():
    await stop_fraud_worker()
    await shutdown_event()


@app.get("/")
async def root():
    return {
        "system": "Real-Time Fraud Detection",
        "endpoints": {
            "create_transaction": "POST /transaction/create",
            "get_fraud_alert": "GET /transaction/{id}/status",
            "get_stats": "GET /stats"
        }
    }
