from __future__ import annotations
import json
import logging
import psycopg2
from psycopg2.extras import Json
from common.config import get_settings

settings = get_settings()


def get_db_conn():
    conn = psycopg2.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
    )
    conn.autocommit = True
    return conn


def ensure_transactions_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                transaction_id VARCHAR(64) UNIQUE NOT NULL,
                step INTEGER,
                type TEXT,
                amount NUMERIC,
                name_orig TEXT,
                oldbalance_org NUMERIC,
                newbalance_org NUMERIC,
                name_dest TEXT,
                oldbalance_dest NUMERIC,
                newbalance_dest NUMERIC,
                is_fraud INTEGER,
                is_flagged_fraud INTEGER,
                decision VARCHAR(16),
                fraud_score INTEGER,
                reasons JSONB,
                processed_at TIMESTAMP,
                raw JSONB
            );
            """
        )


def insert_transaction(conn, record: dict) -> None:
    with conn.cursor() as cur:
        logging.info(f"Inserting transaction {record.get('transaction_id')} into database")
        cur.execute(
            """
            INSERT INTO transactions (
                transaction_id, step, type, amount, name_orig, oldbalance_org,
                newbalance_org, name_dest, oldbalance_dest, newbalance_dest,
                is_fraud, is_flagged_fraud, decision, fraud_score, reasons, processed_at, raw
            ) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING;
            """,
            (
                record.get("transaction_id"),
                record.get("step"),
                record.get("type"),
                record.get("amount"),
                record.get("nameOrig"),
                record.get("oldbalanceOrg"),
                record.get("newbalanceOrig"),
                record.get("nameDest"),
                record.get("oldbalanceDest"),
                record.get("newbalanceDest"),
                record.get("isFraud"),
                record.get("isFlaggedFraud"),
                record.get("decision"),
                record.get("fraud_score"),
                Json(record.get("reasons")),
                record.get("processed_at"),
                Json(record),
            ),
        )
