from __future__ import annotations
import os
from datetime import datetime, date

import psycopg2
from common.config import get_settings

settings = get_settings()

# this will be a daily cron job in production currently we run it manually for demonstration
def summarize(target_date: date | None = None) -> None:
    """
    Summarize decisions from Postgres.
    - target_date: filter by date (UTC) on processed_at; defaults to today.
    """
    target_date = target_date or datetime.utcnow().date()
    start_ts = datetime.combine(target_date, datetime.min.time())
    end_ts = datetime.combine(target_date, datetime.max.time())

    try:
        conn = psycopg2.connect(
            host=settings.db_host,
            port=settings.db_port,
            dbname=settings.db_name,
            user=settings.db_user,
            password=settings.db_password,
        )
    except Exception as exc:
        print(f"Could not connect to Postgres: {exc}")
        return

    with conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT decision, COUNT(*)
            FROM transactions
            WHERE processed_at BETWEEN %s AND %s
            GROUP BY decision;
            """,
            (start_ts, end_ts),
        )
        rows = cur.fetchall()

    counts = {"APPROVE": 0, "FLAG": 0, "BLOCK": 0}
    for decision, cnt in rows:
        if decision:
            counts[decision] = cnt

    print(f"Daily fraud summary for {target_date.isoformat()}:")
    for k, v in counts.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    summarize()
