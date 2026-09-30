"""Daily risk report API owned by the batch/storage layer."""
from __future__ import annotations

import os
from datetime import date

from fastapi import APIRouter, HTTPException, Query
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

router = APIRouter(prefix="/reports", tags=["reports"])
_pool = ConnectionPool(os.getenv("DATABASE_URL", "postgresql://hospital:hospital@localhost:5432/hospital"), open=False)


def _get_pool() -> ConnectionPool:
    if _pool.closed:
        # psycopg_pool's open() takes `wait`, not `waiting`; the wrong keyword
        # raised TypeError on the first request to this endpoint.
        _pool.open(wait=True)
    return _pool


@router.get("/daily/{report_date}")
def daily_report(report_date: date, risk_flag: str | None = Query(default=None, pattern="^(LOW|MEDIUM|HIGH)$")):
    query = """
        SELECT patient_id, report_date, vitals_summary, lab_summary,
               risk_score, risk_flag, generated_at
        FROM daily_risk_report
        WHERE report_date = %s
    """
    params: list[object] = [report_date]
    if risk_flag:
        query += " AND risk_flag = %s"
        params.append(risk_flag)
    query += " ORDER BY risk_score DESC, patient_id"
    with _get_pool().connection() as connection:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No daily risk report found for {report_date.isoformat()}")
    return {"report_date": report_date, "count": len(rows), "reports": rows}
