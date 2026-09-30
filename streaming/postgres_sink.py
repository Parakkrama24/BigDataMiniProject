"""Postgres writes for the speed layer.

Deliberately free of pyspark imports so the SQL and the alert reconciliation
logic can be unit-tested without a Spark session -- the same reasoning that
keeps the transformations in batch.pipeline free of Airflow imports.

streaming/spark_job.py's foreachBatch sink is a thin adapter over this.
"""

from __future__ import annotations

from typing import Any, Mapping

UPSERT_LIVE_SQL = """
    INSERT INTO vitals_live
    (patient_id, window_start, window_end, avg_hr, avg_spo2,
     avg_systolic_bp, avg_diastolic_bp, avg_temp, event_count, anomaly_flags)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (patient_id, window_start) DO UPDATE SET
      window_end = EXCLUDED.window_end, avg_hr = EXCLUDED.avg_hr,
      avg_spo2 = EXCLUDED.avg_spo2, avg_systolic_bp = EXCLUDED.avg_systolic_bp,
      avg_diastolic_bp = EXCLUDED.avg_diastolic_bp, avg_temp = EXCLUDED.avg_temp,
      event_count = EXCLUDED.event_count, anomaly_flags = EXCLUDED.anomaly_flags
"""

# Opens an alert only if the patient has no unresolved alert of the same type,
# so a patient who stays tachycardic across many windows produces one alert
# rather than one per window.
OPEN_ALERT_SQL = """
    INSERT INTO alerts_log (patient_id, alert_type, triggered_at)
    SELECT %s, %s, %s
    WHERE NOT EXISTS (
        SELECT 1 FROM alerts_log
        WHERE patient_id = %s AND alert_type = %s AND resolved_at IS NULL
    )
"""

# Closes any open alert whose condition is no longer flagged in this window.
# `alert_type <> ALL('{}')` is true for every row, so an empty flag list
# correctly resolves all of that patient's open alerts.
RESOLVE_ALERTS_SQL = """
    UPDATE alerts_log SET resolved_at = %s
    WHERE patient_id = %s AND resolved_at IS NULL AND alert_type <> ALL(%s)
"""

LIVE_FIELDS = (
    "patient_id",
    "window_start",
    "window_end",
    "avg_hr",
    "avg_spo2",
    "avg_systolic_bp",
    "avg_diastolic_bp",
    "avg_temp",
    "event_count",
)


def write_window(cursor, row: Mapping[str, Any]) -> list[str]:
    """Persist one window aggregate and reconcile that patient's alerts.

    Args:
        cursor: an open DB-API cursor (psycopg).
        row: one aggregated window, as a plain mapping.

    Returns:
        The anomaly flags that were active for this window.
    """
    from psycopg.types.json import Jsonb

    flags = list(row.get("anomaly_flags") or [])
    patient_id = row["patient_id"]

    values = [row.get(name) for name in LIVE_FIELDS]
    values.append(Jsonb(flags))
    cursor.execute(UPSERT_LIVE_SQL, values)

    for alert_type in flags:
        cursor.execute(
            OPEN_ALERT_SQL,
            (patient_id, alert_type, row["window_start"], patient_id, alert_type),
        )

    cursor.execute(RESOLVE_ALERTS_SQL, (row["window_end"], patient_id, flags))
    return flags
