"""Postgres-backed repository, mirroring MemoryRepository's interface.

MemoryRepository keeps everything in one process's memory, which means the
API can only ever see rows it wrote itself. The Spark streaming job writes
vitals_live and alerts_log to Postgres from a separate process, so the API
needs to read from there for the end-to-end path to actually connect.

Both classes expose the same methods, and api.main.create_app() picks between
them, so tests can keep using the in-memory one with no database running.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

DEFAULT_DATABASE_URL = "postgresql://hospital:hospital@localhost:5432/hospital"

LIVE_COLUMNS = (
    "patient_id",
    "window_start",
    "window_end",
    "avg_hr",
    "min_hr",
    "max_hr",
    "avg_spo2",
    "min_spo2",
    "max_spo2",
    "avg_systolic_bp",
    "avg_diastolic_bp",
    "avg_temp",
    "event_count",
    "anomaly_flags",
)


class PostgresRepository:
    def __init__(self, database_url: str | None = None, pool: ConnectionPool | None = None) -> None:
        self._database_url = database_url or os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)
        # open=False so constructing the repository never blocks on a
        # database that isn't up yet; the pool opens on first real use.
        self._pool = pool or ConnectionPool(self._database_url, open=False)

    def _connection(self):
        if self._pool.closed:
            self._pool.open(wait=True)
        return self._pool.connection()

    def _query(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        with self._connection() as connection:
            with connection.cursor(row_factory=dict_row) as cursor:
                cursor.execute(sql, params or [])
                return cursor.fetchall()

    def _execute(self, sql: str, params: list[Any] | None = None) -> None:
        with self._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, params or [])

    @property
    def patients(self) -> set[str]:
        """Known patient ids.

        A property rather than a stored set so it reflects the seeded
        patients table; api/routers/alerts.py does `patient_id not in
        repository.patients` to decide on a 404.
        """
        return {row["patient_id"] for row in self._query("SELECT patient_id FROM patients")}

    def upsert_live(self, row: dict[str, Any], ward: str | None = None) -> None:
        """Insert or replace one window aggregate.

        `ward` is accepted for interface parity with MemoryRepository but
        ignored: ward is a property of the patient, held in the patients
        table, not of an individual aggregate window.
        """
        columns = [name for name in LIVE_COLUMNS if name in row]
        placeholders = ", ".join(["%s"] * len(columns))
        updates = ", ".join(f"{name} = EXCLUDED.{name}" for name in columns if name not in ("patient_id", "window_start"))
        from psycopg.types.json import Jsonb

        values: list[Any] = []
        for name in columns:
            value = row[name]
            values.append(Jsonb(value) if name == "anomaly_flags" else value)

        self._execute(
            f"""INSERT INTO vitals_live ({", ".join(columns)})
                VALUES ({placeholders})
                ON CONFLICT (patient_id, window_start) DO UPDATE SET {updates}""",
            values,
        )

    def record_alert(self, patient_id: str, alert_type: str, triggered_at: datetime) -> dict[str, Any]:
        """Open an alert, or return the already-open one.

        Matches MemoryRepository: at most one unresolved alert per
        (patient_id, alert_type), so a patient who stays tachycardic for an
        hour produces one alert rather than one per window.
        """
        existing = self._query(
            """SELECT alert_id::text AS alert_id, patient_id, alert_type, triggered_at, resolved_at
               FROM alerts_log
               WHERE patient_id = %s AND alert_type = %s AND resolved_at IS NULL
               ORDER BY triggered_at
               LIMIT 1""",
            [patient_id, alert_type],
        )
        if existing:
            return existing[0]

        created = self._query(
            """INSERT INTO alerts_log (patient_id, alert_type, triggered_at)
               VALUES (%s, %s, %s)
               RETURNING alert_id::text AS alert_id, patient_id, alert_type, triggered_at, resolved_at""",
            [patient_id, alert_type, triggered_at],
        )
        return created[0]

    def resolve_alert(self, patient_id: str, alert_type: str, resolved_at: datetime) -> None:
        self._execute(
            """UPDATE alerts_log SET resolved_at = %s
               WHERE patient_id = %s AND alert_type = %s AND resolved_at IS NULL""",
            [resolved_at, patient_id, alert_type],
        )

    def latest_live(self, ward: str | None = None) -> list[dict[str, Any]]:
        """The most recent window per patient, with an active-alert flag.

        DISTINCT ON keeps one row per patient, and the ORDER BY decides
        which one that is (the newest window).
        """
        return self._query(
            """SELECT DISTINCT ON (v.patient_id)
                      v.patient_id, p.ward, v.window_start, v.window_end,
                      v.avg_hr, v.avg_spo2, v.avg_systolic_bp, v.avg_diastolic_bp,
                      v.avg_temp, v.event_count, v.anomaly_flags,
                      EXISTS (
                          SELECT 1 FROM alerts_log a
                          WHERE a.patient_id = v.patient_id AND a.resolved_at IS NULL
                      ) AS active_alert
               FROM vitals_live v
               JOIN patients p ON p.patient_id = v.patient_id
               WHERE %s::text IS NULL OR p.ward = %s
               ORDER BY v.patient_id, v.window_start DESC""",
            [ward, ward],
        )

    def patient_alerts(self, patient_id: str, status: str) -> list[dict[str, Any]]:
        clause = ""
        if status == "active":
            clause = " AND resolved_at IS NULL"
        elif status == "resolved":
            clause = " AND resolved_at IS NOT NULL"

        return self._query(
            f"""SELECT alert_id::text AS alert_id, patient_id, alert_type, triggered_at, resolved_at
                FROM alerts_log
                WHERE patient_id = %s{clause}
                ORDER BY triggered_at DESC""",
            [patient_id],
        )

    def last_data_received(self) -> datetime | None:
        rows = self._query("SELECT max(window_end) AS last_seen FROM vitals_live")
        return rows[0]["last_seen"] if rows else None

    def close(self) -> None:
        """Release pooled connections.

        Without this the pool's worker threads outlive the interpreter and
        psycopg_pool logs "couldn't stop thread ... within 5.0 seconds" on
        exit. api.main wires this to FastAPI's shutdown event.
        """
        if not self._pool.closed:
            self._pool.close()
