"""Alert lifecycle regression tests for the speed layer's Postgres sink.

Two bugs motivated these:
  * upsert_postgres wrote a hardcoded [] into vitals_live.anomaly_flags,
    discarding the flags aggregate_stream computes.
  * alerts_log was never written at all, so /patients/{id}/alerts was always
    empty and vitals_live.active_alert was always false.

These need a real Postgres (the SQL uses ON CONFLICT, NOT EXISTS and
`<> ALL(array)`), so they skip when no database is reachable. Point them at a
database with:
    DATABASE_URL=postgresql://hospital:hospital@localhost:5433/hospital
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

psycopg = pytest.importorskip("psycopg", reason="psycopg not installed")

from streaming.postgres_sink import write_window  # noqa: E402

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://hospital:hospital@localhost:5432/hospital")
PATIENT_ID = "P_SINK_TEST"
FIRST_WINDOW = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)


@pytest.fixture(scope="session")
def database_available():
    """Probe the database once per session rather than once per test.

    Probing per test made the whole module take ~25s to skip when no
    database was running, since each test paid the connection timeout.
    """
    try:
        psycopg.connect(DATABASE_URL, connect_timeout=3).close()
    except Exception as exc:  # noqa: BLE001 - any connection problem means skip
        return f"no Postgres at {DATABASE_URL}: {exc}"
    return None


@pytest.fixture
def cursor(database_available):
    if database_available is not None:
        pytest.skip(database_available)

    connection = psycopg.connect(DATABASE_URL, connect_timeout=3)

    with connection:
        with connection.cursor() as cur:
            cur.execute(
                "INSERT INTO patients (patient_id, ward, admit_date) VALUES (%s, 'TEST', '2026-01-01')"
                " ON CONFLICT DO NOTHING",
                (PATIENT_ID,),
            )
            _cleanup(cur)
            yield cur
            _cleanup(cur)
            cur.execute("DELETE FROM patients WHERE patient_id = %s", (PATIENT_ID,))


def _cleanup(cur):
    cur.execute("DELETE FROM alerts_log WHERE patient_id = %s", (PATIENT_ID,))
    cur.execute("DELETE FROM vitals_live WHERE patient_id = %s", (PATIENT_ID,))


def _write(cur, window_index: int, flags: list[str]):
    start = FIRST_WINDOW + timedelta(minutes=5 * window_index)
    return write_window(cur, {
        "patient_id": PATIENT_ID,
        "window_start": start,
        "window_end": start + timedelta(minutes=5),
        "avg_hr": 130.0,
        "avg_spo2": 97.0,
        "avg_systolic_bp": 120.0,
        "avg_diastolic_bp": 78.0,
        "avg_temp": 36.8,
        "event_count": 5,
        "anomaly_flags": flags,
    })


def _alerts(cur) -> list[tuple[str, bool]]:
    cur.execute(
        "SELECT alert_type, resolved_at IS NULL FROM alerts_log WHERE patient_id = %s"
        " ORDER BY triggered_at, alert_type",
        (PATIENT_ID,),
    )
    return cur.fetchall()


def test_anomaly_flags_are_persisted_not_discarded(cursor):
    _write(cursor, 0, ["TACHYCARDIA", "FEVER"])

    cursor.execute(
        "SELECT anomaly_flags FROM vitals_live WHERE patient_id = %s AND window_start = %s",
        (PATIENT_ID, FIRST_WINDOW),
    )
    assert cursor.fetchone()[0] == ["TACHYCARDIA", "FEVER"]


def test_repeated_flag_does_not_open_a_second_alert(cursor):
    _write(cursor, 0, ["TACHYCARDIA"])
    _write(cursor, 1, ["TACHYCARDIA"])

    assert _alerts(cursor) == [("TACHYCARDIA", True)]


def test_alert_types_are_tracked_independently(cursor):
    _write(cursor, 0, ["TACHYCARDIA"])
    _write(cursor, 1, ["TACHYCARDIA", "FEVER"])

    assert sorted(_alerts(cursor)) == [("FEVER", True), ("TACHYCARDIA", True)]


def test_alerts_resolve_when_vitals_return_to_normal(cursor):
    _write(cursor, 0, ["TACHYCARDIA", "FEVER"])
    _write(cursor, 1, [])

    assert _alerts(cursor) == [("FEVER", False), ("TACHYCARDIA", False)]


def test_relapse_opens_a_new_alert_episode(cursor):
    _write(cursor, 0, ["TACHYCARDIA"])
    _write(cursor, 1, [])
    _write(cursor, 2, ["TACHYCARDIA"])

    alerts = _alerts(cursor)
    assert alerts.count(("TACHYCARDIA", False)) == 1, "first episode should be closed"
    assert alerts.count(("TACHYCARDIA", True)) == 1, "relapse should open a new alert"


def test_window_upsert_is_idempotent(cursor):
    _write(cursor, 0, ["TACHYCARDIA"])
    _write(cursor, 0, ["TACHYCARDIA"])

    cursor.execute("SELECT count(*) FROM vitals_live WHERE patient_id = %s", (PATIENT_ID,))
    assert cursor.fetchone()[0] == 1
