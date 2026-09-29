from datetime import datetime, timezone

from fastapi.testclient import TestClient

from api.main import create_app
from api.repository import MemoryRepository


def test_api_returns_live_rows_alerts_and_health():
    repository = MemoryRepository()
    timestamp = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
    repository.upsert_live({
        "patient_id": "P001",
        "window_start": timestamp,
        "window_end": datetime(2026, 1, 1, 12, 5, tzinfo=timezone.utc),
        "avg_hr": 130.0,
        "avg_spo2": 97.0,
        "avg_systolic_bp": 120.0,
        "avg_diastolic_bp": 78.0,
        "avg_temp": 36.8,
        "event_count": 2,
        "anomaly_flags": ["TACHYCARDIA"],
    }, ward="A")
    repository.record_alert("P001", "TACHYCARDIA", timestamp)
    client = TestClient(create_app(repository))

    assert client.get("/vitals/live?ward=A").json()[0]["active_alert"] is True
    assert len(client.get("/patients/P001/alerts?status=active").json()) == 1
    assert client.get("/health").json()["database"] == "ok"


def test_unknown_patient_returns_404():
    client = TestClient(create_app(MemoryRepository()))

    response = client.get("/patients/UNKNOWN/alerts")

    assert response.status_code == 404
