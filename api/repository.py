from __future__ import annotations

from datetime import datetime
from typing import Any


class MemoryRepository:
    """Development repository with the same operations needed by the API."""

    def __init__(self) -> None:
        self.patients: set[str] = set()
        self.live_rows: dict[tuple[str, datetime], dict[str, Any]] = {}
        self.alerts: dict[str, dict[str, Any]] = {}

    def upsert_live(self, row: dict[str, Any], ward: str | None = None) -> None:
        row = dict(row)
        row["ward"] = ward
        self.patients.add(row["patient_id"])
        self.live_rows[(row["patient_id"], row["window_start"])] = row

    def record_alert(self, patient_id: str, alert_type: str, triggered_at: datetime) -> dict[str, Any]:
        for alert in self.alerts.values():
            if alert["patient_id"] == patient_id and alert["alert_type"] == alert_type and alert["resolved_at"] is None:
                return alert
        alert = {
            "alert_id": f"{patient_id}:{alert_type}:{triggered_at.isoformat()}",
            "patient_id": patient_id,
            "alert_type": alert_type,
            "triggered_at": triggered_at,
            "resolved_at": None,
        }
        self.alerts[alert["alert_id"]] = alert
        return alert

    def resolve_alert(self, patient_id: str, alert_type: str, resolved_at: datetime) -> None:
        for alert in self.alerts.values():
            if alert["patient_id"] == patient_id and alert["alert_type"] == alert_type and alert["resolved_at"] is None:
                alert["resolved_at"] = resolved_at

    def latest_live(self, ward: str | None = None) -> list[dict[str, Any]]:
        latest: dict[str, dict[str, Any]] = {}
        for row in self.live_rows.values():
            if ward is not None and row.get("ward") != ward:
                continue
            patient_id = row["patient_id"]
            if patient_id not in latest or row["window_start"] > latest[patient_id]["window_start"]:
                latest[patient_id] = row
        active_patients = {alert["patient_id"] for alert in self.alerts.values() if alert["resolved_at"] is None}
        return [{**row, "active_alert": row["patient_id"] in active_patients} for row in latest.values()]

    def patient_alerts(self, patient_id: str, status: str) -> list[dict[str, Any]]:
        rows = [alert for alert in self.alerts.values() if alert["patient_id"] == patient_id]
        if status == "active":
            rows = [alert for alert in rows if alert["resolved_at"] is None]
        elif status == "resolved":
            rows = [alert for alert in rows if alert["resolved_at"] is not None]
        return sorted(rows, key=lambda alert: alert["triggered_at"], reverse=True)

    def last_data_received(self) -> datetime | None:
        timestamps = [row["window_end"] for row in self.live_rows.values()]
        return max(timestamps) if timestamps else None
