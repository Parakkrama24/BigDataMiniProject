from datetime import datetime

from pydantic import BaseModel, ConfigDict


class LiveVitals(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    patient_id: str
    ward: str | None = None
    window_start: datetime
    window_end: datetime
    avg_hr: float | None = None
    avg_spo2: float | None = None
    avg_systolic_bp: float | None = None
    avg_diastolic_bp: float | None = None
    avg_temp: float | None = None
    event_count: int
    anomaly_flags: list[str]
    active_alert: bool


class Alert(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    alert_id: str
    patient_id: str
    alert_type: str
    triggered_at: datetime
    resolved_at: datetime | None = None


class HealthResponse(BaseModel):
    status: str
    database: str
    last_data_received: datetime | None = None
