from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import yaml


VITAL_FIELDS = ("heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature")
ALERT_TYPES = ("TACHYCARDIA", "HYPOXIA", "FEVER", "HYPOTENSION")
VALID_RANGES = {
    "heart_rate": (30, 220),
    "spo2": (50, 100),
    "systolic_bp": (60, 250),
    "diastolic_bp": (30, 150),
    "temperature": (30, 43),
}


def load_thresholds(path: str | Path | None = None) -> dict[str, float]:
    config_path = Path(path) if path else Path(__file__).resolve().parents[1] / "config" / "thresholds.yaml"
    with config_path.open("r", encoding="utf-8") as config_file:
        values = yaml.safe_load(config_file) or {}
    # config/thresholds.yaml nests the vital limits under "vitals:" (batch risk
    # scoring thresholds live under "risk:"). Fall back to treating the whole
    # file as a flat mapping so a flat thresholds file still works.
    vitals = values.get("vitals", values)
    return {key: float(value) for key, value in vitals.items()}


def parse_timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return timestamp.astimezone(timezone.utc)


def clean_event(event: Mapping[str, Any]) -> dict[str, Any] | None:
    """Return a normalized event, or None when identity/time/values are invalid."""
    patient_id = event.get("patient_id")
    event_id = event.get("event_id")
    timestamp = parse_timestamp(event.get("timestamp"))
    if not patient_id or not event_id or timestamp is None:
        return None

    cleaned = dict(event)
    cleaned["patient_id"] = str(patient_id)
    cleaned["event_id"] = str(event_id)
    cleaned["timestamp"] = timestamp
    for field in VITAL_FIELDS:
        value = event.get(field)
        if value is None:
            cleaned[field] = None
            continue
        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return None
        minimum, maximum = VALID_RANGES[field]
        if not minimum <= numeric_value <= maximum:
            return None
        cleaned[field] = numeric_value
    return cleaned


def deduplicate_events(events: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    unique_events = []
    for event in events:
        event_id = str(event["event_id"])
        if event_id in seen:
            continue
        seen.add(event_id)
        unique_events.append(dict(event))
    return unique_events


def detect_alerts(event: Mapping[str, Any], thresholds: Mapping[str, float] | None = None) -> list[str]:
    limits = thresholds or load_thresholds()
    alerts = []
    if event.get("heart_rate") is not None and event["heart_rate"] > limits["hr_high"]:
        alerts.append("TACHYCARDIA")
    if event.get("spo2") is not None and event["spo2"] < limits["spo2_low"]:
        alerts.append("HYPOXIA")
    if event.get("temperature") is not None and event["temperature"] > limits["temp_high"]:
        alerts.append("FEVER")
    if event.get("systolic_bp") is not None and event["systolic_bp"] < limits["sbp_low"]:
        alerts.append("HYPOTENSION")
    return alerts


def window_start(timestamp: datetime, window_minutes: int = 5) -> datetime:
    timestamp = timestamp.astimezone(timezone.utc)
    minute = timestamp.minute - (timestamp.minute % window_minutes)
    return timestamp.replace(minute=minute, second=0, microsecond=0)


def aggregate_windows(events: Iterable[Mapping[str, Any]], window_minutes: int = 5) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, datetime], list[Mapping[str, Any]]] = {}
    for event in events:
        start = window_start(event["timestamp"], window_minutes)
        grouped.setdefault((event["patient_id"], start), []).append(event)

    aggregates = []
    for (patient_id, start), rows in sorted(grouped.items()):
        result: dict[str, Any] = {
            "patient_id": patient_id,
            "window_start": start,
            "window_end": start + timedelta(minutes=window_minutes),
            "event_count": len(rows),
            "anomaly_flags": sorted({alert for row in rows for alert in detect_alerts(row)}),
        }
        for field, output_name in (
            ("heart_rate", "avg_hr"),
            ("spo2", "avg_spo2"),
            ("systolic_bp", "avg_systolic_bp"),
            ("diastolic_bp", "avg_diastolic_bp"),
            ("temperature", "avg_temp"),
        ):
            values = [row[field] for row in rows if row.get(field) is not None]
            result[output_name] = sum(values) / len(values) if values else None
        aggregates.append(result)
    return aggregates
