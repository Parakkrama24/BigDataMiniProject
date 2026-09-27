from datetime import datetime, timezone

from streaming.processing import aggregate_windows, clean_event, deduplicate_events, detect_alerts


def event(**overrides):
    value = {
        "event_id": "e-1",
        "patient_id": "P001",
        "heart_rate": 75,
        "spo2": 97,
        "systolic_bp": 120,
        "diastolic_bp": 78,
        "temperature": 36.8,
        "timestamp": "2026-01-01T12:03:00Z",
    }
    value.update(overrides)
    return value


def test_clean_event_normalizes_timestamp_and_numbers():
    cleaned = clean_event(event(heart_rate="80"))

    assert cleaned["heart_rate"] == 80.0
    assert cleaned["timestamp"] == datetime(2026, 1, 1, 12, 3, tzinfo=timezone.utc)


def test_clean_event_rejects_missing_identity_and_impossible_values():
    assert clean_event(event(patient_id="")) is None
    assert clean_event(event(spo2=101)) is None
    assert clean_event(event(timestamp="not-a-date")) is None


def test_deduplicate_events_keeps_first_event_id_only():
    assert len(deduplicate_events([event(), event(heart_rate=130)])) == 1


def test_all_threshold_alerts_are_detected():
    alerts = detect_alerts(event(heart_rate=121, spo2=89, temperature=38.6, systolic_bp=89))

    assert alerts == ["TACHYCARDIA", "HYPOXIA", "FEVER", "HYPOTENSION"]


def test_window_aggregation_groups_patient_events_and_ignores_null_vitals():
    first = clean_event(event(event_id="e-1", heart_rate=70))
    second = clean_event(event(event_id="e-2", heart_rate=None, timestamp="2026-01-01T12:04:00Z"))

    [aggregate] = aggregate_windows([first, second])

    assert aggregate["event_count"] == 2
    assert aggregate["avg_hr"] == 70.0
    assert aggregate["avg_temp"] == 36.8
    assert aggregate["window_start"] == datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
