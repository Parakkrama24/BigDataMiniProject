from ingestion.producer import validate_reading


def test_valid_reading_passes():
    reading = {
        "patient_id": "P001",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "heart_rate": 75,
        "spo2": None,
        "systolic_bp": 118,
        "diastolic_bp": 76,
        "temperature": 36.8,
    }
    assert validate_reading(reading) is True


def test_missing_patient_id_fails():
    reading = {
        "patient_id": None,
        "timestamp": "2026-01-01T00:00:00+00:00",
        "heart_rate": 75,
        "spo2": 97,
        "systolic_bp": 118,
        "diastolic_bp": 76,
        "temperature": 36.8,
    }
    assert validate_reading(reading) is False


def test_bad_timestamp_fails():
    reading = {
        "patient_id": "P001",
        "timestamp": "not-a-date",
        "heart_rate": 75,
        "spo2": 97,
        "systolic_bp": 118,
        "diastolic_bp": 76,
        "temperature": 36.8,
    }
    assert validate_reading(reading) is False


def test_all_null_vitals_fails():
    reading = {
        "patient_id": "P001",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "heart_rate": None,
        "spo2": None,
        "systolic_bp": None,
        "diastolic_bp": None,
        "temperature": None,
    }
    assert validate_reading(reading) is False
