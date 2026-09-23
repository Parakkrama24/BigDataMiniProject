import random

from simulators.vitals_simulator import generate_baseline, generate_reading


def test_reading_has_expected_schema():
    baseline = generate_baseline("P001")
    reading = generate_reading("P001", baseline)

    expected_keys = {
        "patient_id",
        "heart_rate",
        "spo2",
        "systolic_bp",
        "diastolic_bp",
        "temperature",
        "event_id",
        "timestamp",
    }
    assert isinstance(reading, dict)
    assert set(reading.keys()) == expected_keys

    assert 30 <= reading["heart_rate"] <= 220
    assert 50 <= reading["spo2"] <= 100
    assert 60 <= reading["systolic_bp"] <= 250
    assert 30 <= reading["diastolic_bp"] <= 150
    assert 30.0 <= reading["temperature"] <= 43.0


def test_baseline_is_reproducible_with_seed():
    random.seed(42)
    b1 = generate_baseline("P001")

    random.seed(42)
    b2 = generate_baseline("P001")

    assert b1 == b2
