import random

from simulators.lab_simulator import generate_lab_result


def test_lab_result_has_expected_schema():
    result = generate_lab_result("P001", "WBC")

    expected_keys = {
        "patient_id",
        "test_type",
        "result_value",
        "reference_range",
        "collected_at",
    }
    assert isinstance(result, dict)
    assert set(result.keys()) == expected_keys
    assert result["reference_range"] == "4.0-11.0"


def test_lab_result_is_reproducible_with_seed():
    random.seed(42)
    r1 = generate_lab_result("P001", "WBC")

    random.seed(42)
    r2 = generate_lab_result("P001", "WBC")

    r1_without_ts = {k: v for k, v in r1.items() if k != "collected_at"}
    r2_without_ts = {k: v for k, v in r2.items() if k != "collected_at"}
    assert r1_without_ts == r2_without_ts

