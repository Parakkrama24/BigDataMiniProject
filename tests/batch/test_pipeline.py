from datetime import date

import pytest

from batch.pipeline import build_reports, parse_reference_range, read_lab_file
from batch.risk import Thresholds, score_patient


def test_score_normal_patient_is_low():
    score, flag = score_patient({"avg_hr": 75, "avg_spo2": 97, "avg_temp": 37, "avg_systolic_bp": 120}, {"result_count": 2, "abnormal_count": 0})
    assert score == 0
    assert flag == "LOW"


def test_score_boundary_and_missing_inputs():
    score, flag = score_patient({"avg_hr": 121}, {"result_count": 1, "abnormal_count": 1})
    assert score == 90
    assert flag == "HIGH"

    assert score_patient(None, None) == (0.0, "LOW")


def test_reference_range_and_bad_range():
    assert parse_reference_range("4.0-11.0") == (4.0, 11.0)
    with pytest.raises(ValueError):
        parse_reference_range("bad")


def test_lab_file_is_normalized(tmp_path):
    path = tmp_path / "labs.csv"
    path.write_text("patient_id,test_type,result_value,reference_range,collected_at\nP001,WBC,12,4-11,2026-01-01T00:00:00+00:00\n", encoding="utf-8")
    rows = read_lab_file(path, date(2026, 1, 1))
    assert rows[0]["is_abnormal"] is True
    assert rows[0]["reference_low"] == 4


def test_reports_are_deterministically_sorted_and_overwriteable():
    reports = build_reports(date(2026, 1, 1), {"P002": {"avg_hr": 70}}, {"P001": {"result_count": 1, "abnormal_count": 0}})
    assert [row["patient_id"] for row in reports] == ["P001", "P002"]
    assert len({(row["patient_id"], row["report_date"]) for row in reports}) == len(reports)
