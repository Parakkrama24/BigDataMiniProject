import csv

from ingestion.lab_loader import validate_lab_file

GOOD_ROW = {
    "patient_id": "P001",
    "test_type": "WBC",
    "result_value": "7.5",
    "reference_range": "4.0-11.0",
    "collected_at": "2026-01-01T00:00:00+00:00",
}


def _write_csv(path, fieldnames, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def test_valid_lab_file_passes(tmp_path):
    path = tmp_path / "labs.csv"
    _write_csv(path, list(GOOD_ROW.keys()), [GOOD_ROW])

    assert validate_lab_file(str(path)) is True


def test_wrong_columns_fail(tmp_path):
    path = tmp_path / "labs.csv"
    _write_csv(path, ["patient_id", "test_type", "result_value"], [{"patient_id": "P001", "test_type": "WBC", "result_value": "7.5"}])

    assert validate_lab_file(str(path)) is False


def test_non_numeric_result_value_fails(tmp_path):
    path = tmp_path / "labs.csv"
    bad_row = dict(GOOD_ROW, result_value="not_a_number")
    _write_csv(path, list(GOOD_ROW.keys()), [bad_row])

    assert validate_lab_file(str(path)) is False


def test_empty_file_fails_without_crashing(tmp_path):
    # Regression test: an empty CSV makes csv.DictReader.fieldnames return
    # None, which used to raise TypeError from set(None) inside
    # validate_lab_file. It must return False instead of crashing.
    path = tmp_path / "empty.csv"
    path.write_text("", encoding="utf-8")

    assert validate_lab_file(str(path)) is False
