"""Tests for the patients seed data.

Everything else in the schema has a foreign key to `patients`, so these ids
must match the ones the vitals stream and lab files use or nothing can be
written.
"""

import csv
from datetime import date

from common.config import load_settings
from simulators.patient_generator import FIELDNAMES, WARDS, generate_patients, write_patients_csv


def test_ids_match_the_vitals_simulator_scheme():
    expected_count = load_settings()["simulator"]["num_patients"]

    patients = generate_patients()

    assert len(patients) == expected_count
    assert [p["patient_id"] for p in patients[:3]] == ["P001", "P002", "P003"]


def test_every_ward_is_populated():
    # Round-robin assignment, so the API's ?ward= filter is demonstrable even
    # with a small num_patients.
    wards = {p["ward"] for p in generate_patients()}

    assert wards == set(WARDS)


def test_nobody_is_admitted_after_the_simulation_starts():
    # Anchored to sim_start_date, so this holds no matter when the CSV was
    # generated or whether the simulated clock has since been reset.
    sim_start = load_settings()["sim_clock"]["sim_start_date"]

    for patient in generate_patients():
        assert patient["admit_date"] < sim_start, patient
        assert date.fromisoformat(patient["admit_date"])  # parses as a real date


def test_output_is_reproducible():
    assert generate_patients() == generate_patients()


def test_csv_has_the_columns_the_loader_requires(tmp_path):
    # storage/seed_patients.py rejects a CSV missing any of these.
    path = tmp_path / "patients.csv"

    write_patients_csv(generate_patients(), str(path))

    with path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == FIELDNAMES
        assert len(list(reader)) == load_settings()["simulator"]["num_patients"]
