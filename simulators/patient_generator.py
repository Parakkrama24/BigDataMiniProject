"""Generates the static patient reference data.

This is the seed for the `patients` table, which vitals_live, alerts_log,
lab_results and daily_risk_report all reference by foreign key -- nothing
else can be written until it is loaded, so this runs before the pipeline.

Patient ids use the same P001..PNNN scheme and the same simulator.num_patients
count as simulators/vitals_simulator.py, so the streams, the lab files and
this table all describe the same ward.

Run from the project root:
    python -m simulators.patient_generator            # writes docs/samples/patients.csv
    python -m simulators.patient_generator out.csv    # or a path of your choosing

Then load it (Member 3's loader):
    python storage/seed_patients.py docs/samples/patients.csv \\
        postgresql://hospital:hospital@localhost:5432/hospital
"""

from __future__ import annotations

import csv
import random
import sys
from datetime import timedelta
from pathlib import Path

from datetime import date

from common.config import load_settings

DEFAULT_OUTPUT = "docs/samples/patients.csv"
FIELDNAMES = ["patient_id", "ward", "admit_date"]
WARDS = ["ICU", "CARDIAC", "GENERAL_A", "GENERAL_B"]


def generate_patients() -> list[dict]:
    """One row per simulated patient, reproducible for a given seed."""
    settings = load_settings()["simulator"]
    random.seed(settings["seed"])

    # Anchored to sim_start_date rather than current_sim_date(): the simulated
    # clock advances 288x, so using "today" would bake the generation moment
    # into the committed CSV and leave patients admitted in the future once
    # anyone reset the clock back to day 0.
    sim_start = date.fromisoformat(load_settings()["sim_clock"]["sim_start_date"])

    patients = []
    for index in range(settings["num_patients"]):
        patient_id = f"P{index + 1:03d}"
        patients.append({
            "patient_id": patient_id,
            # Round-robin rather than random so every ward is populated even
            # with a small num_patients, which keeps the API's ?ward= filter
            # demonstrable.
            "ward": WARDS[index % len(WARDS)],
            "admit_date": (sim_start - timedelta(days=random.randint(1, 30))).isoformat(),
        })
    return patients


def write_patients_csv(patients: list[dict], path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(patients)


def main() -> None:
    path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT
    patients = generate_patients()
    write_patients_csv(patients, path)
    print(f"Wrote {len(patients)} patients to {path}")


if __name__ == "__main__":
    main()
