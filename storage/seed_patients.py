"""Load a patient CSV into PostgreSQL using the schema's idempotent key."""
from __future__ import annotations

import csv
import sys
from pathlib import Path

import psycopg


def load_patients(csv_path: str | Path, database_url: str) -> int:
    with Path(csv_path).open(encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        required = {"patient_id", "ward", "admit_date"}
        if set(reader.fieldnames or ()) < required:
            raise ValueError(f"patient CSV must contain {sorted(required)}")
        rows = list(reader)
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """INSERT INTO patients (patient_id, ward, admit_date)
                   VALUES (%(patient_id)s, %(ward)s, %(admit_date)s)
                   ON CONFLICT (patient_id) DO UPDATE SET ward = EXCLUDED.ward, admit_date = EXCLUDED.admit_date""",
                rows,
            )
    return len(rows)


if __name__ == "__main__":
    if len(sys.argv) != 3:
        raise SystemExit("usage: python -m storage.seed_patients PATIENTS.csv DATABASE_URL")
    print(f"loaded {load_patients(sys.argv[1], sys.argv[2])} patients")
