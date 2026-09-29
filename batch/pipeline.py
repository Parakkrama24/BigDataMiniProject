"""Dependency-light daily batch transformations.

Database and Airflow adapters can call these functions without duplicating
validation or scoring rules.
"""

from __future__ import annotations

import csv
import json
import math
import re
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from batch.risk import Thresholds, score_patient

LAB_COLUMNS = {"patient_id", "test_type", "result_value", "reference_range", "collected_at"}
_REFERENCE_RANGE = re.compile(r"^\s*(-?\d+(?:\.\d+)?)\s*-\s*(-?\d+(?:\.\d+)?)\s*$")


def parse_reference_range(value: str) -> tuple[float, float]:
    match = _REFERENCE_RANGE.match(value)
    if not match:
        raise ValueError(f"invalid reference_range: {value!r}")
    lower, upper = map(float, match.groups())
    if lower > upper:
        raise ValueError(f"reference range lower bound exceeds upper bound: {value!r}")
    return lower, upper


def read_lab_file(path: str | Path, report_date: date | None = None) -> list[dict[str, Any]]:
    """Validate and normalize one daily lab CSV, rejecting the whole file on errors."""
    path = Path(path)
    with path.open("r", encoding="utf-8", newline="") as stream:
        reader = csv.DictReader(stream)
        if set(reader.fieldnames or ()) != LAB_COLUMNS:
            raise ValueError(f"lab file must contain exactly {sorted(LAB_COLUMNS)}")
        rows = []
        for line_number, row in enumerate(reader, start=2):
            try:
                if not row["patient_id"] or not row["test_type"]:
                    raise ValueError("patient_id and test_type are required")
                result = float(row["result_value"])
                lower, upper = parse_reference_range(row["reference_range"])
                collected_at = datetime.fromisoformat(row["collected_at"].replace("Z", "+00:00"))
                if report_date and collected_at.date() != report_date:
                    raise ValueError("collected_at is outside the report date")
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"invalid lab row {line_number}: {exc}") from exc
            rows.append({
                "patient_id": row["patient_id"], "test_type": row["test_type"],
                "result_value": result, "reference_range": row["reference_range"],
                "reference_low": lower, "reference_high": upper,
                "collected_at": collected_at.isoformat(),
                "is_abnormal": result < lower or result > upper,
            })
    return rows


def _iter_vitals(path: Path) -> Iterable[Mapping[str, Any]]:
    if path.suffix == ".jsonl":
        with path.open(encoding="utf-8") as stream:
            yield from (json.loads(line) for line in stream if line.strip())
    elif path.suffix == ".json":
        data = json.loads(path.read_text(encoding="utf-8"))
        yield from data if isinstance(data, list) else [data]
    else:
        raise ValueError(f"unsupported vitals archive format: {path.suffix}")


def aggregate_vitals(files: Iterable[str | Path], report_date: date) -> dict[str, dict[str, Any]]:
    values: dict[str, dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
    for file_path in files:
        for row in _iter_vitals(Path(file_path)):
            timestamp = datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
            if timestamp.date() != report_date or not row.get("patient_id"):
                continue
            for source, target in (("heart_rate", "hr"), ("spo2", "spo2"), ("systolic_bp", "systolic_bp"), ("diastolic_bp", "diastolic_bp"), ("temperature", "temp")):
                value = row.get(source)
                if value is not None and math.isfinite(float(value)):
                    values[row["patient_id"]][target].append(float(value))
    result = {}
    for patient_id, metrics in values.items():
        summary: dict[str, Any] = {"event_count": sum(map(len, metrics.values()))}
        for name, series in metrics.items():
            summary[f"avg_{name}"] = round(sum(series) / len(series), 3)
            summary[f"min_{name}"] = round(min(series), 3)
            summary[f"max_{name}"] = round(max(series), 3)
        result[patient_id] = summary
    return result


def summarize_labs(rows: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[str(row["patient_id"])].append(row)
    return {patient_id: {
        "result_count": len(patient_rows),
        "abnormal_count": sum(bool(row["is_abnormal"]) for row in patient_rows),
        "tests": [{"test_type": row["test_type"], "result_value": row["result_value"], "is_abnormal": row["is_abnormal"]} for row in patient_rows],
    } for patient_id, patient_rows in grouped.items()}


def build_reports(report_date: date, vitals: Mapping[str, Mapping[str, Any]], labs: Mapping[str, Mapping[str, Any]], thresholds: Thresholds = Thresholds()) -> list[dict[str, Any]]:
    patient_ids = sorted(set(vitals) | set(labs))
    reports = []
    for patient_id in patient_ids:
        vital_summary = dict(vitals.get(patient_id, {}))
        lab_summary = dict(labs.get(patient_id, {"result_count": 0, "abnormal_count": 0, "tests": []}))
        risk_score, risk_flag = score_patient(vital_summary, lab_summary, thresholds)
        reports.append({"patient_id": patient_id, "report_date": report_date.isoformat(), "vitals_summary": vital_summary, "lab_summary": lab_summary, "risk_score": risk_score, "risk_flag": risk_flag})
    return reports
