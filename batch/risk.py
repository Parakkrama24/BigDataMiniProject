"""Pure, deterministic functions used by the daily risk batch."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


@dataclass(frozen=True)
class Thresholds:
    hr_high: float = 120.0
    spo2_low: float = 90.0
    temp_high: float = 38.5
    sbp_low: float = 90.0


def _number(values: Mapping[str, Any], key: str) -> float | None:
    value = values.get(key)
    return float(value) if value is not None else None


def score_patient(
    vitals: Mapping[str, Any] | None,
    labs: Mapping[str, Any] | None,
    thresholds: Thresholds = Thresholds(),
) -> tuple[float, str]:
    """Return a bounded 0-100 score and LOW/MEDIUM/HIGH flag.

    The score is 60% abnormal-vital burden, 30% abnormal-lab burden, and
    10% trend direction. Missing inputs contribute zero rather than guessing.
    """
    vitals = vitals or {}
    labs = labs or {}
    vital_checks = (
        (_number(vitals, "avg_hr"), lambda value: value > thresholds.hr_high),
        (_number(vitals, "avg_spo2"), lambda value: value < thresholds.spo2_low),
        (_number(vitals, "avg_temp"), lambda value: value > thresholds.temp_high),
        (_number(vitals, "avg_systolic_bp"), lambda value: value < thresholds.sbp_low),
    )
    available_vitals = [check(value) for value, check in vital_checks if value is not None]
    vital_burden = sum(available_vitals) / len(available_vitals) if available_vitals else 0.0

    lab_count = int(labs.get("abnormal_count", 0) or 0)
    total_lab_count = int(labs.get("result_count", 0) or 0)
    lab_burden = min(lab_count / max(total_lab_count, 1), 1.0)

    direction = str(vitals.get("trend_direction", "stable")).lower()
    trend_burden = 1.0 if direction == "worsening" else 0.5 if direction == "mixed" else 0.0
    score = round(min(100.0, 100.0 * (0.6 * vital_burden + 0.3 * lab_burden + 0.1 * trend_burden)), 2)
    flag = "HIGH" if score >= 70 else "MEDIUM" if score >= 30 else "LOW"
    return score, flag
