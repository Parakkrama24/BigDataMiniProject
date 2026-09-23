import random
from common.sim_clock import now as sim_now
import csv
from pathlib import Path
from common.sim_clock import current_sim_date
from common.config import load_settings

TEST_TYPES = {
    "WBC": (4.0, 11.0),
    "CRP": (0.0, 5.0),
    "Creatinine": (0.6, 1.3),
    "Glucose": (70, 100),
}


def generate_lab_result(patient_id: str, test_type: str, abnormal_probability: float = 0.1) -> dict:
    low, high = TEST_TYPES[test_type]

    if random.random() < abnormal_probability:
        side = random.choice(["low", "high"])
        if side == "low":
            result_value = random.uniform(low * 0.5, low)
        else:
            result_value = random.uniform(high, high * 1.5)
    else:
        result_value = random.uniform(low, high)

    return {
        "patient_id": patient_id,
        "test_type": test_type,
        "result_value": result_value,
        "reference_range": f"{low}-{high}",
        "collected_at": sim_now().isoformat(),
    }

def generate_daily_labs(patient_ids: list) -> list:
    rows = []
    for patient_id in patient_ids:
        for test_type in TEST_TYPES:
            rows.append(generate_lab_result(patient_id, test_type))
    return rows

def write_labs_csv(rows: list, path: str) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["patient_id", "test_type", "result_value", "reference_range", "collected_at"]

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

if __name__ == "__main__":
    settings = load_settings()["simulator"]
    patient_ids = [f"P{i+1:03d}" for i in range(settings["num_patients"])]

    rows = generate_daily_labs(patient_ids)
    date_str = current_sim_date().isoformat()
    path = f"data/landing/labs/labs_{date_str}.csv"

    write_labs_csv(rows, path)
    print(f"Wrote {len(rows)} rows to {path}")