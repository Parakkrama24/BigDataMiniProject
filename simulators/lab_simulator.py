import argparse
import csv
import random
import time
from pathlib import Path

from common.config import load_settings
from common.sim_clock import current_sim_date
from common.sim_clock import now as sim_now

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

def emit_day(patient_ids: list, sim_date) -> str:
    """Write one simulated day's lab file into the landing zone."""
    landing = load_settings()["paths"]["landing_labs"]
    path = f"{landing}/labs_{sim_date.isoformat()}.csv"
    rows = generate_daily_labs(patient_ids)
    write_labs_csv(rows, path)
    # flush=True so --loop shows progress live; Python buffers stdout when it
    # is piped rather than attached to a terminal.
    print(f"Wrote {len(rows)} rows to {path}", flush=True)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate daily pathology lab files.")
    parser.add_argument(
        "--loop",
        action="store_true",
        help="Keep running, emitting one file each time the simulated date advances. "
             "A simulated day passes every sim_day_seconds (default 5 real minutes), "
             "which is also the Airflow DAG's schedule, so this keeps the batch layer fed.",
    )
    parser.add_argument("--poll-seconds", type=float, default=5.0,
                        help="How often to check whether the simulated date has advanced.")
    args = parser.parse_args()

    settings = load_settings()["simulator"]
    patient_ids = [f"P{i+1:03d}" for i in range(settings["num_patients"])]

    emitted = set()
    while True:
        sim_date = current_sim_date()
        if sim_date not in emitted:
            emit_day(patient_ids, sim_date)
            emitted.add(sim_date)
        if not args.loop:
            return
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    main()