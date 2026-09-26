import csv
import shutil
from pathlib import Path
import time

from common.config import load_settings
from observability.logging_config import get_logger

logger = get_logger("lab_loader")


EXPECTED_COLUMNS = {"patient_id", "test_type", "result_value", "reference_range", "collected_at"}

def validate_lab_file(path: str) -> bool:
    with open(path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)

        if set(reader.fieldnames) != EXPECTED_COLUMNS:
            return False

        for row in reader:
            if not row.get("patient_id"):
                return False
            if not row.get("test_type"):
                return False
            try:
                float(row["result_value"])
            except ValueError:
                return False

    return True


def process_lab_file(path: str) -> str:
    is_valid = validate_lab_file(path)
    dest_dir = "data/landing/processed" if is_valid else "data/landing/rejected"

    Path(dest_dir).mkdir(parents=True, exist_ok=True)
    dest_path = Path(dest_dir) / Path(path).name

    shutil.move(path, str(dest_path))

    if is_valid:
        logger.info("Lab file accepted", extra={"file": Path(path).name, "destination": str(dest_path)})
    else:
        logger.warning("Lab file rejected", extra={"file": Path(path).name, "destination": str(dest_path)})

    return str(dest_path)


def run(poll_interval_seconds: int = 5) -> None:
    settings = load_settings()["paths"]
    landing_dir = settings["landing_labs"]
    logger.info("Lab loader started", extra={"watching": landing_dir})

    while True:
        for csv_path in Path(landing_dir).glob("*.csv"):
            process_lab_file(str(csv_path))

        time.sleep(poll_interval_seconds)



if __name__ == "__main__":
    run()

