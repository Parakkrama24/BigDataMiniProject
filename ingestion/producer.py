from datetime import datetime
import json

from kafka import KafkaProducer

from common.config import load_settings
from simulators.vitals_simulator import stream_vitals

VITAL_FIELDS = ["heart_rate", "spo2", "systolic_bp", "diastolic_bp", "temperature"]


def validate_reading(reading: dict) -> bool:
    if not reading.get("patient_id"):
        return False

    try:
        datetime.fromisoformat(reading.get("timestamp", ""))
    except ValueError:
        return False

    if all(reading.get(field) is None for field in VITAL_FIELDS):
        return False

    return True

def build_producer() -> KafkaProducer:
    settings = load_settings()["kafka"]
    return KafkaProducer(
        bootstrap_servers=settings["bootstrap_servers"],
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        enable_idempotence=True,
    )


def run():
    settings = load_settings()["kafka"]
    producer = build_producer()

    for reading in stream_vitals():
        if validate_reading(reading):
            producer.send(settings["topic_vitals"], key=reading["patient_id"], value=reading)
        else:
            dlq_record = {"raw_payload": reading, "error_reason": "failed_validation"}
            producer.send(settings["topic_dlq"], key=reading.get("patient_id", "unknown"), value=dlq_record)


if __name__ == "__main__":
    run()
