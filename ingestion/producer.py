from datetime import datetime
import json

from kafka import KafkaProducer

from common.config import load_settings
from observability.logging_config import get_logger
from simulators.vitals_simulator import stream_vitals

logger = get_logger("producer")

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
    logger.info("Producer started", extra={"topic": settings["topic_vitals"]})

    for reading in stream_vitals():
        if validate_reading(reading):
            producer.send(settings["topic_vitals"], key=reading["patient_id"], value=reading)
            logger.info(
                "Published vitals event",
                extra={"trace_id": reading["event_id"], "patient_id": reading["patient_id"]},
            )
        else:
            dlq_record = {"raw_payload": reading, "error_reason": "failed_validation"}
            producer.send(settings["topic_dlq"], key=reading.get("patient_id", "unknown"), value=dlq_record)
            logger.warning(
                "Routed event to DLQ",
                extra={"trace_id": reading.get("event_id"), "patient_id": reading.get("patient_id")},
            )


if __name__ == "__main__":
    run()
