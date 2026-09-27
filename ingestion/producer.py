from datetime import datetime
import json
import uuid

from kafka import KafkaProducer

from common.config import load_settings
from observability.logging_config import get_logger
from simulators.vitals_simulator import stream_vitals

from observability.metrics import counter, gauge, start_metrics_server

logger = get_logger("producer")

EVENTS_PUBLISHED = counter("vitals_published_total", "Valid vitals events sent to Kafka")
EVENTS_DLQ = counter("vitals_dlq_total", "Events routed to the dead-letter queue")
LAST_EVENT_TIME = gauge("vitals_last_event_unix_seconds", "Unix time of the last published vitals event")


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

    start_metrics_server(load_settings()["observability"]["producer_metrics_port"])

    logger.info("Producer started", extra={"topic": settings["topic_vitals"]})

    for reading in stream_vitals():
        # Reuse event_id as the trace_id; fall back to a fresh one if it's missing.
        trace_id = reading.get("event_id") or str(uuid.uuid4())
        headers = [("trace_id", trace_id.encode("utf-8"))]

        if validate_reading(reading):
            producer.send(
                settings["topic_vitals"],
                key=reading["patient_id"],
                value=reading,
                headers=headers,
            )
            EVENTS_PUBLISHED.inc()
            LAST_EVENT_TIME.set_to_current_time()

            logger.info(
                "Published vitals event",
                extra={"trace_id": trace_id, "patient_id": reading["patient_id"]},
            )
        else:
            dlq_record = {"raw_payload": reading, "error_reason": "failed_validation"}
            producer.send(
                settings["topic_dlq"],
                key=reading.get("patient_id") or "unknown",
                value=dlq_record,
                headers=headers,
            )
            EVENTS_DLQ.inc()

            logger.warning(
                "Routed event to DLQ",
                extra={"trace_id": trace_id, "patient_id": reading.get("patient_id")},
            )

if __name__ == "__main__":
    run()
