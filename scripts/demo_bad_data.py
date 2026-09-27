"""
Observability demo #2: inject malformed data and show it hit the DLQ.

Reuses the real validate_reading() and build_producer() from
ingestion/producer.py, so this proves the actual production validation logic
rejects bad data the same way it would if it arrived from a broken sensor --
this isn't a separate mock, it's the real path.

Note: this script does NOT increment the live producer's vitals_dlq_total
metric, even though it imports the same module. Prometheus counters live in
each process's own memory -- this script runs as a separate process from
whatever producer you may have running, so the two never share counter state.
The DLQ message count in Kafka itself (checked below) is the real, always-
correct evidence; the metric only reflects DLQ events sent by whichever
process is actually running with the metrics server started.

Run from the project root:
    python scripts/demo_bad_data.py

Requires: Kafka up (docker compose up -d). Does not require the producer or
simulator to be running.
"""

from common.config import load_settings
from ingestion.producer import build_producer, validate_reading

BAD_EVENTS = [
    {
        "patient_id": None,  # missing patient id
        "timestamp": "2026-01-01T00:00:00+00:00",
        "heart_rate": 80,
        "spo2": 97,
        "systolic_bp": 118,
        "diastolic_bp": 76,
        "temperature": 36.8,
    },
    {
        "patient_id": "P999",
        "timestamp": "not-a-real-timestamp",  # unparseable timestamp
        "heart_rate": 80,
        "spo2": 97,
        "systolic_bp": 118,
        "diastolic_bp": 76,
        "temperature": 36.8,
    },
    {
        "patient_id": "P998",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "heart_rate": None,  # every vital missing -- a dead sensor
        "spo2": None,
        "systolic_bp": None,
        "diastolic_bp": None,
        "temperature": None,
    },
]


def main():
    settings = load_settings()["kafka"]
    producer = build_producer()

    print(f"Injecting {len(BAD_EVENTS)} deliberately malformed events...")
    sent = 0
    for reading in BAD_EVENTS:
        is_valid = validate_reading(reading)
        print(f"  patient_id={reading.get('patient_id')!r:>8}  valid={is_valid}")
        if is_valid:
            # Should never happen -- these were designed to be invalid.
            continue

        dlq_record = {"raw_payload": reading, "error_reason": "failed_validation (demo)"}
        producer.send(
            settings["topic_dlq"],
            key=reading.get("patient_id") or "unknown",
            value=dlq_record,
        )
        sent += 1

    producer.flush()
    print(f"\nSent {sent} events to '{settings['topic_dlq']}'.")
    print("Verify by reading them back from Kafka directly:")
    print(
        f"  docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh "
        f"--topic {settings['topic_dlq']} --bootstrap-server localhost:9092 "
        f"--max-messages {sent} --timeout-ms 15000"
    )
    print(
        "\nIf the real ingestion.producer service is running separately, its own "
        "vitals_dlq_total metric only reflects DLQ events *it* has sent -- to see "
        "this demo's events counted there too, route real bad data through the "
        "producer's own stream instead (e.g. temporarily raise null_field_probability "
        "high enough in config/settings.yaml, though our current validation rule only "
        "rejects all-null readings, not partial ones)."
    )


if __name__ == "__main__":
    main()
