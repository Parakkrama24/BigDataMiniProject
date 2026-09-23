#!/usr/bin/env bash
# Smoke test: brings up the stack, produces briefly, consumes a few messages.
# Run from the project root: bash scripts/smoke.sh
set -e

echo "== Bringing up Docker Compose stack =="
docker compose up -d

echo "== Waiting for Kafka broker to be ready =="
sleep 10

echo "== Ensuring topics exist =="
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic vitals-stream --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists \
  --topic vitals-dlq --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1

echo "== Producing vitals for 8 seconds =="
timeout 8 python -m ingestion.producer || true

echo "== Consuming up to 5 sample messages from vitals-stream =="
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic vitals-stream --bootstrap-server localhost:9092 \
  --from-beginning --max-messages 5 --timeout-ms 15000

echo "== Smoke test passed =="
