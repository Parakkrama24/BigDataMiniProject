#!/usr/bin/env bash
# Smoke test: brings up the stack, starts a consumer, produces while it's
# listening, and fails loudly if no messages arrive.
#
# On Windows, `bash scripts/smoke.sh` run directly from PowerShell/cmd starts
# a non-login Git Bash shell that may not resolve `python` on PATH. Use
# `bash -l scripts/smoke.sh`, or scripts/smoke.ps1 in PowerShell instead.
#
# Run from the project root: bash scripts/smoke.sh
set -e

# On Windows, Git Bash (MSYS) rewrites absolute-looking paths like
# /opt/kafka/bin/... into Windows paths before docker ever sees them, even
# though that path is meant to be resolved *inside* the Linux container.
# This disables that rewriting. It's a no-op (and harmless) on real Linux/Mac.
export MSYS_NO_PATHCONV=1

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

# Start the consumer FIRST, in the background, so it's already subscribed
# before the producer sends anything. A consumer started AFTER production
# stops (without --from-beginning) will never see those messages -- there
# has to be a moment where both are running at once.
echo "== Starting consumer (background) =="
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --topic vitals-stream --bootstrap-server localhost:9092 \
  --max-messages 5 --timeout-ms 20000 > /tmp/smoke_consumer_out.txt 2>&1 &
CONSUMER_PID=$!
sleep 2  # give it time to actually subscribe before we produce

echo "== Producing vitals for 8 seconds =="
python -m ingestion.producer &
PRODUCER_PID=$!
sleep 8
kill "$PRODUCER_PID" 2>/dev/null || true

echo "== Waiting for consumer result =="
wait "$CONSUMER_PID" || true
cat /tmp/smoke_consumer_out.txt

if grep -q "Processed a total of 0 messages" /tmp/smoke_consumer_out.txt; then
  echo "== SMOKE TEST FAILED: consumer received 0 messages =="
  rm -f /tmp/smoke_consumer_out.txt
  exit 1
fi
if ! grep -q "Processed a total of" /tmp/smoke_consumer_out.txt; then
  echo "== SMOKE TEST FAILED: could not confirm the consumer ran =="
  rm -f /tmp/smoke_consumer_out.txt
  exit 1
fi

rm -f /tmp/smoke_consumer_out.txt
echo "== Smoke test passed =="
