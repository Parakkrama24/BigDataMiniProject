# Hospital Patient Vital Signs Monitoring

## Member 2: local development

Install the pinned dependencies with the Python interpreter selected in VS Code:

```powershell
python -m pip install -r requirements.txt
python -m pytest -q
```

Run the API locally:

```powershell
python -m uvicorn api.main:app --reload
```

The development API provides `/vitals/live`, `/patients/{id}/alerts`, `/health`,
and interactive documentation at `/docs`. It uses an in-memory repository until
the Postgres schema is supplied by Member 3.

Process a JSONL sample without Kafka or Spark:

```powershell
python -m streaming.local_runner docs/samples/vitals.jsonl data/vitals/aggregates.jsonl
```

Run the production speed layer after Kafka and Postgres are available:

```powershell
python -m streaming.spark_job --bootstrap-servers localhost:9092
```

EC8203 Applied Big Data Engineering — Mini Project (UC2). Full design rationale,
architecture decision, and phase plan: [PROJECT_PLAN.md](PROJECT_PLAN.md).
Per-member task breakdown: [MEMBER_1_DATA_AND_INGESTION.md](MEMBER_1_DATA_AND_INGESTION.md) ·
[MEMBER_2_STREAMING_AND_API.md](MEMBER_2_STREAMING_AND_API.md) ·
[MEMBER_3_BATCH_AND_STORAGE.md](MEMBER_3_BATCH_AND_STORAGE.md)

## What's implemented so far (Member 1: data sources, ingestion, observability)

- **Simulators** (`simulators/`) — a streaming vitals generator (per-patient
  baselines, drift, injected spikes, null/duplicate/late-event noise) and a
  daily batch lab-results generator, both seeded for reproducibility.
- **Ingestion** (`ingestion/`) — a Kafka producer that validates and publishes
  vitals (keyed by `patient_id`, `acks=all`, retries, idempotence, invalid
  records routed to a DLQ topic), and a lab-file landing-zone loader that
  validates and files daily CSVs into `processed/`/`rejected/`.
- **Observability** (`observability/`) — structured JSON logging with a
  `trace_id` carried through Kafka message headers, Prometheus metrics,
  a Grafana dashboard, and two alert rules (no vitals received; DLQ error
  rate high).
- **Infra** — one `docker-compose.yml` for Kafka, Prometheus, and Grafana.

Streaming processing (Spark) remains the other member's part. Member 3's
batch/storage slice now includes the PostgreSQL schema, pure batch transforms,
the report router, and an Airflow DAG scaffold.

## Prerequisites

- Docker Desktop
- Python 3.11+ with `pip install -r requirements.txt`
- On Windows: PowerShell is the reliable shell for running Python commands
  directly. Git Bash works for most things but has two known quirks handled
  in the scripts below (see comments in `scripts/smoke.sh`).

## One-time setup

```
pip install -r requirements.txt
docker compose up -d
```

This starts Kafka (`localhost:9092`), Prometheus (`localhost:9090`), and
Grafana (`localhost:3000`, login `admin`/`admin`). The Kafka topics
(`vitals-stream`, `vitals-dlq`) aren't created automatically by Compose —
create them once with:

```
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic vitals-stream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic vitals-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

(`scripts/smoke.ps1` / `scripts/smoke.sh` do this automatically as part of
the smoke test below.)

## Before a demo or dev session

The simulated clock (`common/sim_clock.py`) runs 288x faster than real time
by default (1 simulated day = 5 real minutes), anchored to a fixed
`real_start` timestamp in `config/settings.yaml`. If that timestamp is old,
the simulated calendar will already be far in the future. Reset it to "now"
before you start:

```
python -m scripts.reset_sim_clock
```

## Running things

Each of these runs in its own terminal and keeps running until you `Ctrl+C`:

```
python -m ingestion.producer      # simulates + publishes vitals to Kafka
python -m ingestion.lab_loader    # watches data/landing/labs/ and files daily lab CSVs
python -m simulators.lab_simulator  # generates one day's lab CSV into data/landing/labs/
python -m storage.seed_patients patients.csv postgresql://hospital:hospital@localhost:5432/hospital
```

Member 3 batch development can run without Docker dependencies against the
committed samples. The pure transformations are in `batch/pipeline.py`, the
risk formula is in `batch/risk.py`, and the daily report route is
`GET /reports/daily/{date}`. Start PostgreSQL with `docker compose up -d
postgres`; its first startup applies `storage/schema.sql`. Airflow uses the
`daily_risk_report` DAG and supports catchup/backfill by simulated date.

Verify the whole ingestion path in one shot:

```
.\scripts\smoke.ps1     # Windows PowerShell (recommended on this machine)
bash scripts/smoke.sh   # Mac/Linux, or a properly-initialized Git Bash login shell
```

## Observability

- **Prometheus**: http://localhost:9090 — targets at `/targets`, alert rules
  at `/alerts`, ad-hoc queries at `/graph` (try `vitals_published_total`,
  `vitals_dlq_total`, `time() - vitals_last_event_unix_seconds`).
- **Grafana**: http://localhost:3000 (`admin`/`admin`) — the "Ward Monitoring
  Pipeline" dashboard auto-loads with events/sec, DLQ count, seconds-since-
  last-event, lab file counts, and an active-alerts panel.
- **Demo scripts** (`scripts/`):
  - `python scripts/demo_kill_producer.py` — starts the producer, kills it,
    watches the `NoVitalsReceived` alert transition to firing, then restarts
    it and watches the alert clear.
  - `python -m scripts.demo_bad_data` — sends deliberately malformed events
    straight to the DLQ topic using the real `validate_reading` logic, then
    shows you how to read them back from Kafka.

## Tests

```
python -m pytest -v
```

## Project layout

```
simulators/     vitals + lab data generators (Member 1)
ingestion/      Kafka producer, lab landing-zone loader (Member 1)
observability/  logging, metrics, Prometheus/Grafana config, alert rules (Member 1)
common/         shared config loader + simulated clock, used by everyone
streaming/      Spark Structured Streaming job (Member 2 — not yet implemented)
api/            FastAPI serving layer (Member 2 — not yet implemented)
batch/          Spark batch jobs (Member 3 — not yet implemented)
airflow/        Airflow DAGs (Member 3 — not yet implemented)
storage/        PostgreSQL schema, seed loader, and archive contract (Member 3)
config/         settings.yaml — single source of truth for all configuration
docs/samples/   committed example output from the simulators
scripts/        smoke tests, demo scripts, sim-clock reset helper
tests/          pytest suite
```
