# Hospital Patient Vital Signs Monitoring

EC8203 Applied Big Data Engineering — Mini Project (UC2).

A Lambda-architecture pipeline over simulated hospital data: bedside vitals
stream through Kafka into a Spark Structured Streaming speed layer, while
pathology lab results arrive as one file per simulated day and are joined with
the previous day's vitals trend by an Airflow-orchestrated batch layer into a
consolidated patient risk report.

- Design rationale and phase plan: [PROJECT_PLAN.md](PROJECT_PLAN.md)
- Lambda vs Kappa decision: [docs/architecture_decision.md](docs/architecture_decision.md)
- Interface contracts between the layers: [docs/contracts.md](docs/contracts.md)
- Per-member breakdown: [Member 1](MEMBER_1_DATA_AND_INGESTION.md) ·
  [Member 2](MEMBER_2_STREAMING_AND_API.md) ·
  [Member 3](MEMBER_3_BATCH_AND_STORAGE.md)

## Architecture at a glance

```
simulators/          ingestion/              streaming/            api/
vitals_simulator ──► producer ──► Kafka ──► spark_job ──┬──► Postgres ──► FastAPI
                     (validate,   vitals-   (clean,     │    vitals_live   /vitals/live
                      DLQ)        stream     dedup,     │    alerts_log    /patients/../alerts
                                             window,    │                  /health
                                             threshold) │
                                                        └──► Parquet archive
lab_simulator ──► data/landing/labs ──► lab_loader ──► processed/          │
                                        (validate,          │             │
                                         move)              ▼             ▼
                                                    airflow/dags ──► batch/pipeline
                                                    daily_risk_report ──► Postgres
                                                                          /reports/daily/..
```

## Prerequisites

- Docker Desktop
- Python 3.11+ (`pip install -r requirements.txt`)
- On Windows, run Python commands from **PowerShell**. Git Bash works for most
  things but has two quirks the scripts work around — see the comments in
  [scripts/smoke.sh](scripts/smoke.sh).

## Start the stack

```
pip install -r requirements.txt
docker compose up -d
```

That brings up eight services:

| Service | URL | Notes |
|---|---|---|
| postgres | `localhost:5432` | applies `storage/schema.sql` on first start |
| kafka | `localhost:9092` | KRaft mode, single broker |
| api | http://localhost:8000/docs | FastAPI, reads Postgres |
| airflow-webserver | http://localhost:8080 | login `admin` / `admin` |
| airflow-scheduler | — | runs the `daily_risk_report` DAG |
| prometheus | http://localhost:9090 | metrics, alert rules at `/alerts` |
| grafana | http://localhost:3000 | login `admin` / `admin` |
| airflow-init | — | one-shot: DB migrate + create admin user |

> **Already run PostgreSQL natively?** It holds host port 5432 and silently
> shadows the container — connections from your machine reach the native server
> and fail authentication. Start with a different host port instead:
> ```
> POSTGRES_HOST_PORT=5433 docker compose up -d
> ```
> The in-container port stays 5432, so the `postgres:5432` URLs the other
> containers use are unaffected.

Kafka topics are not created by Compose. Create them once (or let
`scripts/smoke.ps1` do it):

```
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic vitals-stream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic vitals-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Before a demo or dev session

**1. Reset the simulated clock.** `common/sim_clock.py` runs 288x faster than
real time (1 simulated day = 5 real minutes), anchored to `real_start` in
`config/settings.yaml`. If that anchor is old, the simulated calendar is
already far in the future — harmless, but confusing to demo.

```
python -m scripts.reset_sim_clock
```

**2. Seed the patients table.** Everything else references it by foreign key,
so nothing can be written until it is loaded.

```
python -m simulators.patient_generator
python storage/seed_patients.py docs/samples/patients.csv postgresql://hospital:hospital@localhost:5432/hospital
```

## Running the pipeline

Each of these keeps running until `Ctrl+C`, so give each its own terminal:

```
python -m ingestion.producer          # vitals simulator -> validate -> Kafka (+ DLQ)
python -m ingestion.lab_loader        # watch landing/labs -> validate -> processed/ or rejected/
python -m simulators.lab_simulator    # drop one simulated day's lab CSV (run repeatedly)
```

Speed layer, once Kafka and Postgres are up:

```
python -m streaming.spark_job --bootstrap-servers localhost:9092
```

Or process a sample file with no Kafka or Spark at all:

```
python -m streaming.local_runner docs/samples/vitals_sample.jsonl data/aggregates.jsonl
```

### Batch layer

The `daily_risk_report` DAG is **paused by default**. Each run maps to one
simulated day: it waits for that day's validated lab file, aggregates the
Parquet vitals archive, joins and scores, then upserts `daily_risk_report`.
Unpause it in the UI, or run one day synchronously:

```
docker exec airflow-scheduler airflow dags test daily_risk_report 2026-09-30T04:00:00+00:00
```

Keep `lab_simulator` and `lab_loader` running while the DAG is unpaused —
otherwise the sensor finds no lab file for the current simulated day and the
run fails after its timeout. Backfill a range on demand with:

```
docker exec airflow-scheduler airflow dags backfill daily_risk_report -s <start> -e <end>
```

## Verify it works

```
.\scripts\smoke.ps1     # Windows PowerShell
bash scripts/smoke.sh   # Mac/Linux, or a Git Bash login shell
```

The smoke test brings up the stack, creates topics, starts a consumer, produces
while it is listening, and fails loudly if no messages arrive.

## Observability

- **Prometheus** http://localhost:9090 — `/targets`, `/alerts`, and `/graph`
  (try `vitals_published_total`, `vitals_dlq_total`,
  `time() - vitals_last_event_unix_seconds`).
- **Grafana** http://localhost:3000 — the "Ward Monitoring Pipeline" dashboard
  auto-loads: events/sec, DLQ count, seconds since last event, lab file counts,
  and active alerts.
- **Demo scenarios**:
  ```
  python scripts/demo_kill_producer.py   # kill the producer, watch NoVitalsReceived fire, restart, watch it clear
  python -m scripts.demo_bad_data        # push malformed events through real validation into the DLQ
  ```

## Tests

```
python -m pytest -q
```

Tests that need Postgres skip automatically when none is reachable. To include
them, point them at your database:

```
$env:DATABASE_URL="postgresql://hospital:hospital@localhost:5432/hospital"; python -m pytest -q
```

## Project layout

```
simulators/     vitals, lab and patient generators            (Member 1)
ingestion/      Kafka producer, lab landing-zone loader       (Member 1)
observability/  logging, metrics, Prometheus/Grafana, alerts  (Member 1)
streaming/      Spark Structured Streaming job, Postgres sink (Member 2)
api/            FastAPI serving layer + repositories          (Member 2)
batch/          pure batch transforms and risk scoring        (Member 3)
airflow/        daily_risk_report DAG                         (Member 3)
storage/        PostgreSQL schema, seed loader                (Member 3)
common/         shared config loader + simulated clock        (shared)
config/         settings.yaml, thresholds.yaml                (shared)
docker/         Dockerfiles and per-image requirements
docs/           architecture decision, contracts, samples
scripts/        smoke tests, demo scripts, sim-clock reset
tests/          pytest suite
```
