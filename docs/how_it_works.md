# How the system works

A component-by-component walkthrough of the pipeline, plus how to start it and
what to show in the demo video.

The assignment states you must be able to **explain and defend every
architectural decision and every line of core pipeline logic in a viva**, so
this document explains *why* each piece exists, not just what it does.

Related: [PROJECT_PLAN.md](../PROJECT_PLAN.md) (design + phases) ·
[architecture_decision.md](architecture_decision.md) (Lambda vs Kappa) ·
[contracts.md](contracts.md) (interfaces between layers) ·
[README.md](../README.md) (setup reference)

---

## 1. The business question

> *Which patients show concerning vital-sign trends right now, and how do
> yesterday's lab results change the risk picture for those patients going
> forward?*

That single sentence forces the architecture. It has **two halves**:

- *"right now"* → needs **low latency**, seconds not hours → a **speed layer**
- *"yesterday's lab results… going forward"* → needs a **stable, re-runnable
  daily artifact** → a **batch layer**

Two different latency and correctness requirements over the same data is the
textbook case for **Lambda architecture**. A Kappa architecture would force
the once-a-day lab file through a streaming log for no benefit, and would
leave Airflow — a required tool — barely used.

## 2. The simulated clock — `common/sim_clock.py`

Everything hinges on this, so understand it first.

Real hospital data takes days to be interesting. We compress it: **one
simulated day passes every 5 real minutes** (`sim_day_seconds: 300`), which is
a **288x** speed-up.

```
speedup       = 86400 / sim_day_seconds          = 288
simulated_now = sim_start_date + speedup * (real_now - real_start)
```

| Function | Purpose |
|---|---|
| `now(real_now=None)` | simulated datetime for a real instant |
| `current_sim_date()` | what simulated day it is right now |
| `sim_date_for(real_ts)` | simulated date a given real instant maps to |
| `real_window_for(sim_date)` | the real time window during which a simulated day is "today" |

**Why `sim_date_for` matters:** Airflow must be deterministic. If a task asked
"what day is it now?" using the wall clock, a retry five seconds later would
get a *different* simulated day (288x, remember). Instead each DAG run
converts its own fixed **logical date** into a simulated date, so retries and
backfills always resolve to the same day.

**Reset it before every demo** (`python -m scripts.reset_sim_clock`), or the
simulated calendar will already be months ahead of `sim_start_date`.

## 3. Data sources (the simulators)

### `simulators/vitals_simulator.py` — the streaming source

Each patient gets a fixed **baseline** (their personal resting vitals) drawn
from a normal distribution, then every reading is `baseline + small drift`, so
values wander realistically instead of being flat or pure noise.

Deliberately injected imperfections, each independently configurable in
`config/settings.yaml`:

| Injection | Default rate | Why it exists |
|---|---|---|
| Abnormal spikes | 5% | Gives the alerting layer something to detect (tachycardia, hypoxia, fever, hypotension) |
| Null fields | 1% | Sensor dropout — the streaming layer must clean it |
| Duplicate events | 0.5% | Same `event_id` twice — forces deduplication |
| Late timestamps | 2% | Backdated up to 5 min — forces watermarking |

All values are **clamped** to physiologically possible ranges, and the whole
generator is **seeded** so runs are reproducible.

`stream_vitals()` is a Python **generator** — it yields readings forever rather
than returning a list, because the stream never ends.

### `simulators/lab_simulator.py` — the daily batch source

Writes one CSV per simulated day: every patient × four test types (WBC, CRP,
Creatinine, Glucose), with ~10% of results deliberately outside their
reference range. `--loop` keeps emitting one file per simulated day so the
batch layer stays fed without running it by hand every 5 minutes.

### `simulators/patient_generator.py` — reference data

The static `patients` table (id, ward, admit_date). **Every other table has a
foreign key to it**, so it must be loaded before anything else runs.

## 4. Ingestion

### `ingestion/producer.py` — simulator → Kafka

- **Keyed by `patient_id`.** Kafka guarantees ordering *within* a partition, and
  the key decides the partition — so one patient's readings always arrive in
  order. Ordering *between* patients doesn't matter clinically.
- **`acks=all` + retries + idempotence.** Waits for durable acknowledgement;
  idempotence makes retries safe (a retry after a lost ack won't duplicate).
- **Validation before publish.** Structurally broken records (no `patient_id`,
  unparseable timestamp, *every* vital null) go to the **`vitals-dlq`** topic
  with an `error_reason`, instead of poisoning downstream processing.
- **`trace_id` header** carries the `event_id` so one event can be followed
  across every stage.

> Note the distinction: *partial* nulls are **not** DLQ material. They're normal
> sensor noise that the streaming layer cleans. Only structurally unusable
> records are rejected.

### `ingestion/lab_loader.py` — landing zone

Polls `data/landing/labs/`, validates each CSV (correct columns, parseable
values), then **moves** it to `processed/` or `rejected/`. Airflow consumes
from `processed/`, so it only ever sees files that already passed validation.

## 5. Speed layer — `streaming/`

`spark_job.py` runs a Spark Structured Streaming query:

1. **Read** from Kafka (`vitals-stream`), parse JSON to a typed schema.
2. **`clean_stream()`** — drop rows missing identity fields, drop
   physiologically impossible values, apply a **2-minute watermark**, then
   **`dropDuplicates(["event_id"])`**. The watermark bounds how long dedup
   state is kept, so memory doesn't grow forever.
3. **`aggregate_stream()`** — group into **5-minute tumbling windows** per
   patient; compute avg/min/max per vital; compare against
   `config/thresholds.yaml` to produce `anomaly_flags`.
4. **Two sinks run in parallel:**
   - `foreachBatch(upsert_postgres)` → `vitals_live` + `alerts_log` (serving)
   - Parquet, partitioned by date → the **archive** the batch layer reads

`postgres_sink.py` holds the SQL, deliberately kept free of pyspark imports so
it can be **unit-tested without a Spark session**.

**Alert lifecycle** (worth being able to explain — it's subtle):
per window, open an alert for each flag with no unresolved alert of that type,
then resolve any open alert whose flag is now absent. So a patient tachycardic
for an hour gets **one** alert, not twelve; and a relapse after recovery opens
a **new** alert.

## 6. Batch layer — `batch/` + `airflow/`

The DAG `daily_risk_report` runs once per simulated day (every 5 real minutes):

| Task | What it does |
|---|---|
| `wait_for_lab_file` | Sensor: waits for that simulated day's validated lab file in `processed/` |
| `ingest_labs` | Parses the CSV, flags out-of-range results |
| `aggregate_vitals` | Reads the **Parquet archive** for that day, aggregates per patient |
| `join_and_score` | Joins vitals trend + labs, computes `risk_score` and `risk_flag` |
| `write_report` | Upserts `daily_risk_report` |
| `notify` | Completion marker |

Key design points:
- **Transformations live in `batch/pipeline.py`**, not in the DAG, so they're
  unit-testable without installing Airflow.
- **Idempotent:** the write upserts on `(patient_id, report_date)`, so
  re-running a date overwrites rather than duplicating. This is what makes the
  daily report a stable, auditable artifact — a core Lambda argument.
- **`catchup=False`**, because a 5-minute schedule with a start date months
  back would queue tens of thousands of runs.
- Runs whose simulated date precedes `sim_start_date` **skip** — they only
  arise after a clock reset and could never find a file.

## 7. Storage — `storage/schema.sql`

| Table | Holds | Written by |
|---|---|---|
| `patients` | Static reference (ward, admit date) | seed script |
| `vitals_live` | Rolling window aggregates | Spark |
| `alerts_log` | Alert open/resolve history | Spark |
| `lab_results` | Daily lab landing table | batch |
| `daily_risk_report` | The consolidated report | batch |

Constraints do real work: `alert_type` and `risk_flag` are CHECK-constrained to
their valid values, `resolved_at >= triggered_at` is enforced, and primary keys
on `(patient_id, window_start)` / `(patient_id, report_date)` are what make the
upserts idempotent.

## 8. Serving — `api/`

| Endpoint | Answers |
|---|---|
| `GET /vitals/live?ward=` | "What's the ward doing right now?" |
| `GET /patients/{id}/alerts?status=` | "What's wrong with this patient?" |
| `GET /reports/daily/{date}?risk_flag=` | "Who is at risk today?" |
| `GET /health` | Service + DB health, last data received |

Two interchangeable repositories: `MemoryRepository` (tests, no DB needed) and
`PostgresRepository` (real serving), selected by `API_REPOSITORY`. A test
enforces that their method signatures stay identical.

## 9. Observability — `observability/`

- **Structured JSON logs** with `stage` and `trace_id` on every line, so you can
  follow one event across ingestion → streaming → batch.
- **Prometheus metrics**: `vitals_published_total`, `vitals_dlq_total`,
  `vitals_last_event_unix_seconds`, `lab_files_accepted/rejected_total`.
- **Three alert rules**:
  - `VitalsProducerDown` — the producer process has crashed (`up == 0`)
  - `NoVitalsReceived` — producer alive but publishing nothing for 60s
  - `DlqErrorRateHigh` — too many records being rejected

> **Why two rules for "no vitals", not one?** A crashed producer and a stalled
> producer look completely different to Prometheus. When the process dies, the
> scrape fails and `vitals_last_event_unix_seconds` is marked **stale** — it
> stops returning any value, so an expression over it evaluates to *no data*
> and can never fire. Only `up`, which Prometheus synthesises for every target,
> reliably catches a crash. This is worth being able to explain: it's a real
> Prometheus subtlety we hit when testing the alert.
- **Grafana dashboard**: events/sec, DLQ count, seconds-since-last-event, lab
  file counts, active alerts.

> Metrics are **per process**, which is why each service exposes its own port.

---

## 10. Starting the system, step by step

### Step 1 — Start the infrastructure

```powershell
$env:POSTGRES_HOST_PORT="5433"     # only if you have PostgreSQL installed natively
docker compose up -d
docker compose ps                  # expect 7 running + airflow-init exited(0)
```

### Step 2 — Create the Kafka topics (first run only)

```powershell
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic vitals-stream --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic vitals-dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Step 3 — Reset the clock and seed patients

```powershell
python -m scripts.reset_sim_clock
python -m simulators.patient_generator
python storage/seed_patients.py docs/samples/patients.csv postgresql://hospital:hospital@localhost:5433/hospital
```

### Step 4 — Start the pipeline (one terminal each, all stay running)

```powershell
# Terminal A - vitals into Kafka
python -m ingestion.producer

# Terminal B - lab file validator
python -m ingestion.lab_loader

# Terminal C - one lab file per simulated day
python -u -m simulators.lab_simulator --loop
```

### Step 5 — Start the Spark speed layer

Must run in a container on Windows (see README for why):

```powershell
docker run --rm --network bigdataminiproject_default -v "${PWD}:/opt/project" -w /opt/project -e PYTHONPATH=/opt/project -e DATABASE_URL="postgresql://hospital:hospital@postgres:5432/hospital" --user root apache/spark:3.5.4-python3 bash -c "pip install --quiet 'psycopg[binary]==3.2.3' PyYAML==6.0.3; /opt/spark/bin/spark-submit --master 'local[2]' --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 streaming/spark_job.py --bootstrap-servers kafka:29092"
```

Takes ~2 minutes to start. It's live when `/vitals/live` stops returning `[]`.

### Step 6 — Turn on the batch layer

Open http://localhost:8080 (`admin`/`admin`) → unpause **`daily_risk_report`**.

### Step 7 — Watch it

| What | Where |
|---|---|
| Live ward | http://localhost:8000/vitals/live |
| API docs | http://localhost:8000/docs |
| Daily report | http://localhost:8000/reports/daily/2026-01-02 |
| Grafana | http://localhost:3000 (`admin`/`admin`) |
| Prometheus alerts | http://localhost:9090/alerts |
| Airflow | http://localhost:8080 (`admin`/`admin`) |

### Shutting down

```powershell
docker compose down          # add -v to also delete the database
```

---

## 11. Demo video checklist (5–10 minutes)

The assignment requires a video showing **the pipeline running end-to-end and
the observability results**, and says to **state assumptions and simulated-time
compression clearly**. Suggested structure:

| Time | Show | Rubric marks it serves |
|---|---|---|
| 0:00–1:00 | **Use case + business question.** State the simulated clock: 1 day = 5 min = 288x | Sets up everything |
| 1:00–2:30 | **Architecture decision.** Lambda vs Kappa, why Lambda, and honestly why you rejected Kappa | **20** — the biggest single item |
| 2:30–3:15 | **Tech stack.** Justify each choice against *this* use case, not popularity | **10** |
| 3:15–4:15 | **Ingestion live.** Producer logs scrolling; show a DLQ event via `scripts/demo_bad_data.py`; mention keying by `patient_id` and the injected noise | **15** |
| 4:15–5:30 | **Processing.** Spark job running; `vitals_live` filling; an alert appearing in `alerts_log`; then the Airflow DAG graph going green | **15** |
| 5:30–6:30 | **Storage & serving.** `/vitals/live` showing a flagged patient, `/patients/{id}/alerts`, and `/reports/daily/{date}` — the consolidated report that answers the business question | **10** |
| 6:30–8:00 | **Observability.** Grafana dashboard; then run `scripts/demo_kill_producer.py` and show `VitalsProducerDown` go inactive → pending → firing → clear (~50s) | **10** |
| 8:00–9:00 | **Limitations, honestly stated** (see below) | Counts toward report/viva credibility |

**Must say out loud:**
- The simulated clock compression (1 day = 5 real minutes = 288x)
- That data sources are simulated, not real hospital data
- Every assumption and simplification

**Practical filming tips:**
- Start everything **before** recording — Spark takes ~2 minutes to boot
- Have all tabs pre-opened (API, Grafana, Prometheus, Airflow)
- The alert demo takes ~90 seconds to fire; start it early or cut the wait
- Each member should speak to their own layer (individual contributions are
  required)

## 12. Limitations to state honestly

Being candid here earns marks; the rubric explicitly rewards "honesty about
limitations".

- **Risk score is a heuristic**, not clinically validated.
- **Thresholds are global**, not per-patient baselines — a fit athlete and an
  elderly patient get the same tachycardia threshold.
- **Single broker, single Postgres node** — no replication or failover.
- **Kafka has no persistent volume**; topics are lost if the container is
  recreated.
- **Alerts have no notification channel** — they surface in Prometheus/Grafana
  but nothing pages a nurse. Alertmanager would be the production step.
- **Spark runs in `local[2]`**, not a real cluster.
- **PySpark 3.5.4 requires Python 3.8–3.11**; on 3.12+ its Python workers fail.
- **Spark cannot write files on Windows** without `winutils.exe`, so the
  streaming job runs in a Linux container.
- **No authentication** anywhere — the API and databases are open. Real patient
  data would demand authentication, encryption and audit logging (and would be
  subject to health-data regulation).
