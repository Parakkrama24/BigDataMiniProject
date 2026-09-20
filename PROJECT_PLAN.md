# Project Plan — Hospital Patient Vital Signs Monitoring

**Module:** EC8203 Applied Big Data Engineering — Mini Project
**Use Case:** UC2 — Hospital Patient Vital Signs Monitoring
**Duration:** 2 weeks (14 days)
**Architecture:** Lambda (see decision below)

---

## 1. Business Context

A hospital ward wants near-real-time monitoring of patient vitals from bedside
sensors, correlated daily with lab test results uploaded once a day by pathology.

**Business question:** *Which patients show concerning vital-sign trends right
now, and how do yesterday's lab results change the risk picture for those
patients going forward?*

| Source | Type | Fields | Frequency |
|---|---|---|---|
| Bedside monitors | Streaming | `patient_id, heart_rate, spo2, systolic_bp, diastolic_bp, temperature, timestamp` (with occasional simulated abnormal spikes) | Every few seconds |
| Pathology lab | Daily batch | `patient_id, test_type, result_value, reference_range, collected_at` | Once per simulated day |

**Suggested outputs**
- API endpoint returning real-time ward monitoring figures
- Threshold-based alerts per patient (e.g. tachycardia, hypoxia, fever)
- A daily consolidated patient risk report joining vitals trends with the latest lab results

**Simulated clock:** 1 simulated day = 5 minutes real time (documented in report; adjustable via config).

---

## 2. Architecture Decision — Lambda vs Kappa

**Decision: Lambda architecture.**

| Criterion | Assessment |
|---|---|
| Latency | Vitals need sub-minute detection for patient safety → dedicated speed layer with low-latency windowed aggregation. |
| Replay / correction | Lab results can arrive late, get amended, or be re-uploaded (lab corrections) — a distinct batch layer with idempotent re-runs (Airflow) handles this cleanly. |
| Consistency | Daily risk report must be a stable, reproducible artifact per date (audit/clinical record), which favours a batch-recomputed view rather than a continuously-mutating stream state. |
| Cost/complexity | Required stack already gives us Kafka (stream) + Airflow (batch orchestration) — Lambda is the natural fit; forcing lab data through a Kappa log-replay model adds complexity without benefit since it's not truly a high-volume stream. |

**Rejected alternative — Kappa:** Treating the daily lab file as a compacted
Kafka topic and recomputing all state via replay was considered. Rejected
because: (1) lab data volume/frequency doesn't justify stream-native
treatment, (2) Airflow — an explicitly required tool — would be underused,
and (3) clinical reporting benefits from a clearly bounded, re-runnable daily
batch job rather than continuous reprocessing of an event log.

- **Speed layer:** Kafka → Spark Structured Streaming → live vitals table + real-time alerts
- **Batch layer:** Airflow (daily) → Spark batch job → joins prior day's vitals trend with lab results → consolidated risk report

---

## 3. Technology Stack

| Layer | Tool | Justification |
|---|---|---|
| Ingestion | Apache Kafka (topics: `vitals-stream`, DLQ: `vitals-dlq`) | Durable buffering, partition by `patient_id` for per-patient ordering |
| Stream processing | Apache Spark Structured Streaming | Native windowing/watermarking, exactly-once sink semantics, fits speed layer |
| Orchestration | Apache Airflow | Schedules/retries the daily batch join + report job |
| Storage (serving) | PostgreSQL | Relational, queryable, good fit for dashboards/API and daily report tables |
| Storage (raw archive) | Parquet on local FS (S3-compatible optional) | Cheap columnar archive of raw vitals/labs, partitioned by date |
| Serving API | FastAPI | Lightweight REST endpoints for live metrics/alerts/reports |
| Observability | Structured JSON logs + Prometheus + Grafana (or simple health endpoint) | Metrics export, alerting rule, dashboards |
| Packaging | Docker Compose | One-command reproducible environment |

---

## 4. Data Design (high level)

**Kafka topic `vitals-stream`** — key: `patient_id`, partitions: e.g. 3–6.

**Postgres tables**
- `patients` (static reference: patient_id, ward, admit_date)
- `vitals_live` (rolling window aggregates: patient_id, window_start, window_end, avg_hr, avg_spo2, avg_bp, anomaly_flags)
- `lab_results` (daily batch landing table)
- `daily_risk_report` (patient_id, report_date, vitals_summary, lab_summary, risk_score, risk_flag)
- `alerts_log` (alert_id, patient_id, alert_type, triggered_at, resolved_at)

---

## 5. Phase-by-Phase Plan

### Phase 0 — Setup & Design (Day 1)
- Finalize scope, simulated clock, and field definitions
- Draw architecture diagram (ingestion → processing → storage → serving)
- Initialize Git repo, folder structure, `docker-compose.yml` skeleton
- Write architecture decision section of report (Lambda vs Kappa) as a living doc

**Deliverable:** Repo scaffold + architecture diagram v1 + decision doc draft

```
project/
  simulators/        # streaming + batch generators
  ingestion/          # kafka producer/consumer configs
  streaming/          # spark structured streaming jobs
  batch/              # spark batch jobs (lab join, daily report)
  airflow/            # DAGs
  api/                # FastAPI serving layer
  storage/             # DB schema/migrations
  observability/       # logging config, metrics, alert rules
  tests/
  docs/                # report, diagrams, screenshots
```

### Phase 1 — Simulated Data Sources (Day 2–3)
- Build streaming simulator: emits vitals per patient every few seconds, with
  configurable probability of abnormal spikes (tachycardia, low SpO2, fever, hypotension)
- Build batch simulator: generates one daily lab-results file (CSV/JSON) per simulated day
- Add data quality noise: occasional nulls, duplicate events, late/out-of-order timestamps
- Unit tests for generators (valid schema, reproducible with seed)

**Deliverable:** Two runnable Python simulators + sample output files

### Phase 2 — Ingestion Layer (Day 3–4)
- Stand up Kafka (Docker Compose), create `vitals-stream` topic + DLQ + partitioning by `patient_id`
- Kafka producer wired to streaming simulator (with retries/acks)
- File-watcher / landing-zone loader for the daily batch file
- Basic schema validation at ingestion; malformed events → DLQ

**Deliverable:** Kafka running, producer publishing live events, batch landing zone working

### Phase 3 — Stream Processing / Speed Layer (Day 5–7)
- Spark Structured Streaming job consuming `vitals-stream`
- Cleaning + deduplication + watermarking for late events
- Windowed aggregation per patient (e.g. 1–5 min tumbling/sliding windows): avg HR, SpO2, BP, temp
- Threshold-based anomaly detection (e.g. HR > 120, SpO2 < 90, temp > 38.5) → write to `alerts_log`
- Sink live aggregates to `vitals_live` table in Postgres

**Deliverable:** Streaming job running end-to-end, live table populating, alerts firing on injected spikes

### Phase 4 — Batch Layer (Day 8–9)
- Airflow DAG scheduled per simulated day:
  1. Ingest/validate daily lab file → `lab_results` table
  2. Aggregate prior day's vitals trend per patient (from archived Parquet or `vitals_live` history)
  3. Join vitals trend with lab results
  4. Compute `risk_score` / `risk_flag` per patient → write `daily_risk_report`
- Make the DAG idempotent (safe to re-run per date, supports backfill)
- Failure callbacks/retries configured in Airflow

**Deliverable:** Airflow DAG producing daily consolidated risk report, re-runnable per date

### Phase 5 — Storage & Serving Layer (Day 9–10)
- Finalize Postgres schema/migrations
- Raw event archive to Parquet, partitioned by date (for replay/audit)
- FastAPI endpoints:
  - `GET /vitals/live` — current ward monitoring snapshot
  - `GET /patients/{id}/alerts` — active/historical alerts
  - `GET /reports/daily/{date}` — consolidated risk report
  - `GET /health` — service health check
- Optional: simple dashboard (Grafana or lightweight HTML) visualizing live ward status

**Deliverable:** Working API + queryable storage + sample dashboard/report output

### Phase 6 — Observability (Day 11–12)
- Structured JSON logging across ingestion, streaming, batch stages, with a shared `trace_id`/`batch_id` for cross-stage tracing
- Metrics export (Prometheus or simple counters): events/sec, consumer lag, processing latency, DLQ count, DAG run duration
- At least one alert/health-check rule: e.g. "no vitals received for patient/ward in N minutes" or "error rate above threshold"
- Demo scenario: kill the producer / inject bad data and show the alert firing

**Deliverable:** Logging + metrics + one working alert rule, demonstrated live

### Phase 7 — Testing, Documentation, Report & Demo (Day 13–14)
- Unit tests for transformation/aggregation logic and risk-scoring
- Finalize README (architecture summary, setup/run instructions, Docker Compose reproduction steps)
- Write full report (8–15 pages):
  1. Use case & business requirements
  2. Architecture decision (Lambda vs Kappa) with justification & rejected alternative
  3. Architecture diagrams
  4. Tech stack justification
  5. Observability design
  6. Results: sample report/dashboard screenshots
  7. Limitations & production trade-offs
  8. Individual contributions (if group)
- Record 5–10 minute demo video showing pipeline running end-to-end + observability + an alert firing

**Deliverable:** Final codebase, PDF report, demo video, Git repo link

---

## 6. Milestone Timeline

| Day | Milestone |
|---|---|
| 1 | Repo, diagram, architecture decision drafted |
| 2–3 | Simulators complete |
| 3–4 | Kafka ingestion working |
| 5–7 | Streaming job + live alerts working |
| 8–9 | Airflow batch DAG + daily risk report working |
| 9–10 | API + storage finalized |
| 11–12 | Observability (logs, metrics, alert) complete |
| 13–14 | Tests, README, report, demo video, final submission |

---

## 7. Rubric Coverage Check

| Rubric Item | Marks | Covered in Phase |
|---|---|---|
| Architecture decision & justification | 20 | Phase 0, 7 |
| Tech stack selection & justification | 10 | Phase 0, 7 |
| Data ingestion implementation | 15 | Phase 1, 2 |
| Processing layer implementation | 15 | Phase 3, 4 |
| Storage & serving layer | 10 | Phase 5 |
| Observability | 10 | Phase 6 |
| Report | 15 | Phase 0, 7 |
| Code quality & documentation | 5 | All phases, finalized Phase 7 |

---

## 8. Risks & Mitigations

| Risk | Mitigation |
|---|---|
| Spark/Kafka integration complexity eats time | Get a minimal end-to-end path working Day 1–4 before adding features |
| Late/out-of-order events break windowing | Use watermarking early; test with intentionally delayed events |
| Airflow DAG not idempotent → bad demo | Design batch job to overwrite/upsert by `report_date` from the start |
| Running out of time for report | Draft report sections incrementally per phase, not all at the end |
| Docker Compose environment differs across team machines | Pin image versions, test `docker compose up` on a clean machine before submission |

---

*Next step: scaffold the repo structure and `docker-compose.yml` for Phase 0.*
