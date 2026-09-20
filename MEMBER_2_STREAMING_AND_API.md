# Member 2 — Speed Layer (Spark Streaming), Alerts & Serving API

**Owner:** _<name>_
**Project:** Hospital Patient Vital Signs Monitoring (see [PROJECT_PLAN.md](PROJECT_PLAN.md))
**Pipeline position:** Kafka → **Spark Structured Streaming → live tables/alerts → FastAPI**
**Plan phases owned:** 3, the API half of 5, optional dashboard, and your share of 7.

Teammates: [Member 1 — Data & Ingestion](MEMBER_1_DATA_AND_INGESTION.md) ·
[Member 3 — Batch Layer & Storage](MEMBER_3_BATCH_AND_STORAGE.md)

---

## 1. Scope in one paragraph

You own the **speed layer** of the Lambda architecture: consume vitals from
Kafka, clean and deduplicate them, compute windowed per-patient aggregates,
detect threshold-based anomalies, and write live results and alerts to
Postgres. You also own the FastAPI service that exposes live ward figures and
alerts. Member 3 owns the DB schema and the daily report endpoint — you host
the FastAPI app and they add their router to it.

**Don't wait for Kafka.** Until Member 1 delivers the live stream (Day 4), develop
against `docs/samples/` (file source or a local test producer), and until Member 3
publishes the schema (Day 2), agree the `vitals_live` / `alerts_log` columns
with them and stub the tables locally.

## 2. Folders you own

```
streaming/           # spark structured streaming jobs, cleaning, dedup, windowing, anomaly rules
api/                 # FastAPI app: main.py, routers/vitals.py, routers/alerts.py, routers/health.py, models
config/thresholds.yaml   # alert thresholds — single source of truth (Member 3 reuses for risk scoring)
tests/streaming/  tests/api/
```

Member 3's router `api/routers/reports.py` lives in your `api/` folder — **only they edit that file**; you just include it in `main.py`.

## 3. Task checklist

### Phase 3 — Streaming job (Day 5–7)
- [ ] Spark session + Kafka source on `vitals-stream`, parse JSON to an explicit schema (`event_id, patient_id, heart_rate, spo2, systolic_bp, diastolic_bp, temperature, timestamp`)
- [ ] Route unparseable rows to a bad-record count/log (Member 1's producer already sends most bad data to the DLQ — this is the second line of defence)
- [ ] **Cleaning:** drop rows missing `patient_id`/`timestamp`; null-handle vital fields; reject physiologically impossible values
- [ ] **Deduplication** on `event_id` (bounded by watermark so state doesn't grow forever)
- [ ] **Watermark** (e.g. 2 min) to tolerate late/out-of-order events; test with intentionally delayed events
- [ ] **Windowed aggregation** per patient: 1–5 min tumbling (or sliding) windows → avg HR, SpO2, systolic/diastolic BP, temp, plus min/max and event count
- [ ] **Anomaly detection** from `config/thresholds.yaml`: tachycardia (HR > 120), hypoxia (SpO2 < 90), fever (temp > 38.5), hypotension (systolic < 90). Alert types must match Member 3's `alerts_log` enum exactly
- [ ] Write alerts to `alerts_log` — avoid alert storms: one open alert per `(patient_id, alert_type)`, set `resolved_at` when the vital returns to normal
- [ ] Sink aggregates to `vitals_live` via `foreachBatch` with **upsert** semantics (idempotent under micro-batch retry); set a checkpoint location
- [ ] **Raw archive sink:** second streaming query writes cleaned raw events to Parquet, partitioned `date=YYYY-MM-DD` (uses `common/sim_clock.py`), at the path below — Member 3's batch job reads it
- [ ] Add `spark` service to `docker-compose.yml` (PR to Member 1's file)
- [ ] **Deliverable:** run the simulator with spikes on → `vitals_live` populates and alerts appear within the window latency

### Phase 5 — Serving API (Day 9–10)
- [ ] FastAPI app skeleton with DB connection pooling and config from env vars
- [ ] `GET /vitals/live` — current ward snapshot: latest window per patient, ward/optional `?ward=` filter, active-alert indicator
- [ ] `GET /patients/{id}/alerts` — active + historical alerts, `?status=active|resolved|all`, sensible 404 for unknown patient
- [ ] `GET /health` — checks DB connectivity and reports last-data-received time (Member 1's alert rule can reuse this)
- [ ] Include Member 3's `reports` router for `GET /reports/daily/{date}`
- [ ] Pydantic response models + auto-generated `/docs` (use in the demo and report screenshots)
- [ ] Optional: lightweight live-ward dashboard (Grafana on Postgres, or a simple HTML page polling `/vitals/live`)
- [ ] Add `api` service to Docker Compose

### Cross-cutting (Day 5–12)
- [ ] Use Member 1's `get_logger("streaming")` and `trace_id` from Kafka headers in every log line
- [ ] Expose metrics: input rows/sec, processing latency per micro-batch, late-event count, dedup drop count, alerts fired, API request latency
- [ ] Add streaming/batch progress info (`lastProgress`) to logs

### Phase 7 — Testing & docs (Day 13–14)
- [ ] Unit tests for pure logic: threshold rules, cleaning function, dedup, window aggregation (use `pytest` with a local Spark session or extract logic into testable functions)
- [ ] API tests with `TestClient` (happy path, empty DB, unknown patient)
- [ ] Write your **run instructions** section for the README and send it to Member 1
- [ ] Attend demo recording; you present the live stream + alert firing + API

## 4. Report sections you write

| Section | Notes |
|---|---|
| §3 Architecture diagrams | Draw and maintain the diagram (ingestion → processing → storage → serving); Members 1/3 send corrections |
| §4 Tech stack — Spark, FastAPI | Why Structured Streaming (watermarks, exactly-once sinks), why FastAPI |
| §6 Results — live/streaming/API | Screenshots of `vitals_live`, alert firing, `/docs`, dashboard |
| §7 Limitations — speed layer | e.g. threshold rules are static, not per-patient baselines; single-node Spark |
| §8 Your individual contributions | Fill in honestly at the end |

Also **review** Member 3's §2 (Lambda vs Kappa) — it is worth 20 marks.

## 5. Interfaces (contracts)

**You consume**

| Artifact | From | Contract |
|---|---|---|
| Kafka `vitals-stream` | Member 1 | See Member 1's file §5: JSON, key = `patient_id`, includes `event_id`, may contain nulls/dupes/late events |
| `common/sim_clock.py`, logging & metrics helpers | Member 1 | Import — don't reimplement |
| Postgres schema for `vitals_live`, `alerts_log`, `patients` | Member 3 | Owned in `storage/schema.sql` |
| `docs/samples/` | Member 1 | Use until live Kafka is available |

**You provide**

| Artifact | Consumer | Contract |
|---|---|---|
| `vitals_live` rows | API, Member 3 (optional trend source) | `patient_id, window_start, window_end, avg_hr, avg_spo2, avg_bp, anomaly_flags` — **agree the exact columns with Member 3 on Day 1–2** (add `avg_temp`, `event_count`) |
| `alerts_log` rows | API, Member 3 (risk score input) | `alert_id, patient_id, alert_type, triggered_at, resolved_at`; `alert_type ∈ {TACHYCARDIA, HYPOXIA, FEVER, HYPOTENSION}` |
| Raw Parquet archive | Member 3 | `data/archive/vitals/date=YYYY-MM-DD/*.parquet`, one row per cleaned event, same columns as the Kafka message plus `ingested_at` |
| `config/thresholds.yaml` | Member 3, Member 1 (tests) | Keys per vital: `hr_high`, `spo2_low`, `temp_high`, `sbp_low` |
| FastAPI app `api/main.py` | Member 3 | Includes `reports` router from `api/routers/reports.py` |
| Metric names & log fields | Member 1 | Send list by Day 5 |

## 6. Sync points

| Day | What must be true |
|---|---|
| 1 | Contracts agreed (column names for `vitals_live`, `alerts_log`, thresholds, Parquet path) |
| 2 | Schema from Member 3 committed; you can create tables locally |
| 4 | Live Kafka stream available — switch from sample files |
| 5 | Send metric/log field names to Member 1 |
| 7 | Streaming job running end-to-end with alerts firing on injected spikes; Parquet archive being written (Member 3 needs it by Day 8) |
| 10 | API complete, integrated in Compose — **integration freeze** |
| 12 | **Code freeze**; only bug fixes after this |

## 7. Definition of done

- [ ] Streaming job survives restart from checkpoint without duplicate `vitals_live` rows or duplicate alerts
- [ ] Late and duplicate events demonstrably handled (test + log evidence)
- [ ] All four alert types fire on injected spikes and resolve when vitals normalise
- [ ] Four API endpoints return correct data; `/docs` page usable
- [ ] Unit tests for cleaning, dedup, thresholds and API pass
- [ ] Your report sections drafted and reviewed by another member

## 8. Team ground rules (same in all three files)

- One branch per task: `m2/<topic>`; never push straight to `main`; another member reviews each PR
- Stay inside your folders; cross-boundary changes go through a PR/issue
- Interface changes are announced in the group chat **and** updated in `docs/contracts.md`
- Commit small and often; pin all dependency and image versions
- Blocked > half a day? Say so immediately — don't wait for the next sync
