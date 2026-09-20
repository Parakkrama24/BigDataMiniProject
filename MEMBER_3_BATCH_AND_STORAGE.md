# Member 3 — Batch Layer, Airflow, Storage & Architecture Decision

**Owner:** _<name>_
**Project:** Hospital Patient Vital Signs Monitoring (see [PROJECT_PLAN.md](PROJECT_PLAN.md))
**Pipeline position:** daily lab batch + archived vitals → **Airflow/Spark batch → risk report**, plus the **storage layer** everyone writes to.
**Plan phases owned:** 4, the storage half of 5, Phase 0 architecture decision, and your share of 7.

Teammates: [Member 1 — Data & Ingestion](MEMBER_1_DATA_AND_INGESTION.md) ·
[Member 2 — Speed Layer & API](MEMBER_2_STREAMING_AND_API.md)

---

## 1. Scope in one paragraph

You own the **batch layer** of the Lambda architecture and the **database**: the
Postgres schema/migrations that Member 2's streaming job writes into, the
Airflow DAG that runs once per simulated day, the Spark batch job that joins the
previous day's vitals trend with lab results, and the risk scoring that produces
`daily_risk_report`. You also own the written **Lambda vs Kappa decision**, which
is worth 20 marks — the largest single item in the rubric.

**Your schema is the first thing others need (Day 2).** Publish `storage/schema.sql`
early, even if it changes slightly later. Until Member 2 delivers the Parquet
archive (Day 7–8), develop the batch job against a **seed script** that fabricates
archive partitions and `vitals_live` rows from `docs/samples/`.

## 2. Folders you own

```
storage/             # schema.sql, migrations, seed scripts, Parquet archive layout docs
batch/               # spark batch jobs: lab ingest, vitals trend aggregation, join, risk scoring
airflow/             # dags/, plugins/, callbacks, airflow config
api/routers/reports.py   # GET /reports/daily/{date}  (Member 2 hosts the FastAPI app, you own this file)
docs/architecture_decision.md   # living document for the Lambda vs Kappa section
tests/batch/  tests/storage/
```

## 3. Task checklist

### Phase 0 — Architecture decision (Day 1–2, refine throughout)
- [ ] Start `docs/architecture_decision.md` from PROJECT_PLAN §2: Lambda chosen, Kappa rejected, with the criteria table (latency, replay/correction, consistency, cost/complexity)
- [ ] Add a concrete "what would change our mind" paragraph (e.g. lab data becomes high-volume streaming → Kappa becomes attractive)
- [ ] Update it as real trade-offs show up during the build (late labs, idempotent re-runs) — real examples score better than theory

### Phase 5 (storage half) — Schema (Day 1–2 draft, final by Day 9–10)
- [ ] `storage/schema.sql` creating: `patients`, `vitals_live`, `lab_results`, `daily_risk_report`, `alerts_log` (columns in PROJECT_PLAN §4)
- [ ] Constraints and indexes that make writes idempotent:
  - `vitals_live` unique on `(patient_id, window_start)` → Member 2 upserts
  - `alerts_log` enum/check on `alert_type`; index on `(patient_id, triggered_at)`
  - `lab_results` unique on `(patient_id, test_type, collected_at)` → amendments upsert cleanly
  - `daily_risk_report` primary key `(patient_id, report_date)` → re-runs overwrite
- [ ] `patients` seed loader from Member 1's `patients` CSV
- [ ] Schema auto-applied on `docker compose up` (init script mounted into Postgres container)
- [ ] Parquet archive layout doc: `data/archive/vitals/date=YYYY-MM-DD/` (Member 2 writes; you read); also archive raw lab files under `data/archive/labs/date=YYYY-MM-DD/`
- [ ] Retention/cleanup note (what would be pruned in production) for the report

### Phase 4 — Batch layer (Day 8–9)
- [ ] **Lab ingest task:** validate the daily file from `data/landing/labs/labs_YYYY-MM-DD.csv` (schema, types, parse `reference_range`), upsert into `lab_results`, flag out-of-range results, archive raw file
- [ ] **Vitals trend task:** for report date *D-1*, aggregate per patient from the Parquet archive (or `vitals_live` history): mean/min/max HR, SpO2, BP, temp, time above threshold, alert counts from `alerts_log`
- [ ] **Join & score task:** join vitals trend with the latest lab results; compute `risk_score` and `risk_flag` (e.g. LOW / MEDIUM / HIGH) — document the formula (weighted vitals-abnormality + abnormal-lab count + trend direction). Thresholds come from Member 2's `config/thresholds.yaml`
- [ ] **Write `daily_risk_report`** with `vitals_summary` and `lab_summary` (JSON columns) — **overwrite/upsert by `report_date`**
- [ ] **Airflow DAG** `daily_risk_report`: `wait_for_lab_file` (sensor) → `ingest_labs` → `aggregate_vitals` → `join_and_score` → `write_report` → `notify`
  - Schedule mapped to the simulated clock via `common/sim_clock.py` (1 sim day = 5 real minutes), sim date passed as the run parameter
  - **Idempotent:** re-running the same date gives the same result (delete-then-insert or upsert in one transaction)
  - **Backfill** works for a range of simulated dates (`airflow dags backfill` or catchup)
  - Retries with backoff, `on_failure_callback` that writes a structured log + metric
  - Handles late/amended lab files: if a new file appears for an already-processed date, re-run that date only
- [ ] Add `airflow` service(s) to `docker-compose.yml` (PR to Member 1's file); mount `airflow/dags`, `batch/`, and shared data volume
- [ ] **Deliverable:** DAG produces a daily risk report, re-runnable per date, with visible run history in the Airflow UI

### Phase 5 (serving half) — Report endpoint (Day 9–10)
- [ ] `api/routers/reports.py` → `GET /reports/daily/{date}` returning the consolidated report (optional `?risk_flag=HIGH`, sorted by risk score); 404 with clear message if no report for that date
- [ ] Optional: export daily report to CSV/Parquet in `data/reports/` as the "stable, reproducible artifact per date" (supports the Lambda consistency argument)

### Cross-cutting (Day 5–12)
- [ ] Use Member 1's `get_logger("batch")` and a `batch_id` = `dag_id:sim_date:run_id` in every log line
- [ ] Metrics: DAG run duration, rows ingested, rows rejected, patients scored, high-risk count
- [ ] Add failure-path demo: corrupt a lab file → DAG fails, retries, then alerts/logs cleanly

### Phase 7 — Testing & docs (Day 13–14)
- [ ] Unit tests for **risk scoring** (boundary cases, missing labs, missing vitals, all-normal → LOW), lab parsing, and idempotency (run twice → identical table state)
- [ ] Run the full DAG for ≥3 consecutive simulated days for the demo and screenshots
- [ ] Write your **run instructions** section for the README and send to Member 1
- [ ] Attend demo recording; you present the Airflow DAG, a re-run/backfill, and the risk report

## 4. Report sections you write

| Section | Notes |
|---|---|
| **§2 Architecture decision (Lambda vs Kappa)** | Justification, rejected alternative, trade-offs — **20 marks; whole team reviews** |
| §4 Tech stack — Airflow, Postgres, Parquet | Why each; why relational serving + columnar archive |
| §6 Results — daily report | Airflow DAG graph screenshot, sample `daily_risk_report` rows, `/reports/daily/{date}` output |
| §7 Limitations — batch/storage | e.g. risk score is a heuristic, not clinically validated; single Postgres node; no PHI/security handling |
| §8 Your individual contributions | Fill in honestly at the end |

## 5. Interfaces (contracts)

**You provide**

| Artifact | Consumer | Contract |
|---|---|---|
| `storage/schema.sql` (auto-applied) | Members 1, 2 | Tables/columns per PROJECT_PLAN §4; add `avg_temp`, `event_count` to `vitals_live` (agree with Member 2 Day 1–2) |
| `alerts_log.alert_type` enum | Member 2 | `TACHYCARDIA, HYPOXIA, FEVER, HYPOTENSION` |
| `daily_risk_report` | API, report | `patient_id, report_date, vitals_summary (JSON), lab_summary (JSON), risk_score (0–100), risk_flag (LOW/MEDIUM/HIGH)` |
| `api/routers/reports.py` (`router` object) | Member 2 | Included in `api/main.py` |
| Airflow service in Compose | Member 1 | Mounts DAGs, batch code, shared `data/` volume |
| Metric names & log fields | Member 1 | Send list by Day 5 |

**You consume**

| Artifact | From | Contract |
|---|---|---|
| Lab files `data/landing/labs/labs_YYYY-MM-DD.csv` | Member 1 | CSV, columns per PROJECT_PLAN §1 |
| `patients` seed CSV, `docs/samples/` | Member 1 | Use for Day 2–7 development |
| `common/sim_clock.py`, logging & metrics helpers | Member 1 | Import — don't reimplement |
| Raw Parquet archive `data/archive/vitals/date=YYYY-MM-DD/` | Member 2 | One row per cleaned event + `ingested_at` |
| `vitals_live`, `alerts_log` rows | Member 2 | Alternative/additional trend inputs |
| `config/thresholds.yaml` | Member 2 | Reuse for abnormal-time calculations |

## 6. Sync points

| Day | What must be true |
|---|---|
| 1 | Contracts agreed; decision doc started |
| 2 | **`storage/schema.sql` committed** — Member 2 is blocked on this |
| 3 | Sample labs/patients from Member 1; seed script creates fake archive partitions |
| 7–8 | Real Parquet archive from Member 2 replaces the seed data |
| 9 | DAG runs end-to-end for at least one simulated day |
| 10 | Report endpoint live; Airflow in Compose — **integration freeze** |
| 12 | **Code freeze**; DAG demonstrated across ≥3 simulated days |

## 7. Definition of done

- [ ] Schema applies cleanly on a fresh Postgres container
- [ ] Re-running the DAG for the same date yields identical `daily_risk_report` rows (no duplicates)
- [ ] Backfill across multiple simulated dates works
- [ ] Amended/late lab file for a past date correctly updates that date's report
- [ ] Risk-score tests pass and the formula is documented in the report
- [ ] Architecture decision section is complete, specific to our system, and reviewed by both teammates

## 8. Team ground rules (same in all three files)

- One branch per task: `m3/<topic>`; never push straight to `main`; another member reviews each PR
- Stay inside your folders; cross-boundary changes go through a PR/issue
- Interface changes are announced in the group chat **and** updated in `docs/contracts.md`
- Commit small and often; pin all dependency and image versions
- Blocked > half a day? Say so immediately — don't wait for the next sync
