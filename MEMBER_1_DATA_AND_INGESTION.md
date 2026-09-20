# Member 1 — Data Sources, Ingestion, Infrastructure & Observability

**Owner:** _<name>_
**Project:** Hospital Patient Vital Signs Monitoring (see [PROJECT_PLAN.md](PROJECT_PLAN.md))
**Pipeline position:** everything *before* Spark, plus the cross-cutting infra and observability.
**Plan phases owned:** 0 (scaffold/infra), 1, 2, 6, and your share of 7.

Teammates: [Member 2 — Speed Layer & API](MEMBER_2_STREAMING_AND_API.md) ·
[Member 3 — Batch Layer & Storage](MEMBER_3_BATCH_AND_STORAGE.md)

---

## 1. Scope in one paragraph

You build the data that everyone else consumes and the platform everyone else
runs on: the two simulators, the Kafka ingestion path (with DLQ), the daily lab
landing zone, the Docker Compose environment, the shared logging/metrics
helpers, and the observability demo. Until your simulators and Kafka are up,
Members 2 and 3 are working against sample files — so **Days 1–4 are the
critical path for the whole team. Ship a minimal working version early, polish
later.**

## 2. Folders you own

```
simulators/          # vitals_simulator.py, lab_simulator.py, noise injection
ingestion/           # kafka producer, schema validation, DLQ routing, lab landing-zone loader
observability/       # logging_config.py, metrics helpers, prometheus.yml, alert rules, grafana provisioning
common/              # sim_clock.py, config loading (shared by everyone — keep it tiny and stable)
config/              # settings.yaml (sim clock, patient count, spike probability, topic names)
docker-compose.yml   # full stack; other members add their own service blocks via PR
tests/simulators/  tests/ingestion/
```

Do **not** edit `streaming/`, `api/` (Member 2) or `batch/`, `airflow/`, `storage/` (Member 3) without asking — open a PR or an issue instead.

## 3. Task checklist

### Phase 0 — Scaffold (Day 1) — *do this first, everyone is blocked on it*
- [ ] Create the repo folder structure from PROJECT_PLAN §5 (empty folders with `.gitkeep`) and push to `main`
- [ ] `docker-compose.yml` skeleton with **pinned image versions**: Kafka (+ Zookeeper/KRaft), Postgres, Spark, Airflow, Prometheus, Grafana
- [ ] `common/sim_clock.py`: converts real time → simulated date (default 1 sim day = 5 real minutes, configurable in `config/settings.yaml`). **Everyone imports this; nobody re-implements it.**
- [ ] `.env.example`, `requirements.txt` (or per-service), `.gitignore` additions (`data/`, `.env`)
- [ ] Hold the Day-1 **contract session** with the team (see §5) and commit the agreed contracts to `docs/contracts.md`

### Phase 1 — Simulators (Day 2–3)
- [ ] `vitals_simulator.py`: N patients (configurable), one reading per patient every few seconds, realistic baselines with small random drift
- [ ] Abnormal spikes with configurable probability: tachycardia (HR > 120), hypoxia (SpO2 < 90), fever (temp > 38.5), hypotension (systolic < 90)
- [ ] Data-quality noise (each individually toggleable): null fields, duplicate events (same `event_id`), late/out-of-order timestamps
- [ ] `lab_simulator.py`: one file per simulated day (`labs_YYYY-MM-DD.csv`) with `patient_id, test_type, result_value, reference_range, collected_at`; include some out-of-range values and occasional amended re-uploads
- [ ] Seed parameter → reproducible output; generate `patients` seed data (id, ward, admit_date) and hand to Member 3 for loading
- [ ] Commit small **sample output files** to `docs/samples/` so Members 2/3 can develop without Kafka
- [ ] Unit tests: valid schema, seed reproducibility, spike probability roughly honoured

### Phase 2 — Ingestion (Day 3–4)
- [ ] Kafka topics created on startup (init script/container): `vitals-stream` (3–6 partitions, key = `patient_id`) and `vitals-dlq`
- [ ] Producer wired to simulator: `acks=all`, retries, idempotence, keyed by `patient_id`
- [ ] Schema validation before publish: required fields, types, plausible ranges; malformed → `vitals-dlq` with `error_reason`
- [ ] Lab landing-zone loader: watches `data/landing/labs/`, validates the file, moves it to `data/landing/processed/` (or `rejected/`), and signals Airflow (file presence is the trigger — Member 3's DAG sensor reads the same path)
- [ ] Smoke test script: `make smoke` / `scripts/smoke.sh` — start stack, produce 30 s of data, consume and print a few messages
- [ ] **Day-4 target:** Member 2 can consume live events from `vitals-stream` on their machine

### Phase 6 — Observability (Day 11–12)
- [ ] `observability/logging_config.py`: structured JSON logger with `trace_id` / `batch_id` fields — **publish by Day 5** so others adopt it while writing their code, not at the end
- [ ] Add `trace_id` to Kafka message headers at the producer so it can be followed through streaming and batch
- [ ] `observability/metrics.py`: small helper (Prometheus client) that Members 2/3 import for counters/gauges
- [ ] Metrics: events/sec produced, DLQ count, consumer lag (kafka exporter), processing latency, DAG run duration
- [ ] Prometheus + Grafana provisioning: at least one dashboard (ingest rate, DLQ, lag, alert count)
- [ ] **One alert rule**: "no vitals received for N minutes" (and/or DLQ error-rate above threshold)
- [ ] **Demo scenario script**: `scripts/demo_kill_producer.sh` — kill the producer, show the alert firing, restart, show it clearing. Also `scripts/demo_bad_data.sh` — inject malformed events, show DLQ growing

### Phase 7 — Finalisation (Day 13–14)
- [ ] `README.md`: setup/run instructions and Docker Compose reproduction steps (Members 2/3 send you their run sections)
- [ ] **Test `docker compose up` from a clean clone on a machine that is not yours**
- [ ] Record the 5–10 min demo video (all three members attend; you drive the pipeline start-up and observability portion)

## 4. Report sections you write

| Section | Notes |
|---|---|
| §1 Use case & business requirements | Sources table, business question, simulated clock explanation |
| §5 Observability design | Logging format, metrics list, alert rule, demo screenshots |
| §4 Tech stack — ingestion/infra part | Why Kafka, partitioning by `patient_id`, DLQ, Docker Compose |
| §7 Limitations — ingestion/observability | e.g. single broker, no schema registry, simulated data only |
| §8 Your individual contributions | Fill in honestly at the end |

Draft each section **right after finishing its phase** — not on Day 13.

## 5. Interfaces (contracts)

**You provide**

| Artifact | Consumer | Contract |
|---|---|---|
| Kafka topic `vitals-stream` | Member 2 | JSON value, key = `patient_id`. Fields: `event_id` (uuid, **proposed addition** — needed for dedup), `patient_id`, `heart_rate`, `spo2`, `systolic_bp`, `diastolic_bp`, `temperature`, `timestamp` (ISO-8601 UTC, event time). Nulls and duplicates possible by design. |
| Kafka topic `vitals-dlq` | Member 2 (metrics), you | JSON: `raw_payload`, `error_reason`, `failed_at`, `source` |
| Lab files `data/landing/labs/labs_YYYY-MM-DD.csv` | Member 3 | Columns exactly as PROJECT_PLAN §1; UTF-8 CSV with header |
| `common/sim_clock.py` | All | `current_sim_date()`, `sim_date_for(real_ts)`, `real_window_for(sim_date)` |
| `observability/logging_config.py`, `metrics.py` | All | `get_logger(stage)`, `counter(name)`, `gauge(name)` |
| `docs/samples/` | Members 2, 3 | A few hundred vitals events (JSONL) + 2–3 days of lab CSVs |
| `patients` seed data | Member 3 | CSV: `patient_id, ward, admit_date` |

**You consume**
- Metric names/log fields from Members 2 & 3 (agree names Day 5)
- Service blocks for `spark`, `airflow`, `api` in `docker-compose.yml` (they PR them in)

## 6. Sync points

| Day | What must be true |
|---|---|
| 1 | Repo scaffold pushed; contracts agreed and written to `docs/contracts.md` |
| 3 | Sample files in `docs/samples/` (unblocks Members 2 & 3) |
| 4 | Live Kafka stream consumable by Member 2 |
| 5 | Logging/metrics helpers published |
| 7 | Streaming job integrated with real Kafka (help Member 2 debug connectivity) |
| 10 | Full stack `docker compose up` works end-to-end — **integration freeze** |
| 12 | Observability demo rehearsed — **code freeze** |

## 7. Definition of done

- [ ] `docker compose up` starts the whole stack from a clean clone with no manual steps beyond `.env`
- [ ] Simulators reproducible via seed and covered by unit tests
- [ ] Malformed events land in DLQ; DLQ count visible in metrics
- [ ] Alert rule fires on demand and is captured in the demo video
- [ ] Your report sections drafted and reviewed by another member

## 8. Team ground rules (same in all three files)

- One branch per task: `m1/<topic>`; never push straight to `main`; another member reviews each PR
- Stay inside your folders; cross-boundary changes go through a PR/issue
- Interface changes are announced in the group chat **and** updated in `docs/contracts.md`
- Commit small and often; pin all dependency and image versions
- Blocked > half a day? Say so immediately — don't wait for the next sync
