# Demo video script (Zoom, 3 speakers, ~9 minutes)

What to **say** and what to **show on screen** at each moment, for the 5–10
minute demo video the assignment requires.

The assignment asks for a video "showing the pipeline running end-to-end and
the observability results", and requires you to "clearly state any
assumptions, simplifications, or simulated-time compression used".

**Speaker roles** (match them to the layer each person built, since individual
contributions must be stated):

| | Member | Owns | Speaks about |
|---|---|---|---|
| **M1** | _name_ | `simulators/`, `ingestion/`, `observability/` | Use case, data sources, ingestion, observability |
| **M2** | _name_ | `streaming/`, `api/` | Spark speed layer, serving API |
| **M3** | _name_ | `batch/`, `airflow/`, `storage/` | Architecture decision, batch layer, storage |

---

## Before you hit record

### The system runs on one machine only

**M1 shares their screen for the entire recording.** M2 and M3 narrate over
that shared screen when it's their turn. Do **not** hand the screen share back
and forth mid-demo — Zoom share transitions drop frames, resize windows and
waste 10–15 seconds each time. One share, three voices.

### Start the system ~10 minutes before recording

Spark takes about 2 minutes to boot, and you want data already flowing so the
dashboards look alive. Follow [how_it_works.md §10](how_it_works.md), then
confirm:

- [ ] `docker compose ps` — 7 running, `airflow-init` exited (0)
- [ ] `http://localhost:8000/vitals/live` returns patients, **not** `[]`
- [ ] Grafana dashboard showing a moving line
- [ ] Airflow has at least one **green** DAG run
- [ ] `daily_risk_report` has rows for at least one date

### Pre-open these browser tabs, in this order

1. `http://localhost:8000/docs` — API docs
2. `http://localhost:8000/vitals/live` — live ward
3. `http://localhost:8080` — Airflow (logged in already)
4. `http://localhost:3000` — Grafana, dashboard already open
5. `http://localhost:9090/alerts` — Prometheus alerts

Also have open: VS Code with the repo, and 3–4 terminals (producer, lab loader,
lab simulator, and one free terminal for running demo commands).

### Zoom settings

- [ ] **Record to this Computer** (better quality than cloud)
- [ ] Share **entire screen**, not a single window — you'll switch apps
- [ ] Everyone else **mutes** when not speaking (keyboard noise ruins audio)
- [ ] Turn off "HD video" for participants if bandwidth is tight; audio matters more
- [ ] Do one 30-second test recording and **play it back** — check audio levels
- [ ] Close Slack/WhatsApp/email notifications

---

## The script

### 0:00–0:45 — Opening (M1)

**Show:** VS Code with `README.md` open, or a title slide.

**Say:**
> "We chose Use Case 2, Hospital Patient Vital Signs Monitoring. A hospital
> ward gets continuous vitals from bedside sensors, and once a day the
> pathology lab uploads test results. The business question is: *which patients
> show concerning vital-sign trends right now, and how do yesterday's lab
> results change the risk picture going forward?*
>
> One important simplification up front: **we compress time. One simulated day
> passes every five real minutes — a 288 times speed-up** — so the whole daily
> cycle is demonstrable in this recording. All data is simulated by Python
> scripts; there is no real patient data."

> ⚠️ Say the time compression here, explicitly. The assignment requires it.

### 0:45–2:30 — Architecture decision (M3) ← **20 marks, the biggest item**

**Show:** `docs/architecture_decision.md`, then the diagram in `README.md`.

**Say:**
> "That business question has two halves with different requirements, and
> that's what drove our architecture.
>
> *'Right now'* needs low latency — a nurse can't wait an hour to learn a
> patient is hypoxic. That calls for a **speed layer**.
>
> *'Yesterday's lab results, going forward'* is different. Lab results get
> amended and re-uploaded, and the daily risk report is a clinical record, so
> it has to be stable and reproducible for a given date. That calls for a
> **batch layer** we can safely re-run.
>
> Two different latency and correctness requirements over the same data is
> precisely the case for **Lambda architecture**, so that's what we built.
>
> We seriously considered **Kappa** — treating the daily lab file as a
> compacted Kafka topic and recomputing everything by replay. We rejected it
> for three reasons. First, the lab feed is one small file a day; forcing it
> through a streaming log adds complexity with no latency benefit. Second, a
> clinical report benefits from a bounded, re-runnable job with a clear
> idempotency guarantee, which is easier to reason about in batch. Third,
> Airflow is a required tool for this module and Kappa would leave it barely
> used.
>
> The honest trade-off is that Lambda means **two code paths**, and we do have
> some duplicated threshold logic between the streaming and batch layers. We
> keep them consistent by reading the same `thresholds.yaml` — we actually hit
> a bug where a change to that file broke the streaming job, which is why we
> now document every cross-layer interface in `contracts.md`."

> 💡 That last admission is worth marks — the rubric explicitly rewards honest
> discussion of trade-offs.

### 2:30–3:15 — Technology stack (M3)

**Show:** `docker-compose.yml`, scrolling slowly through the services.

**Say:**
> "Each choice is tied to this use case, not to popularity.
>
> **Kafka** for ingestion: we partition by `patient_id`, which guarantees one
> patient's readings stay in order. Ordering *between* patients doesn't matter
> clinically, so this gives us parallelism for free.
>
> **Spark Structured Streaming** for the speed layer: we need event-time
> windowing and watermarking because our sensors emit late and duplicate
> events, and Spark handles both natively.
>
> **Airflow** for orchestration: the daily job needs retries, backfill, and a
> visible run history — that's a scheduler's job, not a cron line.
>
> **PostgreSQL** for serving: the dashboard and API need relational queries and
> upserts, and the `ON CONFLICT` upsert is what makes our re-runs idempotent.
>
> **Parquet** for the raw archive: columnar and cheap for the batch layer to
> re-read by date, and it keeps a replayable record of raw events."

### 3:15–4:30 — Ingestion (M1) ← 15 marks

**Show:** split view — `simulators/vitals_simulator.py` briefly, then the
**producer terminal with JSON logs scrolling**.

**Say:**
> "Our streaming source simulates 20 bedside monitors. Each patient has a fixed
> baseline and readings drift around it, so it's realistic rather than random.
>
> Crucially we **inject imperfections on purpose**: 5% abnormal spikes so the
> alerting has something to catch, plus null fields, duplicate events, and
> backdated timestamps. That's what makes the downstream cleaning, dedup and
> watermarking meaningful rather than decorative.
>
> These are the producer's structured logs. Every line is JSON with a `stage`
> and a `trace_id`, and that `trace_id` travels in the Kafka message header, so
> we can follow a single event across every stage of the pipeline.
>
> The producer validates before publishing. Anything structurally broken goes
> to a **dead-letter queue** instead of poisoning downstream."

**Then run** in your free terminal:
```powershell
python -m scripts.demo_bad_data
```

**Say:**
> "Here we inject three deliberately malformed events — a missing patient ID, an
> unparseable timestamp, and one where every vital is null. All three fail
> validation and land in the DLQ topic rather than the main stream.
>
> One distinction worth making: a *partial* null is **not** a DLQ case. A
> dropped SpO2 reading is normal sensor noise and the streaming layer cleans
> it. Only structurally unusable records get rejected."

### 4:30–6:00 — Processing (M2) ← 15 marks

**Show:** the **Spark container terminal**, then Airflow's DAG graph view.

**Say:**
> "The speed layer is a Spark Structured Streaming job. It reads from Kafka and
> does four things.
>
> It **cleans** — drops rows missing identity fields, and drops physiologically
> impossible values like an SpO2 above 100.
>
> It **deduplicates** on `event_id`, bounded by a **two-minute watermark**. The
> watermark matters: without it, dedup state grows forever. With it, Spark can
> drop state older than the watermark.
>
> It **aggregates** into five-minute tumbling windows per patient — average,
> min and max for each vital.
>
> Then it applies **thresholds** to raise alerts: tachycardia above 120,
> hypoxia below 90% SpO2, fever above 38.5, hypotension below 90 systolic.
>
> The alert logic is more careful than it looks. We open **one** alert per
> patient per condition — a patient tachycardic for an hour gets one alert, not
> twelve — and we resolve it when the vital returns to normal. A relapse opens a
> genuinely new alert."

**Switch to Airflow DAG graph.**

> "The batch layer is this Airflow DAG, running once per simulated day — every
> five real minutes. It waits for that day's validated lab file, aggregates the
> previous day's vitals from the Parquet archive, joins the two sources,
> computes a risk score, and writes the consolidated report.
>
> It's **idempotent**: the write upserts on patient and report date, so
> re-running a date overwrites rather than duplicating. That's what makes the
> daily report a stable, auditable artifact — and it's a core part of why we
> chose Lambda."

### 6:00–6:50 — Storage & serving (M2) ← 10 marks

**Show:** browser → `/vitals/live`, then `/patients/{id}/alerts`, then
`/reports/daily/{date}`.

**Say:**
> "The serving layer is FastAPI over PostgreSQL.
>
> `/vitals/live` is the real-time ward view — the latest window per patient,
> their ward, and whether they currently have an active alert. *[point at a
> patient with a non-empty `anomaly_flags`]* This patient is flagged right now.
>
> `/patients/P001/alerts` gives that patient's alert history, showing which
> fired and which have resolved.
>
> And this is the deliverable that answers the business question:
> `/reports/daily/` — the consolidated risk report, joining each patient's
> vitals trend with their latest lab results, with a risk score and flag,
> sorted so the highest-risk patients come first."

### 6:50–8:20 — Observability (M1) ← 10 marks

**Show:** Grafana dashboard first.

**Say:**
> "The pipeline is observable, not just functional. Three layers of that.
>
> **Structured logging** across ingestion, processing and batch, all JSON with a
> shared `trace_id`.
>
> **Metrics** — this Grafana dashboard reads Prometheus: events per second, the
> DLQ count, seconds since the last event, and lab files accepted versus
> rejected.
>
> **Alert rules** — and rather than just showing you the config, let's break
> the pipeline on purpose."

**Run:**
```powershell
python scripts/demo_kill_producer.py
```

**Say while it runs:**
> "This kills the producer, simulating a crashed ingestion service. Our
> `NoVitalsReceived` rule watches the gauge holding the time of the last
> published event. When nothing arrives for 60 seconds, the alert goes pending,
> then fires."

**Switch to** `http://localhost:9090/alerts` and let it go red.

> "There it is — firing. And when the producer restarts, it clears."

> ⏱️ This takes ~90 seconds (60s threshold + 30s `for:` duration). Either fill
> the time by talking through the metrics, or cut the wait in editing.

### 8:20–9:00 — Limitations & contributions (M3)

**Show:** `docs/how_it_works.md` §12, or a closing slide.

**Say:**
> "Finally, honest limitations.
>
> The risk score is a **heuristic we designed, not a clinically validated
> instrument**. Thresholds are global rather than per-patient baselines — a fit
> athlete and an elderly patient get the same tachycardia threshold, which a
> real system would personalise.
>
> Infrastructure-wise: single Kafka broker, single Postgres node, no
> replication. Spark runs in local mode, not a cluster. Alerts surface in
> Prometheus and Grafana but nothing pages a nurse — Alertmanager would be the
> production step. And there's no authentication anywhere; real patient data
> would demand encryption, access control and audit logging.
>
> On contributions: _[M1]_ built the simulators, Kafka ingestion and
> observability; _[M2]_ built the Spark speed layer and the serving API; _[M3]_
> built the batch layer, database schema and the architecture decision."

---

## Backup plans

| If this happens | Do this |
|---|---|
| Spark isn't running / `/vitals/live` is empty | Use `python -m streaming.local_runner docs/samples/vitals_sample.jsonl data/out.jsonl` to show the same processing logic without Spark, and explain why |
| The alert won't fire | Show `observability/alerts.yml` and the `vitals_last_event_unix_seconds` graph in Prometheus climbing instead |
| The DAG is red | Show a previously **successful** run from the Airflow run history; explain the sensor waits for that simulated day's lab file |
| Something crashes live | Don't panic or apologise repeatedly — say what should happen, show the code, move on. Composure reads better than perfection |
| You run over 10 minutes | Cut the tech-stack section to 20 seconds and trim the alert wait. **Never** cut the architecture decision — it's 20 marks |

## After recording

- [ ] Watch it once, all the way through, with sound
- [ ] Check every speaker is audible and the screen text is legible
- [ ] Confirm you explicitly said the **288x time compression** and that data is **simulated**
- [ ] Confirm **all three members spoke**
- [ ] Check length is between 5 and 10 minutes
