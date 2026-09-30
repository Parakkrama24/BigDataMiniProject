# Interface contracts

The three layers are developed on separate branches, so every handoff between
them is an interface that one member produces and another consumes. This file
is the agreed definition of each one.

**If you change anything on this page, say so in the group chat and update this
file in the same commit.** Every integration bug we hit came from a change made
on one side of a handoff without the other side knowing:

| What changed | What broke |
|---|---|
| `thresholds.yaml` was re-nested under `vitals:`/`risk:` | `load_thresholds()` did `float()` over a dict — the Spark job crashed on startup |
| Vitals archive written as Parquet, reader accepted only JSON | `aggregate_vitals` returned `{}`, so risk reports were built with empty vitals and no error |
| DAG keyed on Airflow's `ds` (a real date) | Never matched the simulated dates in lab filenames, so the sensor could not fire |
| Lab sensor watched the same folder the loader empties | Sensor and loader raced for the same file |
| `patients` seed CSV was never produced | Every foreign key insert would have failed |

---

## 1. Simulated clock — `common/sim_clock.py`

**Owner:** Member 1. **Consumed by:** everyone.

One simulated day passes every `sim_clock.sim_day_seconds` real seconds
(default 300, i.e. 288x real time), anchored at `sim_clock.real_start` in
`config/settings.yaml`.

| Function | Returns |
|---|---|
| `now(real_now=None)` | simulated datetime (UTC, tz-aware) for a real instant; defaults to the wall clock |
| `current_sim_date()` | simulated date right now |
| `sim_date_for(real_ts)` | simulated date a given real instant maps to |
| `real_window_for(sim_date)` | `[start, end)` real window during which `sim_date` is "today" |

**Rules**
- Nobody reimplements this arithmetic. Different anchors mean different dates
  and the layers stop lining up.
- Anything that must be reproducible across retries (an Airflow task, a
  backfill) passes an explicit `real_ts` rather than relying on the wall clock.
- Reset `real_start` with `python -m scripts.reset_sim_clock` before a demo.

## 2. Vitals stream — Kafka topic `vitals-stream`

**Owner:** Member 1 (producer). **Consumed by:** Member 2 (Spark).

- Key: `patient_id`, UTF-8 bytes. Guarantees per-patient ordering.
- Value: JSON, UTF-8 bytes.
- Header: `trace_id` — the event's `event_id`, for tracing across stages.
- Partitions: 3.

**Two bootstrap addresses, depending on where your client runs:**

| Client location | Bootstrap server |
|---|---|
| Host process (`ingestion.producer`, smoke tests) | `localhost:9092` |
| Another container (a Spark container, for example) | `kafka:29092` |

A broker advertises one address per listener. With only `localhost:9092`
advertised, a container connecting to `kafka:9092` is told to continue the
conversation with "localhost", which resolves to the container itself, so it
never reaches the broker. Hence the separate `INTERNAL` listener.

Kafka has **no persistent volume** in `docker-compose.yml`, so topics are lost
whenever the container is recreated. Recreate them with the commands in the
README (or run `scripts/smoke.ps1`, which does it).

```json
{
  "event_id": "uuid4 string",
  "patient_id": "P001",
  "heart_rate": 77.03, "spo2": 96.86,
  "systolic_bp": 115.85, "diastolic_bp": 79.36,
  "temperature": 36.75,
  "timestamp": "2026-01-02T05:33:29.245824+00:00"
}
```

**The consumer must expect, by design:**
- **Nulls** in any vital field (sensor dropout). `patient_id`, `event_id` and
  `timestamp` are never null.
- **Duplicates** — the same `event_id` emitted twice. Dedupe on `event_id`.
- **Late/out-of-order** `timestamp` values, backdated up to 5 simulated
  minutes. Handle with a watermark.

Rates are configurable under `simulator:` in `config/settings.yaml`.

## 3. Dead-letter queue — Kafka topic `vitals-dlq`

**Owner:** Member 1. **Consumed by:** observability/metrics.

Events failing structural validation (missing `patient_id`, unparseable
`timestamp`, or every vital null) go here instead of `vitals-stream`:

```json
{ "raw_payload": { "...the rejected event..." }, "error_reason": "failed_validation" }
```

Partial nulls are *not* DLQ material — they are normal sensor noise for the
streaming layer to clean.

## 4. Patient reference data

**Owner:** Member 1 (generator). **Consumed by:** Member 3 (loader).

`vitals_live`, `alerts_log`, `lab_results` and `daily_risk_report` all have a
foreign key to `patients`, so **this must be loaded before anything else runs.**

```
python -m simulators.patient_generator        # -> docs/samples/patients.csv
python storage/seed_patients.py docs/samples/patients.csv <DATABASE_URL>
```

CSV columns: `patient_id, ward, admit_date`. Ids use the same `P001..PNNN`
scheme and the same `simulator.num_patients` count as the vitals stream, so
all three sources describe the same ward.

## 5. Daily lab files

**Owner:** Member 1 (simulator + loader). **Consumed by:** Member 3 (DAG).

```
data/landing/labs/labs_<sim_date>.csv        # lab_simulator writes here
        │
        ▼  ingestion/lab_loader.py validates and MOVES
data/landing/processed/labs_<sim_date>.csv   # the DAG reads here
data/landing/rejected/labs_<sim_date>.csv    # failed validation
```

- `<sim_date>` is a **simulated** date (`current_sim_date()`), not a real one.
- Columns: `patient_id, test_type, result_value, reference_range, collected_at`.
  `reference_range` is `"low-high"`, e.g. `"4.0-11.0"`.
- Paths come from `paths:` in `config/settings.yaml` — never hardcoded.

**The DAG reads `processed/`, never `labs/`.** The loader empties `labs/`
within seconds of a file appearing, so a sensor watching it races the loader
and usually loses.

## 6. Vitals archive (Parquet)

**Owner:** Member 2 (Spark). **Consumed by:** Member 3 (batch).

```
data/archive/vitals/date=<sim_date>/part-*.parquet
```

- **Format is Parquet**, partitioned by `date` (Hive style, so `date` is in the
  directory name and not a column inside the file).
- Columns: the cleaned event fields from §2 plus `ingested_at`.
- `batch/pipeline.py` reads it with pyarrow, so no Spark session is needed for
  the batch layer. See [archive_layout.md](archive_layout.md).

## 7. Alert thresholds — `config/thresholds.yaml`

**Owner:** Member 2 (`vitals:`), Member 3 (`risk:`). **Consumed by:** both.

```yaml
vitals:            # speed layer threshold detection
  hr_high: 120
  spo2_low: 90
  temp_high: 38.5
  sbp_low: 90
risk:              # batch risk scoring bands
  medium_score: 30
  high_score: 70
```

`streaming.processing.load_thresholds()` returns the **flat** `vitals:` section
(`hr_high`, `spo2_low`, `temp_high`, `sbp_low`), because that is what
`spark_job.py` indexes. Adding a section is safe; renaming or re-nesting these
four keys is not.

## 8. Database tables — `storage/schema.sql`

**Owner:** Member 3. **Written by:** Members 2 and 3. **Read by:** the API.

| Table | Written by | Notes |
|---|---|---|
| `patients` | `storage/seed_patients.py` | must be loaded first (FK target) |
| `vitals_live` | Spark, via `streaming/postgres_sink.py` | upsert on `(patient_id, window_start)` |
| `alerts_log` | Spark, via `streaming/postgres_sink.py` | at most one unresolved alert per `(patient_id, alert_type)` |
| `lab_results` | batch lab ingest | upsert on `(patient_id, test_type, collected_at)` |
| `daily_risk_report` | `write_report_task` | upsert on `(patient_id, report_date)` so re-runs are idempotent |

**`alert_type` is exactly** `TACHYCARDIA | HYPOXIA | FEVER | HYPOTENSION`
(enforced by a CHECK constraint, and matched by the flags the speed layer
emits). **`risk_flag` is exactly** `LOW | MEDIUM | HIGH`.

**`vitals_live.anomaly_flags` is `JSONB` holding a JSON array.** Bind it with
`psycopg.types.json.Jsonb([...])`. Passing a bare Python list stores an empty
JSON *object* (`{}`) instead, which fails the API's `list[str]` validation and
turns `/vitals/live` into a 500 for every affected row.

### Alert lifecycle

Per window, for each patient: open an alert for every flag that has no
unresolved alert of that type, then resolve any open alert whose flag is absent
from this window. A patient tachycardic for an hour therefore has **one** alert,
not one per window, and a relapse after recovery opens a **new** alert.

## 9. Serving API

**Owner:** Member 2. **`/reports/daily/{date}` owned by Member 3.**

| Endpoint | Returns |
|---|---|
| `GET /vitals/live?ward=` | latest window per patient + `active_alert` |
| `GET /patients/{id}/alerts?status=active\|resolved\|all` | that patient's alerts; 404 if unknown |
| `GET /health` | status + `last_data_received` |
| `GET /reports/daily/{date}?risk_flag=` | the day's risk report; 404 if none |

The repository is selected by the `API_REPOSITORY` env var: `memory` (default,
for tests — no database needed) or `postgres`. `MemoryRepository` and
`PostgresRepository` must keep identical method signatures;
`tests/api/test_repository_parity.py` enforces this.

## 10. Logging and metrics

**Owner:** Member 1. **Used by:** everyone.

```python
from observability.logging_config import get_logger
logger = get_logger("producer")          # stage name: producer, lab_loader, streaming, batch
logger.info("Published vitals event", extra={"trace_id": trace_id, "patient_id": pid})
```

One JSON object per line with `timestamp`, `level`, `message`, `stage`,
`trace_id`, plus any other `extra` keys. Pass `trace_id` on anything traceable
so an event can be followed across stages.

```python
from observability.metrics import counter, gauge, start_metrics_server
```

Prometheus scrapes each process separately, so **each service needs its own
metrics port** (see `observability:` in `config/settings.yaml`) and a matching
target in `observability/prometheus.yml`. Counters are per-process: a script
incrementing a counter cannot change another running process's `/metrics`.
