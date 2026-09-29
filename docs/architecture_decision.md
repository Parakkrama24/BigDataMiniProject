# Architecture Decision: Lambda over Kappa

## Decision

We chose a Lambda architecture: Kafka and Spark Structured Streaming form the speed layer for sub-minute vital-sign alerts, while Airflow and the batch layer produce a stable daily risk report from archived vitals and laboratory results. PostgreSQL serves the current aggregates, alerts, and report; date-partitioned Parquet remains the replay and audit source.

## Criteria

| Criterion | Lambda in this system | Kappa considered |
| --- | --- | --- |
| Latency | Streaming vitals detect tachycardia, hypoxia, fever, and hypotension within a window. | One stream could provide low latency, but would add lab-file ingestion complexity. |
| Replay and correction | Airflow can re-run one simulated date when a lab is late or amended. | Replaying a compacted lab topic would reprocess more state than the use case needs. |
| Consistency | A report is an immutable-by-key, reproducible daily artifact; reruns overwrite `(patient_id, report_date)`. | Continuous replay makes a bounded clinical reporting cut less explicit. |
| Cost and complexity | The required Kafka and Airflow components have clear, separate responsibilities. | Kappa would remove a layer but force low-volume daily labs into the stream path. |

## Concrete trade-offs observed

The batch boundary makes late lab handling straightforward: ingesting a corrected file and rerunning the same simulated date replaces the report rows rather than appending duplicates. The cost is duplicated transformation paths and an additional orchestration service. The shared pure batch functions keep the scoring rule consistent across local tests, Airflow, and a future Spark adapter.

## What would change our mind

If laboratory results became a high-volume event stream, needed sub-minute correlation, or required one continuously replayable source of truth, Kappa would become attractive. We would also reconsider if operating Airflow and the batch storage path cost more than the audit and correction guarantees justify.

## Risk scoring formula

The heuristic score is bounded to 0-100:

`score = 100 * (0.60 * abnormal_vital_fraction + 0.30 * abnormal_lab_fraction + 0.10 * trend_fraction)`

`LOW` is below 30, `MEDIUM` is 30 through 69.99, and `HIGH` is 70 or above. Missing vitals and labs do not create abnormalities. This is an engineering demo heuristic, not a clinically validated decision tool.

## Storage and retention

Raw vitals are written to `data/archive/vitals/date=YYYY-MM-DD/`; raw labs are copied to `data/archive/labs/date=YYYY-MM-DD/`. PostgreSQL retains serving data needed by the demo. In production, raw archives would move to object storage with a policy-controlled retention period, while report and audit records would be retained according to hospital policy rather than deleted solely for cost.
