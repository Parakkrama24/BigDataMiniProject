# Archive Layout

Member 2 writes cleaned vitals as Parquet under:

```text
data/archive/vitals/date=YYYY-MM-DD/*.parquet
```

Each row contains the cleaned event fields and `ingested_at`. Member 3 reads one or more partitions for the report date. Raw daily labs are copied after validation to:

```text
data/archive/labs/date=YYYY-MM-DD/labs_YYYY-MM-DD.csv
```

The batch adapter may use JSONL during local development because the committed sample is JSONL; production replay should use the Parquet partition. Archive cleanup is a production policy decision: move old raw partitions to lower-cost object storage, retaining report inputs for the required audit period.
