# BigDataMiniProject

## Member 2: local development

Install the pinned dependencies with the Python interpreter selected in VS Code:

```powershell
python -m pip install -r requirements.txt
python -m pytest -q
```

Run the API locally:

```powershell
python -m uvicorn api.main:app --reload
```

The development API provides `/vitals/live`, `/patients/{id}/alerts`, `/health`,
and interactive documentation at `/docs`. It uses an in-memory repository until
the Postgres schema is supplied by Member 3.

Process a JSONL sample without Kafka or Spark:

```powershell
python -m streaming.local_runner docs/samples/vitals.jsonl data/vitals/aggregates.jsonl
```

Run the production speed layer after Kafka and Postgres are available:

```powershell
python -m streaming.spark_job --bootstrap-servers localhost:9092
```

