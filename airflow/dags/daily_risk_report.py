"""Airflow orchestration for one simulated report date.

The transformations live in batch.pipeline so they remain unit-testable without
an Airflow installation. Database operators can be attached when deployed.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from batch.pipeline import aggregate_vitals as aggregate_vitals_from_archive, build_reports, read_lab_file, summarize_labs
from common.config import load_settings
from common.sim_clock import sim_date_for

try:
    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator
    from airflow.sensors.python import PythonSensor
except ImportError:  # Allows repository tooling to inspect this file locally.
    DAG = PythonSensor = PythonOperator = EmptyOperator = None


def on_failure(context):
    from observability.logging_config import get_logger
    get_logger("batch").error("daily risk DAG task failed", extra={"batch_id": context.get("run_id"), "task_id": context.get("task_instance").task_id})


def _data_root() -> Path:
    return Path(os.getenv("DATA_ROOT", "."))


def _report_date(context) -> date:
    """The simulated report date this run is responsible for.

    The DAG fires every 5 real minutes and exactly one simulated day passes
    in that time, so the run's logical date (a real instant) has to be
    translated through the shared simulated clock. Using context["ds"]
    directly -- as this DAG originally did -- yields the real calendar date,
    which never matches the simulated dates the simulators stamp onto lab
    filenames and vitals events, so the sensor could never fire.

    Deriving from logical_date rather than the wall clock keeps the run
    idempotent: retries and backfills of the same run resolve to the same
    simulated date.
    """
    return sim_date_for(context["logical_date"])


def _processed_lab_path(report_date: date) -> Path:
    """Path to the validated lab file for a simulated date.

    Reads from the *processed* folder, not the landing folder:
    ingestion/lab_loader.py validates each dropped file and moves it here
    within seconds, so a sensor watching the landing folder would race the
    loader and usually lose.
    """
    processed = load_settings()["paths"]["landing_processed"]
    return _data_root() / processed / f"labs_{report_date}.csv"


def lab_file_ready(**context) -> bool:
    report_date = _report_date(context)
    return _processed_lab_path(report_date).exists()


def ingest_labs_task(**context):
    report_date = _report_date(context)
    return read_lab_file(_processed_lab_path(report_date), report_date)


def aggregate_vitals_task(**context):
    report_date = _report_date(context)
    archive_root = load_settings()["paths"]["archive_vitals"]
    archive = _data_root() / archive_root / f"date={report_date}"
    files = sorted(archive.glob("*.parquet")) + sorted(archive.glob("*.jsonl")) + sorted(archive.glob("*.json"))
    return aggregate_vitals_from_archive(files, report_date) if files else {}


def join_and_score_task(**context):
    task_instance = context["ti"]
    report_date = _report_date(context)
    labs = summarize_labs(task_instance.xcom_pull(task_ids="ingest_labs"))
    vitals = task_instance.xcom_pull(task_ids="aggregate_vitals") or {}
    return build_reports(report_date, vitals, labs)


def write_report_task(**context):
    import psycopg
    from psycopg.types.json import Jsonb

    reports = context["ti"].xcom_pull(task_ids="join_and_score") or []
    database_url = os.getenv("DATABASE_URL", "postgresql://hospital:hospital@postgres:5432/hospital")
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(
                """INSERT INTO daily_risk_report
                   (patient_id, report_date, vitals_summary, lab_summary, risk_score, risk_flag)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (patient_id, report_date) DO UPDATE SET
                     vitals_summary = EXCLUDED.vitals_summary,
                     lab_summary = EXCLUDED.lab_summary,
                     risk_score = EXCLUDED.risk_score,
                     risk_flag = EXCLUDED.risk_flag,
                     generated_at = now()""",
                [(row["patient_id"], row["report_date"], Jsonb(row["vitals_summary"]), Jsonb(row["lab_summary"]), row["risk_score"], row["risk_flag"]) for row in reports],
            )
    return len(reports)


if DAG is not None:
    with DAG(
        dag_id="daily_risk_report",
        # One run every 5 real minutes == one simulated day (see
        # config/settings.yaml sim_clock). catchup is off deliberately: with a
        # 5-minute schedule, catchup=True plus a start_date months in the past
        # queues tens of thousands of runs the moment the scheduler starts.
        # Backfilling a specific range is still supported on demand via
        # `airflow dags backfill daily_risk_report -s ... -e ...`.
        schedule="*/5 * * * *",
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 2, "retry_delay": timedelta(seconds=30), "on_failure_callback": on_failure},
        tags=["batch", "risk-report"],
    ) as dag:
        # PythonSensor rather than FileSensor: the path depends on the
        # simulated date, which has to be computed in Python from the run's
        # logical date and can't be expressed as a Jinja-templated filepath.
        wait_for_lab_file = PythonSensor(task_id="wait_for_lab_file", python_callable=lab_file_ready, poke_interval=15, timeout=240, mode="reschedule")
        ingest_labs = PythonOperator(task_id="ingest_labs", python_callable=ingest_labs_task)
        aggregate_vitals = PythonOperator(task_id="aggregate_vitals", python_callable=aggregate_vitals_task)
        join_and_score = PythonOperator(task_id="join_and_score", python_callable=join_and_score_task)
        write_report = PythonOperator(task_id="write_report", python_callable=write_report_task)
        notify = EmptyOperator(task_id="notify")
        wait_for_lab_file >> ingest_labs >> aggregate_vitals >> join_and_score >> write_report >> notify
