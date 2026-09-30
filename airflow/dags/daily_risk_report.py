"""Airflow orchestration for one simulated report date.

The transformations live in batch.pipeline so they remain unit-testable without
an Airflow installation. Database operators can be attached when deployed.
"""
from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from batch.pipeline import aggregate_vitals as aggregate_vitals_from_archive, build_reports, read_lab_file, summarize_labs

try:
    from airflow import DAG
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator
    from airflow.sensors.filesystem import FileSensor
except ImportError:  # Allows repository tooling to inspect this file locally.
    DAG = FileSensor = PythonOperator = EmptyOperator = None


def on_failure(context):
    from observability.logging_config import get_logger
    get_logger("batch").error("daily risk DAG task failed", extra={"batch_id": context.get("run_id"), "task_id": context.get("task_instance").task_id})


def ingest_labs_task(**context):
    report_date = date.fromisoformat(context["ds"])
    path = Path(os.getenv("DATA_ROOT", ".")) / "data" / "landing" / "labs" / f"labs_{report_date}.csv"
    return read_lab_file(path, report_date)


def aggregate_vitals_task(**context):
    report_date = date.fromisoformat(context["ds"])
    archive = Path(os.getenv("DATA_ROOT", ".")) / "data" / "archive" / "vitals" / f"date={report_date}"
    files = list(archive.glob("*.jsonl")) + list(archive.glob("*.json"))
    return aggregate_vitals_from_archive(files, report_date) if files else {}


def join_and_score_task(**context):
    task_instance = context["ti"]
    report_date = date.fromisoformat(context["ds"])
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
        schedule="*/5 * * * *",
        start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
        catchup=True,
        max_active_runs=1,
        default_args={"retries": 2, "retry_delay": timedelta(seconds=30), "on_failure_callback": on_failure},
        tags=["batch", "risk-report"],
    ) as dag:
        wait_for_lab_file = FileSensor(task_id="wait_for_lab_file", filepath="data/landing/labs/labs_{{ ds }}.csv", poke_interval=15, timeout=240, mode="reschedule")
        ingest_labs = PythonOperator(task_id="ingest_labs", python_callable=ingest_labs_task)
        aggregate_vitals = PythonOperator(task_id="aggregate_vitals", python_callable=aggregate_vitals_task)
        join_and_score = PythonOperator(task_id="join_and_score", python_callable=join_and_score_task)
        write_report = PythonOperator(task_id="write_report", python_callable=write_report_task)
        notify = EmptyOperator(task_id="notify")
        wait_for_lab_file >> ingest_labs >> aggregate_vitals >> join_and_score >> write_report >> notify
