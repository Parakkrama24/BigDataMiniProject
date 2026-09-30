"""Regression tests for the streaming -> batch vitals archive handoff.

The Spark streaming job archives raw vitals as Parquet partitioned by date,
but batch/pipeline.py originally only accepted .jsonl/.json and the Airflow
task globbed only those extensions. The result was silent: no files matched,
aggregate_vitals returned {}, and daily risk reports were built with empty
vitals instead of failing loudly.
"""

from datetime import date, datetime, timezone

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from batch.pipeline import aggregate_vitals

REPORT_DATE = date(2026, 1, 5)


def _write_parquet(tmp_path, rows):
    """Write a Parquet file shaped like streaming/spark_job.py's archive sink."""
    target = tmp_path / f"date={REPORT_DATE}"
    target.mkdir(parents=True, exist_ok=True)
    path = target / "part-00000.parquet"
    pq.write_table(pa.Table.from_pylist(rows), path)
    return path


def _event(patient_id="P001", hour=10, **overrides):
    row = {
        "event_id": f"e-{patient_id}-{hour}",
        "patient_id": patient_id,
        "heart_rate": 70.0,
        "spo2": 98.0,
        "systolic_bp": 120.0,
        "diastolic_bp": 78.0,
        "temperature": 36.7,
        "timestamp": datetime(2026, 1, 5, hour, 0, tzinfo=timezone.utc),
        "ingested_at": datetime(2026, 1, 5, hour, 0, 1, tzinfo=timezone.utc),
    }
    row.update(overrides)
    return row


def test_parquet_archive_is_read(tmp_path):
    path = _write_parquet(tmp_path, [_event(hour=10), _event(hour=11, heart_rate=130.0)])

    result = aggregate_vitals([path], REPORT_DATE)

    assert "P001" in result, "Parquet archive produced no patients"
    assert result["P001"]["avg_hr"] == 100.0
    assert result["P001"]["min_hr"] == 70.0
    assert result["P001"]["max_hr"] == 130.0


def test_event_count_counts_events_not_vital_values(tmp_path):
    # Two events, each carrying five vitals. event_count must be 2, not 10.
    path = _write_parquet(tmp_path, [_event(hour=10), _event(hour=11)])

    result = aggregate_vitals([path], REPORT_DATE)

    assert result["P001"]["event_count"] == 2


def test_rows_outside_the_report_date_are_ignored(tmp_path):
    path = _write_parquet(
        tmp_path,
        [
            _event(hour=10),
            _event(hour=10, timestamp=datetime(2026, 1, 6, 10, 0, tzinfo=timezone.utc)),
        ],
    )

    result = aggregate_vitals([path], REPORT_DATE)

    assert result["P001"]["event_count"] == 1


def test_unsupported_extension_still_raises(tmp_path):
    path = tmp_path / "vitals.txt"
    path.write_text("not a supported archive", encoding="utf-8")

    with pytest.raises(ValueError):
        aggregate_vitals([path], REPORT_DATE)
