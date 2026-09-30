from __future__ import annotations

import argparse
import os
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array,
    array_compact,
    avg,
    col,
    current_timestamp,
    explode,
    lit,
    max as spark_max,
    min as spark_min,
    size,
    sum as spark_sum,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from streaming.postgres_sink import write_window
from streaming.processing import load_thresholds


VITALS_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("patient_id", StringType()),
    StructField("heart_rate", DoubleType()),
    StructField("spo2", DoubleType()),
    StructField("systolic_bp", DoubleType()),
    StructField("diastolic_bp", DoubleType()),
    StructField("temperature", DoubleType()),
    StructField("timestamp", TimestampType()),
])


def clean_stream(events: DataFrame) -> DataFrame:
    valid_ranges = (
        (col("heart_rate").isNull() | col("heart_rate").between(30, 220))
        & (col("spo2").isNull() | col("spo2").between(50, 100))
        & (col("systolic_bp").isNull() | col("systolic_bp").between(60, 250))
        & (col("diastolic_bp").isNull() | col("diastolic_bp").between(30, 150))
        & (col("temperature").isNull() | col("temperature").between(30, 43))
    )
    return (
        events.filter(col("event_id").isNotNull() & col("patient_id").isNotNull() & col("timestamp").isNotNull())
        .filter(valid_ranges)
        .withWatermark("timestamp", "2 minutes")
        .dropDuplicates(["event_id"])
    )


def aggregate_stream(events: DataFrame, thresholds: dict[str, float]) -> DataFrame:
    return (
        events.groupBy(window("timestamp", "5 minutes"), "patient_id")
        .agg(
            avg("heart_rate").alias("avg_hr"),
            avg("spo2").alias("avg_spo2"),
            avg("systolic_bp").alias("avg_systolic_bp"),
            avg("diastolic_bp").alias("avg_diastolic_bp"),
            avg("temperature").alias("avg_temp"),
            spark_min("heart_rate").alias("min_hr"),
            spark_max("heart_rate").alias("max_hr"),
            spark_min("spo2").alias("min_spo2"),
            spark_max("spo2").alias("max_spo2"),
            spark_sum(lit(1)).alias("event_count"),
            spark_max(when(col("heart_rate") > thresholds["hr_high"], lit(1)).otherwise(lit(0))).alias("tachycardia"),
            spark_max(when(col("spo2") < thresholds["spo2_low"], lit(1)).otherwise(lit(0))).alias("hypoxia"),
            spark_max(when(col("temperature") > thresholds["temp_high"], lit(1)).otherwise(lit(0))).alias("fever"),
            spark_max(when(col("systolic_bp") < thresholds["sbp_low"], lit(1)).otherwise(lit(0))).alias("hypotension"),
        )
        .select(
            "patient_id",
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "avg_hr", "avg_spo2", "avg_systolic_bp", "avg_diastolic_bp", "avg_temp",
            "min_hr", "max_hr", "min_spo2", "max_spo2", "event_count",
            # array_compact drops the NULLs left by the unmatched when()
            # branches. array_remove(arr, lit(None)) was used here before, but
            # Spark's array_remove returns NULL when the element to remove is
            # NULL, so anomaly_flags could come out NULL instead of filtered.
            array_compact(array(
                when(col("tachycardia") == 1, lit("TACHYCARDIA")),
                when(col("hypoxia") == 1, lit("HYPOXIA")),
                when(col("fever") == 1, lit("FEVER")),
                when(col("hypotension") == 1, lit("HYPOTENSION")),
            )).alias("anomaly_flags"),
        )
    )


def upsert_postgres(batch: DataFrame, batch_id: int) -> None:
    """foreachBatch sink: persist window aggregates and reconcile alerts.

    The SQL and alert logic live in streaming.postgres_sink so they can be
    tested without a Spark session.
    """
    import psycopg

    connection_string = os.environ["DATABASE_URL"]
    rows = batch.collect()
    if not rows:
        return
    with psycopg.connect(connection_string) as connection:
        with connection.cursor() as cursor:
            for row in rows:
                write_window(cursor, row.asDict())


def build_query(spark: SparkSession, bootstrap_servers: str, topic: str, archive_path: str, checkpoint_path: str):
    raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrap_servers).option("subscribe", topic).option("startingOffsets", "latest").load()
    events = raw.selectExpr("CAST(value AS STRING) AS payload").selectExpr("from_json(payload, 'event_id STRING, patient_id STRING, heart_rate DOUBLE, spo2 DOUBLE, systolic_bp DOUBLE, diastolic_bp DOUBLE, temperature DOUBLE, timestamp TIMESTAMP') AS event").select("event.*")
    cleaned = clean_stream(events)
    aggregates = aggregate_stream(cleaned, load_thresholds())
    aggregate_query = aggregates.writeStream.foreachBatch(upsert_postgres).option("checkpointLocation", checkpoint_path + "/live").outputMode("update").start()
    archive_query = cleaned.withColumn("ingested_at", current_timestamp()).withColumn("date", col("timestamp").cast("date")).writeStream.format("parquet").option("path", archive_path).option("checkpointLocation", checkpoint_path + "/archive").partitionBy("date").outputMode("append").start()
    return aggregate_query, archive_query


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "vitals-stream"))
    parser.add_argument("--archive-path", default=os.getenv("VITALS_ARCHIVE_PATH", "data/archive/vitals"))
    parser.add_argument("--checkpoint-path", default=os.getenv("SPARK_CHECKPOINT_PATH", "data/checkpoints/vitals"))
    args = parser.parse_args()
    spark = SparkSession.builder.appName("vitals-speed-layer").getOrCreate()
    queries = build_query(spark, args.bootstrap_servers, args.topic, args.archive_path, args.checkpoint_path)
    spark.streams.awaitAnyTermination()
    for query in queries:
        query.stop()


if __name__ == "__main__":
    main()
