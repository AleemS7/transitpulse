"""Collector heartbeats: one JSON file per collector run -> one ops row per run and feed.

The heartbeat is the collector's own account of each run (see design/specs/collector.md R-006), so the
lakehouse can tell "MARTA was down" apart from "the collector was down" apart from "the pipeline is behind".
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from transitpulse.common.quality import Rules

FEED_RESULT = StructType(
    [
        StructField("feed", StringType()),
        StructField("status", StringType()),
        StructField("http_status", IntegerType()),
        StructField("fetch_attempts", IntegerType()),
        StructField("upload_attempts", IntegerType()),
        StructField("bytes_raw", LongType()),
        StructField("bytes_stored", LongType()),
        StructField("feed_ts", StringType()),
        StructField("entity_count", IntegerType()),
        StructField("unparsed", BooleanType()),
        StructField("path", StringType()),
        StructField("last_modified", StringType()),
        StructField("error", StringType()),
    ]
)

RAW_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("kind", StringType()),
        StructField("collector_version", StringType()),
        StructField("started_at", StringType()),
        StructField("finished_at", StringType()),
        StructField("duration_ms", LongType()),
        StructField("replayed", IntegerType()),
        StructField("deadletter_pending", IntegerType()),
        StructField("feeds", ArrayType(FEED_RESULT)),
    ]
)

STATUSES = ("ok", "unchanged", "failed", "deadlettered")

RULES: Rules = {
    "has_run": "run_id IS NOT NULL AND feed IS NOT NULL",
    "times_parsed": "started_at IS NOT NULL AND finished_at IS NOT NULL",
    "known_status": f"status IN ({', '.join(repr(s) for s in STATUSES)})",
}


def bronze(raw: DataFrame) -> DataFrame:
    """Heartbeats as written, plus load metadata. Expects the reader's _metadata column."""
    return raw.select(
        *RAW_SCHEMA.fieldNames(),
        "_rescued_data",
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )


def _iso(name: str) -> Column:
    # ISO 8601 with an offset, e.g. 2026-11-16T00:14:02.056584+00:00; the cast keeps the instant.
    return F.col(name).try_cast("timestamp")


def collector_runs(bronze_rows: DataFrame) -> DataFrame:
    """One row per collector run and feed, typed. Rows aren't filtered here; RULES decide."""
    return bronze_rows.select("*", F.explode_outer("feeds").alias("f")).select(
        F.col("run_id"),
        F.col("f.feed").alias("feed"),
        F.col("kind"),
        F.col("collector_version"),
        _iso("started_at").alias("started_at"),
        _iso("finished_at").alias("finished_at"),
        F.col("duration_ms"),
        F.col("replayed"),
        F.col("deadletter_pending"),
        F.col("f.status").alias("status"),
        F.col("f.http_status").alias("http_status"),
        F.col("f.fetch_attempts").alias("fetch_attempts"),
        F.col("f.bytes_stored").alias("bytes_stored"),
        F.col("f.feed_ts").try_cast("timestamp").alias("feed_ts"),
        F.col("f.entity_count").alias("entity_count"),
        F.col("f.unparsed").alias("unparsed"),
        F.col("f.error").alias("error"),
        F.col("_source_file"),
        F.col("_ingested_at"),
        F.current_timestamp().alias("_updated_at"),
    )
