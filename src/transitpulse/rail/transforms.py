"""Rail arrivals: bronze keeps MARTA's records as text; silver types, cleans and fingerprints them.

One silver row is one prediction exactly as MARTA published it. The feed repeats the same prediction in
every snapshot until it changes, so rows are identified by a fingerprint of all source fields.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from transitpulse.common.quality import Rules
from transitpulse.common.time import local_text_to_local, local_text_to_utc, local_to_utc, service_date

SOURCE_FIELDS = [
    "TRAIN_ID",
    "LINE",
    "DIRECTION",
    "STATION",
    "DESTINATION",
    "EVENT_TIME",
    "NEXT_ARR",
    "WAITING_SECONDS",
    "WAITING_TIME",
    "DELAY",
    "IS_REALTIME",
    "LATITUDE",
    "LONGITUDE",
]

# Every field arrives as text; bronze keeps it that way and silver casts.
RAW_SCHEMA = StructType([StructField(name, StringType()) for name in SOURCE_FIELDS])

EVENT_TIME_PATTERN = "M/d/yyyy h:mm:ss a"
NEXT_ARR_PATTERN = "yyyy-MM-dd h:mm:ss a"

LINES = ("RED", "GOLD", "BLUE", "GREEN")
DIRECTIONS = ("N", "S", "E", "W")

RULES: Rules = {
    "valid_line": f"line IN ({', '.join(repr(v) for v in LINES)})",
    "valid_direction": f"direction IN ({', '.join(repr(v) for v in DIRECTIONS)})",
    "has_station": "station IS NOT NULL",
    "event_time_parsed": "event_ts IS NOT NULL",
    "next_arrival_parsed": "next_arrival_ts IS NOT NULL",
    "waiting_not_negative": "waiting_seconds >= 0",
    "realtime_has_train": "NOT is_realtime OR train_id IS NOT NULL",
}


def bronze(raw: DataFrame) -> DataFrame:
    """Raw records plus where and when they were loaded. Expects the reader's _metadata column."""
    return raw.select(
        *SOURCE_FIELDS,
        "_rescued_data",
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_file_modified_at"),
        F.current_timestamp().alias("_ingested_at"),
    )


def _blank_to_null(name: str) -> Column:
    return F.nullif(F.trim(F.col(name)), F.lit(""))


def silver(bronze_rows: DataFrame) -> DataFrame:
    """Typed, cleaned rail predictions. Rows aren't filtered here; the quality rules decide."""
    event_local = local_text_to_local(F.col("EVENT_TIME"), EVENT_TIME_PATTERN)
    same_day_next = local_text_to_local(
        F.concat(F.date_format(event_local, "yyyy-MM-dd"), F.lit(" "), F.col("NEXT_ARR")), NEXT_ARR_PATTERN
    )
    # NEXT_ARR has no date. A time far "before" the event time belongs to the next day (after midnight).
    next_local = F.when(
        same_day_next < event_local - F.expr("INTERVAL 12 HOURS"), same_day_next + F.expr("INTERVAL 1 DAY")
    ).otherwise(same_day_next)

    fingerprint = F.sha2(F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("")) for c in SOURCE_FIELDS]), 256)

    return bronze_rows.select(
        fingerprint.alias("record_id"),
        _blank_to_null("TRAIN_ID").alias("train_id"),
        F.upper(_blank_to_null("LINE")).alias("line"),
        F.upper(_blank_to_null("DIRECTION")).alias("direction"),
        _blank_to_null("STATION").alias("station"),
        _blank_to_null("DESTINATION").alias("destination"),
        (F.lower(F.col("IS_REALTIME")) == "true").alias("is_realtime"),
        local_text_to_utc(F.col("EVENT_TIME"), EVENT_TIME_PATTERN).alias("event_ts"),
        F.hour(event_local).alias("hour_local"),
        local_to_utc(next_local).alias("next_arrival_ts"),
        F.col("WAITING_SECONDS").try_cast("int").alias("waiting_seconds"),
        F.regexp_extract(F.col("DELAY"), r"^T(-?\d+)S$", 1).try_cast("int").alias("delay_seconds"),
        F.col("LATITUDE").try_cast("double").alias("latitude"),
        F.col("LONGITUDE").try_cast("double").alias("longitude"),
        service_date(event_local).alias("service_date"),
        "_source_file",
        "_ingested_at",
        F.current_timestamp().alias("_updated_at"),
    )
