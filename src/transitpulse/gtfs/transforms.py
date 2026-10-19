"""GTFS static schedule: bronze keeps each version's CSVs as text; silver types them per version.

Every row carries feed_version (the zip's content hash), so several schedule versions can sit side by side
and realtime data can be compared with the version that was in effect on its service day.
"""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from transitpulse.common.quality import Rules

FEED_VERSION_PATTERN = r"feed_version=([^/]+)/"


def bronze(raw: DataFrame) -> DataFrame:
    """All CSV columns as text, plus the feed version and load metadata. Expects the reader's _metadata.

    Auto Loader may already infer feed_version from the folder name; it's dropped and re-read from the path
    so the column exists exactly once, with one meaning.
    """
    return (
        raw.drop("feed_version")
        .select(
            "*",
            F.regexp_extract(F.col("_metadata.file_path"), FEED_VERSION_PATTERN, 1).alias("feed_version"),
            F.col("_metadata.file_path").alias("_source_file"),
            F.col("_metadata.file_modification_time").alias("_file_modified_at"),
            F.current_timestamp().alias("_ingested_at"),
        )
        .drop("_metadata")
    )


def gtfs_seconds(text: Column) -> Column:
    """'HH:MM:SS' -> seconds after the start of the service day. Hours can exceed 23 (e.g. 25:10:00)."""
    parts = F.split(F.trim(text), ":")
    h, m, s = (F.get(parts, i).try_cast("int") for i in range(3))
    return F.when(F.size(parts) == 3, h * 3600 + m * 60 + s)


def gtfs_date(text: Column) -> Column:
    """'YYYYMMDD' -> DATE, or NULL."""
    return F.try_to_timestamp(F.trim(text), F.lit("yyyyMMdd")).cast("date")


def _text(name: str) -> Column:
    return F.nullif(F.trim(F.col(name)), F.lit(""))


def _int(name: str) -> Column:
    return _text(name).try_cast("int")


def _audit(df: DataFrame, *cols: Column) -> DataFrame:
    return df.select(
        F.col("feed_version"),
        *cols,
        F.col("_source_file"),
        F.col("_ingested_at"),
        F.current_timestamp().alias("_updated_at"),
    )


def routes(df: DataFrame) -> DataFrame:
    return _audit(
        df,
        _text("route_id").alias("route_id"),
        _text("route_short_name").alias("route_short_name"),
        _text("route_long_name").alias("route_long_name"),
        _int("route_type").alias("route_type"),
        F.upper(_text("route_color")).alias("route_color"),
    )


def stops(df: DataFrame) -> DataFrame:
    return _audit(
        df,
        _text("stop_id").alias("stop_id"),
        _text("stop_code").alias("stop_code"),
        _text("stop_name").alias("stop_name"),
        _text("stop_lat").try_cast("double").alias("stop_lat"),
        _text("stop_lon").try_cast("double").alias("stop_lon"),
        F.coalesce(_int("location_type"), F.lit(0)).alias("location_type"),
        _text("parent_station").alias("parent_station"),
    )


def trips(df: DataFrame) -> DataFrame:
    return _audit(
        df,
        _text("trip_id").alias("trip_id"),
        _text("route_id").alias("route_id"),
        _text("service_id").alias("service_id"),
        _int("direction_id").alias("direction_id"),
        _text("trip_headsign").alias("trip_headsign"),
        _text("block_id").alias("block_id"),
    )


def stop_times(df: DataFrame) -> DataFrame:
    return _audit(
        df,
        _text("trip_id").alias("trip_id"),
        _text("stop_id").alias("stop_id"),
        _int("stop_sequence").alias("stop_sequence"),
        gtfs_seconds(F.col("arrival_time")).alias("arrival_secs"),
        gtfs_seconds(F.col("departure_time")).alias("departure_secs"),
        _int("timepoint").alias("timepoint"),
    )


DAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")


def calendar(df: DataFrame) -> DataFrame:
    return _audit(
        df,
        _text("service_id").alias("service_id"),
        *[(F.trim(F.col(day)) == "1").alias(day) for day in DAYS],
        gtfs_date(F.col("start_date")).alias("start_date"),
        gtfs_date(F.col("end_date")).alias("end_date"),
    )


def calendar_dates(df: DataFrame) -> DataFrame:
    return _audit(
        df,
        _text("service_id").alias("service_id"),
        gtfs_date(F.col("date")).alias("date"),
        _int("exception_type").alias("exception_type"),
    )


# Atlanta-area bounding box, deliberately generous.
ATLANTA_BBOX = "stop_lat BETWEEN 33.0 AND 34.5 AND stop_lon BETWEEN -85.5 AND -83.5"

# The schedule is the agency's reference data: rules warn (rows kept, pass rates recorded) rather than drop,
# because silently losing a trip would be worse than keeping an odd one.
TABLES: dict[str, tuple[Callable[[DataFrame], DataFrame], Rules]] = {
    "routes": (
        routes,
        {"has_route_id": "route_id IS NOT NULL", "known_route_type": "route_type IN (0, 1, 2, 3)"},
    ),
    "stops": (stops, {"has_stop_id": "stop_id IS NOT NULL", "inside_atlanta": ATLANTA_BBOX}),
    "trips": (
        trips,
        {
            "has_keys": "trip_id IS NOT NULL AND route_id IS NOT NULL AND service_id IS NOT NULL",
            "valid_direction": "direction_id IN (0, 1)",
        },
    ),
    "stop_times": (
        stop_times,
        {
            "has_keys": "trip_id IS NOT NULL AND stop_id IS NOT NULL AND stop_sequence IS NOT NULL",
            "arrival_parsed": "arrival_secs IS NOT NULL",
            "departs_after_arriving": "departure_secs >= arrival_secs",
        },
    ),
    "calendar": (calendar, {"dates_parsed": "start_date IS NOT NULL AND end_date >= start_date"}),
    "calendar_dates": (
        calendar_dates,
        {"date_parsed": "date IS NOT NULL", "known_exception": "exception_type IN (1, 2)"},
    ),
}
