"""Bus realtime: protobuf snapshots -> bronze rows -> silver positions, trips and observed stop arrivals.

Bronze keeps one row per vehicle (positions) or per trip (trip updates, with that trip's stop updates as a
list), exactly as decoded. Silver types the values and keeps one row per business key.

Observed arrival (ADR-0005): MARTA keeps a stop in a trip's update for a while after the bus has passed it.
Once a stop's arrival time is at or before the snapshot's own timestamp, that time is a report of something
that already happened. The latest such report per trip, stop and service day is the observed arrival.
"""

from __future__ import annotations

from pyspark import cloudpickle
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DataType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from transitpulse.bus import decode
from transitpulse.bus.decode import trip_updates as trip_feed
from transitpulse.bus.decode import vehicle_positions as vehicle_feed
from transitpulse.common.quality import Rules
from transitpulse.common.time import epoch_to_local
from transitpulse.gtfs.transforms import gtfs_date


def _struct(**fields: DataType) -> StructType:
    return StructType([StructField(name, kind) for name, kind in fields.items()])


TRIP_FIELDS: dict[str, DataType] = {
    "trip_id": StringType(),
    "start_date": StringType(),
    "start_time": StringType(),
    "route_id": StringType(),
    "direction_id": IntegerType(),
    "trip_status": StringType(),
}

VEHICLE = _struct(
    entity_id=StringType(),
    **TRIP_FIELDS,
    vehicle_id=StringType(),
    vehicle_label=StringType(),
    latitude=DoubleType(),
    longitude=DoubleType(),
    bearing=DoubleType(),
    speed=DoubleType(),
    position_epoch=LongType(),
    current_status=StringType(),
    stop_id=StringType(),
    occupancy_status=StringType(),
)

STOP_TIME_UPDATE = _struct(
    stop_sequence=IntegerType(),
    stop_id=StringType(),
    stop_status=StringType(),
    arrival_epoch=LongType(),
    scheduled_arrival_epoch=LongType(),
    departure_epoch=LongType(),
)

TRIP_UPDATE = _struct(
    entity_id=StringType(),
    **TRIP_FIELDS,
    vehicle_id=StringType(),
    update_epoch=LongType(),
    stop_time_updates=ArrayType(STOP_TIME_UPDATE),
)


def _decoded(entity: StructType) -> StructType:
    return _struct(feed_epoch=LongType(), decode_error=StringType(), entities=ArrayType(entity))


# Python workers don't have this repo on their import path, so the decoder travels with the UDF itself.
cloudpickle.register_pickle_by_value(decode)  # type: ignore[no-untyped-call]

GTFS_RT_SCHEMA = decode.load_schema()

# One Python call per file, not per row: gzip and protobuf have no Spark built-in, and a file holds hundreds
# of entities, so the UDF cost is small next to the rows it produces.
# The lambdas name the functions, not the module: pickling the module would carry its whole namespace,
# including driver-only objects the workers can't rebuild.
decode_vehicle_positions = F.udf(lambda content: vehicle_feed(content, GTFS_RT_SCHEMA), _decoded(VEHICLE))
decode_trip_updates = F.udf(lambda content: trip_feed(content, GTFS_RT_SCHEMA), _decoded(TRIP_UPDATE))

# Generous box around metro Atlanta; it catches (0, 0) and swapped coordinates, not detours.
ATLANTA_BBOX = "latitude BETWEEN 33.0 AND 34.5 AND longitude BETWEEN -85.5 AND -83.5"

# A trip that MARTA has cancelled or removed never produces an arrival, even if stops linger in the feed.
NOT_RUNNING = ("CANCELED", "DELETED")


def bronze(raw_files: DataFrame, decoder: Column, entity: StructType) -> DataFrame:
    """One row per decoded entity. A file that fails to decode becomes one row with only the error set.

    Expects Auto Loader's binaryFile columns (content) and _metadata. `decoder` is the UDF applied to content.
    """
    decoded = raw_files.select(
        decoder.alias("d"),
        F.col("_metadata.file_path").alias("_source_file"),
        F.col("_metadata.file_modification_time").alias("_file_modified_at"),
    )
    failed_marker = F.array(F.lit(None).cast(entity))
    entities = F.when(F.col("d.decode_error").isNull(), F.col("d.entities")).otherwise(failed_marker)
    return decoded.select(
        F.col("d.feed_epoch").alias("feed_epoch"),
        F.explode(entities).alias("e"),
        F.col("d.decode_error").alias("_decode_error"),
        "_source_file",
        "_file_modified_at",
        F.current_timestamp().alias("_ingested_at"),
    ).select(
        "feed_epoch",
        *[F.col(f"e.{name}").alias(name) for name in entity.fieldNames()],
        "_decode_error",
        "_source_file",
        "_file_modified_at",
        "_ingested_at",
    )


def _audit() -> list[Column]:
    return [
        F.col("_decode_error"),
        F.col("_source_file"),
        F.col("_ingested_at"),
        F.current_timestamp().alias("_updated_at"),
    ]


def _trip_columns() -> list[Column]:
    return [
        F.col("trip_id"),
        gtfs_date(F.col("start_date")).alias("service_date"),
        F.col("route_id"),
        F.col("direction_id"),
        F.col("trip_status"),
    ]


VEHICLE_RULES: Rules = {
    "decoded": "_decode_error IS NULL",
    "has_vehicle": "vehicle_id IS NOT NULL AND position_ts IS NOT NULL",
    "has_trip": "trip_id IS NOT NULL AND route_id IS NOT NULL AND service_date IS NOT NULL",
    "inside_atlanta": ATLANTA_BBOX,
    "not_in_future": "position_ts <= feed_ts + INTERVAL 60 SECONDS",
}


def vehicle_positions(bronze_rows: DataFrame) -> DataFrame:
    """Typed vehicle positions. Rows aren't filtered here; VEHICLE_RULES decide."""
    return bronze_rows.select(
        F.col("vehicle_id"),
        F.timestamp_seconds("position_epoch").alias("position_ts"),
        *_trip_columns(),
        F.col("vehicle_label"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("bearing"),
        F.col("speed").alias("speed_mps"),
        F.col("current_status"),
        F.col("occupancy_status"),
        F.hour(epoch_to_local(F.col("position_epoch"))).alias("hour_local"),
        F.timestamp_seconds("feed_epoch").alias("feed_ts"),
        *_audit(),
    )


TRIP_RULES: Rules = {
    "decoded": "_decode_error IS NULL",
    "has_trip": "trip_id IS NOT NULL AND service_date IS NOT NULL AND route_id IS NOT NULL",
    "has_feed_time": "feed_ts IS NOT NULL",
}


def trips(bronze_rows: DataFrame) -> DataFrame:
    """One row per trip per snapshot: its status (scheduled, cancelled...) and when it was reported."""
    return bronze_rows.select(
        *_trip_columns(),
        F.col("start_time"),
        F.col("vehicle_id"),
        F.size("stop_time_updates").alias("stop_updates"),
        F.timestamp_seconds("feed_epoch").alias("feed_ts"),
        *_audit(),
    )


ARRIVAL_RULES: Rules = {
    "has_keys": "trip_id IS NOT NULL AND service_date IS NOT NULL AND stop_sequence IS NOT NULL",
    "has_stop": "stop_id IS NOT NULL",
    # A bus three hours off its timetable is almost certainly a bad record, not a real delay.
    "near_schedule": "delay_seconds IS NULL OR abs(delay_seconds) <= 3 * 3600",
}


def observed_arrivals(bronze_rows: DataFrame) -> DataFrame:
    """Stop updates whose arrival time had already passed when MARTA published the snapshot.

    Each row is one report of an arrival; silver keeps the latest report per trip, stop and service day.
    """
    stops = bronze_rows.where(
        F.col("_decode_error").isNull() & ~F.col("trip_status").isin(*NOT_RUNNING)
    ).select(
        "*",
        F.explode("stop_time_updates").alias("s"),
    )
    arrival = F.col("s.arrival_epoch")
    scheduled = F.col("s.scheduled_arrival_epoch")
    return stops.where(
        (F.col("s.stop_status") == "SCHEDULED") & arrival.isNotNull() & (arrival <= F.col("feed_epoch"))
    ).select(
        F.col("trip_id"),
        gtfs_date(F.col("start_date")).alias("service_date"),
        F.col("s.stop_sequence").alias("stop_sequence"),
        F.col("s.stop_id").alias("stop_id"),
        F.col("route_id"),
        F.col("direction_id"),
        F.col("vehicle_id"),
        F.timestamp_seconds(arrival).alias("arrival_ts"),
        F.timestamp_seconds(scheduled).alias("scheduled_arrival_ts"),
        (arrival - scheduled).cast("int").alias("delay_seconds"),
        F.hour(epoch_to_local(arrival)).alias("hour_local"),
        F.timestamp_seconds("feed_epoch").alias("reported_at"),
        F.col("_source_file"),
        F.col("_ingested_at"),
        F.current_timestamp().alias("_updated_at"),
    )
