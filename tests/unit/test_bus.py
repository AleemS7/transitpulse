import gzip
from datetime import date, datetime
from typing import Any

import pytest
from google.transit import gtfs_realtime_pb2 as rt
from pyspark import cloudpickle
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from tests.conftest import utc
from transitpulse.bus import decode
from transitpulse.bus import transforms as bus
from transitpulse.common.quality import FAILED_RULES, with_failed_rules

SCHEMA = decode.load_schema()

# 2026-11-15 18:15:48 in Atlanta (EST).
FEED_TS = 1794784548


def vehicle_feed() -> bytes:
    msg = rt.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = FEED_TS
    ok = msg.entity.add(id="1604")
    ok.vehicle.trip.trip_id = "11723886"
    ok.vehicle.trip.start_date = "20261115"
    ok.vehicle.trip.route_id = "29178"
    ok.vehicle.trip.direction_id = 5
    ok.vehicle.position.latitude = 33.68
    ok.vehicle.position.longitude = -84.25
    ok.vehicle.position.speed = 8.9
    ok.vehicle.timestamp = FEED_TS - 12
    ok.vehicle.vehicle.id = "2304"
    ok.vehicle.occupancy_status = rt.VehiclePosition.MANY_SEATS_AVAILABLE
    null_island = msg.entity.add(id="1700")
    null_island.vehicle.trip.trip_id = "t2"
    null_island.vehicle.trip.start_date = "20261115"
    null_island.vehicle.trip.route_id = "29178"
    null_island.vehicle.position.latitude = 0
    null_island.vehicle.position.longitude = 0
    null_island.vehicle.timestamp = FEED_TS
    null_island.vehicle.vehicle.id = "2400"
    return gzip.compress(msg.SerializeToString())


def add_stop(tu: Any, seq: int, arrival: int | None, scheduled: int | None = None, status: int = 0) -> None:
    stu = tu.stop_time_update.add(stop_sequence=seq, stop_id=f"s{seq}", schedule_relationship=status)
    if arrival is not None:
        stu.arrival.time = arrival
        stu.departure.time = arrival
    if scheduled is not None:
        stu.arrival.scheduled_time = scheduled


def trip_feed(feed_ts: int = FEED_TS) -> bytes:
    msg = rt.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = feed_ts
    running = msg.entity.add(id="11723886").trip_update
    running.trip.trip_id = "11723886"
    running.trip.start_date = "20261115"
    running.trip.start_time = "18:00:00"
    running.trip.route_id = "29178"
    running.vehicle.id = "2304"
    add_stop(running, 2, arrival=FEED_TS - 600, scheduled=FEED_TS - 900)  # passed, 5 min late
    add_stop(running, 3, arrival=FEED_TS, scheduled=FEED_TS + 30)  # reached exactly now, 30 s early
    add_stop(running, 4, arrival=FEED_TS + 120, scheduled=FEED_TS + 60)  # still a prediction
    add_stop(running, 5, arrival=None, status=rt.TripUpdate.StopTimeUpdate.NO_DATA)
    add_stop(running, 6, arrival=FEED_TS - 60, status=rt.TripUpdate.StopTimeUpdate.SKIPPED)
    cancelled = msg.entity.add(id="11729479").trip_update
    cancelled.trip.trip_id = "11729479"
    cancelled.trip.start_date = "20261115"
    cancelled.trip.route_id = "29187"
    cancelled.trip.schedule_relationship = rt.TripDescriptor.CANCELED
    add_stop(cancelled, 1, arrival=FEED_TS - 300)
    return gzip.compress(msg.SerializeToString())


def test_udfs_ship_only_functions_and_the_schema() -> None:
    # Pickling a whole module drags its namespace along; on Databricks that includes driver-only objects.
    for udf in (bus.decode_vehicle_positions, bus.decode_trip_updates):
        payload = cloudpickle.dumps(udf.func)
        assert b"pathlib" not in payload and b"SCHEMA_FILE" not in payload


def test_bundled_schema_matches_the_official_bindings() -> None:
    assert rt.DESCRIPTOR.serialized_pb == SCHEMA


def test_vehicle_feed_decodes_to_one_row_per_vehicle() -> None:
    out = decode.vehicle_positions(vehicle_feed(), SCHEMA)
    assert out["decode_error"] is None and out["feed_epoch"] == FEED_TS
    first = out["entities"][0]
    assert first["vehicle_id"] == "2304" and first["trip_id"] == "11723886" and first["direction_id"] == 5
    assert first["position_epoch"] == FEED_TS - 12 and first["latitude"] == pytest.approx(33.68)
    assert first["occupancy_status"] == "MANY_SEATS_AVAILABLE" and first["trip_status"] == "SCHEDULED"
    assert first["bearing"] is None and first["stop_id"] is None and first["current_status"] is None


def test_trip_feed_keeps_each_trips_stop_updates_together() -> None:
    out = decode.trip_updates(trip_feed(), SCHEMA)
    trip = out["entities"][0]
    assert [s["stop_sequence"] for s in trip["stop_time_updates"]] == [2, 3, 4, 5, 6]
    assert trip["stop_time_updates"][0]["scheduled_arrival_epoch"] == FEED_TS - 900
    no_data = trip["stop_time_updates"][3]
    assert no_data["stop_status"] == "NO_DATA" and no_data["arrival_epoch"] is None
    assert out["entities"][1]["trip_status"] == "CANCELED"


def test_uncompressed_payload_is_accepted() -> None:
    assert decode.vehicle_positions(gzip.decompress(vehicle_feed()), SCHEMA)["feed_epoch"] == FEED_TS


@pytest.mark.parametrize(
    ("content", "error"),
    [(b"", "empty file"), (b"\x1f\x8b not really gzip", "BadGzipFile"), (b"\xff\xff\xff\xff", "DecodeError")],
)
def test_unreadable_files_return_an_error_instead_of_raising(content: bytes, error: str) -> None:
    out = decode.trip_updates(content, SCHEMA)
    assert error in out["decode_error"] and out["entities"] == [] and out["feed_epoch"] is None


def landed(spark: SparkSession, *files: bytes) -> DataFrame:
    meta = (
        "struct(concat('landing/bus/', id, '.pb.gz') AS file_path,"
        " TIMESTAMP '2026-11-15' AS file_modification_time)"
    )
    rows = [(str(i), content) for i, content in enumerate(files)]
    return spark.createDataFrame(rows, "id string, content binary").withColumn("_metadata", F.expr(meta))


@pytest.fixture(scope="module")
def vehicles(spark: SparkSession) -> list[dict[str, Any]]:
    raw = landed(spark, vehicle_feed(), b"\x00broken")
    bronze = bus.bronze(raw, bus.decode_vehicle_positions("content"), bus.VEHICLE)
    checked = with_failed_rules(bus.vehicle_positions(bronze), bus.VEHICLE_RULES)
    return [r.asDict() for r in checked.collect()]


def test_bronze_has_a_row_per_vehicle_and_one_per_broken_file(vehicles: list[dict[str, Any]]) -> None:
    assert len(vehicles) == 3
    broken = [v for v in vehicles if v["_decode_error"]]
    assert len(broken) == 1 and broken[0]["vehicle_id"] is None
    assert broken[0]["_source_file"] == "landing/bus/1.pb.gz"


def test_vehicle_position_is_typed_in_utc_with_the_atlanta_hour(vehicles: list[dict[str, Any]]) -> None:
    row = next(v for v in vehicles if v["vehicle_id"] == "2304")
    assert utc(row["position_ts"]) == datetime(2026, 11, 15, 23, 15, 36)
    assert row["hour_local"] == 18 and row["service_date"] == date(2026, 11, 15)
    assert row["speed_mps"] == pytest.approx(8.9) and row[FAILED_RULES] == []


def test_vehicle_rules_catch_bad_coordinates_and_broken_files(vehicles: list[dict[str, Any]]) -> None:
    failed = {v["vehicle_id"]: v[FAILED_RULES] for v in vehicles}
    assert failed["2400"] == ["inside_atlanta"]
    assert "decoded" in failed[None]


@pytest.fixture(scope="module")
def trip_bronze(spark: SparkSession) -> DataFrame:
    raw = landed(spark, trip_feed(), b"")
    return bus.bronze(raw, bus.decode_trip_updates("content"), bus.TRIP_UPDATE)


def test_trips_carry_marta_status_per_service_day(trip_bronze: DataFrame) -> None:
    rows = with_failed_rules(bus.trips(trip_bronze), bus.TRIP_RULES).collect()
    by_trip = {r["trip_id"]: r for r in rows}
    assert by_trip["11729479"]["trip_status"] == "CANCELED"
    assert by_trip["11723886"]["stop_updates"] == 5 and by_trip["11723886"][FAILED_RULES] == []
    assert by_trip[None][FAILED_RULES] == ["decoded", "has_trip", "has_feed_time"]


def test_only_stops_already_passed_count_as_observed_arrivals(trip_bronze: DataFrame) -> None:
    rows = {r["stop_sequence"]: r for r in bus.observed_arrivals(trip_bronze).collect()}
    # Stop 4 is still in the future, 5 has no time, 6 was skipped and the cancelled trip never ran.
    assert set(rows) == {2, 3}
    assert rows[2]["delay_seconds"] == 300 and rows[3]["delay_seconds"] == -30
    assert utc(rows[2]["arrival_ts"]) == datetime(2026, 11, 15, 23, 5, 48)
    assert rows[2]["hour_local"] == 18 and utc(rows[2]["reported_at"]) == datetime(2026, 11, 15, 23, 15, 48)


def test_arrival_rules_flag_impossible_delays(spark: SparkSession) -> None:
    far_off = spark.createDataFrame(
        [("t", date(2026, 11, 15), 1, "s1", 4 * 3600), ("t", date(2026, 11, 15), 2, None, None)],
        "trip_id string, service_date date, stop_sequence int, stop_id string, delay_seconds int",
    )
    failed = [r[FAILED_RULES] for r in with_failed_rules(far_off, bus.ARRIVAL_RULES).collect()]
    assert failed == [["near_schedule"], ["has_stop"]]
