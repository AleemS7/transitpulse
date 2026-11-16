from datetime import date, datetime
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from tests import sql

# Mon 16 to Wed 18 Nov 2026 have bus data, so only Tuesday 17 was collected from start to finish.
MON, TUE, WED = date(2026, 11, 16), date(2026, 11, 17), date(2026, 11, 18)


def ts(hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime(2026, 11, 17, hh, mm, ss)


def view(spark: SparkSession, name: str, rows: list[tuple[Any, ...]], schema: str) -> None:
    spark.createDataFrame(rows, schema).createOrReplaceTempView(name)


@pytest.fixture(scope="module", autouse=True)
def network(spark: SparkSession) -> None:
    weekdays = (True, True, True, True, True, False, False)
    calendar = [
        ("A", "wk", *weekdays, date(2026, 11, 1), date(2026, 12, 31)),
        ("A", "wk2", *weekdays, date(2026, 11, 1), date(2026, 12, 31)),
        ("A", "hol", *([False] * 7), date(2026, 11, 1), date(2026, 12, 31)),
    ]
    spark.createDataFrame(
        calendar,
        "feed_version string, service_id string, monday boolean, tuesday boolean, wednesday boolean,"
        " thursday boolean, friday boolean, saturday boolean, sunday boolean, start_date date, end_date date",
    ).withColumn("_ingested_at", F.current_timestamp()).createOrReplaceTempView("gtfs_calendar")
    sql.run(spark, "gtfs_feed_versions.sql").createOrReplaceTempView("gtfs_feed_versions")
    view(
        spark,
        "gtfs_calendar_dates",
        [("A", "hol", TUE, 1), ("A", "wk2", TUE, 2)],
        "feed_version string, service_id string, date date, exception_type int",
    )
    view(
        spark,
        "gtfs_routes",
        [("A", "10", "10", "Peachtree", 3, "FF0000"), ("A", "RED", "RED", "Red Line", 1, "FF0000")],
        "feed_version string, route_id string, route_short_name string, route_long_name string,"
        " route_type int, route_color string",
    )
    view(
        spark,
        "gtfs_stops",
        [("A", s, s, f"Stop {s}", 33.75, -84.39, 0, None) for s in ("s1", "s2", "s3")],
        "feed_version string, stop_id string, stop_code string, stop_name string, stop_lat double,"
        " stop_lon double, location_type int, parent_station string",
    )
    trips = [
        ("t1", "10", "wk"),  # runs, observed
        ("t7", "10", "wk"),  # runs right behind t1
        ("t2", "10", "wk"),  # scheduled, never seen
        ("t3", "10", "wk"),  # cancelled by MARTA
        ("t5", "10", "hol"),  # added for the day by calendar_dates
        ("t6", "10", "wk2"),  # removed for the day by calendar_dates
        ("r1", "RED", "wk"),  # rail: not a bus trip
    ]
    view(
        spark,
        "gtfs_trips",
        [("A", t, r, s, 0, "Downtown") for t, r, s in trips],
        "feed_version string, trip_id string, route_id string, service_id string, direction_id int,"
        " trip_headsign string",
    )
    stop_times = [
        ("A", t, f"s{n}", n, 36000 + n * 60, 36000 + n * 60) for t, _, _ in trips for n in (1, 2, 3)
    ]
    view(
        spark,
        "gtfs_stop_times",
        stop_times,
        "feed_version string, trip_id string, stop_id string, stop_sequence int, arrival_secs int,"
        " departure_secs int",
    )
    for name in ("dim_date", "dim_route", "dim_stop"):
        sql.run(spark, f"{name}.sql").createOrReplaceTempView(name)

    view(
        spark,
        "bus_vehicle_positions",
        [("v1", "t1", MON), ("v1", "t1", TUE), ("v2", "t7", TUE), ("v1", "t1", WED)],
        "vehicle_id string, trip_id string, service_date date",
    )
    view(
        spark,
        "bus_trips",
        [("t1", TUE, "SCHEDULED"), ("t3", TUE, "CANCELED"), ("t5", TUE, "SCHEDULED")],
        "trip_id string, service_date date, trip_status string",
    )
    arrivals = [
        # t1: early at s1, on time at s2 (realtime stop ID differs from the timetable), late at s3.
        ("t1", 1, "s1", ts(15, 0), ts(15, 1, 30), -90),
        ("t1", 2, "99s2", ts(15, 10), ts(15, 8), 120),
        ("t1", 3, "s3", ts(15, 20), ts(15, 13, 20), 400),
        # t7 was due 20 minutes after t1 at s2 but arrived 2 minutes behind it: bunched.
        ("t7", 2, "s2", ts(15, 12), ts(15, 28), -960),
        ("t5", 1, "s1", ts(16, 0), None, None),
    ]
    view(
        spark,
        "bus_stop_arrivals",
        [(t, TUE, seq, stop, "10", "v1", a, s, d, 10) for t, seq, stop, a, s, d in arrivals],
        "trip_id string, service_date date, stop_sequence int, stop_id string, route_id string,"
        " vehicle_id string, arrival_ts timestamp, scheduled_arrival_ts timestamp, delay_seconds int,"
        " hour_local int",
    )
    view(
        spark,
        "rail_arrivals",
        [(TUE, True, 60), (TUE, True, 120), (TUE, False, None)],
        "service_date date, is_realtime boolean, delay_seconds int",
    )
    for name in ("bus_scheduled_trips", "fact_stop_arrival", "ghost_trips", "headway_bunching"):
        sql.run(spark, f"{name}.sql").createOrReplaceTempView(name)


def test_scheduled_trips_follow_the_calendar_and_its_exceptions(spark: SparkSession) -> None:
    rows = spark.table("bus_scheduled_trips").collect()
    assert {r.service_date for r in rows} == {TUE}
    assert {r.trip_id for r in rows} == {"t1", "t7", "t2", "t3", "t5"}
    assert rows[0].scheduled_start_local == "10:01"


def test_ghost_trips_are_cancelled_or_never_seen(spark: SparkSession) -> None:
    reasons = {r.trip_id: r.reason for r in spark.table("ghost_trips").collect()}
    assert reasons == {"t2": "no_bus_seen", "t3": "cancelled"}


def test_fact_takes_the_stop_from_the_timetable_and_classifies_lateness(spark: SparkSession) -> None:
    rows = {(r.trip_id, r.stop_sequence): r for r in spark.table("fact_stop_arrival").collect()}
    assert len(rows) == 5
    assert rows[("t1", 2)].stop_id == "s2" and rows[("t1", 2)].realtime_stop_id == "99s2"
    assert [rows[("t1", n)].arrival_status for n in (1, 2, 3)] == ["early", "on_time", "late"]
    assert rows[("t5", 1)].arrival_status is None
    assert rows[("t1", 1)].date_key == 20261117 and rows[("t1", 1)].route_sk is not None
    assert all(r.stop_sk is not None and r.in_timetable for r in rows.values())


def test_bunching_compares_the_gap_with_the_planned_gap(spark: SparkSession) -> None:
    rows = spark.table("headway_bunching").where("stop_id = 's2'").collect()
    assert len(rows) == 1
    assert rows[0].trip_id == "t7" and rows[0].previous_trip_id == "t1"
    assert rows[0].headway_seconds == 120 and rows[0].scheduled_headway_seconds == 1200
    assert rows[0].is_bunched is True


def test_route_hour_otp_ignores_arrivals_without_a_schedule(spark: SparkSession) -> None:
    row = sql.run(spark, "otp_route_hour.sql").collect()[0]
    assert (row.arrivals, row.on_time, row.early, row.late) == (5, 1, 2, 1)
    assert row.on_time_pct == 25.0 and row.route_short_name == "10"


def test_stop_delay_has_one_row_per_stop(spark: SparkSession) -> None:
    rows = {r.stop_id: r for r in sql.run(spark, "stop_delay.sql").collect()}
    assert rows["s2"].arrivals == 2 and rows["s2"].stop_name == "Stop s2"
    assert rows["s3"].late_pct == 100.0


def test_daily_kpis_cover_only_fully_collected_days(spark: SparkSession) -> None:
    rows = sql.run(spark, "daily_kpis.sql").collect()
    assert len(rows) == 1
    day = rows[0]
    assert day.service_date == TUE and day.day_name == "Tuesday"
    assert (day.scheduled_trips, day.ghost_trips, day.cancelled_trips) == (5, 2, 1)
    assert day.ghost_trip_pct == 40.0 and day.observed_arrivals == 5
    assert day.bunched_pct == 100.0 and day.rail_avg_delay_seconds == 90
