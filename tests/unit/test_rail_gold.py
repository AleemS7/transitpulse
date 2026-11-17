from datetime import date, datetime
from typing import Any

import pytest
from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from tests import sql

DAY = date(2026, 11, 17)


def ts(hh: int, mm: int, ss: int = 0) -> datetime:
    return datetime(2026, 11, 17, hh, mm, ss)


def view(spark: SparkSession, name: str, rows: list[tuple[Any, ...]], schema: str) -> None:
    spark.createDataFrame(rows, schema).createOrReplaceTempView(name)


@pytest.fixture(scope="module")
def passes(spark: SparkSession) -> dict[tuple[str, datetime], Row]:
    spark.createDataFrame(
        [("A", "wk", *([True] * 7), date(2026, 11, 1), date(2026, 12, 31))],
        "feed_version string, service_id string, monday boolean, tuesday boolean, wednesday boolean,"
        " thursday boolean, friday boolean, saturday boolean, sunday boolean, start_date date, end_date date",
    ).withColumn("_ingested_at", F.current_timestamp()).createOrReplaceTempView("gtfs_calendar")
    sql.run(spark, "gtfs_feed_versions.sql").createOrReplaceTempView("gtfs_feed_versions")
    view(
        spark,
        "gtfs_calendar_dates",
        [],
        "feed_version string, service_id string, date date, exception_type int",
    )
    view(
        spark,
        "gtfs_routes",
        [("A", "29229", "RED", "RED", 1, "FF0000"), ("A", "29226", "BLUE", "BLUE", 1, "0000FF")],
        "feed_version string, route_id string, route_short_name string, route_long_name string,"
        " route_type int, route_color string",
    )
    view(
        spark,
        "gtfs_stops",
        [
            ("A", "510015", "", "FIVE POINTS STATION", 33.75, -84.39, 1, None),
            # A platform with the station's name must not double the arrival.
            ("A", "908000", "", "FIVE POINTS STATION", 33.75, -84.39, 0, "510015"),
            ("A", "510039", "", "SEC DISTRICT STATION", 33.76, -84.40, 1, None),
        ],
        "feed_version string, stop_id string, stop_code string, stop_name string, stop_lat double,"
        " stop_lon double, location_type int, parent_station string",
    )
    for name in ("dim_date", "dim_route", "dim_stop"):
        sql.run(spark, f"{name}.sql").createOrReplaceTempView(name)

    five_points = ("411", "RED", "S", "FIVE POINTS STATION", "Airport")
    omni = ("520", "BLUE", "W", "OMNI DOME STATION", "H.E. Holmes")
    predictions = [
        # First pass: counts down to a minute out, 100 s behind schedule.
        (*five_points, ts(20, 5), ts(20, 31), 1500, 40),
        (*five_points, ts(20, 15), ts(20, 31), 900, 80),
        (*five_points, ts(20, 29, 50), ts(20, 31), 60, 100),
        # Same train back at the same station an hour and a half later: a second pass, 400 s late.
        (*five_points, ts(21, 59), ts(22, 14), 525, 400),
        (*five_points, ts(22, 13), ts(22, 14), 200, 400),
        (*five_points, ts(22, 13), ts(22, 14), 19, 400),
        # Dropped out of the feed five minutes before arriving: not observed.
        (*omni, ts(9, 0), ts(9, 15), 900, 30),
        (*omni, ts(9, 10), ts(9, 15), 300, 30),
    ]
    rows = [(DAY, True, *p[:5], *p[5:], p[5].hour) for p in predictions]
    timetable_only = ("RED", "N", "FIVE POINTS STATION", "North Springs", ts(9, 0), ts(9, 20), 1200, None, 9)
    rows.append((DAY, False, None, *timetable_only))
    view(
        spark,
        "rail_arrivals",
        rows,
        "service_date date, is_realtime boolean, train_id string, line string, direction string,"
        " station string, destination string, event_ts timestamp, next_arrival_ts timestamp,"
        " waiting_seconds int, delay_seconds int, hour_local int",
    )
    return {(r.station, r.first_seen_ts): r for r in sql.run(spark, "fact_rail_arrival.sql").collect()}


def test_a_train_returning_later_is_a_new_pass(passes: dict[tuple[str, datetime], Row]) -> None:
    assert len(passes) == 3
    first = passes[("FIVE POINTS STATION", ts(20, 5))]
    assert (first.predictions, first.last_seen_ts, first.last_wait_seconds) == (3, ts(20, 29, 50), 60)
    second = passes[("FIVE POINTS STATION", ts(21, 59))]
    assert second.predictions == 3 and second.last_seen_ts == ts(22, 13)


def test_last_prediction_is_the_arrival_and_sets_the_status(passes: dict[tuple[str, datetime], Row]) -> None:
    first = passes[("FIVE POINTS STATION", ts(20, 5))]
    assert first.observed and first.delay_seconds == 100 and first.arrival_status == "on_time"
    second = passes[("FIVE POINTS STATION", ts(21, 59))]
    # Two predictions in the same snapshot: the closer one wins.
    assert second.last_wait_seconds == 19 and second.arrival_status == "late"


def test_a_pass_that_vanished_early_has_no_status(passes: dict[tuple[str, datetime], Row]) -> None:
    omni = passes[("OMNI DOME STATION", ts(9, 0))]
    assert not omni.observed and omni.last_wait_seconds == 300 and omni.arrival_status is None


def test_passes_join_the_station_and_line_dimensions(
    spark: SparkSession, passes: dict[tuple[str, datetime], Row]
) -> None:
    stops = {r.stop_id: r.stop_sk for r in spark.table("dim_stop").collect()}
    routes = {r.route_short_name: r.route_sk for r in spark.table("dim_route").collect()}
    first = passes[("FIVE POINTS STATION", ts(20, 5))]
    assert first.stop_sk == stops["510015"] and first.route_sk == routes["RED"]
    assert first.date_key == 20261117
    # The realtime feed's old name still finds the renamed station.
    assert passes[("OMNI DOME STATION", ts(9, 0))].stop_sk == stops["510039"]
