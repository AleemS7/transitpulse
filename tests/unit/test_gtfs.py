from datetime import date

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from tests import sql
from transitpulse.gtfs import transforms as gtfs


def with_audit(df: DataFrame, version: str = "v1") -> DataFrame:
    return (
        df.withColumn("feed_version", F.lit(version))
        .withColumn("_source_file", F.lit("x"))
        .withColumn("_ingested_at", F.current_timestamp())
    )


@pytest.mark.parametrize(
    ("text", "seconds"),
    [("07:05:09", 25509), ("25:10:00", 90600), (" 7:05:09", 25509), ("bad", None), ("", None), (None, None)],
)
def test_gtfs_times_become_seconds_into_the_service_day(
    spark: SparkSession, text: str | None, seconds: int | None
) -> None:
    row = spark.createDataFrame([(text,)], "t string").select(gtfs.gtfs_seconds(F.col("t"))).first()
    assert row is not None and row[0] == seconds


def test_bronze_takes_the_feed_version_from_the_folder(spark: SparkSession) -> None:
    meta = (
        "struct('/Volumes/workspace/bronze/gtfs_files/routes/feed_version=f4a3/routes.txt' AS file_path,"
        " TIMESTAMP '2026-10-15 00:00:00' AS file_modification_time)"
    )
    df = spark.createDataFrame([("1", "inferred")], "route_id string, feed_version string")
    out = gtfs.bronze(df.withColumn("_metadata", F.expr(meta)))
    row = out.first()
    assert row is not None and row["feed_version"] == "f4a3" and "_metadata" not in row.asDict()
    assert out.columns.count("feed_version") == 1


def test_stops_and_calendar_are_typed(spark: SparkSession) -> None:
    stops = with_audit(
        spark.createDataFrame(
            [("s1", "1", "FIVE POINTS STATION", "33.75", "-84.39", "", "510")],
            "stop_id string, stop_code string, stop_name string, stop_lat string, stop_lon string,"
            " location_type string, parent_station string",
        )
    )
    stop = gtfs.stops(stops).first()
    assert stop is not None
    assert (stop["stop_lat"], stop["location_type"], stop["parent_station"]) == (33.75, 0, "510")

    days = ", ".join(f"{d} string" for d in gtfs.DAYS)
    cal_df = spark.createDataFrame(
        [("wk", "1", "1", "1", "1", "1", "0", "0", "20260822", "20261211")],
        f"service_id string, {days}, start_date string, end_date string",
    )
    cal = gtfs.calendar(with_audit(cal_df)).first()
    assert cal is not None
    assert cal["monday"] is True and cal["sunday"] is False
    assert (cal["start_date"], cal["end_date"]) == (date(2026, 8, 22), date(2026, 12, 11))


@pytest.fixture
def two_versions(spark: SparkSession) -> None:
    """Version A starts Aug 22, B on Dec 12. In B route 1 is renamed, route 4 is gone, route 3 is new."""
    spark.createDataFrame(
        [("A", date(2026, 8, 22), date(2026, 12, 11)), ("B", date(2026, 12, 12), date(2027, 4, 1))],
        "feed_version string, start_date date, end_date date",
    ).withColumn("_ingested_at", F.current_timestamp()).createOrReplaceTempView("gtfs_calendar")
    sql.run(spark, "gtfs_feed_versions.sql").createOrReplaceTempView("gtfs_feed_versions")

    rows = [
        ("A", "1", "1", "Old Name", 3, "9B3694"),
        ("A", "2", "2", "Same", 3, "DB2F26"),
        ("A", "4", "4", "Retired", 3, "000000"),
        ("B", "1", "1", "New Name", 3, "9B3694"),
        ("B", "2", "2", "Same", 3, "DB2F26"),
        ("B", "3", "RED", "Red Line", 1, "FF0000"),
    ]
    spark.createDataFrame(
        rows,
        "feed_version string, route_id string, route_short_name string, route_long_name string,"
        " route_type int, route_color string",
    ).createOrReplaceTempView("gtfs_routes")

    spark.createDataFrame(
        [
            ("A", "sun", date(2026, 11, 26), 1),
            ("B", "sun", date(2026, 11, 26), 1),
            ("B", "sun", date(2027, 1, 1), 1),
        ],
        "feed_version string, service_id string, date date, exception_type int",
    ).createOrReplaceTempView("gtfs_calendar_dates")


@pytest.mark.usefixtures("two_versions")
def test_feed_versions_hand_over_on_the_next_start_date(spark: SparkSession) -> None:
    rows = {r.feed_version: r for r in spark.table("gtfs_feed_versions").collect()}
    assert rows["A"].valid_to == date(2026, 12, 12) and rows["A"].is_latest is False
    assert rows["B"].valid_to is None and rows["B"].is_latest is True


@pytest.mark.usefixtures("two_versions")
def test_dim_route_keeps_history_when_a_route_changes(spark: SparkSession) -> None:
    rows = sql.run(spark, "dim_route.sql").collect()
    by_id: dict[str, list] = {}
    for r in sorted(rows, key=lambda r: (r.route_id, r.valid_from)):
        by_id.setdefault(r.route_id, []).append(r)

    renamed = by_id["1"]
    assert [r.route_long_name for r in renamed] == ["Old Name", "New Name"]
    assert renamed[0].valid_to == date(2026, 12, 12) and renamed[0].is_current is False
    assert renamed[1].valid_to is None and renamed[1].is_current is True

    assert len(by_id["2"]) == 1 and by_id["2"][0].is_current is True
    assert by_id["3"][0].transit_mode == "rail" and by_id["3"][0].valid_from == date(2026, 12, 12)
    assert by_id["4"][0].is_current is False
    assert len({r.route_sk for r in rows}) == len(rows)


@pytest.mark.usefixtures("two_versions")
def test_dim_date_flags_weekends_and_schedule_exceptions(spark: SparkSession) -> None:
    days = {r.calendar_date: r for r in sql.run(spark, "dim_date.sql").collect()}
    assert len(days) == 730
    thanksgiving, saturday, new_year = (
        days[date(2026, 11, 26)],
        days[date(2026, 11, 28)],
        days[date(2027, 1, 1)],
    )
    assert thanksgiving.date_key == 20261126 and thanksgiving.day_name == "Thursday"
    assert thanksgiving.is_schedule_exception is True and thanksgiving.is_weekend is False
    assert saturday.is_weekend is True and saturday.day_of_week == 6
    assert new_year.is_schedule_exception is True
    assert days[date(2026, 11, 27)].is_schedule_exception is False
