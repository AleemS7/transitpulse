from datetime import date, datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from tests.conftest import utc
from transitpulse.common.config import PipelineConfig
from transitpulse.common.quality import FAILED_RULES, passed, quarantined, with_failed_rules
from transitpulse.common.time import local_text_to_utc, local_to_utc, service_date


def test_table_names_include_the_environment_prefix() -> None:
    dev = PipelineConfig.from_conf({"tp_silver": "dev_silver"}.get)
    assert dev.table("silver", "rail_arrivals") == "workspace.dev_silver.rail_arrivals"
    prod = PipelineConfig.from_conf({}.get)
    assert prod.table("gold", "rail_line_health") == "workspace.gold.rail_line_health"
    assert prod.landing("rail_arrivals") == "/Volumes/workspace/bronze/landing/rail_arrivals/"


def test_local_time_converts_to_utc_in_summer_and_winter(spark: SparkSession) -> None:
    row = (
        spark.createDataFrame([("10/14/2026 4:35:48 PM", "1/15/2026 8:00:00 AM")], ["summer", "winter"])
        .select(
            local_text_to_utc(F.col("summer"), "M/d/yyyy h:mm:ss a").alias("summer"),
            local_text_to_utc(F.col("winter"), "M/d/yyyy h:mm:ss a").alias("winter"),
        )
        .first()
    )
    assert row is not None
    assert utc(row.summer) == datetime(2026, 10, 14, 20, 35, 48)  # EDT is UTC-4
    assert utc(row.winter) == datetime(2026, 1, 15, 13, 0, 0)  # EST is UTC-5


def test_utc_conversion_does_not_depend_on_session_time_zone(spark: SparkSession) -> None:
    spark.conf.set("spark.sql.session.timeZone", "America/Los_Angeles")
    try:
        value = (
            spark.createDataFrame([("10/14/2026 4:35:48 PM",)], ["t"])
            .select(
                F.date_format(local_text_to_utc(F.col("t"), "M/d/yyyy h:mm:ss a"), "yyyy-MM-dd HH:mm:ssXXX")
            )
            .first()
        )
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")
    assert value is not None
    assert value[0] == "2026-10-14 13:35:48-07:00"  # the same instant as 20:35:48 UTC


def test_local_wall_clock_to_utc(spark: SparkSession) -> None:
    row = (
        spark.sql("SELECT TIMESTAMP_NTZ '2026-10-15 00:03:00' AS local")
        .select(local_to_utc(F.col("local")))
        .first()
    )
    assert row is not None and utc(row[0]) == datetime(2026, 10, 15, 4, 3, 0)


def test_service_day_starts_at_3am_local(spark: SparkSession) -> None:
    rows = (
        spark.sql(
            "SELECT * FROM VALUES (TIMESTAMP_NTZ '2026-10-15 02:59:00'),"
            " (TIMESTAMP_NTZ '2026-10-15 03:00:00') AS t(l)"
        )
        .select(service_date(F.col("l")))
        .collect()
    )
    assert [r[0] for r in rows] == [date(2026, 10, 14), date(2026, 10, 15)]


def test_unparseable_local_time_becomes_null(spark: SparkSession) -> None:
    row = (
        spark.createDataFrame([("not a time",)], ["t"])
        .select(local_text_to_utc(F.col("t"), "M/d/yyyy h:mm:ss a"))
        .first()
    )
    assert row is not None and row[0] is None


def test_rows_breaking_rules_are_quarantined_with_reasons(spark: SparkSession) -> None:
    rules = {"positive": "x > 0", "has_name": "name IS NOT NULL"}
    df = with_failed_rules(
        spark.createDataFrame([(1, "a"), (-1, "b"), (None, None)], "x int, name string"), rules
    )

    assert [r.x for r in passed(df).collect()] == [1]
    bad = {r.name: r[FAILED_RULES] for r in quarantined(df).collect()}
    assert bad == {"b": ["positive"], None: ["positive", "has_name"]}
    assert FAILED_RULES not in passed(df).columns
