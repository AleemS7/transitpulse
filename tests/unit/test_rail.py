import json
from datetime import date, datetime
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from tests.conftest import utc
from transitpulse.common.quality import FAILED_RULES, passed, quarantined, with_failed_rules
from transitpulse.rail import transforms as rail

FIXTURE = Path(__file__).parents[1] / "fixtures" / "rail_edge_cases.json"


def with_text_column(schema: StructType, name: str) -> StructType:
    return StructType([*schema.fields, StructField(name, StringType())])


@pytest.fixture(scope="module")
def raw(spark: SparkSession) -> DataFrame:
    records = json.loads(FIXTURE.read_text())
    rows = [(*tuple(r.get(f) for f in rail.SOURCE_FIELDS), r["_case"]) for r in records]
    df = spark.createDataFrame(rows, with_text_column(rail.RAW_SCHEMA, "_case"))
    return df.withColumn("_source_file", F.lit("landing/rail_arrivals/x.json.gz")).withColumn(
        "_ingested_at", F.current_timestamp()
    )


@pytest.fixture(scope="module")
def silver(raw: DataFrame) -> dict[str, dict]:  # type: ignore[type-arg]
    out = rail.silver(raw.drop("_case")).withColumn("_case", F.lit(None))
    cases = [r["_case"] for r in raw.select("_case").collect()]
    return {case: row.asDict() for case, row in zip(cases, out.collect(), strict=True)}


def test_bronze_keeps_text_and_adds_load_metadata(spark: SparkSession) -> None:
    meta = (
        "struct('landing/rail_arrivals/a.json.gz' AS file_path,"
        " TIMESTAMP '2026-10-15 00:00:00' AS file_modification_time)"
    )
    df = spark.createDataFrame(
        [(*tuple("x" for _ in rail.SOURCE_FIELDS), None)], with_text_column(rail.RAW_SCHEMA, "_rescued_data")
    )
    out = rail.bronze(df.withColumn("_metadata", F.expr(meta))).first()
    assert out is not None
    assert out["_source_file"] == "landing/rail_arrivals/a.json.gz"
    assert utc(out["_file_modified_at"]) == datetime(2026, 10, 15)
    assert out["LINE"] == "x" and out["_ingested_at"] is not None


def test_realtime_record_is_typed_and_in_utc(silver: dict[str, dict]) -> None:  # type: ignore[type-arg]
    row = silver["realtime"]
    assert row["train_id"] == "411" and row["line"] == "RED" and row["is_realtime"] is True
    assert utc(row["event_ts"]) == datetime(2026, 10, 14, 20, 35, 48)
    assert row["hour_local"] == 16
    assert utc(row["next_arrival_ts"]) == datetime(2026, 10, 14, 20, 41, 18)
    assert row["waiting_seconds"] == 325 and row["delay_seconds"] == 564
    assert row["latitude"] == pytest.approx(33.84907)
    assert row["service_date"] == date(2026, 10, 14)


def test_negative_delay_means_early(silver: dict[str, dict]) -> None:  # type: ignore[type-arg]
    assert silver["early_train"]["delay_seconds"] == -45


def test_scheduled_rows_have_no_train_or_delay(silver: dict[str, dict]) -> None:  # type: ignore[type-arg]
    row = silver["scheduled"]
    assert row["is_realtime"] is False
    assert row["train_id"] is None and row["delay_seconds"] is None and row["latitude"] is None


def test_arrival_after_midnight_rolls_to_next_day(silver: dict[str, dict]) -> None:  # type: ignore[type-arg]
    row = silver["after_midnight"]
    assert utc(row["event_ts"]) == datetime(2026, 10, 15, 3, 58)
    assert utc(row["next_arrival_ts"]) == datetime(2026, 10, 15, 4, 3)


def test_late_night_rides_count_toward_the_previous_service_day(silver: dict[str, dict]) -> None:  # type: ignore[type-arg]
    assert silver["early_morning"]["service_date"] == date(2026, 10, 14)


def test_winter_record_uses_standard_time_and_normalises_case(silver: dict[str, dict]) -> None:  # type: ignore[type-arg]
    row = silver["winter_time"]
    assert utc(row["event_ts"]) == datetime(2026, 1, 15, 13, 0)
    assert (row["line"], row["direction"]) == ("RED", "S")


def test_identical_records_share_a_fingerprint(silver: dict[str, dict]) -> None:  # type: ignore[type-arg]
    assert silver["realtime"]["record_id"] == silver["realtime_repeat"]["record_id"]
    assert len({row["record_id"] for row in silver.values()}) == len(silver) - 1


def test_quality_rules_split_good_and_bad_rows(raw: DataFrame) -> None:
    checked = with_failed_rules(rail.silver(raw.drop("_case")), rail.RULES)
    assert passed(checked).count() == 7
    reasons = {r["line"]: sorted(r[FAILED_RULES]) for r in quarantined(checked).collect()}
    assert reasons["PURPLE"] == ["valid_line"]
    assert reasons["RED"] == [
        "event_time_parsed",
        "has_station",
        "next_arrival_parsed",
        "realtime_has_train",
        "waiting_not_negative",
    ]
