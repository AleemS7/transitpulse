"""Rail arrivals: landing files -> bronze -> silver, with bad rows kept in a quarantine table.

Logic lives in transitpulse.rail.transforms; this file only reads, wires and names tables.
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from transitpulse.common.config import PipelineConfig
from transitpulse.common.quality import passed, quarantined, with_failed_rules
from transitpulse.rail import transforms as rail

spark = SparkSession.active()
cfg = PipelineConfig.from_conf(spark.conf.get)

BRONZE = cfg.table("bronze", "rail_arrivals")
SILVER = cfg.table("silver", "rail_arrivals")
QUARANTINE = cfg.table("silver", "rail_arrivals_quarantine")


@dp.table(name=BRONZE, comment="Rail predictions exactly as MARTA sent them, one row per record per file.")
def rail_arrivals_bronze() -> DataFrame:
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("multiLine", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(rail.RAW_SCHEMA)
        .load(cfg.landing("rail_arrivals"))
        .select("*", "_metadata")
    )
    return rail.bronze(raw)


@dp.temporary_view(name="rail_arrivals_checked")
@dp.expect_all(rail.RULES)
def rail_arrivals_checked() -> DataFrame:
    return with_failed_rules(rail.silver(spark.readStream.table(BRONZE)), rail.RULES)


@dp.temporary_view(name="rail_arrivals_passed")
def rail_arrivals_passed() -> DataFrame:
    return passed(spark.readStream.table("rail_arrivals_checked"))


dp.create_streaming_table(
    name=SILVER,
    comment="One row per distinct rail prediction from MARTA. Times are UTC; hour_local is the Atlanta hour.",
)

# A prediction repeats in every snapshot until it changes; keying on its fingerprint keeps one copy.
dp.create_auto_cdc_flow(
    target=SILVER,
    source="rail_arrivals_passed",
    keys=["record_id"],
    sequence_by="_ingested_at",
    stored_as_scd_type=1,
)


@dp.table(name=QUARANTINE, comment="Rail rows that broke a quality rule, with the rules they broke.")
def rail_arrivals_quarantine() -> DataFrame:
    return quarantined(spark.readStream.table("rail_arrivals_checked"))
