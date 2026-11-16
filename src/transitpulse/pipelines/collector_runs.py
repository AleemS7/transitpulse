"""Collector heartbeats: landing JSON -> bronze -> ops.collector_runs, with bad rows quarantined.

Freshness and data-quality summaries are SQL files in this folder (feed_freshness.sql, dq_metrics.sql).
"""

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from transitpulse.common.config import BRONZE_PROPERTIES, PipelineConfig
from transitpulse.common.quality import passed, quarantined, with_failed_rules
from transitpulse.ops import heartbeats

spark = SparkSession.active()
cfg = PipelineConfig.from_conf(spark.conf.get)

BRONZE = cfg.table("bronze", "collector_heartbeats")
RUNS = cfg.table("ops", "collector_runs")


@dp.table(
    name=BRONZE,
    comment="Collector heartbeats as written, one row per collector run.",
    table_properties=BRONZE_PROPERTIES,
)
def collector_heartbeats() -> DataFrame:
    raw = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("rescuedDataColumn", "_rescued_data")
        .schema(heartbeats.RAW_SCHEMA)
        .load(cfg.landing("_heartbeat"))
        .select("*", "_metadata")
    )
    return heartbeats.bronze(raw)


@dp.temporary_view(name="collector_runs_checked")
@dp.expect_all(heartbeats.RULES)
def collector_runs_checked() -> DataFrame:
    return with_failed_rules(heartbeats.collector_runs(spark.readStream.table(BRONZE)), heartbeats.RULES)


@dp.temporary_view(name="collector_runs_passed")
def collector_runs_passed() -> DataFrame:
    return passed(spark.readStream.table("collector_runs_checked"))


dp.create_streaming_table(
    name=RUNS,
    comment="One row per collector run and feed: outcome, HTTP status, attempts, size and source time.",
)
dp.create_auto_cdc_flow(
    target=RUNS,
    source="collector_runs_passed",
    keys=["run_id", "feed"],
    sequence_by="_ingested_at",
    stored_as_scd_type=1,
)


@dp.table(name=cfg.table("ops", "collector_runs_quarantine"), comment="Heartbeat rows that broke a rule.")
def collector_runs_quarantine() -> DataFrame:
    return quarantined(spark.readStream.table("collector_runs_checked"))
