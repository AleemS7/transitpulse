"""Static GTFS schedule: extracted CSVs -> bronze -> silver, one pair of tables per GTFS file.

Files are unpacked by the extract_gtfs job task; each version lives under feed_version=<hash>/.
Versions, dimensions and SCD2 history are SQL files in this folder.
"""

from collections.abc import Callable

from pyspark import pipelines as dp
from pyspark.sql import DataFrame, SparkSession

from transitpulse.common.config import PipelineConfig
from transitpulse.common.quality import Rules
from transitpulse.gtfs import transforms as gtfs

spark = SparkSession.active()
cfg = PipelineConfig.from_conf(spark.conf.get)


def define(table: str, to_silver: Callable[[DataFrame], DataFrame], rules: Rules) -> None:
    bronze_name = cfg.table("bronze", f"gtfs_{table}")

    @dp.table(
        name=bronze_name, comment=f"GTFS {table}.txt as published, all text, one row per line per version."
    )
    def bronze() -> DataFrame:
        raw = (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("pathGlobFilter", f"{table}.txt")
            .load(f"{cfg.gtfs_files_root.rstrip('/')}/{table}/")
            .select("*", "_metadata")
        )
        return gtfs.bronze(raw)

    @dp.table(name=cfg.table("silver", f"gtfs_{table}"), comment=f"GTFS {table}, typed, per feed_version.")
    @dp.expect_all(rules)
    def silver() -> DataFrame:
        return to_silver(spark.readStream.table(bronze_name))


for name, (transform, table_rules) in gtfs.TABLES.items():
    define(name, transform, table_rules)
