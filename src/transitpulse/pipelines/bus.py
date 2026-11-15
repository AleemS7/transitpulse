"""Bus realtime: landed protobuf snapshots -> bronze -> silver, with bad rows kept in quarantine tables.

Logic lives in transitpulse.bus; this file only reads, wires and names tables.
"""

from collections.abc import Callable

from pyspark import pipelines as dp
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql.types import StructType

from transitpulse.bus import transforms as bus
from transitpulse.common.config import PipelineConfig
from transitpulse.common.quality import Rules, passed, quarantined, with_failed_rules

spark = SparkSession.active()
cfg = PipelineConfig.from_conf(spark.conf.get)


def landed_files(feed: str) -> DataFrame:
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")
        .option("pathGlobFilter", "*.pb.gz")
    )
    # Only consulted on a table's first run; afterwards the stream's checkpoint decides what is new.
    if cfg.realtime_start:
        reader = reader.option("modifiedAfter", cfg.realtime_start)
    return reader.load(cfg.landing(feed)).select("content", "_metadata")


def define_bronze(feed: str, decoder: Column, entity: StructType, comment: str) -> str:
    name = cfg.table("bronze", feed)

    @dp.table(name=name, comment=comment)
    def bronze() -> DataFrame:
        return bus.bronze(landed_files(feed), decoder, entity)

    return name


def define_silver(
    name: str,
    source: Callable[[], DataFrame],
    rules: Rules,
    keys: list[str],
    sequence_by: str,
    comment: str,
) -> None:
    checked = f"{name}_checked"

    @dp.temporary_view(name=checked)
    @dp.expect_all(rules)
    def check() -> DataFrame:
        return with_failed_rules(source(), rules)

    @dp.temporary_view(name=f"{name}_passed")
    def keep() -> DataFrame:
        # Only failed rows can carry a decode error, so silver doesn't need the column.
        return passed(spark.readStream.table(checked)).drop("_decode_error")

    target = cfg.table("silver", name)
    dp.create_streaming_table(name=target, comment=comment)
    dp.create_auto_cdc_flow(
        target=target,
        source=f"{name}_passed",
        keys=keys,
        sequence_by=sequence_by,
        stored_as_scd_type=1,
    )

    @dp.table(name=cfg.table("silver", f"{name}_quarantine"), comment=f"{name} rows that broke a rule.")
    def quarantine() -> DataFrame:
        return quarantined(spark.readStream.table(checked))


VP_BRONZE = define_bronze(
    "bus_vehicle_positions",
    bus.decode_vehicle_positions("content"),
    bus.VEHICLE,
    "Bus positions as MARTA sent them, one row per vehicle per snapshot.",
)
TU_BRONZE = define_bronze(
    "bus_trip_updates",
    bus.decode_trip_updates("content"),
    bus.TRIP_UPDATE,
    "Bus trip updates as MARTA sent them, one row per trip per snapshot, stop updates kept as a list.",
)

define_silver(
    "bus_vehicle_positions",
    lambda: bus.vehicle_positions(spark.readStream.table(VP_BRONZE)),
    bus.VEHICLE_RULES,
    keys=["vehicle_id", "position_ts"],
    sequence_by="_ingested_at",
    comment="One row per bus position report. Times are UTC; hour_local is the Atlanta hour.",
)
define_silver(
    "bus_trips",
    lambda: bus.trips(spark.readStream.table(TU_BRONZE)),
    bus.TRIP_RULES,
    keys=["trip_id", "service_date"],
    sequence_by="feed_ts",
    comment="One row per bus trip and service day with MARTA's latest status for it (e.g. CANCELED).",
)
define_silver(
    "bus_stop_arrivals",
    lambda: bus.observed_arrivals(spark.readStream.table(TU_BRONZE)),
    bus.ARRIVAL_RULES,
    keys=["trip_id", "service_date", "stop_sequence"],
    sequence_by="reported_at",
    comment="Observed bus arrivals: the latest time MARTA reported for a stop after the bus had passed it.",
)
