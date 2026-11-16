"""Job: delete landing day folders that are older than the retention window and fully loaded into bronze.

Every run records one row per day folder in ops.landing_retention, so the volume's size over time and every
keep/delete decision can be queried. With --mode report (the default) nothing is deleted.
"""

import argparse
import logging
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, date, datetime

# Only bus bronze records decode failures; rail and heartbeat bronze keep unparsed text in _rescued_data.
FAILED_SQL = {
    "bus_trip_updates": "_decode_error IS NOT NULL",
    "bus_vehicle_positions": "_decode_error IS NOT NULL",
}
COLUMNS = (
    "run_id STRING, checked_at TIMESTAMP, mode STRING, feed STRING, day DATE, files INT, size_bytes BIGINT,"
    " not_loaded INT, failed INT, action STRING, deleted BOOLEAN, reason STRING"
)
DAY_SQL = r"regexp_extract({col}, '/dt=([0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}})/', 1)"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-root", required=True, help="folder that contains the transitpulse package")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--bronze", required=True)
    parser.add_argument("--ops", required=True)
    parser.add_argument("--landing-root", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--mode", choices=["report", "delete"], default="report")
    parser.add_argument("--keep-days", type=int, default=30)
    parser.add_argument("--max-days", type=int, default=3, help="oldest days deleted per run")
    args = parser.parse_args(argv)

    sys.path.insert(0, args.src_root)
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.errors import NotFound
    from pyspark.sql import SparkSession

    from transitpulse.common.config import PipelineConfig
    from transitpulse.ops import retention

    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s %(levelname)s run={args.run_id} %(message)s")
    log = logging.getLogger("landing_retention")
    if args.keep_days < 7:
        raise ValueError(f"--keep-days {args.keep_days} is below the 7-day floor")
    spark = SparkSession.builder.getOrCreate()
    files = WorkspaceClient().files
    cfg = PipelineConfig(catalog=args.catalog, bronze=args.bronze, ops=args.ops)
    now = datetime.now(UTC)
    root = args.landing_root.rstrip("/")

    def list_folders(feed: str) -> list[retention.DayFolder]:
        found: list[retention.DayFolder] = []
        try:
            entries = list(files.list_directory_contents(f"{root}/{feed}"))
        except NotFound:
            return found
        for entry in entries:
            day = retention.folder_day(entry.name or "")
            if not entry.is_directory or day is None:
                continue
            listed = [f for f in files.list_directory_contents(entry.path or "") if not f.is_directory]
            found.append(
                retention.DayFolder(
                    feed,
                    day,
                    frozenset(f"{feed}/{entry.name}/{f.name}" for f in listed),
                    sum(f.file_size or 0 for f in listed),
                )
            )
        return found

    def keys(paths: list[str], feed: str) -> set[str]:
        return {k for p in paths if (k := retention.file_key(p, feed))}

    def loaded(feed: str, table: str, oldest: date) -> retention.Loaded:
        failed_sql = FAILED_SQL.get(feed, "false")
        rows = spark.sql(
            f"SELECT _source_file AS path, MAX({failed_sql}) AS failed FROM IDENTIFIER(:t)"
            f" WHERE {DAY_SQL.format(col='_source_file')} >= :lo GROUP BY _source_file",
            args={"t": table, "lo": oldest.isoformat()},
        ).collect()
        seen = spark.sql(
            "SELECT path FROM cloud_files_state(TABLE(IDENTIFIER(:t)))"
            f" WHERE {DAY_SQL.format(col='path')} >= :lo",
            args={"t": table, "lo": oldest.isoformat()},
        ).collect()
        with_rows = keys([r.path for r in rows], feed)
        days = [d for k in with_rows if (d := retention.folder_day(k.split("/")[1]))]
        return retention.Loaded(
            with_rows=with_rows,
            seen=keys([r.path for r in seen], feed),
            failed=keys([r.path for r in rows if r.failed], feed),
            newest_day=max(days, default=None),
        )

    decisions: list[retention.Decision] = []
    folders: dict[tuple[str, date], retention.DayFolder] = {}
    for feed, bronze_name in retention.FEEDS.items():
        listed = list_folders(feed)
        if not listed:
            log.info("%s: no day folders", feed)
            continue
        state = loaded(feed, cfg.table("bronze", bronze_name), min(f.day for f in listed))
        for folder in listed:
            folders[(feed, folder.day)] = folder
            decisions.append(retention.decide(folder, state, now.date(), args.keep_days))
        log.info(
            "%s: %d day folders, %d files, %.1f MB, %d deletable",
            feed,
            len(listed),
            sum(len(f.files) for f in listed),
            sum(f.size_bytes for f in listed) / 1e6,
            sum(d.action == "delete" and d.feed == feed for d in decisions),
        )

    chosen = retention.to_delete(decisions, args.max_days)
    for d in decisions:
        if d.action == "delete" or d.not_loaded or d.failed:
            log.info("%s %s %s: %s", d.action.upper(), d.feed, d.day, d.reason)

    target = cfg.table("ops", "landing_retention")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS IDENTIFIER(:t) ({COLUMNS})"
        " COMMENT 'One row per landing day folder per retention run: its size and what was done with it.'",
        args={"t": target},
    )

    deleted: set[tuple[str, date]] = set()
    if args.mode == "delete":
        for d in chosen:
            folder = folders[(d.feed, d.day)]
            path = f"{root}/{d.feed}/dt={d.day.isoformat()}"
            # A dead-letter replay can write into an old folder after it was checked; such a folder waits.
            now_there = {
                f"{d.feed}/dt={d.day.isoformat()}/{f.name}" for f in files.list_directory_contents(path)
            }
            if now_there != folder.files:
                log.warning("%s changed since it was checked; left for the next run", path)
                continue
            with ThreadPoolExecutor(max_workers=8) as pool:
                list(pool.map(lambda k: files.delete(f"{root}/{k}"), sorted(folder.files)))
            files.delete_directory(path)
            deleted.add((d.feed, d.day))
            log.info("deleted %s (%d files, %.1f MB)", path, d.files, d.size_bytes / 1e6)

    report = spark.createDataFrame(
        [
            (
                args.run_id,
                now,
                args.mode,
                d.feed,
                d.day,
                d.files,
                d.size_bytes,
                d.not_loaded,
                d.failed,
                d.action,
                (d.feed, d.day) in deleted,
                d.reason,
            )
            for d in decisions
        ],
        COLUMNS,
    )
    report.createOrReplaceTempView("retention_run")
    spark.sql(
        "MERGE INTO IDENTIFIER(:t) AS r USING retention_run AS n"
        " ON r.run_id = n.run_id AND r.feed = n.feed AND r.day = n.day"
        " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
        args={"t": target},
    )
    log.info(
        "%d day folders checked, %d deletable, %d chosen, %d deleted (mode=%s)",
        len(decisions),
        sum(d.action == "delete" for d in decisions),
        len(chosen),
        len(deleted),
        args.mode,
    )


# Serverless runs this file in an IPython shell, where any SystemExit (even 0) fails the task,
# so finish normally and let real errors raise.
if __name__ == "__main__":
    main()
