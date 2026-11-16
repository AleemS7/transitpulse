"""Job task: after the hourly refresh, check feed freshness, data quality and bus arrivals.

Results are kept in ops.health_checks. Any failed check fails this task, and the job's failure email is the
alert, so no SQL warehouse has to wake up just to evaluate alerts.
"""

import argparse
import logging
import sys
from datetime import UTC, datetime


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-root", required=True, help="folder that contains the transitpulse package")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--silver", required=True)
    parser.add_argument("--ops", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)

    sys.path.insert(0, args.src_root)
    from pyspark.sql import SparkSession

    from transitpulse.common.config import PipelineConfig
    from transitpulse.ops import health

    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s %(levelname)s run={args.run_id} %(message)s")
    log = logging.getLogger("health_checks")
    spark = SparkSession.builder.getOrCreate()
    cfg = PipelineConfig(catalog=args.catalog, silver=args.silver, ops=args.ops)
    now = datetime.now(UTC)

    def ts(epoch: int | None) -> datetime | None:
        return None if epoch is None else datetime.fromtimestamp(epoch, UTC)

    # Times come back as epoch seconds so nothing depends on the session time zone.
    feeds = [
        health.FeedFreshness(r.feed, ts(r.attempt), ts(r.collected), ts(r.source))
        for r in spark.sql(
            "SELECT feed, UNIX_SECONDS(last_attempt_at) AS attempt,"
            " UNIX_SECONDS(last_collected_at) AS collected, UNIX_SECONDS(last_source_ts) AS source"
            " FROM IDENTIFIER(:t)",
            args={"t": cfg.table("ops", "feed_freshness")},
        ).collect()
    ]
    rates = [
        health.RulePassRate(r.dataset, r.rule_name, int(r.passed), int(r.failed))
        for r in spark.sql(
            "SELECT dataset, rule_name, SUM(passed_records) AS passed, SUM(failed_records) AS failed"
            " FROM IDENTIFIER(:t) WHERE event_ts >= current_timestamp() - INTERVAL 24 HOURS"
            " GROUP BY dataset, rule_name",
            args={"t": cfg.table("ops", "dq_metrics")},
        ).collect()
    ]
    latest = spark.sql(
        "SELECT UNIX_SECONDS(MAX(arrival_ts)) AS latest FROM IDENTIFIER(:t)",
        args={"t": cfg.table("silver", "bus_stop_arrivals")},
    ).first()

    checks = [
        *health.collector_checks(feeds, now),
        *health.dq_checks(rates),
        *health.arrival_checks(ts(latest.latest if latest else None), now),
    ]
    for c in checks:
        log.info(
            "%s %s %s: %s (expected %s)",
            "OK  " if c.ok else "FAIL",
            c.name,
            c.subject,
            c.observed,
            c.expected,
        )

    results = spark.createDataFrame(
        [(args.run_id, now, c.name, c.subject, c.ok, c.observed, c.expected) for c in checks],
        "run_id string, checked_at timestamp, check_name string, subject string, ok boolean,"
        " observed string, expected string",
    )
    results.createOrReplaceTempView("new_checks")
    target = cfg.table("ops", "health_checks")
    spark.sql(
        "CREATE TABLE IF NOT EXISTS IDENTIFIER(:t) (run_id STRING, checked_at TIMESTAMP, check_name STRING,"
        " subject STRING, ok BOOLEAN, observed STRING, expected STRING)"
        " COMMENT 'One row per health check per job run.'",
        args={"t": target},
    )
    # Re-running the same job run replaces its rows instead of adding duplicates.
    spark.sql(
        "MERGE INTO IDENTIFIER(:t) AS h USING new_checks AS n"
        " ON h.run_id = n.run_id AND h.check_name = n.check_name AND h.subject = n.subject"
        " WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *",
        args={"t": target},
    )

    failed = health.failures(checks)
    log.info("%d checks, %d failed", len(checks), len(failed))
    if failed:
        summary = "; ".join(f"{c.name} {c.subject}: {c.observed}" for c in failed)
        raise RuntimeError(f"{len(failed)} health check(s) failed: {summary}")


# Serverless runs this file in an IPython shell, where any SystemExit (even 0) fails the task,
# so finish normally and let real errors raise.
if __name__ == "__main__":
    main()
