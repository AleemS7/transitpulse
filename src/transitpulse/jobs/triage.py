"""Job task: after the health checks, passed or not, propose an incident for anything that failed.

Runs with run_if ALL_DONE, so it also covers a failed pipeline refresh, where the checks never ran. When this
run needs triage, the task fails with the incident's summary, so the job's failure email carries it. When
every check passes, open incidents are marked cleared.
"""

import argparse
import logging
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_CLIENT_ID = re.compile(r"^[0-9a-f-]{36}$")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-root", required=True, help="folder that contains the transitpulse package")
    parser.add_argument("--runbooks", required=True, help="folder with the runbook markdown files")
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--gold", required=True)
    parser.add_argument("--ops", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--model", default="", help="serving endpoint; empty means rules only")
    parser.add_argument("--app-name", default="", help="app whose service principal may acknowledge")
    args = parser.parse_args(argv)

    sys.path.insert(0, args.src_root)
    from pyspark.sql import SparkSession

    from transitpulse.agent import functions, record
    from transitpulse.agent.loop import ServingChat
    from transitpulse.agent.tools import Runbooks, SqlTools
    from transitpulse.agent.triage import triage
    from transitpulse.agent.trigger import FailedCheck, Trigger
    from transitpulse.common.config import PipelineConfig

    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s %(levelname)s run={args.run_id} %(message)s")
    log = logging.getLogger("triage")
    spark = SparkSession.builder.getOrCreate()
    cfg = PipelineConfig(catalog=args.catalog, gold=args.gold, ops=args.ops)
    incidents = cfg.table("ops", "incidents")
    now = datetime.now(UTC)

    for statement in functions.statements(cfg):
        spark.sql(statement)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS IDENTIFIER(:t) ({record.COLUMNS})"
        " COMMENT 'Incidents proposed by the triage step; a person acknowledges them through the API.'",
        args={"t": incidents},
    )
    if args.app_name:
        _grant_app(spark, args.app_name, cfg.catalog, cfg.ops, incidents, log)

    checks = spark.sql(
        "SELECT check_name, subject, ok, observed, expected FROM IDENTIFIER(:t) WHERE run_id = :run",
        args={"t": cfg.table("ops", "health_checks"), "run": args.run_id},
    ).collect()
    trigger = Trigger(
        run_id=args.run_id,
        failed=tuple(
            FailedCheck(r.check_name, r.subject, r.observed, r.expected) for r in checks if not r.ok
        ),
        upstream_failed=not checks,
    )
    log.info(
        "%d checks for this run, %d failed, upstream_failed=%s",
        len(checks),
        len(trigger.failed),
        trigger.upstream_failed,
    )

    if not trigger.needs_triage:
        spark.sql(
            "UPDATE IDENTIFIER(:t) SET cleared_at = current_timestamp(), _updated_at = current_timestamp()"
            " WHERE cleared_at IS NULL",
            args={"t": incidents},
        )
        log.info("all checks passed; open incidents marked cleared")
        return

    fingerprint = trigger.fingerprint()
    existing = spark.sql(
        "SELECT * FROM IDENTIFIER(:t) WHERE fingerprint = :fp AND cleared_at IS NULL"
        " ORDER BY opened_at DESC LIMIT 1",
        args={"t": incidents, "fp": fingerprint},
    ).first()
    if existing is not None:
        # Same problem as an open incident: count it, and don't spend model tokens on it again. A retry of
        # the same job run matches last_run_id and counts nothing.
        spark.sql(
            "UPDATE IDENTIFIER(:t) SET last_seen_at = current_timestamp(), occurrences = occurrences + 1,"
            " last_run_id = :run, _updated_at = current_timestamp()"
            " WHERE incident_id = :id AND last_run_id <> :run",
            args={"t": incidents, "run": args.run_id, "id": existing.incident_id},
        )
        raise RuntimeError(record.alert(existing.asDict(), new=existing.run_id == args.run_id))

    def query(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        return [r.asDict(recursive=True) for r in spark.sql(sql, args=params).collect()]

    data = SqlTools(query, f"{cfg.catalog}.{cfg.ops}")
    model = ServingChat(args.model) if args.model else None
    result = triage(trigger, data, Runbooks.load(Path(args.runbooks)), model)
    if result.agent is not None:
        log.info(
            "agent: steps=%d tokens=%d rejections=%d error=%s",
            result.agent.steps,
            result.agent.prompt_tokens + result.agent.completion_tokens,
            len(result.agent.rejections),
            result.agent.error,
        )
    log.info(
        "classifier=%s cause=%s rules_cause=%s",
        result.classifier,
        result.incident.cause_category,
        result.rules_incident.cause_category,
    )

    values = record.row(trigger, result, args.model or None, now)
    spark.createDataFrame([values], record.COLUMNS).createOrReplaceTempView("new_incident")
    spark.sql(
        "MERGE INTO IDENTIFIER(:t) AS i USING new_incident AS n ON i.incident_id = n.incident_id"
        " WHEN NOT MATCHED THEN INSERT *",
        args={"t": incidents},
    )
    raise RuntimeError(record.alert(values, new=True))


def _grant_app(spark: Any, app_name: str, catalog: str, ops: str, table: str, log: logging.Logger) -> None:
    """The API acknowledges incidents as the app's service principal, which only needs this one table."""
    from databricks.sdk import WorkspaceClient

    client_id = WorkspaceClient().apps.get(app_name).service_principal_client_id or ""
    # A principal can't be a bound parameter, so the ID is checked against its UUID shape first.
    if not _CLIENT_ID.match(client_id):
        raise RuntimeError(f"app {app_name!r} has no usable service principal")
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{ops} TO `{client_id}`")
    spark.sql(f"GRANT SELECT, MODIFY ON TABLE {table} TO `{client_id}`")
    log.info("granted %s read and acknowledge on %s", app_name, table)


# Serverless runs this file in an IPython shell, where any SystemExit (even 0) fails the task,
# so finish normally and let real errors raise.
if __name__ == "__main__":
    main()
