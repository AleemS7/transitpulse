# ADR-0007: Alert from a health-check task in the hourly job, not from SQL alerts

- **Status:** Accepted
- **Date:** 2026-11-15
- **Related:** MON-002, MON-003, MON-004, `design/specs/ops.md`, `src/transitpulse/ops/health.py`

## Context
The ops layer needs to raise an alarm when:
- a feed goes stale,
- data quality drops, or
- bus arrivals stop flowing.

Databricks SQL alerts evaluate a query on a schedule using the SQL warehouse. Free Edition has one 2X-Small warehouse and a daily compute quota. If the quota runs out, compute stops for the rest of the day, so every warehouse wake-up has a cost.

## Options considered
1. **SQL alerts, one per condition, hourly.** Native and visible in the UI. But each check wakes the warehouse, which then idles until auto-stop: up to 24 starts a day just for alerting, from the same quota the pipeline needs.
2. **A health-check task at the end of the hourly job.** The job is already running on serverless compute. The task reads the ops tables, records every result in `ops.health_checks`, and fails if any check fails. The job's failure email is the alert.
3. **Alerts from Azure Monitor on the collector.** That only sees the collector, not MARTA's data or the pipeline.

## Decision
Option 2.
- **Where the rules live:** the thresholds are plain Python in `transitpulse.ops.health`, with unit tests.
- **Where the inputs live:** the inputs are ops tables built in SQL (`ops.feed_freshness`, `ops.dq_metrics`).
- **Who gets emailed:** the job emails the workspace user on any task failure, so a failed refresh and a failed check both alert. The address comes from the workspace (`${workspace.current_user.userName}`), so none is committed.

## Consequences
- **No extra compute:** alerts cost no warehouse time and run right after fresh data lands.
- **No alert without the job:** if the job isn't running at all (paused, or quota exhausted), there's no alert. Azure's budget alert and the collector's Application Insights still cover the collector.
- **History:** `ops.health_checks` records every check, so a later triage step can read it.
- **One alert for several problems:** a failed check marks the hourly run as failed even when the data refreshed. That's intended: the run is unhealthy.

## How I'd explain it in an interview
"I had one small warehouse and a daily quota, so I didn't want 24 warehouse wake-ups a day just to evaluate alerts. The checks run as the last task of the hourly job, and a failed check fails the job, which emails me. The thresholds are tested Python, and every result is stored, so I have history as well as alerts."
