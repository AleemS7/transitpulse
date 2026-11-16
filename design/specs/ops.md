# Ops layer: collector runs, freshness, data quality and health checks (slice S8)

## Outcome
When something goes wrong, the system says which part failed: MARTA, the collector, the pipeline, or the data itself. It also emails, without spending warehouse time on alerting (ADR-0007).

## Outputs
- **`bronze.collector_heartbeats` → `ops.collector_runs`:** one row per collector run and feed, with status, HTTP code, attempts, bytes and MARTA's snapshot time. Quarantine: `ops.collector_runs_quarantine`.
- **`ops.feed_freshness`:** for each realtime feed, the latest attempt, the latest success, MARTA's latest snapshot time, and the last day's runs and failures.
- **`ops.pipeline_events`:** the pipeline's event log, published by the pipeline setting `event_log`.
- **`ops.dq_metrics`:** rows passed and failed per update, dataset and quality rule, read from the event log.
- **`ops.health_checks`:** one row per check per job run, written by the `health_checks` job task with a MERGE on (run_id, check_name, subject).

## Requirements

### R-001: Heartbeats are data (MON-001)
WHEN a heartbeat lands THE SYSTEM SHALL load it once (Auto Loader), explode it to one row per feed, and keep one row per `run_id × feed` (Auto CDC).

Rules:
- `has_run`
- `times_parsed`
- `known_status`: ok, unchanged, failed or deadlettered

### R-002: Freshness split by component (MON-002)
THE SYSTEM SHALL report for each realtime feed:
- `last_attempt_at`: is the collector alive?
- `last_collected_at`: did MARTA answer?
- `last_source_ts`: how fresh was MARTA's own snapshot?

### R-003: Quality rules measured (MON-003)
THE SYSTEM SHALL expose the expectation counts the pipeline records for every rule set in `transitpulse.common.quality`.

### R-004: Health checks after each refresh (MON-004)
WHEN the hourly refresh finishes THE SYSTEM SHALL evaluate these checks:

| Check | Fails when | When it runs |
|---|---|---|
| `collector_running` | no heartbeat for 15 minutes | always |
| `feed_available` | no successful collection of the feed for 15 minutes | service hours, 05:00–24:00 Atlanta |
| `feed_current` | MARTA's snapshot was over 15 minutes old when fetched | service hours, 05:00–24:00 Atlanta |
| `dq_pass_rate` | under 95% of a rule's rows passed in the last 24 hours | always; skipped for rules with fewer than 100 rows |
| `bus_arrivals_flowing` | no observed arrival for 90 minutes | 06:00–22:00 Atlanta |

THE SYSTEM SHALL record every result, then fail the task if any check failed, which sends the job's failure email.

### R-005: Runbooks (MON-005)
Each failure mode has a first step in `design/runbooks/health-checks.md` or `collector-failure.md`.

## Constraints and assumptions
- **Service hours:** the windows are judgement calls. Rail stops overnight and MARTA's rail API returns HTTP 500 between about 1 and 6 AM, so feed checks run only in service hours.
- **When there's no alert:** if the job doesn't run at all (paused, or the quota is exhausted), there's no alert. The collector's own monitoring (Application Insights) still covers collection.
- **Landing retention (MON-006):** not built yet. It deletes data, so it needs approval first.
