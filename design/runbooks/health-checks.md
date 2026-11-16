# Runbook: a health check failed

The hourly `transitpulse-refresh` job emails when any task fails. If the failed task is `health_checks`, the data refreshed but a check didn't pass. The task log lists every check with `OK` or `FAIL`. Results are also kept:

```sql
SELECT checked_at, check_name, subject, observed, expected
FROM workspace.ops.health_checks
WHERE NOT ok
ORDER BY checked_at DESC
LIMIT 20;
```

## Which component, by check

| Check | What it means | First look |
|---|---|---|
| `collector_running` | No heartbeat from the Azure collector for more than 15 minutes. The collector itself is down. | `collector-failure.md` |
| `feed_available` (service hours) | The collector runs, but MARTA hasn't answered for this feed in 15 minutes. | `SELECT finished_at, status, http_status, error FROM workspace.ops.collector_runs WHERE feed = '<feed>' ORDER BY finished_at DESC LIMIT 20`. HTTP 5xx or timeouts mean a MARTA outage: nothing to fix, and the gap is explained by the heartbeats. 401/403 on rail: renew the key (`collector-failure.md`). |
| `feed_current` (service hours) | MARTA answers, but its snapshot was more than 15 minutes old when fetched, so MARTA is serving stale data. | Compare `feed_ts` with `finished_at` in `ops.collector_runs`. Usually MARTA-side; record it and wait. |
| `dq_pass_rate` | A quality rule rejected more than 5% of rows over the last day. | Look at the quarantine table for that dataset: `SELECT EXPLODE(_failed_rules) AS rule, COUNT(*) FROM workspace.silver.<table>_quarantine WHERE _quarantined_at > current_timestamp() - INTERVAL 1 DAY GROUP BY 1`. A new format from MARTA needs a parser fix and a test; a real data problem stays quarantined. |
| `bus_arrivals_flowing` (06:00–22:00 Atlanta) | No observed bus arrival in 90 minutes. | If `feed_available` for `bus_trip_updates` also failed, MARTA is down. Otherwise, check the pipeline's `bus_trip_updates` flow in the latest update (decode errors land in `silver.bus_trips_quarantine` with `_decode_error`). |

## If `refresh_pipeline` failed instead
1. Open the failed update from the job run.
2. The first `ERROR` event names the flow.
3. Fix it on `dev` (`databricks bundle deploy -t dev`, then run the pipeline), then open a PR. The merge redeploys prod and runs the job.

Two known errors:
- **`MEMORY_LIMIT_SERVERLESS`:** check that `max_files_per_batch` is still set for `bus_trip_updates` (`design/specs/bus.md`).
- **Failure on `bus_trip_updates` after a change to the 25-file cap:** a stream replays an already-planned batch, so a new cap only applies after that bronze table is fully refreshed.

## Verify
The next hourly run shows every check `OK` in the task log, and no new `NOT ok` rows appear in `ops.health_checks`.
