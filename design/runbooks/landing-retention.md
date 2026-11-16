# Runbook: landing retention

The daily `transitpulse-landing-retention` job (10:45 UTC) deletes landing day folders older than 30 days once every file is in bronze (ADR-0009). Each run writes one row per day folder to `ops.landing_retention`.

## What did the last run decide?
```sql
SELECT feed, day, files, ROUND(size_bytes / 1e6, 1) AS mb, action, deleted, reason
FROM workspace.ops.landing_retention
WHERE run_id = (SELECT MAX_BY(run_id, checked_at) FROM workspace.ops.landing_retention)
  AND (action = 'delete' OR not_loaded > 0 OR failed > 0)
ORDER BY day, feed;
```

## A day folder stays past 30 days
| Reason | Meaning | What to do |
|---|---|---|
| `N file(s) not in bronze yet` | Bronze doesn't hold those files, and Auto Loader hasn't moved past that day. | Check the pipeline's latest update for that feed. If the pipeline is healthy, look for the file in `cloud_files_state(TABLE(workspace.bronze.<table>))`. Never delete such a folder by hand. |
| `N file(s) failed to decode; kept for a reload` | Bus bronze holds only the error for those files. | Follow the `dq_pass_rate` row in `health-checks.md`. Once the decoder is fixed and the files are reloaded, the folder can go. |

## Turning deletion on or off
- **Mode:** set by the `retention_mode` variable. It defaults to `report`, which only records. To switch deletion on, set `retention_mode: delete` under the prod target in `databricks.yml`, in a PR. Dev must stay `report`, because it reads the same landing folder as prod.
- **To pause deletion:** set prod back to `report` in a PR. The merge redeploys.

## Bronze can't be fully refreshed
The realtime bronze tables (`bus_trip_updates`, `bus_vehicle_positions`, `rail_arrivals`, `collector_heartbeats`) have `pipelines.reset.allowed = false`. A refresh that selects one of them fails with "is not resettable", and a pipeline-wide full refresh skips them ✱. Landing only holds the last 30 days, so a reset would lose older history. Don't remove the property to force a reset; fix forward on dev.

## Verify
The latest run has `deleted = true` on the days it chose. `LIST '/Volumes/workspace/bronze/landing/<feed>/'` no longer shows those `dt=` folders. The next hourly refresh still passes every health check.
