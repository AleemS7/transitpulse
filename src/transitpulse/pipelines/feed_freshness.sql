-- Where each realtime feed stands, from the collector's heartbeats:
-- last_attempt_at (collector alive), last_collected_at (MARTA answered) and last_source_ts (MARTA's own
-- snapshot time). The health checks compare these with the clock; see transitpulse.ops.health.

CREATE OR REFRESH MATERIALIZED VIEW ${tp_catalog}.${tp_ops}.feed_freshness
COMMENT 'Latest collector attempt, success and source timestamp per realtime feed, plus last-day failures.'
AS
WITH runs AS (
    SELECT
        feed,
        status,
        finished_at,
        feed_ts
    FROM ${tp_catalog}.${tp_ops}.collector_runs
    WHERE kind = 'realtime'
),

latest AS (
    SELECT MAX(finished_at) AS latest_run_at
    FROM runs
)

SELECT
    r.feed,
    MAX(r.finished_at) AS last_attempt_at,
    MAX(CASE WHEN r.status IN ('ok', 'unchanged') THEN r.finished_at END) AS last_collected_at,
    MAX(r.feed_ts) AS last_source_ts,
    COUNT_IF(r.finished_at >= l.latest_run_at - INTERVAL 24 HOURS) AS runs_last_day,
    COUNT_IF(
        r.finished_at >= l.latest_run_at - INTERVAL 24 HOURS AND r.status IN ('failed', 'deadlettered')
    ) AS failures_last_day
FROM runs AS r
CROSS JOIN latest AS l
GROUP BY r.feed
