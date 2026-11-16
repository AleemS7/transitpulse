-- Read-only tool functions for the triage step (transitpulse.agent.tools). Callers pass at most an hour
-- window and never write SQL. The triage job re-creates these on every run, so this file is the source.

CREATE OR REPLACE FUNCTION ${tp_catalog}.${tp_ops}.get_check_history (hours INT)
RETURNS TABLE (
    check_name STRING,
    subject STRING,
    failed_runs BIGINT,
    runs BIGINT,
    first_failed_at TIMESTAMP,
    last_failed_at TIMESTAMP,
    latest_observed STRING
)
COMMENT 'Health checks that failed in the last N hours, with how often and the latest observed value.'
RETURN
SELECT
    check_name,
    subject,
    COUNT_IF(NOT ok) AS failed_runs,
    COUNT(*) AS runs,
    MIN(CASE WHEN NOT ok THEN checked_at END) AS first_failed_at,
    MAX(CASE WHEN NOT ok THEN checked_at END) AS last_failed_at,
    MAX_BY(observed, checked_at) AS latest_observed
FROM ${tp_catalog}.${tp_ops}.health_checks
WHERE checked_at >= CURRENT_TIMESTAMP() - MAKE_DT_INTERVAL(0, hours)
GROUP BY check_name, subject
HAVING COUNT_IF(NOT ok) > 0;

CREATE OR REPLACE FUNCTION ${tp_catalog}.${tp_ops}.get_feed_freshness ()
RETURNS TABLE (
    feed STRING,
    minutes_since_attempt BIGINT,
    minutes_since_success BIGINT,
    snapshot_age_minutes BIGINT,
    runs_last_day BIGINT,
    failures_last_day BIGINT
)
COMMENT 'Per realtime feed: minutes since the last attempt and success, and how old the source snapshot was.'
RETURN
SELECT
    feed,
    TIMESTAMPDIFF(MINUTE, last_attempt_at, CURRENT_TIMESTAMP()) AS minutes_since_attempt,
    TIMESTAMPDIFF(MINUTE, last_collected_at, CURRENT_TIMESTAMP()) AS minutes_since_success,
    TIMESTAMPDIFF(MINUTE, last_source_ts, last_collected_at) AS snapshot_age_minutes,
    runs_last_day,
    failures_last_day
FROM ${tp_catalog}.${tp_ops}.feed_freshness;

CREATE OR REPLACE FUNCTION ${tp_catalog}.${tp_ops}.get_collector_runs (hours INT)
RETURNS TABLE (
    feed STRING,
    status STRING,
    http_status INT,
    runs BIGINT,
    last_finished_at TIMESTAMP,
    latest_error STRING
)
COMMENT 'Collector outcomes per realtime feed, status and HTTP status in the last N hours.'
RETURN
SELECT
    feed,
    status,
    http_status,
    COUNT(*) AS runs,
    MAX(finished_at) AS last_finished_at,
    MAX_BY(error, finished_at) AS latest_error
FROM ${tp_catalog}.${tp_ops}.collector_runs
WHERE kind = 'realtime' AND finished_at >= CURRENT_TIMESTAMP() - MAKE_DT_INTERVAL(0, hours)
GROUP BY feed, status, http_status;

CREATE OR REPLACE FUNCTION ${tp_catalog}.${tp_ops}.get_pipeline_updates (hours INT)
RETURNS TABLE (
    update_id STRING,
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    final_state STRING,
    failed_flow STRING,
    error_code STRING,
    error_message STRING
)
COMMENT 'Pipeline updates in the last N hours: final state, and the first failing flow and error.'
RETURN
WITH events AS (
    SELECT
        ev.`timestamp` AS event_ts,
        ev.level,
        ev.origin.update_id AS update_id,  -- noqa: AL09, RF01
        ev.origin.flow_name AS flow_name,  -- noqa: AL09, RF01
        GET_JSON_OBJECT(ev.details, '$.update_progress.state') AS update_state,
        ev.error.exceptions[0].message AS error_message  -- noqa: RF01
    FROM ${tp_catalog}.${tp_ops}.pipeline_events AS ev
    WHERE
        ev.`timestamp` >= CURRENT_TIMESTAMP() - MAKE_DT_INTERVAL(0, hours)  -- noqa: RF03
        AND ev.event_type IN ('update_progress', 'flow_progress')
),

updates AS (
    SELECT
        update_id,
        MIN(event_ts) AS started_at,
        MAX(event_ts) AS ended_at,
        MAX_BY(update_state, CASE WHEN update_state IS NOT NULL THEN event_ts END) AS final_state,
        MIN_BY(flow_name, CASE WHEN level = 'ERROR' AND flow_name IS NOT NULL THEN event_ts END)
            AS failed_flow,
        MIN_BY(error_message, CASE WHEN level = 'ERROR' AND error_message IS NOT NULL THEN event_ts END)
            AS error_message
    FROM events
    GROUP BY update_id
)

SELECT
    update_id,
    started_at,
    ended_at,
    final_state,
    failed_flow,
    -- The innermost Spark error class, e.g. UDF_PYSPARK_USER_CODE_ERROR.MEMORY_LIMIT_SERVERLESS.
    ELEMENT_AT(REGEXP_EXTRACT_ALL(error_message, '\\[([A-Z_]+(\\.[A-Z_]+)*)\\]', 1), -1) AS error_code,
    LEFT(error_message, 300) AS error_message
FROM updates;

CREATE OR REPLACE FUNCTION ${tp_catalog}.${tp_ops}.get_dq_failures (hours INT)
RETURNS TABLE (
    dataset STRING,
    rule_name STRING,
    passed_records BIGINT,
    failed_records BIGINT,
    pass_pct DOUBLE
)
COMMENT 'Quality rules that rejected rows in the last N hours, worst first.'
RETURN
SELECT
    dataset,
    rule_name,
    SUM(passed_records) AS passed_records,
    SUM(failed_records) AS failed_records,
    ROUND(100 * SUM(passed_records) / NULLIF(SUM(passed_records) + SUM(failed_records), 0), 1) AS pass_pct
FROM ${tp_catalog}.${tp_ops}.dq_metrics
WHERE event_ts >= CURRENT_TIMESTAMP() - MAKE_DT_INTERVAL(0, hours)
GROUP BY dataset, rule_name
HAVING SUM(failed_records) > 0;

CREATE OR REPLACE FUNCTION ${tp_catalog}.${tp_ops}.get_bus_service (hours INT)
RETURNS TABLE (
    hour_start_utc TIMESTAMP,
    arrivals BIGINT,
    on_time_pct DOUBLE,
    avg_delay_seconds DOUBLE,
    arrivals_week_before BIGINT
)
COMMENT 'Observed bus arrivals per hour in the last N hours, next to the same hour a week earlier.'
RETURN
WITH recent AS (
    SELECT
        DATE_TRUNC('HOUR', arrival_ts) AS hour_start,
        COUNT(*) AS arrivals,
        ROUND(100 * COUNT_IF(arrival_status = 'on_time') / NULLIF(COUNT(arrival_status), 0), 1)
            AS on_time_pct,
        ROUND(AVG(delay_seconds)) AS avg_delay_seconds
    FROM ${tp_catalog}.${tp_gold}.fact_stop_arrival
    WHERE arrival_ts >= CURRENT_TIMESTAMP() - MAKE_DT_INTERVAL(0, hours)
    GROUP BY DATE_TRUNC('HOUR', arrival_ts)
),

week_before AS (
    SELECT
        DATE_TRUNC('HOUR', arrival_ts) + INTERVAL 7 DAYS AS hour_start,
        COUNT(*) AS arrivals
    FROM ${tp_catalog}.${tp_gold}.fact_stop_arrival
    WHERE
        arrival_ts >= CURRENT_TIMESTAMP() - MAKE_DT_INTERVAL(7, hours)
        AND arrival_ts < CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
    GROUP BY DATE_TRUNC('HOUR', arrival_ts) + INTERVAL 7 DAYS
)

SELECT
    COALESCE(r.hour_start, w.hour_start) AS hour_start_utc,
    COALESCE(r.arrivals, 0) AS arrivals,
    r.on_time_pct,
    r.avg_delay_seconds,
    w.arrivals AS arrivals_week_before
FROM recent AS r
FULL OUTER JOIN week_before AS w ON r.hour_start = w.hour_start;
