-- Rail reliability per service day, line, direction, station and Atlanta hour.
-- delay_seconds is MARTA's DELAY field (seconds behind schedule; negative = early). See ADR-0002.

CREATE OR REFRESH MATERIALIZED VIEW ${tp_catalog}.${tp_gold}.rail_line_health
COMMENT 'Rail reliability by service day, line, direction, station and Atlanta hour.'
AS
WITH predictions AS (
    SELECT
        service_date,
        line,
        direction,
        station,
        train_id,
        is_realtime,
        waiting_seconds,
        delay_seconds,
        hour_local
    FROM ${tp_catalog}.${tp_silver}.rail_arrivals
)

SELECT
    service_date,
    line,
    direction,
    station,
    hour_local,
    COUNT(*) AS predictions,
    COUNT_IF(is_realtime) AS realtime_predictions,
    COUNT(DISTINCT train_id) AS trains_seen,
    ROUND(AVG(delay_seconds)) AS avg_delay_seconds,
    PERCENTILE_APPROX(delay_seconds, 0.9) AS p90_delay_seconds,
    MAX(waiting_seconds) AS longest_wait_seconds
FROM predictions
GROUP BY service_date, line, direction, station, hour_local
