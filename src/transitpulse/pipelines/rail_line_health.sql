-- Rail reliability per service day, line, direction, station and Atlanta hour.
-- delay_seconds is MARTA's DELAY field (seconds behind schedule; negative = early). See ADR-0002.

CREATE OR REFRESH MATERIALIZED VIEW ${tp_catalog}.${tp_gold}.rail_line_health (
    service_date DATE NOT NULL
    COMMENT 'MARTA service day (trips after midnight belong to the day they started).',
    line STRING NOT NULL COMMENT 'Rail line: RED, GOLD, BLUE or GREEN.',
    direction STRING NOT NULL COMMENT 'Direction of travel: N, S, E or W.',
    station STRING NOT NULL COMMENT 'Station name as MARTA sends it.',
    hour_local INT NOT NULL COMMENT 'Hour of day in Atlanta time, 0-23.',
    predictions BIGINT COMMENT 'Arrival predictions collected.',
    realtime_predictions BIGINT COMMENT 'Predictions based on a tracked train rather than the timetable.',
    trains_seen BIGINT COMMENT 'Distinct trains predicted.',
    avg_delay_seconds DOUBLE COMMENT 'Mean MARTA DELAY in seconds, rounded; negative means early (ADR-0002).',
    p90_delay_seconds INT COMMENT '90th percentile delay in seconds.',
    longest_wait_seconds INT COMMENT 'Longest predicted wait for a train, seconds.',
    CONSTRAINT rail_line_health_pk PRIMARY KEY (service_date, line, direction, station, hour_local)
)
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
