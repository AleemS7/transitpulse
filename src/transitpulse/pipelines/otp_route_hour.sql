-- Bus on-time performance per service day, route and Atlanta hour (window in ADR-0006).
-- on_time_pct counts only arrivals that have a scheduled time to compare with.

CREATE OR REFRESH MATERIALIZED VIEW ${tp_catalog}.${tp_gold}.otp_route_hour (
    service_date DATE NOT NULL
    COMMENT 'MARTA service day (trips after midnight belong to the day they started).',
    route_id STRING NOT NULL COMMENT 'GTFS route ID.',
    route_short_name STRING COMMENT 'Route number riders see.',
    hour_local INT NOT NULL COMMENT 'Hour of day in Atlanta time, 0-23.',
    arrivals BIGINT COMMENT 'Observed arrivals.',
    on_time BIGINT COMMENT 'Arrivals from 1 minute early to 5 minutes late.',
    early BIGINT COMMENT 'Arrivals more than 1 minute early.',
    late BIGINT COMMENT 'Arrivals more than 5 minutes late.',
    on_time_pct DOUBLE COMMENT 'on_time as a percentage of arrivals with a scheduled time.',
    avg_delay_seconds DOUBLE COMMENT 'Mean delay in seconds, rounded.',
    p90_delay_seconds INT COMMENT '90th percentile delay in seconds.',
    CONSTRAINT otp_route_hour_pk PRIMARY KEY (service_date, route_id, hour_local)
)
COMMENT 'Bus on-time performance by service day, route and Atlanta hour. On time = -1 to +5 minutes.'
AS
SELECT
    f.service_date,
    f.route_id,
    r.route_short_name,
    f.hour_local,
    COUNT(*) AS arrivals,
    COUNT_IF(f.arrival_status = 'on_time') AS on_time,
    COUNT_IF(f.arrival_status = 'early') AS early,
    COUNT_IF(f.arrival_status = 'late') AS late,
    ROUND(TRY_DIVIDE(100 * COUNT_IF(f.arrival_status = 'on_time'), COUNT(f.arrival_status)), 1)
        AS on_time_pct,
    ROUND(AVG(f.delay_seconds)) AS avg_delay_seconds,
    PERCENTILE_APPROX(f.delay_seconds, 0.9) AS p90_delay_seconds
FROM ${tp_catalog}.${tp_gold}.fact_stop_arrival AS f
LEFT JOIN ${tp_catalog}.${tp_gold}.dim_route AS r
    ON f.route_sk = r.route_sk
GROUP BY f.service_date, f.route_id, r.route_short_name, f.hour_local
