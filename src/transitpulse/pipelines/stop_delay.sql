-- Bus delay per service day and stop: where on the network buses run late.

CREATE OR REFRESH MATERIALIZED VIEW ${tp_catalog}.${tp_gold}.stop_delay (
    service_date DATE NOT NULL
    COMMENT 'MARTA service day (trips after midnight belong to the day they started).',
    stop_id STRING NOT NULL COMMENT 'GTFS stop ID.',
    stop_name STRING COMMENT 'Stop name.',
    stop_lat DOUBLE COMMENT 'Latitude, WGS84.',
    stop_lon DOUBLE COMMENT 'Longitude, WGS84.',
    arrivals BIGINT COMMENT 'Observed arrivals at the stop.',
    routes BIGINT COMMENT 'Distinct routes observed at the stop.',
    avg_delay_seconds DOUBLE COMMENT 'Mean delay in seconds, rounded.',
    p90_delay_seconds INT COMMENT '90th percentile delay in seconds.',
    late_pct DOUBLE COMMENT 'Share of arrivals more than 5 minutes late, percent.',
    CONSTRAINT stop_delay_pk PRIMARY KEY (service_date, stop_id)
)
COMMENT 'Bus delay by service day and stop: mean and 90th percentile seconds behind schedule.'
AS
SELECT
    f.service_date,
    f.stop_id,
    s.stop_name,
    s.stop_lat,
    s.stop_lon,
    COUNT(*) AS arrivals,
    COUNT(DISTINCT f.route_id) AS routes,
    ROUND(AVG(f.delay_seconds)) AS avg_delay_seconds,
    PERCENTILE_APPROX(f.delay_seconds, 0.9) AS p90_delay_seconds,
    ROUND(TRY_DIVIDE(100 * COUNT_IF(f.arrival_status = 'late'), COUNT(f.arrival_status)), 1) AS late_pct
FROM ${tp_catalog}.${tp_gold}.fact_stop_arrival AS f
LEFT JOIN ${tp_catalog}.${tp_gold}.dim_stop AS s
    ON f.stop_sk = s.stop_sk
GROUP BY f.service_date, f.stop_id, s.stop_name, s.stop_lat, s.stop_lon
