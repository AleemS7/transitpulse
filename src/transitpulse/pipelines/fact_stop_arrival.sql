-- Bus arrival fact: one row per observed arrival (one trip reaching one stop on one service day).
-- Stop and direction come from the timetable in effect that day, because realtime stop IDs don't always
-- match it. On time = at most 1 minute early and at most 5 minutes late (ADR-0006).

CREATE OR REFRESH MATERIALIZED VIEW ${tp_catalog}.${tp_gold}.fact_stop_arrival (
    date_key INT COMMENT 'Service day as yyyyMMdd; joins dim_date.',
    route_sk BIGINT COMMENT 'Route version in effect that day; joins dim_route.',
    stop_sk BIGINT COMMENT 'Stop version in effect that day; joins dim_stop.',
    trip_id STRING NOT NULL COMMENT 'GTFS trip ID.',
    service_date DATE NOT NULL
    COMMENT 'MARTA service day (trips after midnight belong to the day they started).',
    stop_sequence INT NOT NULL COMMENT 'Position of the stop on the trip.',
    stop_id STRING COMMENT 'Stop from the timetable, falling back to the realtime stop ID.',
    realtime_stop_id STRING COMMENT 'Stop ID as sent in the MARTA realtime feed.',
    route_id STRING COMMENT 'GTFS route ID.',
    direction_id INT COMMENT 'GTFS direction, 0 or 1; null when the trip is not in the timetable.',
    vehicle_id STRING COMMENT 'Bus that served the trip.',
    arrival_ts TIMESTAMP COMMENT 'Observed arrival time, UTC.',
    scheduled_arrival_ts TIMESTAMP COMMENT 'Timetabled arrival time, UTC.',
    delay_seconds INT COMMENT 'Seconds behind schedule; negative means early.',
    hour_local INT COMMENT 'Hour of day in Atlanta time, 0-23.',
    arrival_status STRING COMMENT 'early (over 1 min early), on_time or late (over 5 min late); ADR-0006.',
    in_timetable BOOLEAN COMMENT 'True when the trip and stop matched the timetable in effect.',
    CONSTRAINT fact_stop_arrival_pk PRIMARY KEY (trip_id, service_date, stop_sequence),
    CONSTRAINT fact_stop_arrival_dim_date_fk
        FOREIGN KEY (date_key)
        REFERENCES ${tp_catalog}.${tp_gold}.dim_date (date_key),
    CONSTRAINT fact_stop_arrival_dim_route_fk
        FOREIGN KEY (route_sk)
        REFERENCES ${tp_catalog}.${tp_gold}.dim_route (route_sk),
    CONSTRAINT fact_stop_arrival_dim_stop_fk
        FOREIGN KEY (stop_sk)
        REFERENCES ${tp_catalog}.${tp_gold}.dim_stop (stop_sk)
)
COMMENT 'Observed bus arrivals placed on the timetable. Grain: trip_id x service_date x stop_sequence.'
AS
WITH placed AS (
    SELECT
        a.trip_id,
        a.service_date,
        a.stop_sequence,
        COALESCE(st.stop_id, a.stop_id) AS stop_id,
        a.stop_id AS realtime_stop_id,
        COALESCE(t.route_id, a.route_id) AS route_id,
        t.direction_id,
        a.vehicle_id,
        a.arrival_ts,
        a.scheduled_arrival_ts,
        a.delay_seconds,
        a.hour_local,
        st.trip_id IS NOT NULL AS in_timetable
    FROM ${tp_catalog}.${tp_silver}.bus_stop_arrivals AS a
    LEFT JOIN ${tp_catalog}.${tp_silver}.gtfs_feed_versions AS v
        ON a.service_date >= v.valid_from AND (v.valid_to IS NULL OR a.service_date < v.valid_to)
    LEFT JOIN ${tp_catalog}.${tp_silver}.gtfs_trips AS t
        ON v.feed_version = t.feed_version AND a.trip_id = t.trip_id
    LEFT JOIN ${tp_catalog}.${tp_silver}.gtfs_stop_times AS st
        ON
            v.feed_version = st.feed_version
            AND a.trip_id = st.trip_id
            AND a.stop_sequence = st.stop_sequence
)

SELECT
    CAST(DATE_FORMAT(p.service_date, 'yyyyMMdd') AS INT) AS date_key,
    r.route_sk,
    s.stop_sk,
    p.trip_id,
    p.service_date,
    p.stop_sequence,
    p.stop_id,
    p.realtime_stop_id,
    p.route_id,
    p.direction_id,
    p.vehicle_id,
    p.arrival_ts,
    p.scheduled_arrival_ts,
    p.delay_seconds,
    p.hour_local,
    CASE
        WHEN p.delay_seconds < -60 THEN 'early'
        WHEN p.delay_seconds > 300 THEN 'late'
        WHEN p.delay_seconds IS NOT NULL THEN 'on_time'
    END AS arrival_status,
    p.in_timetable
FROM placed AS p
LEFT JOIN ${tp_catalog}.${tp_gold}.dim_route AS r
    ON
        p.route_id = r.route_id
        AND p.service_date >= r.valid_from
        AND (r.valid_to IS NULL OR p.service_date < r.valid_to)
LEFT JOIN ${tp_catalog}.${tp_gold}.dim_stop AS s
    ON
        p.stop_id = s.stop_id
        AND p.service_date >= s.valid_from
        AND (s.valid_to IS NULL OR p.service_date < s.valid_to)
