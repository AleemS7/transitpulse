-- Ghost trips: scheduled bus trips that didn't run as far as the realtime data can tell.
-- 'cancelled' = MARTA's latest status for the trip was CANCELED.
-- 'no_bus_seen' = no bus reported a position on the trip and no arrival was observed for it.

CREATE OR REFRESH MATERIALIZED VIEW ${tp_catalog}.${tp_gold}.ghost_trips (
    date_key INT COMMENT 'Service day as yyyyMMdd; joins dim_date.',
    route_sk BIGINT COMMENT 'Route version in effect that day; joins dim_route.',
    service_date DATE NOT NULL
    COMMENT 'MARTA service day (trips after midnight belong to the day they started).',
    trip_id STRING NOT NULL COMMENT 'Scheduled GTFS trip ID.',
    route_id STRING COMMENT 'GTFS route ID.',
    route_short_name STRING COMMENT 'Route number riders see.',
    direction_id INT COMMENT 'GTFS direction, 0 or 1.',
    trip_headsign STRING COMMENT 'Destination shown on the bus.',
    scheduled_start_local STRING
    COMMENT 'Timetabled first departure, Atlanta time, HH:MM (after midnight it passes 24:00).',
    reason STRING COMMENT 'cancelled (MARTA marked it CANCELED) or no_bus_seen (no position or arrival).',
    CONSTRAINT ghost_trips_pk PRIMARY KEY (service_date, trip_id),
    CONSTRAINT ghost_trips_dim_date_fk
        FOREIGN KEY (date_key)
        REFERENCES ${tp_catalog}.${tp_gold}.dim_date (date_key),
    CONSTRAINT ghost_trips_dim_route_fk
        FOREIGN KEY (route_sk)
        REFERENCES ${tp_catalog}.${tp_gold}.dim_route (route_sk)
)
COMMENT 'Scheduled bus trips with no sign of running, per fully collected service day, with the reason.'
AS
WITH positions AS (
    SELECT DISTINCT
        trip_id,
        service_date
    FROM ${tp_catalog}.${tp_silver}.bus_vehicle_positions
),

arrivals AS (
    SELECT DISTINCT
        trip_id,
        service_date
    FROM ${tp_catalog}.${tp_silver}.bus_stop_arrivals
),

classified AS (
    SELECT
        s.service_date,
        s.trip_id,
        s.route_id,
        s.direction_id,
        s.trip_headsign,
        s.scheduled_start_local,
        CASE
            WHEN bt.trip_status = 'CANCELED' THEN 'cancelled'
            WHEN p.trip_id IS NULL AND a.trip_id IS NULL THEN 'no_bus_seen'
        END AS reason
    FROM ${tp_catalog}.${tp_silver}.bus_scheduled_trips AS s
    LEFT JOIN ${tp_catalog}.${tp_silver}.bus_trips AS bt
        ON s.trip_id = bt.trip_id AND s.service_date = bt.service_date
    LEFT JOIN positions AS p
        ON s.trip_id = p.trip_id AND s.service_date = p.service_date
    LEFT JOIN arrivals AS a
        ON s.trip_id = a.trip_id AND s.service_date = a.service_date
)

SELECT
    CAST(DATE_FORMAT(c.service_date, 'yyyyMMdd') AS INT) AS date_key,
    r.route_sk,
    c.service_date,
    c.trip_id,
    c.route_id,
    r.route_short_name,
    c.direction_id,
    c.trip_headsign,
    c.scheduled_start_local,
    c.reason
FROM classified AS c
LEFT JOIN ${tp_catalog}.${tp_gold}.dim_route AS r
    ON
        c.route_id = r.route_id
        AND c.service_date >= r.valid_from
        AND (r.valid_to IS NULL OR c.service_date < r.valid_to)
WHERE c.reason IS NOT NULL
