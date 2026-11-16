-- One row per fully collected service day: the headline bus and rail numbers for a dashboard.

CREATE OR REFRESH MATERIALIZED VIEW ${tp_catalog}.${tp_gold}.daily_kpis
COMMENT 'Headline reliability numbers per fully collected service day (bus and rail).'
AS
WITH trips AS (
    SELECT
        service_date,
        COUNT(*) AS scheduled_trips
    FROM ${tp_catalog}.${tp_silver}.bus_scheduled_trips
    GROUP BY service_date
),

ghosts AS (
    SELECT
        service_date,
        COUNT(*) AS ghost_trips,
        COUNT_IF(reason = 'cancelled') AS cancelled_trips
    FROM ${tp_catalog}.${tp_gold}.ghost_trips
    GROUP BY service_date
),

arrivals AS (
    SELECT
        service_date,
        COUNT(*) AS observed_arrivals,
        ROUND(TRY_DIVIDE(100 * COUNT_IF(arrival_status = 'on_time'), COUNT(arrival_status)), 1)
            AS bus_on_time_pct,
        ROUND(AVG(delay_seconds)) AS bus_avg_delay_seconds
    FROM ${tp_catalog}.${tp_gold}.fact_stop_arrival
    GROUP BY service_date
),

bunching AS (
    SELECT
        service_date,
        ROUND(TRY_DIVIDE(100 * COUNT_IF(is_bunched), COUNT(is_bunched)), 1) AS bunched_pct
    FROM ${tp_catalog}.${tp_gold}.headway_bunching
    GROUP BY service_date
),

rail AS (
    SELECT
        service_date,
        ROUND(AVG(delay_seconds)) AS rail_avg_delay_seconds
    FROM ${tp_catalog}.${tp_silver}.rail_arrivals
    WHERE is_realtime
    GROUP BY service_date
)

SELECT
    d.date_key,
    t.service_date,
    d.day_name,
    d.is_weekend,
    d.is_schedule_exception,
    t.scheduled_trips,
    COALESCE(g.ghost_trips, 0) AS ghost_trips,
    COALESCE(g.cancelled_trips, 0) AS cancelled_trips,
    ROUND(TRY_DIVIDE(100 * COALESCE(g.ghost_trips, 0), t.scheduled_trips), 1) AS ghost_trip_pct,
    COALESCE(a.observed_arrivals, 0) AS observed_arrivals,
    a.bus_on_time_pct,
    a.bus_avg_delay_seconds,
    b.bunched_pct,
    r.rail_avg_delay_seconds
FROM trips AS t
INNER JOIN ${tp_catalog}.${tp_gold}.dim_date AS d
    ON t.service_date = d.calendar_date
LEFT JOIN ghosts AS g
    ON t.service_date = g.service_date
LEFT JOIN arrivals AS a
    ON t.service_date = a.service_date
LEFT JOIN bunching AS b
    ON t.service_date = b.service_date
LEFT JOIN rail AS r
    ON t.service_date = r.service_date
