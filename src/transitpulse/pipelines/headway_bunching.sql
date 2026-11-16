-- Headways and bunching: for each bus arriving at a stop, the gap since the previous bus on the same route
-- and direction (LAG), next to the gap the timetable planned between those two trips.
-- A bus is bunched when it arrives within a quarter of the planned gap behind the one before it.

CREATE OR REFRESH MATERIALIZED VIEW ${tp_catalog}.${tp_gold}.headway_bunching
COMMENT 'One row per bus arrival with the actual and scheduled gap to the previous bus at that stop.'
AS
WITH ordered AS (
    SELECT
        service_date,
        route_id,
        direction_id,
        stop_id,
        trip_id,
        hour_local,
        arrival_ts,
        scheduled_arrival_ts,
        LAG(trip_id) OVER (
            PARTITION BY service_date, route_id, direction_id, stop_id ORDER BY arrival_ts
        ) AS previous_trip_id,
        LAG(arrival_ts) OVER (
            PARTITION BY service_date, route_id, direction_id, stop_id ORDER BY arrival_ts
        ) AS previous_arrival_ts,
        LAG(scheduled_arrival_ts) OVER (
            PARTITION BY service_date, route_id, direction_id, stop_id ORDER BY arrival_ts
        ) AS previous_scheduled_ts
    FROM ${tp_catalog}.${tp_gold}.fact_stop_arrival
    WHERE direction_id IS NOT NULL
),

gaps AS (
    SELECT
        *,
        UNIX_SECONDS(arrival_ts) - UNIX_SECONDS(previous_arrival_ts) AS headway_seconds,
        UNIX_SECONDS(scheduled_arrival_ts) - UNIX_SECONDS(previous_scheduled_ts) AS scheduled_headway_seconds
    FROM ordered
    WHERE previous_arrival_ts IS NOT NULL
)

SELECT
    service_date,
    route_id,
    direction_id,
    stop_id,
    trip_id,
    previous_trip_id,
    hour_local,
    arrival_ts,
    headway_seconds,
    scheduled_headway_seconds,
    scheduled_headway_seconds > 0 AND headway_seconds < 0.25 * scheduled_headway_seconds AS is_bunched
FROM gaps
