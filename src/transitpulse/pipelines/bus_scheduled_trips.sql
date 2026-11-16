-- Bus trips the timetable says should run on each fully collected service day.
-- A service runs on a day when its weekly calendar covers it, unless calendar_dates removes it (type 2);
-- calendar_dates can also add a service for one day (type 1). Days are limited to those the collector saw
-- from start to finish: after the first day with bus positions and before the current one.

CREATE OR REFRESH MATERIALIZED VIEW ${tp_catalog}.${tp_silver}.bus_scheduled_trips
COMMENT 'Scheduled bus trips per fully collected service day, from the timetable version in effect.'
AS
WITH observed_days AS (
    SELECT
        MIN(service_date) AS first_day,
        MAX(service_date) AS current_day
    FROM ${tp_catalog}.${tp_silver}.bus_vehicle_positions
),

collected_days AS (
    SELECT
        d.calendar_date AS service_date,
        v.feed_version
    FROM ${tp_catalog}.${tp_gold}.dim_date AS d
    CROSS JOIN observed_days AS o
    INNER JOIN ${tp_catalog}.${tp_silver}.gtfs_feed_versions AS v
        ON d.calendar_date >= v.valid_from AND (v.valid_to IS NULL OR d.calendar_date < v.valid_to)
    WHERE d.calendar_date > o.first_day AND d.calendar_date < o.current_day
),

weekly AS (
    SELECT
        cd.service_date,
        cd.feed_version,
        c.service_id
    FROM collected_days AS cd
    INNER JOIN ${tp_catalog}.${tp_silver}.gtfs_calendar AS c
        ON cd.feed_version = c.feed_version AND cd.service_date BETWEEN c.start_date AND c.end_date
    WHERE
        CASE WEEKDAY(cd.service_date)
            WHEN 0 THEN c.monday
            WHEN 1 THEN c.tuesday
            WHEN 2 THEN c.wednesday
            WHEN 3 THEN c.thursday
            WHEN 4 THEN c.friday
            WHEN 5 THEN c.saturday
            WHEN 6 THEN c.sunday
        END
),

exceptions AS (
    SELECT
        cd.service_date,
        cd.feed_version,
        x.service_id,
        x.exception_type
    FROM collected_days AS cd
    INNER JOIN ${tp_catalog}.${tp_silver}.gtfs_calendar_dates AS x
        ON cd.feed_version = x.feed_version AND cd.service_date = x.date
),

running AS (
    (
        SELECT
            service_date,
            feed_version,
            service_id
        FROM weekly
        UNION DISTINCT
        SELECT
            service_date,
            feed_version,
            service_id
        FROM exceptions
        WHERE exception_type = 1
    )
    EXCEPT
    SELECT
        service_date,
        feed_version,
        service_id
    FROM exceptions
    WHERE exception_type = 2
),

first_departures AS (
    SELECT
        feed_version,
        trip_id,
        MIN(departure_secs) AS first_departure_secs
    FROM ${tp_catalog}.${tp_silver}.gtfs_stop_times
    GROUP BY feed_version, trip_id
)

SELECT
    r.service_date,
    t.trip_id,
    t.route_id,
    t.direction_id,
    t.trip_headsign,
    f.first_departure_secs,
    -- Labelled local presentation value; GTFS hours can run past 24 for trips after midnight.
    CONCAT(
        LPAD(CAST(FLOOR(f.first_departure_secs / 3600) AS STRING), 2, '0'), ':',
        LPAD(CAST(FLOOR(f.first_departure_secs % 3600 / 60) AS STRING), 2, '0')
    ) AS scheduled_start_local
FROM running AS r
INNER JOIN ${tp_catalog}.${tp_silver}.gtfs_trips AS t
    ON r.feed_version = t.feed_version AND r.service_id = t.service_id
INNER JOIN ${tp_catalog}.${tp_silver}.gtfs_routes AS rt
    ON t.feed_version = rt.feed_version AND t.route_id = rt.route_id
LEFT JOIN first_departures AS f
    ON t.feed_version = f.feed_version AND t.trip_id = f.trip_id
WHERE rt.route_type = 3
