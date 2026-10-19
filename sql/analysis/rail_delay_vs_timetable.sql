-- Evidence for ADR-0002: is the rail feed's DELAY "seconds behind schedule"?
-- For tracked predictions on one weekday, compare each predicted arrival (raw, minus DELAY, plus DELAY)
-- with the nearest scheduled arrival for the same line, direction and station in the timetable in effect.
-- 2026-10-16 result: median gap 99 s raw, 45 s minus DELAY, 181 s plus DELAY.

WITH active AS (
    SELECT service_id FROM workspace.silver.gtfs_calendar
    WHERE friday AND DATE '2026-10-16' BETWEEN start_date AND end_date
),

sched AS (
    SELECT
        r.route_short_name AS line,
        SUBSTR(REGEXP_EXTRACT(t.trip_headsign, '(NORTH|SOUTH|EAST|WEST)BOUND', 1), 1, 1) AS direction,
        s.stop_name AS station,
        st.arrival_secs
    FROM workspace.silver.gtfs_stop_times AS st
    INNER JOIN workspace.silver.gtfs_trips AS t ON st.feed_version = t.feed_version AND st.trip_id = t.trip_id
    INNER JOIN workspace.silver.gtfs_routes AS r
        ON t.feed_version = r.feed_version AND t.route_id = r.route_id
    INNER JOIN workspace.silver.gtfs_stops AS s
        ON st.feed_version = s.feed_version AND st.stop_id = s.stop_id
    WHERE r.route_type = 1 AND t.service_id IN (SELECT a.service_id FROM active AS a)
),

rt AS (
    SELECT
        record_id,
        line,
        direction,
        station,
        delay_seconds,
        UNIX_TIMESTAMP(next_arrival_ts)
        - UNIX_TIMESTAMP(TO_UTC_TIMESTAMP(CAST(service_date AS TIMESTAMP), 'America/New_York')) AS pred_secs
    FROM workspace.silver.rail_arrivals
    WHERE service_date = DATE '2026-10-16' AND is_realtime AND ABS(delay_seconds) >= 60
),

matched AS (
    SELECT
        rt.record_id,
        rt.delay_seconds,
        MIN(ABS(rt.pred_secs - s.arrival_secs)) AS gap_raw,
        MIN(ABS(rt.pred_secs - rt.delay_seconds - s.arrival_secs)) AS gap_minus,
        MIN(ABS(rt.pred_secs + rt.delay_seconds - s.arrival_secs)) AS gap_plus
    FROM rt
    INNER JOIN sched AS s ON rt.line = s.line AND rt.direction = s.direction AND rt.station = s.station
    GROUP BY rt.record_id, rt.delay_seconds
)

SELECT
    COUNT(*) AS predictions,
    PERCENTILE_APPROX(gap_raw, 0.5) AS median_gap_raw,
    PERCENTILE_APPROX(gap_minus, 0.5) AS median_gap_pred_minus_delay,
    PERCENTILE_APPROX(gap_plus, 0.5) AS median_gap_pred_plus_delay,
    ROUND(AVG(CASE WHEN gap_raw <= 30 THEN 1 ELSE 0 END), 3) AS within_30s_raw,
    ROUND(AVG(CASE WHEN gap_minus <= 30 THEN 1 ELSE 0 END), 3) AS within_30s_minus,
    ROUND(AVG(CASE WHEN gap_plus <= 30 THEN 1 ELSE 0 END), 3) AS within_30s_plus
FROM matched
