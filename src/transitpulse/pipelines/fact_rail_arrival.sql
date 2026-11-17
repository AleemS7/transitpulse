-- Rail arrival fact: one row per pass, i.e. one train coming into one station in one direction.
-- MARTA only publishes predictions. A train's predictions for a station belong to one pass until they stop
-- for over 20 minutes; the pass's last prediction stands in for the arrival, and counts as observed when the
-- train was at most 2 minutes out, about one collector poll (ADR-0011).

CREATE OR REFRESH MATERIALIZED VIEW ${tp_catalog}.${tp_gold}.fact_rail_arrival (
    date_key INT COMMENT 'Service day as yyyyMMdd; joins dim_date.',
    route_sk BIGINT COMMENT 'Rail line version in effect that day; joins dim_route.',
    stop_sk BIGINT COMMENT 'Station version in effect that day; joins dim_stop.',
    service_date DATE NOT NULL
    COMMENT 'MARTA service day (trips after midnight belong to the day they started).',
    train_id STRING NOT NULL COMMENT 'Train number as MARTA sends it.',
    line STRING NOT NULL COMMENT 'Rail line: RED, GOLD, BLUE or GREEN.',
    direction STRING NOT NULL COMMENT 'Direction of travel: N, S, E or W.',
    station STRING NOT NULL COMMENT 'Station name as MARTA sends it.',
    first_seen_ts TIMESTAMP NOT NULL COMMENT 'First prediction of this pass, UTC.',
    destination STRING COMMENT 'Where the train was heading.',
    last_seen_ts TIMESTAMP COMMENT 'Last prediction of this pass, UTC.',
    predictions BIGINT COMMENT 'Distinct predictions MARTA published for this pass.',
    arrival_ts TIMESTAMP COMMENT 'Arrival time in the last prediction, UTC.',
    last_wait_seconds INT COMMENT 'Seconds the train was still away in the last prediction.',
    delay_seconds INT COMMENT 'MARTA DELAY in the last prediction; negative means early (ADR-0002).',
    hour_local INT COMMENT 'Hour of day in Atlanta time, 0-23, of the last prediction.',
    observed BOOLEAN COMMENT 'True when the last prediction was at most 2 minutes before arrival.',
    arrival_status STRING
    COMMENT 'early (over 1 min early), on_time or late (over 5 min late); null unless observed.',
    CONSTRAINT fact_rail_arrival_pk
    PRIMARY KEY (service_date, train_id, line, direction, station, first_seen_ts),
    CONSTRAINT fact_rail_arrival_dim_date_fk
        FOREIGN KEY (date_key)
        REFERENCES ${tp_catalog}.${tp_gold}.dim_date (date_key),
    CONSTRAINT fact_rail_arrival_dim_route_fk
        FOREIGN KEY (route_sk)
        REFERENCES ${tp_catalog}.${tp_gold}.dim_route (route_sk),
    CONSTRAINT fact_rail_arrival_dim_stop_fk
        FOREIGN KEY (stop_sk)
        REFERENCES ${tp_catalog}.${tp_gold}.dim_stop (stop_sk)
)
COMMENT 'Rail arrivals inferred from MARTA predictions. Grain: one train passing a station in one direction.'
AS
WITH predictions AS (
    SELECT
        service_date,
        train_id,
        line,
        direction,
        station,
        destination,
        event_ts,
        next_arrival_ts,
        waiting_seconds,
        delay_seconds,
        hour_local,
        LAG(event_ts) OVER (
            PARTITION BY service_date, train_id, line, direction, station
            ORDER BY event_ts, waiting_seconds
        ) AS previous_event_ts
    FROM ${tp_catalog}.${tp_silver}.rail_arrivals
    WHERE is_realtime
),

-- A running count of pass starts numbers the passes of one train at one station.
numbered AS (
    SELECT
        *,
        SUM(
            CASE
                WHEN previous_event_ts IS NULL THEN 1
                WHEN event_ts > previous_event_ts + INTERVAL 20 MINUTES THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY service_date, train_id, line, direction, station
            ORDER BY event_ts, waiting_seconds
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS pass_number
    FROM predictions
),

passes AS (
    SELECT
        *,
        MIN(event_ts) OVER (
            PARTITION BY service_date, train_id, line, direction, station, pass_number
        ) AS first_seen_ts,
        COUNT(*) OVER (
            PARTITION BY service_date, train_id, line, direction, station, pass_number
        ) AS predictions,
        ROW_NUMBER() OVER (
            PARTITION BY service_date, train_id, line, direction, station, pass_number
            ORDER BY event_ts DESC, waiting_seconds ASC, next_arrival_ts DESC
        ) AS newest
    FROM numbered
),

-- The realtime feed still uses older names for these stations; everything else matches the timetable.
station_names AS (
    SELECT
        realtime_name,
        timetable_name
    FROM
        VALUES
        ('BROOKHAVEN STATION', 'BROOKHAVEN-OGLETHORPE STATION'),
        ('EDGEWOOD CANDLER PARK STATION', 'EDGEWOOD-CANDLER PARK STATION'),
        ('INMAN PARK STATION', 'INMAN PARK-REYNOLDSTOWN STATION'),
        ('LAKEWOOD STATION', 'LAKEWOOD-FT MCPHERSON STATION'),
        ('LINDBERGH STATION', 'LINDBERGH CENTER STATION'),
        ('OMNI DOME STATION', 'SEC DISTRICT STATION') AS t (realtime_name, timetable_name)
),

arrivals AS (
    SELECT
        p.service_date,
        p.train_id,
        p.line,
        p.direction,
        p.station,
        COALESCE(n.timetable_name, UPPER(p.station)) AS timetable_station,
        p.first_seen_ts,
        p.destination,
        p.event_ts AS last_seen_ts,
        p.predictions,
        p.next_arrival_ts AS arrival_ts,
        p.waiting_seconds AS last_wait_seconds,
        p.delay_seconds,
        p.hour_local,
        COALESCE(p.waiting_seconds <= 120, FALSE) AS observed
    FROM passes AS p
    LEFT JOIN station_names AS n
        ON UPPER(p.station) = n.realtime_name
    WHERE p.newest = 1
)

SELECT
    CAST(DATE_FORMAT(a.service_date, 'yyyyMMdd') AS INT) AS date_key,
    r.route_sk,
    s.stop_sk,
    a.service_date,
    a.train_id,
    a.line,
    a.direction,
    a.station,
    a.first_seen_ts,
    a.destination,
    a.last_seen_ts,
    a.predictions,
    a.arrival_ts,
    a.last_wait_seconds,
    a.delay_seconds,
    a.hour_local,
    a.observed,
    CASE
        WHEN NOT a.observed THEN NULL
        WHEN a.delay_seconds < -60 THEN 'early'
        WHEN a.delay_seconds > 300 THEN 'late'
        WHEN a.delay_seconds IS NOT NULL THEN 'on_time'
    END AS arrival_status
FROM arrivals AS a
LEFT JOIN ${tp_catalog}.${tp_gold}.dim_route AS r
    ON
        a.line = r.route_short_name
        AND r.route_type = 1
        AND a.service_date >= r.valid_from
        AND (r.valid_to IS NULL OR a.service_date < r.valid_to)
LEFT JOIN ${tp_catalog}.${tp_gold}.dim_stop AS s
    ON
        a.timetable_station = UPPER(s.stop_name)
        AND s.location_type = 1
        AND a.service_date >= s.valid_from
        AND (s.valid_to IS NULL OR a.service_date < s.valid_to)
