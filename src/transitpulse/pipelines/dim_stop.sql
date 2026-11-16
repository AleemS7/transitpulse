-- Stop dimension with history (SCD Type 2), built the same way as dim_route.

CREATE OR REFRESH MATERIALIZED VIEW ${tp_catalog}.${tp_gold}.dim_stop (
    stop_sk BIGINT NOT NULL COMMENT 'Surrogate key for one version of a stop. Primary key.',
    stop_id STRING COMMENT 'MARTA stop ID from the GTFS schedule.',
    stop_code STRING COMMENT 'Stop code shown at the stop.',
    stop_name STRING COMMENT 'Stop name.',
    stop_lat DOUBLE COMMENT 'Latitude, WGS84.',
    stop_lon DOUBLE COMMENT 'Longitude, WGS84.',
    location_type INT COMMENT 'GTFS location type: 0 stop or platform, 1 station.',
    parent_station STRING COMMENT 'stop_id of the station this platform belongs to, if any.',
    valid_from DATE COMMENT 'First service day this version applies to.',
    valid_to DATE COMMENT 'First service day of the next version; null while open.',
    is_current BOOLEAN COMMENT 'True for the version in the latest schedule.',
    CONSTRAINT dim_stop_pk PRIMARY KEY (stop_sk)
)
COMMENT 'One row per stop per period its details stayed the same. stop_sk is the surrogate key.'
AS
WITH per_version AS (
    SELECT
        s.stop_id,
        s.stop_code,
        s.stop_name,
        s.stop_lat,
        s.stop_lon,
        s.location_type,
        s.parent_station,
        v.valid_from,
        v.is_latest,
        SHA2(
            CONCAT_WS(
                '|', s.stop_name, CAST(s.stop_lat AS STRING), CAST(s.stop_lon AS STRING),
                CAST(s.location_type AS STRING), s.parent_station
            ),
            256
        ) AS details
    FROM ${tp_catalog}.${tp_silver}.gtfs_stops AS s
    INNER JOIN ${tp_catalog}.${tp_silver}.gtfs_feed_versions AS v
        ON s.feed_version = v.feed_version
),

changes AS (
    SELECT
        *,
        LAG(details) OVER (PARTITION BY stop_id ORDER BY valid_from) AS previous_details,
        MAX(CAST(is_latest AS INT)) OVER (PARTITION BY stop_id) = 1 AS in_latest_version
    FROM per_version
),

periods AS (
    SELECT *
    FROM changes
    WHERE previous_details IS NULL OR previous_details != details
)

SELECT
    XXHASH64(stop_id, valid_from) AS stop_sk,
    stop_id,
    stop_code,
    stop_name,
    stop_lat,
    stop_lon,
    location_type,
    parent_station,
    valid_from,
    LEAD(valid_from) OVER (PARTITION BY stop_id ORDER BY valid_from) AS valid_to,
    (LEAD(valid_from) OVER (PARTITION BY stop_id ORDER BY valid_from) IS NULL AND in_latest_version)
        AS is_current
FROM periods
