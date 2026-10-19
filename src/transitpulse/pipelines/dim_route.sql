-- Route dimension with history (SCD Type 2): a new row each time a route's details change between
-- schedule versions. LAG spots the change; LEAD closes the previous row.

CREATE OR REFRESH MATERIALIZED VIEW ${tp_catalog}.${tp_gold}.dim_route
COMMENT 'One row per route per period its details stayed the same. route_sk is the surrogate key.'
AS
WITH per_version AS (
    SELECT
        r.route_id,
        r.route_short_name,
        r.route_long_name,
        r.route_type,
        r.route_color,
        v.valid_from,
        v.is_latest,
        SHA2(
            CONCAT_WS(
                '|', r.route_short_name, r.route_long_name, CAST(r.route_type AS STRING), r.route_color
            ),
            256
        ) AS details
    FROM ${tp_catalog}.${tp_silver}.gtfs_routes AS r
    INNER JOIN ${tp_catalog}.${tp_silver}.gtfs_feed_versions AS v
        ON r.feed_version = v.feed_version
),

changes AS (
    SELECT
        *,
        LAG(details) OVER (PARTITION BY route_id ORDER BY valid_from) AS previous_details,
        MAX(CAST(is_latest AS INT)) OVER (PARTITION BY route_id) = 1 AS in_latest_version
    FROM per_version
),

periods AS (
    SELECT *
    FROM changes
    WHERE previous_details IS NULL OR previous_details != details
)

SELECT
    XXHASH64(route_id, valid_from) AS route_sk,
    route_id,
    route_short_name,
    route_long_name,
    route_type,
    CASE route_type WHEN 0 THEN 'streetcar' WHEN 1 THEN 'rail' WHEN 3 THEN 'bus' ELSE 'other' END
        AS transit_mode,
    route_color,
    valid_from,
    LEAD(valid_from) OVER (PARTITION BY route_id ORDER BY valid_from) AS valid_to,
    (LEAD(valid_from) OVER (PARTITION BY route_id ORDER BY valid_from) IS NULL AND in_latest_version)
        AS is_current
FROM periods
