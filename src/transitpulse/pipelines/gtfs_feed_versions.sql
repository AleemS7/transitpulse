-- One row per GTFS schedule version and the service dates it covers.
-- A version is in effect from its earliest calendar start date until the next version takes over.

CREATE OR REFRESH MATERIALIZED VIEW ${tp_catalog}.${tp_silver}.gtfs_feed_versions
COMMENT 'GTFS schedule versions with the service dates each one is in effect for.'
AS
WITH versions AS (
    SELECT
        feed_version,
        MIN(start_date) AS valid_from,
        MAX(end_date) AS last_service_date,
        MIN(_ingested_at) AS loaded_at
    FROM ${tp_catalog}.${tp_silver}.gtfs_calendar
    GROUP BY feed_version
)

SELECT
    feed_version,
    valid_from,
    LEAD(valid_from) OVER (ORDER BY valid_from, loaded_at) AS valid_to,
    last_service_date,
    loaded_at,
    LEAD(valid_from) OVER (ORDER BY valid_from, loaded_at) IS NULL AS is_latest
FROM versions
