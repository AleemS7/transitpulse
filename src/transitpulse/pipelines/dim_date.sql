-- Calendar dimension: one row per date, so weekday, weekend and holiday questions are plain columns.
-- is_schedule_exception marks dates where MARTA's schedule overrides the normal pattern (usually holidays).

CREATE OR REFRESH MATERIALIZED VIEW ${tp_catalog}.${tp_gold}.dim_date (
    date_key INT NOT NULL COMMENT 'Calendar date as yyyyMMdd. Primary key.',
    calendar_date DATE COMMENT 'The calendar date.',
    calendar_year INT COMMENT 'Year, e.g. 2026.',
    calendar_month INT COMMENT 'Month number, 1-12.',
    month_name STRING COMMENT 'Month name, e.g. November.',
    day_of_month INT COMMENT 'Day of the month, 1-31.',
    day_of_week INT COMMENT '1 = Monday through 7 = Sunday.',
    day_name STRING COMMENT 'Weekday name, e.g. Monday.',
    is_weekend BOOLEAN COMMENT 'True on Saturday and Sunday.',
    is_schedule_exception BOOLEAN
    COMMENT 'True when MARTA runs a special schedule that day, usually a holiday.',
    CONSTRAINT dim_date_pk PRIMARY KEY (date_key)
)
COMMENT 'One row per calendar date, 2026-2027. date_key is yyyyMMdd.'
AS
WITH days AS (
    SELECT EXPLODE(SEQUENCE(DATE '2026-01-01', DATE '2027-12-31', INTERVAL 1 DAY)) AS calendar_date
),

exceptions AS (
    SELECT DISTINCT cd.date AS calendar_date
    FROM ${tp_catalog}.${tp_silver}.gtfs_calendar_dates AS cd
    INNER JOIN ${tp_catalog}.${tp_silver}.gtfs_feed_versions AS v
        ON cd.feed_version = v.feed_version
    WHERE cd.date >= v.valid_from AND (v.valid_to IS NULL OR cd.date < v.valid_to)
)

SELECT
    CAST(DATE_FORMAT(d.calendar_date, 'yyyyMMdd') AS INT) AS date_key,
    d.calendar_date,
    YEAR(d.calendar_date) AS calendar_year,
    MONTH(d.calendar_date) AS calendar_month,
    DATE_FORMAT(d.calendar_date, 'MMMM') AS month_name,
    DAY(d.calendar_date) AS day_of_month,
    WEEKDAY(d.calendar_date) + 1 AS day_of_week,
    DATE_FORMAT(d.calendar_date, 'EEEE') AS day_name,
    WEEKDAY(d.calendar_date) >= 5 AS is_weekend,
    e.calendar_date IS NOT NULL AS is_schedule_exception
FROM days AS d
LEFT JOIN exceptions AS e
    ON d.calendar_date = e.calendar_date
