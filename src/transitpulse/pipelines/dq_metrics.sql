-- Data-quality results per pipeline update, dataset and rule, read from the pipeline's own event log.
-- The same rules that split rows into silver and quarantine run as expectations, so their pass/fail
-- counts land in the event log on every update (transitpulse.common.quality).

CREATE OR REFRESH MATERIALIZED VIEW ${tp_catalog}.${tp_ops}.dq_metrics
COMMENT 'Rows passing and failing each quality rule, per pipeline update and dataset.'
AS
WITH progress AS (
    SELECT
        ev.`timestamp` AS event_ts,
        ev.origin.update_id AS update_id,  -- noqa: AL09, RF01
        FROM_JSON(
            GET_JSON_OBJECT(ev.details, '$.flow_progress.data_quality.expectations'),
            'ARRAY<STRUCT<name: STRING, dataset: STRING, passed_records: BIGINT, failed_records: BIGINT>>'
        ) AS expectations
    FROM ${tp_catalog}.${tp_ops}.pipeline_events AS ev
    WHERE
        ev.event_type = 'flow_progress'
        AND GET_JSON_OBJECT(ev.details, '$.flow_progress.data_quality.expectations') IS NOT NULL
),

rules AS (
    -- INLINE turns the array of structs into one row per rule, with the struct fields as columns.
    SELECT
        event_ts,
        update_id,
        INLINE(expectations)  -- noqa: AL03
    FROM progress
)

SELECT
    event_ts,
    update_id,
    dataset,
    name AS rule_name,
    passed_records,
    failed_records
FROM rules
