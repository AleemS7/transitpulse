-- One-time setup of the medallion schemas. Kept out of the bundle so a redeploy or destroy never drops data.
-- Run with prefix '' for prod and 'dev_' for dev. workspace.bronze already exists (see landing.sql).

CREATE SCHEMA IF NOT EXISTS workspace.dev_bronze
COMMENT 'Development copy of bronze.';

CREATE SCHEMA IF NOT EXISTS workspace.dev_silver
COMMENT 'Development copy of silver.';

CREATE SCHEMA IF NOT EXISTS workspace.dev_gold
COMMENT 'Development copy of gold.';

CREATE SCHEMA IF NOT EXISTS workspace.dev_ops
COMMENT 'Development copy of ops.';

CREATE SCHEMA IF NOT EXISTS workspace.silver
COMMENT 'Cleaned, typed and de-duplicated data; one row per business entity version.';

CREATE SCHEMA IF NOT EXISTS workspace.gold
COMMENT 'Business metrics; the contract for dashboards, the API and the delay model.';

CREATE SCHEMA IF NOT EXISTS workspace.ops
COMMENT 'Pipeline health: freshness, data quality, collector runs and incidents.';

-- Each GTFS schedule version is unpacked here (one folder per table) by the extract_gtfs job task.
CREATE VOLUME IF NOT EXISTS workspace.dev_bronze.gtfs_files
COMMENT 'Development copy of the unpacked GTFS schedule files.';

CREATE VOLUME IF NOT EXISTS workspace.bronze.gtfs_files
COMMENT 'GTFS schedule files unpacked per version: <table>/feed_version=<hash>/<table>.txt.';
