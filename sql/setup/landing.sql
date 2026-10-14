-- One-time setup for the landing zone the collector writes to.
-- Kept out of the bundle on purpose: a bundle destroy must never delete collected history.

CREATE SCHEMA IF NOT EXISTS workspace.bronze
COMMENT 'Raw data as received from MARTA, plus load metadata.';

CREATE VOLUME IF NOT EXISTS workspace.bronze.landing
COMMENT 'Files pushed by the collector: one folder per feed, plus _heartbeat. Never edited by hand.';
