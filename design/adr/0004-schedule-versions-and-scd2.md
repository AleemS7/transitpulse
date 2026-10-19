# ADR-0004: Keep each schedule version whole; build SCD Type 2 dimensions with window functions

- **Status:** Accepted
- **Date:** 2026-10-19
- **Related:** GTFS-001, GTFS-002, MODEL-001, MODEL-002, `design/specs/gtfs.md`

## Context
MARTA republishes its whole GTFS schedule a few times a year; the current version covers service from 2026-08-22 to 2026-12-11. Realtime data must be compared with the schedule that was in effect on its service day, even after a newer version lands. Reports also need route and stop details as they were at the time (a renamed route shouldn't rewrite history).

## Options considered
1. **Silver keeps every version side by side (`feed_version` column); a small `gtfs_feed_versions` table says which dates each version covers.** Joins pick the version by service date. Simple to reason about, and nothing is overwritten.
2. **Row-level change tracking in silver (Auto CDC / MERGE per row).** Keeps one copy of unchanged rows, but stop_times has about 2 million rows per version, and "which schedule applied on date X" becomes harder to answer.
3. **Gold SCD2 dimensions with Databricks' `create_auto_cdc_from_snapshot_flow`.** Declarative, but hides the logic behind an API.
4. **Gold SCD2 dimensions in plain SQL:** `LAG` compares each version of a route with the previous one, keeps only the rows where something changed, and `LEAD` sets `valid_to` from the next change.

## Decision
Options 1 and 4. A version is in effect from its earliest calendar start date until the next version's start date (`LEAD` over versions). `dim_route` and `dim_stop` are SQL materialized views with a surrogate key (`XXHASH64(id, valid_from)`), `valid_from`, `valid_to` and `is_current`.

## Consequences
- History is never lost, and a new version needs no special handling: it's extracted, loaded and picked up by the window functions.
- Storage grows by one full schedule (~110 MB of CSV, ~2 M stop_times rows) per version, a few times a year. That's acceptable.
- A route that disappears from the newest version keeps its last `valid_to` empty but gets `is_current = false`. Closing it on a date would need a "presence per version" step; it isn't worth the complexity yet.
- The SQL files run unchanged in local tests against small hand-made versions (`tests/unit/test_gtfs.py`).

## How I'd explain it in an interview
"Each schedule version is kept whole and tagged with its effective dates, so delays are always measured against the timetable that applied that day. For the route and stop dimensions, I used LAG to detect when details changed between versions and LEAD to close the old row: a classic SCD Type 2, written in plain SQL so anyone can read it."
