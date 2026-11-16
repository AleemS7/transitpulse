# ADR-0010: Declare every gold table's columns, keys and comments in its definition

- **Status:** Accepted
- **Date:** 2026-11-16
- **Related:** MODEL-004, `design/DESIGN.md`, `src/transitpulse/pipelines/*.sql`, `tests/sql.py`

## Context
Gold is the contract for the dashboard, the Genie space and the API. Until now each gold materialized view took its schema from its query, so:
- a change to a query could quietly rename, retype or drop a column;
- nothing told a reader or Genie what a column meant;
- the star schema's keys lived only in the documentation.

Unity Catalog supports primary and foreign key constraints ✱. They are informational: not enforced, but visible to BI tools, Genie and anyone browsing the catalog. Lakeflow materialized views accept a column list with types, comments and constraints in `CREATE OR REFRESH MATERIALIZED VIEW name (...)`.

## Options considered
1. **Leave schemas inferred, document them in a markdown file.** Nothing to maintain in SQL, but the documentation drifts and Genie can't see it.
2. **Add comments and keys afterwards with `ALTER TABLE`.** Pipeline-managed tables don't allow that, and it would be a second place to keep in sync.
3. **Declare the schema in each gold file.** Every column gets a type and a comment, and every table a primary key; facts get foreign keys to the dimensions.

## Decision
Option 3, plus a test. `tests/sql.py` compares the columns and types a query actually returns with the ones the file declares, every time a unit test runs that file. A query change that breaks the contract fails locally before it reaches a pipeline.

Each primary key is the table's grain. They were checked on prod before being declared: unique, not null, and every fact foreign key matched a dimension row.

The check found one wrong grain: `headway_bunching` had 2,186 duplicate `(service_date, trip_id, stop_id)` rows, because a loop route reaches the same stop twice. `stop_sequence` was added, and the key is `(service_date, trip_id, stop_sequence)`. That's an added column, so existing consumers are unaffected.

## Consequences
- **Readable catalog:** Catalog Explorer, Genie and the dashboard editor show what every gold column means and how tables join.
- **Two places to edit:** adding a gold column means editing the column list and the query together. The test points at the mismatch when they differ.
- **Keys aren't enforced.** A duplicate wouldn't be rejected. The grain is protected upstream: Auto CDC keys in silver and the `GROUP BY` columns in gold.
- **Primary key columns are `NOT NULL`**, which is enforced ✱. A null in a key column fails the refresh loudly instead of producing an unjoinable row.

## How I'd explain it in an interview
"Gold is what everyone downstream depends on, so every gold table declares its columns, types, comments and keys in the SQL. A unit test checks the query still returns exactly that schema. I checked each key against prod data before declaring it, and that caught one table whose grain I'd got wrong."
