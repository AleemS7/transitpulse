# ADR-0009: Delete landing files after 30 days, only once bronze holds them

- **Status:** Accepted
- **Date:** 2026-11-16
- **Related:** MON-006, `design/specs/ops.md` R-006, `src/transitpulse/ops/retention.py`, `design/runbooks/landing-retention.md`

## Context
The collector lands about 113 MB a day in `workspace.bronze.landing`, almost all of it bus trip updates (about 106 MB). The volume keeps growing at roughly 41 GB a year. The plan was always to keep raw files about 30 days, long enough to reload bronze after a decoder fix, and treat bronze as the permanent record.

Facts that shape the design:
- **Shared folder.** Dev and prod pipelines read the same landing folder. Dev starts from 2026-11-13 (`realtime_start`).
- **Empty snapshots.** An empty overnight snapshot (35 bytes) decodes to no rows, so its path never appears in bronze. On 2026-11-15, 31 of 720 trip-update files were like this.
- **Auto Loader's record.** Auto Loader keeps its own record of every file it has picked up. `cloud_files_state` lists that record for a streaming table ✱.
- **Full refresh.** A pipeline full refresh rebuilds a streaming table from its source. After retention, the source no longer holds the full history.
- **Measured on 2026-11-16 (prod bronze, report mode):** 133 day folders and 3.8 GB in total. 9 folders (2026-10-14 to 10-16, 218 MB) would be deleted; every other day was newer than 30 days.

## Options considered
1. **Auto Loader's `cloudFiles.cleanSource` ✱.**
   - What it does: deletes or moves a source file after a set time.
   - Against: it runs inside every stream, including dev's, which reads the same folder. It has no check against bronze and leaves no record of what it removed.
2. **A storage lifecycle rule.** Not available: the landing volume is managed by Unity Catalog, so there is no storage account of our own to put a rule on.
3. **A daily job that checks before it deletes.**
   - What it does: lists each day folder and compares every file with what bronze and Auto Loader report. It deletes the folder only when all files pass, and records every decision.

## Decision
Option 3.
- **When a folder can go:** a day folder is deleted when it is more than 30 days old and every file in it is either:
  - in bronze (`_source_file`), or
  - listed by Auto Loader, with the stream having already loaded a later day.

  That second branch covers empty snapshots without trusting a file that is still waiting in a batch.
- **Decode failures:** a folder with a file that failed to decode is kept. For those files, bronze only has the error.
- **Report first.** The job runs in `report` mode by default. Deletion needs `retention_mode: delete` in the prod target. Dev pins `report` because it shares the folder. Prod switched to `delete` once its first scheduled report matched the preview below.
- **Pace:** at most the 3 oldest deletable days per run, so the first runs work through the backlog in small steps.
- **Bronze can't be reset.** The realtime bronze tables carry `pipelines.reset.allowed = false`, so a full refresh can no longer wipe history that landing doesn't hold any more. Verified on dev: a full refresh that selected `dev_bronze.rail_arrivals` failed with "is not resettable" and the table kept all 7,686,335 rows.
- **GTFS stays.** `gtfs_static` is never touched: one small zip per schedule version, and the hourly job re-reads it.

## Consequences
- **Storage:** stays at about 3.5 GB of landing files instead of growing without limit.
- **Audit trail:** `ops.landing_retention` records every folder on every run: files, bytes, what was decided and why. That doubles as a storage trend.
- **Window for fixes:** a decoder bug found more than 30 days later can only be fixed going forward. Health checks flag a decode-error spike within a day, which is well inside the window.
- **No bronze rebuilds:** a change that once needed a bronze rebuild (for example the 25-file batch cap) now has to be fixed forward. Removing the property to force a reset would lose everything older than 30 days.
- **Rail reprocessing:** rail bronze stores parsed fields plus anything that didn't parse in `_rescued_data`, not the original bytes. Reprocessing rail after 30 days starts from bronze.

## How I'd explain it in an interview
"Raw files are only worth keeping while I might need to reload them, so the job deletes a day folder after 30 days, but only when every file in it is provably in bronze. Empty overnight snapshots don't produce rows, so for those I use Auto Loader's own record of which files it has loaded. It ran in report mode first, it records every decision, and I locked bronze against full refresh, because once the raw files are gone bronze is the only copy."
