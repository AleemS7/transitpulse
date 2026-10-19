# ADR-0002: Treat the rail feed's DELAY as seconds behind schedule

- **Status:** Accepted (confirmed against the timetable, 2026-10-19)
- **Date:** 2026-10-15
- **Related:** RAIL-002, RAIL-003, SPK-010, `design/specs/rail.md`, `sql/analysis/rail_delay_vs_timetable.sql`

## Context
MARTA's rail arrivals feed has a `DELAY` field that its documentation doesn't define. `gold.rail_line_health` reports average and 90th-percentile delay from it, so the meaning has to be pinned down, or at least stated.

Observed in 6,660 records sampled from 25 snapshots (2026-10-14/15):
- The format is always `T<integer>S`, e.g. `T564S`, `T-45S`.
- It's present only on live-tracked rows (`IS_REALTIME = true`), never on timetable rows.
- Values range from -1,399 to +689. The median is 26, the 90th percentile 174, and 29% are exactly 0.

## Options considered
1. **Seconds behind schedule (negative = early).** Fits every observation: a duration-style format with an S suffix, only on tracked trains, mostly small positive values, a cluster at zero for on-time trains, and some early trains.
2. **Seconds until arrival.** Ruled out: `WAITING_SECONDS` already carries that, and the two don't match.
3. **Don't use the field.** Safe, but it throws away the only direct lateness signal in the rail feed.

## Decision
Option 1. Silver stores it as `delay_seconds` (an integer, NULL when absent or malformed), and gold describes it as "seconds behind schedule; negative = early", with a link to this ADR.

## Confirmation
Once the static schedule was loaded (S5), every tracked prediction on Friday 2026-10-16 with a delay of at least 60 s (40,552 rows) was compared with the nearest scheduled arrival for the same line, direction and station:

| Reading | Median gap to a scheduled arrival | Within 30 s |
|---|---|---|
| Predicted arrival as sent | 99 s | 13.6% |
| **Predicted arrival − DELAY** | **45 s** | **36.1%** |
| Predicted arrival + DELAY | 181 s | 4.5% |

Subtracting DELAY brings predictions back onto the timetable far more often than either alternative, so DELAY is how far behind schedule the train is. The remaining gap comes from prediction noise and from matching to the nearest of several close scheduled trains.

## Consequences
- Rail delay metrics rest on a reading that's now backed by evidence; the query is kept in `sql/analysis/` and can be re-run on any day.

## How I'd explain it in an interview
"The rail feed had an undocumented DELAY field. I profiled it and wrote down a hypothesis, seconds behind schedule, then tested it once the timetable was loaded: subtracting DELAY from 40,000 predictions put them back on scheduled times nearly three times as often as the raw prediction. So the metric rests on evidence, not a guess."
