# ADR-0011: Infer rail arrivals from the last prediction of each train pass

- **Status:** Accepted
- **Date:** 2026-11-16
- **Related:** MODEL-003, `design/specs/rail.md` (R-008), `src/transitpulse/pipelines/fact_rail_arrival.sql`, ADR-0002, ADR-0006

## Context
`gold.rail_line_health` averages every prediction MARTA published, so a train predicted 20 times weighs 20 times as much as one predicted twice, and nothing says whether a train actually arrived. The star schema needs a rail fact at arrival grain, like `fact_stop_arrival` for buses.

The rail feed has no arrival message. Each snapshot lists, per station and direction, the trains due next: train ID, predicted arrival, seconds still to wait and MARTA's `DELAY` (seconds behind schedule, ADR-0002). A train's entries for a station count down over about 30 minutes, then disappear once it has arrived. Trains run back and forth all day, so the same train reaches the same station in the same direction many times a day.

Measured on prod silver, 2026-11-10 to 11-13 (realtime predictions only):
- **Gaps between a train's consecutive predictions for one station and direction:** 452,686 under 5 minutes, 20,118 at 5–10, 6,610 at 10–15, 2,368 at 15–20, then a low point of 1,166 at 20–30 minutes, before rising again (5,612 at 30–45, 8,352 at 45–60, 28,627 over an hour) as trains come back.
- **Passes, split at 20 minutes:** 45,323. Median 11 predictions per pass.
- **How close the last prediction was:** at most 60 s away in 21,989 passes (49%), at most 120 s in 40,374 (89%), at most 300 s in 43,309 (96%). The collector polls about every 2 minutes, so the last snapshot before an arrival is usually up to 2 minutes ahead of it.
- **Timetable rows:** `IS_REALTIME = false` rows have no train ID and no `DELAY`, so they can't be followed.

## Options considered
1. **One row per prediction.** That's silver again. It doesn't answer "did the train arrive on time".
2. **One row per train, station, direction and day.** Simple, but merges every pass of a day into one row.
3. **Split each train's predictions into passes at a gap, and take each pass's last prediction as the arrival.** Uses window functions only: `LAG` finds the gap, a running `SUM` numbers the passes, `ROW_NUMBER` picks the last prediction.

## Decision
Option 3.
- **A pass** is a run of realtime predictions for one `service_date × train_id × line × direction × station` with no gap over 20 minutes.
- **The arrival** is the pass's last prediction: latest `event_ts`, then the shortest wait when one snapshot lists the train twice.
- **Observed** means that last prediction was at most 120 s from arrival. Only observed passes get an `arrival_status`, using the bus window: at most 1 minute early and at most 5 minutes late on `DELAY` (ADR-0006).
- **Dimensions:** `line` joins `dim_route` (rail routes are named RED, GOLD, BLUE, GREEN), and the station joins `dim_stop` on the timetable station (`location_type = 1`). Six stations still go by older names in the realtime feed (for example `OMNI DOME STATION` is `SEC DISTRICT STATION` in the timetable), so a six-row lookup maps them. The other 32 match by name.
- **Daily KPIs** gain `rail_arrivals` and `rail_on_time_pct`. `rail_avg_delay_seconds` stays as it is.

## Consequences
- **What "arrival" means:** MARTA's last prediction within 2 minutes of arrival, not a measured arrival. Column comments say so.
- **Edge cases:** a train that waits over 20 minutes between predictions for the same station would count as two passes. The histogram shows such gaps are rare.
- **Unobserved passes** (about 11%) stay in the fact with `observed = false` and no status, so coverage is visible rather than silent.
- **New station names:** a station renamed in the timetable but not in the feed gets a null `stop_sk` until the lookup is updated.

## How I'd explain it in an interview
"The rail feed only has predictions, and trains go back and forth all day. So I split each train's predictions for a station into passes wherever there's a gap of over 20 minutes, which is where the data has a clear dip. The last prediction of a pass is the arrival, and I only count it as observed when the train was within 2 minutes, which is one polling interval. It's all window functions: LAG for the gap, a running sum to number the passes, and ROW_NUMBER for the last one."
