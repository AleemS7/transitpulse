# ADR-0001: An external collector in Azure Functions pushes feeds into Databricks

- **Status:** Proposed
- **Date:** 2026-10-14
- **Related:** COL-001 to COL-007, SPK-001, SPK-002, `design/specs/collector.md`

## Context
MARTA publishes live bus and rail feeds but no history, so data exists only from the moment something starts polling. The feeds update about every 30 seconds and need polling every ~2 minutes, around the clock.

Databricks Free Edition documents outbound internet from serverless compute as limited to trusted domains, and it has a daily compute quota: going over shuts compute off for the rest of the day. A spike on 2026-10-14 found that serverless compute *could* reach the bus feed and the rail API on port 18096 (HTTP 200), so egress isn't blocked today. It may still be restricted later, without notice.

## Options considered
1. **Azure Function on Flex Consumption (timer, every 2 minutes)** pushing files through the Databricks Files API.
   - Pros: runs inside the free grant (about 21.6k executions and 45k GB-s a month at 512 MB); uses none of the Databricks quota; doesn't depend on Databricks egress policy; keeps working while Databricks is down (dead-letter and replay).
   - Cons: a second cloud account to run, and a token to manage.
2. **A Databricks job polling MARTA directly.**
   - Pros: one platform.
   - Cons: 720 serverless job runs a day, each with start-up overhead, against a quota that also has to cover the pipeline, dashboards and ML. It breaks if the documented egress restriction starts being enforced. Collection stops whenever the workspace is down or over quota.
3. **GitHub Actions cron.**
   - Pros: free.
   - Cons: the minimum interval is 5 minutes and isn't guaranteed; scheduled runs are delayed or dropped at busy times.

## Decision
Option 1. The collector is the only component that can't be re-run later, so it runs on infrastructure independent of the platform it feeds. Option 2 is the documented fallback if Azure becomes unavailable. Option 3 is dropped.

## Consequences
- Databricks never calls MARTA, so pipeline code has no network dependencies and is fully testable offline.
- A Databricks token lives in Key Vault and is tracked for expiry. Free Edition has no account console, so there is no OAuth federation alternative.
- Landing files are the integration contract between the two sides: names, gzip and folder layout are versioned in the collector spec.

## How I'd explain it in an interview
"The collector is the one piece you can't replay, so I ran it outside the data platform on a serverless Function. It pushes files into a landing zone, survives Databricks outages with a dead-letter and replay, and never touches the compute quota the pipeline needs. It's the same collector, landing zone and canonical model pattern a utility uses for meter telemetry."
