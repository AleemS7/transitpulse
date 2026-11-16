# TransitPulse ATL

[![PR validation](https://github.com/AleemS7/transitpulse/actions/workflows/pr-validation.yml/badge.svg)](https://github.com/AleemS7/transitpulse/actions/workflows/pr-validation.yml)

How reliable is Atlanta transit, route by route and hour by hour? MARTA publishes live bus and rail feeds and a schedule, but no reliability numbers. This project collects the live feeds, compares them with the schedule in a bronze/silver/gold lakehouse on Databricks, and serves the results as on-time metrics through a dashboard, a Genie space and a REST API. It monitors itself hourly, and an LLM triage step proposes incidents for a person to acknowledge.

**Stack:** Python · PySpark · SQL · Databricks (Lakeflow, Unity Catalog, AI/BI, Genie, Apps) · Azure Functions · FastAPI

## What the data shows

From 2026-10-15 to 2026-11-15 (32 service days, 4.76 million observed bus stop arrivals):

- **On time:** 61.0% of bus arrivals were on time, meaning at most 1 minute early and at most 5 minutes late.
- **Cancellations:** MARTA cancelled between 319 and 1,077 scheduled bus trips a day.
- **Weakest routes:** among routes with over 20,000 observed arrivals, route 165 was the least punctual, on time for 33% of arrivals.
- **Realtime coverage:** 11 of 81 bus routes never appear in MARTA's realtime feed. Trips with no realtime trace are reported separately as `no_bus_seen`, not counted as cancelled.

## How it works

```
MARTA feeds ──► Azure Function (every 2 min) ──► Databricks landing volume
                 retries, dedup, heartbeats,        │ Auto Loader
                 dead-letter + replay               ▼
                                     bronze ──► silver ──► gold (star schema + metrics)
                                       │  rules + quarantine      │
                                       ▼                          ├──► AI/BI dashboard
                              ops: freshness, quality,            ├──► Genie space
                              hourly health checks ──► triage     └──► FastAPI (Databricks Apps)
```

The full diagram, the data model and the safety net at each hop are in [`design/DESIGN.md`](design/DESIGN.md). Decisions are recorded as ADRs in [`design/adr/`](design/adr/), and failure handling in [`design/runbooks/`](design/runbooks/).

## Status

| Piece | State |
|---|---|
| Collector (Azure Function → Databricks volume) | Live, collecting every 2 minutes since 2026-10-15 |
| Rail: bronze → silver → `gold.rail_line_health` | Live, refreshed hourly |
| Schedule (GTFS, versioned) → `dim_date`, `dim_route`, `dim_stop` (SCD Type 2) | Live, refreshed hourly |
| Bus: protobuf → positions, trip statuses, observed stop arrivals | Live, refreshed hourly |
| Bus gold: `fact_stop_arrival`, on-time by route and hour, stop delay, bunching, ghost trips, daily KPIs | Live, refreshed hourly |
| Gold contract: declared columns, comments, primary and foreign keys, checked by tests | Built |
| Ops: collector runs, feed freshness, data-quality metrics, hourly health checks with email | Live |
| Landing retention: raw files deleted after 30 days once bronze holds them, bronze locked against full refresh | Built, reporting only until deletion is switched on |
| AI/BI dashboard, Genie space, FastAPI on Databricks Apps | Live |
| Incident triage: tool-calling agent with a rules fallback, incidents acknowledged through the API, MLflow evaluation | Built |

## Layout

```
collector/   Azure Function that polls the feeds and lands raw files
src/         Pipeline code: pure transforms (tested locally), thin Lakeflow pipeline files, and the triage agent
api/         FastAPI app over gold and incidents, deployed as a Databricks App
dashboards/  AI/BI dashboard definition
resources/   Databricks Asset Bundle definitions (pipeline, jobs, app, dashboard, Genie space)
tests/       Unit tests on local PySpark with synthetic edge cases
design/      Design overview, ADRs, specs and runbooks
infra/       Bicep for the Azure resources
sql/         One-time setup SQL (schemas, volumes) and analysis queries
samples/     Example outputs: gold rows, an incident record
scripts/     check.ps1 runs every local check
```

## Run the checks

```powershell
./scripts/check.ps1
```

Needs uv, Java 17 (for local Spark), the Azure CLI and the Databricks CLI. Pull requests run the same script in GitHub Actions (`-SkipBundle`, since bundle validation needs a workspace sign-in).

## Run your own

Everything runs on Databricks Free Edition plus an Azure subscription for the Function and its storage account.

1. **Databricks:** run [`sql/setup/landing.sql`](sql/setup/landing.sql) and [`sql/setup/schemas.sql`](sql/setup/schemas.sql) once, then deploy the bundle with your own CLI profile:
   ```bash
   databricks bundle deploy -t dev
   ```
2. **Collector:** deploy the Azure Function as described in [`collector/README.md`](collector/README.md). The rail feed needs a MARTA API key.
3. **CI/CD:** set `DATABRICKS_HOST` and the Azure IDs as GitHub variables, and put the Databricks token in a `prod` environment secret. A merge to `main` then deploys prod (`.github/workflows/deploy.yml`).

## Data

Uses MARTA open data from MARTA's [App Developer Resources](https://itsmarta.com/app-developer-resources.aspx). This project isn't affiliated with or endorsed by MARTA.
