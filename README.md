# TransitPulse ATL

[![PR validation](https://github.com/AleemS7/transitpulse/actions/workflows/pr-validation.yml/badge.svg)](https://github.com/AleemS7/transitpulse/actions/workflows/pr-validation.yml)

How reliable is Atlanta transit, route by route and hour by hour? MARTA publishes live bus and rail feeds and a schedule, but no reliability numbers. This project collects the live feeds, compares them with the schedule in a bronze/silver/gold lakehouse on Databricks, and serves the results as on-time metrics through a dashboard, a Genie space and a REST API, with hourly self-monitoring.

**Stack:** Python · PySpark · SQL · Databricks (Lakeflow, Unity Catalog, AI/BI, Genie, Apps) · Azure Functions · FastAPI

## Status

| Piece | State |
|---|---|
| Collector (Azure Function → Databricks volume) | Live, collecting every 2 minutes since 2026-10-15 |
| Rail: bronze → silver → `gold.rail_line_health` | Live, refreshed hourly |
| Schedule (GTFS, versioned) → `dim_date`, `dim_route`, `dim_stop` (SCD Type 2) | Live, refreshed hourly |
| Bus: protobuf → positions, trip statuses, observed stop arrivals | Live, refreshed hourly |
| Bus gold: `fact_stop_arrival`, on-time by route and hour, stop delay, bunching, ghost trips, daily KPIs | Live, refreshed hourly |
| Ops: collector runs, feed freshness, data-quality metrics, hourly health checks with email | Live |
| AI/BI dashboard, Genie space, FastAPI on Databricks Apps | Deployed with the bundle |
| Incident triage agent | Planned |

## Layout

```
collector/   Azure Function that polls the feeds and lands raw files
src/         Pipeline code: pure transforms (tested locally) and thin Lakeflow pipeline files
api/         Read-only FastAPI app over gold, deployed as a Databricks App
dashboards/  AI/BI dashboard definition
resources/   Databricks Asset Bundle definitions (pipeline, job, app, dashboard, Genie space)
tests/       Unit tests on local PySpark with synthetic edge cases
design/      ADRs, specs and runbooks
infra/       Bicep for the Azure resources
sql/         One-time setup SQL (schemas, volumes) and analysis queries
scripts/     check.ps1 runs every local check
```

## Run the checks

```powershell
./scripts/check.ps1
```

Needs uv, Java 17 (for local Spark), the Azure CLI and the Databricks CLI. Pull requests run the same script in GitHub Actions (`-SkipBundle`, since bundle validation needs a workspace sign-in).

## Data

Uses MARTA open data from MARTA's [App Developer Resources](https://itsmarta.com/app-developer-resources.aspx). This project isn't affiliated with or endorsed by MARTA.
