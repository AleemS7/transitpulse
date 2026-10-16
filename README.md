# TransitPulse ATL

[![PR validation](https://github.com/AleemS7/transitpulse/actions/workflows/pr-validation.yml/badge.svg)](https://github.com/AleemS7/transitpulse/actions/workflows/pr-validation.yml)

How reliable is Atlanta transit, route by route and hour by hour? MARTA publishes live bus and rail feeds and a schedule, but no reliability numbers. This project collects the live feeds, compares them with the schedule in a bronze/silver/gold lakehouse on Databricks, and turns the results into on-time metrics, delay predictions, a REST API and self-monitoring.

**Stack:** Python · PySpark · SQL · Databricks (Lakeflow, Unity Catalog, MLflow) · Azure Functions · FastAPI

## Status

| Piece | State |
|---|---|
| Collector (Azure Function → Databricks volume) | Live, collecting every 2 minutes since 2026-10-15 |
| Rail: bronze → silver → `gold.rail_line_health` | Live, refreshed hourly |
| Bus and schedule pipeline | Next |
| Dashboard, API, delay model, ops monitoring | Planned |

## Layout

```
collector/   Azure Function that polls the feeds and lands raw files
src/         Pipeline code: pure transforms (tested locally) and thin Lakeflow pipeline files
resources/   Databricks Asset Bundle definitions (pipeline, jobs); databricks.yml at the root
tests/       Unit tests on local PySpark with synthetic edge cases
design/      ADRs, specs and runbooks
infra/       Bicep for the Azure resources
sql/         One-time setup SQL (schemas, landing volume)
scripts/     check.ps1 runs every local check
```

## Run the checks

```powershell
./scripts/check.ps1
```

Needs uv, Java 17 (for local Spark), the Azure CLI and the Databricks CLI. Pull requests run the same script in GitHub Actions (`-SkipBundle`, since bundle validation needs a workspace sign-in).

## Data

Uses MARTA open data from MARTA's [App Developer Resources](https://itsmarta.com/app-developer-resources.aspx). This project isn't affiliated with or endorsed by MARTA.
