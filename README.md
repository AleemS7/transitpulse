# TransitPulse ATL

How reliable is Atlanta transit, route by route and hour by hour? MARTA publishes live bus and rail feeds and a schedule, but no reliability numbers. This project collects the live feeds, compares them with the schedule in a bronze/silver/gold lakehouse on Databricks, and turns the results into on-time metrics, delay predictions, a REST API and self-monitoring.

**Stack:** Python · PySpark · SQL · Databricks (Lakeflow, Unity Catalog, MLflow) · Azure Functions · FastAPI

## Status

| Piece | State |
|---|---|
| Collector (Azure Function → Databricks volume) | Live, collecting every 2 minutes since 2026-10-15 |
| Bronze / silver / gold pipeline | Next |
| Dashboard, API, delay model, ops monitoring | Planned |

## Layout

```
collector/   Azure Function that polls the feeds and lands raw files
design/      ADRs, specs and runbooks
infra/       Bicep for the Azure resources
sql/         Setup and analysis SQL
scripts/     check.ps1 runs every local check
```

## Data

Uses MARTA open data from MARTA's [App Developer Resources](https://itsmarta.com/app-developer-resources.aspx). This project isn't affiliated with or endorsed by MARTA.
