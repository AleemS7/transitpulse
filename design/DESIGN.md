# TransitPulse ATL: design

How the system is put together and why. Detailed requirements are in `specs/`, decisions in `adr/`, and operating steps in `runbooks/`.

## Architecture

```mermaid
flowchart LR
    subgraph MARTA["MARTA open data"]
        BUS["Bus GPS + trip updates<br/>(GTFS-realtime protobuf)"]
        RAIL["Rail arrivals<br/>(JSON API)"]
        GTFS["Schedule<br/>(GTFS zip)"]
    end

    subgraph AZ["Azure"]
        FN["Azure Function<br/>every 2 min, daily schedule"]
        DL[("Dead-letter blobs<br/>replayed next run")]
    end

    subgraph DBX["Databricks (serverless, Unity Catalog)"]
        LAND[("Landing volume<br/>30 days")]
        BR["Bronze<br/>raw rows + file metadata"]
        SV["Silver<br/>typed, deduplicated, rules + quarantine"]
        GD["Gold<br/>star schema + metrics"]
        OPS["Ops<br/>runs, freshness, quality, health checks, incidents"]
        JOB["Hourly job<br/>extract schedule, refresh, checks, triage"]
    end

    subgraph USE["Consumers"]
        DASH["AI/BI dashboard"]
        GENIE["Genie space"]
        API["FastAPI on Databricks Apps"]
    end

    BUS & RAIL & GTFS --> FN
    FN -- "Files API" --> LAND
    FN -. "upload failed" .-> DL -. replay .-> LAND
    LAND -- "Auto Loader" --> BR --> SV --> GD
    BR & SV --> OPS
    JOB --> BR
    GD --> DASH & GENIE & API
    OPS --> API
```

| Hop | Safety net |
|---|---|
| MARTA → collector | Retries with backoff, size caps, a heartbeat per run (ADR-0001) |
| Collector → landing | Content-hash file names and check-before-upload (no duplicates), dead-letter and replay |
| Landing → bronze | Auto Loader reads only new files. Bronze can't be fully refreshed, because landing keeps only 30 days (ADR-0009) |
| Bronze → silver | Explicit schemas. Rules send failing rows to quarantine tables with the reason. Auto CDC keeps one row per key |
| Silver → gold | Materialized views with declared columns, primary and foreign keys, and a column comment on every column |
| Every hour | Health checks per component. A failure emails, and a triage step proposes an incident for a person to acknowledge (ADR-0007, ADR-0008) |

## Data model (gold)

Two fact tables, one per mode: a single observed bus arrival, and a single train passing a station. Three shared dimensions sit around them, and metric tables are built from the facts.

```mermaid
erDiagram
    dim_date ||--o{ fact_stop_arrival : "date_key"
    dim_route ||--o{ fact_stop_arrival : "route_sk"
    dim_stop ||--o{ fact_stop_arrival : "stop_sk"
    dim_date ||--o{ fact_rail_arrival : "date_key"
    dim_route ||--o{ fact_rail_arrival : "route_sk"
    dim_stop ||--o{ fact_rail_arrival : "stop_sk"
    dim_date ||--o{ ghost_trips : "date_key"
    dim_route ||--o{ ghost_trips : "route_sk"
    dim_date ||--o{ daily_kpis : "date_key"

    fact_stop_arrival {
        string trip_id PK
        date service_date PK
        int stop_sequence PK
        int date_key FK
        bigint route_sk FK
        bigint stop_sk FK
        timestamp arrival_ts
        timestamp scheduled_arrival_ts
        int delay_seconds
        string arrival_status
    }
    fact_rail_arrival {
        date service_date PK
        string train_id PK
        string line PK
        string direction PK
        string station PK
        timestamp first_seen_ts PK
        int date_key FK
        bigint route_sk FK
        bigint stop_sk FK
        timestamp arrival_ts
        int delay_seconds
        boolean observed
        string arrival_status
    }
    dim_date {
        int date_key PK
        date calendar_date
        string day_name
        boolean is_weekend
        boolean is_schedule_exception
    }
    dim_route {
        bigint route_sk PK
        string route_id
        string route_short_name
        date valid_from
        date valid_to
        boolean is_current
    }
    dim_stop {
        bigint stop_sk PK
        string stop_id
        string stop_name
        double stop_lat
        double stop_lon
        date valid_from
        date valid_to
    }
    ghost_trips {
        date service_date PK
        string trip_id PK
        int date_key FK
        bigint route_sk FK
        string reason
    }
    daily_kpis {
        date service_date PK
        int date_key FK
        double bus_on_time_pct
        double ghost_trip_pct
        double bunched_pct
        double rail_on_time_pct
    }
```

- **Dimensions keep history (SCD Type 2).** `dim_route` and `dim_stop` get a new row, with a new surrogate key, whenever a schedule version changes a route or stop. The fact joins the version that was in effect on its service day (ADR-0004).
- **Keys are declared, not enforced.** Unity Catalog primary and foreign keys document the model for BI tools and Genie. The keys were checked against prod data before they were declared: every primary key is unique and not null, and no fact row points at a missing dimension row.
- **On time** means at most 1 minute early and at most 5 minutes late (ADR-0006), for both modes.
- **Rail arrivals are inferred.** The rail feed only has predictions, so each train's predictions for a station are split into passes at a 20-minute gap, and a pass's last prediction is the arrival when it was within 2 minutes (ADR-0011).

| Gold table | Grain (primary key) | Built from |
|---|---|---|
| `fact_stop_arrival` | trip × service day × stop sequence | silver arrivals placed on the timetable in effect |
| `fact_rail_arrival` | service day × train × line × direction × station × pass start | silver rail predictions, split into passes (ADR-0011) |
| `dim_date` | calendar date | generated, with schedule exceptions |
| `dim_route`, `dim_stop` | one version of a route or stop | schedule versions (SCD Type 2) |
| `otp_route_hour` | service day × route × Atlanta hour | fact |
| `stop_delay` | service day × stop | fact + `dim_stop` |
| `headway_bunching` | service day × trip × stop sequence | fact, with `LAG` for the previous bus |
| `ghost_trips` | service day × trip | timetable vs. positions and arrivals |
| `daily_kpis` | service day | the tables above, including both facts |
| `rail_line_health` | service day × line × direction × station × hour | silver rail predictions |

## Layers

| Schema | Tables |
|---|---|
| `bronze` | `bus_trip_updates`, `bus_vehicle_positions`, `rail_arrivals`, `collector_heartbeats`, `gtfs_*` (one per schedule file) |
| `silver` | `bus_trips`, `bus_vehicle_positions`, `bus_stop_arrivals`, `rail_arrivals`, `gtfs_*`, `gtfs_feed_versions`, `bus_scheduled_trips`, plus a `_quarantine` table for each realtime dataset |
| `gold` | the tables above |
| `ops` | `collector_runs`, `feed_freshness`, `dq_metrics`, `pipeline_events`, `health_checks`, `incidents`, `landing_retention` |

Dev uses the same names with a `dev_` schema prefix. Both environments are deployed from one Asset Bundle (`databricks.yml`, `resources/`). A merge to `main` deploys prod.
