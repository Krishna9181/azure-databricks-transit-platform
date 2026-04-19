# MTA Real-Time Transit Pipeline — Project Context
**Last Updated**: April 19, 2026
**Owner**: saikrishnareddypoluri26@outlook.com
**Workspace**: https://adb-7405613547805720.0.azuredatabricks.net
**GitHub**: https://github.com/Krishna9181/azure-databricks-transit-platform

---

## Project Overview

End-to-end Azure data engineering project ingesting NYC MTA real-time subway GTFS-RT feeds through a medallion architecture (Bronze -> Silver -> Gold) on Databricks. Includes CI/CD with GitHub Actions, pipeline health monitoring, DLT (Lakeflow Spark Declarative Pipelines) for declarative ETL, Azure SQL export for Power BI, and table maintenance automation.

---

## Architecture

```
MTA GTFS-RT API --> Cosmos DB --> Event Hubs --> Databricks (Delta Lake) --> Azure SQL --> Power BI
                                                    |
                                          Bronze --> Silver --> Gold
                                                    |
                                    DLT Pipeline (Expectations + Lineage)
                                    Bronze ST --> Silver ST (Append Flows) --> Gold MV
```

### Two Pipeline Approaches (side by side)
| Aspect | Regular Pipeline (Notebooks 01-09) | DLT Pipeline (Lakeflow SDP) |
|---|---|---|
| Orchestration | Databricks Jobs (DAG) | DLT auto-resolves dependencies |
| Ingestion | Imperative (Python SDK, writeStream) | Declarative (@dp.table, readStream) |
| Deduplication | Window ROW_NUMBER + MERGE | Append Flows (multi-source fan-in) |
| Data Quality | Manual is_valid flag | Built-in expectations (WARN/DROP/FAIL) |
| State Management | Custom watermark table, checkpoints | DLT-managed checkpoints |
| Lineage | Implicit | Visual DAG + Unity Catalog lineage |

### Azure Services
| Service | Purpose |
|---|---|
| Databricks | Spark processing, Delta Lake, Unity Catalog, Jobs, DLT, Genie, AI/BI Dashboard |
| ADLS Gen2 | External table storage: `abfss://demo@adlsgen2deportfolioeus.dfs.core.windows.net/mta_rtransit/` |
| Cosmos DB | Raw event store (`gtfs` database, `gtfs_rt_batch` + `gtfs_rt_stream` containers) |
| Event Hubs | Real-time streaming via Kafka protocol |
| Key Vault | `mta-gtfs-rt-kv` vault, Databricks scope `mta-kv` |
| Application Insights | Telemetry for API calls, micro-batch metrics, route-level breakdowns |
| Azure SQL Database | Gold layer serving (`mta-sql-eus.database.windows.net / mta_analytics`) |
| GitHub Actions | CI/CD: push to main --> `databricks bundle deploy -t prod` |

---

## Workspace Structure

```
notebooks/
  01_setup_and_configs.py              # DDL for all schemas and tables
  02_mta_gtfs_rt_to_cosmos.py          # API poll --> Cosmos + Event Hubs (continuous)
  03_batch_cosmos_to_delta.py          # Cosmos --> Bronze Delta (incremental watermark)
  04_streaming_eventhubs_to_bronze.py  # Event Hubs --> Bronze Delta (Kafka streaming)
  04a_ingest_mta_route_ref.py          # MTA GTFS routes.txt --> Bronze reference
  05_silver_fact_trip_delay_event.py   # Both bronze --> Silver fact (dedup + MERGE)
  06_silver_dim_route.py               # Route dimension from GTFS ref (SCD2)
  07_gold_route_delay_kpi_daily.py     # Silver --> Gold KPIs + enriched BI view
  08_pipeline_health_monitor.py        # Health checks: freshness, jobs, data quality
  09_gold_to_azure_sql.py              # Gold --> Azure SQL via JDBC
  10_delta_table_maintenance.py        # OPTIMIZE, VACUUM, liquid clustering, ANALYZE (SQL)

dlt/transformations/
  eventhub_raw.py                      # Bronze ST (Event Hubs Delta source)
  cosmos_events.py                     # Bronze ST (Cosmos Delta source)
  fact_trip_delay_event.py             # Silver ST (Append Flows + expectations)
  dim_route.py                         # Silver MV (route dimension)
  route_delay_kpi_daily.py             # Gold MV (KPI aggregation)
  route_delay_kpi_enriched.py          # Gold MV (enriched join)
```

---

## Unity Catalog

### Catalogs
| Catalog | Environment | Storage |
|---|---|---|
| `mta_rtransit` | PROD | EXTERNAL (ADLS `/mta_rtransit/`) |
| `mta_rtransit_dev` | DEV | MANAGED |

### Regular Pipeline Tables
| Layer | Table | Type | Description |
|---|---|---|---|
| bronze | `gtfs_rt_events` | EXTERNAL | Raw GTFS-RT events from Cosmos |
| bronze | `eventhub_gtfs_raw` | EXTERNAL | Raw events from Event Hubs stream |
| bronze | `dim_route_ref` | EXTERNAL | MTA route reference snapshots |
| bronze | `_cosmos_watermarks` | EXTERNAL | Incremental load tracking |
| silver | `fact_trip_delay_event` | EXTERNAL | Deduped trip delay events |
| silver | `dim_route` | EXTERNAL | Route dimension (SCD Type 2) |
| gold | `route_delay_kpi_daily` | EXTERNAL | Daily aggregated KPIs per route |
| gold | `route_delay_kpi_enriched` | VIEW | KPIs joined with route names |

### DLT Pipeline Tables (`mta_rtransit.dlt`)
| Dataset | Type | Expectations | Source |
|---|---|---|---|
| `bronze_eventhub_raw` | Streaming Table | 2 WARN | `bronze.eventhub_gtfs_raw` |
| `bronze_cosmos_events` | Streaming Table | 3 WARN | `bronze.gtfs_rt_events` |
| `silver_fact_trip_delay_event` | Streaming Table | 4 DROP | Append Flows from both bronze |
| `silver_dim_route` | Materialized View | 2 FAIL | `silver.dim_route` |
| `gold_route_delay_kpi_daily` | Materialized View | 3 WARN | Silver fact |
| `gold_route_delay_kpi_enriched` | Materialized View | 1 WARN | Gold KPI + Silver dim (join) |

DLT tables are MANAGED by Unity Catalog in `mta_rtransit.dlt` — isolated from the EXTERNAL tables above.

---

## DLT Pipeline (Lakeflow Spark Declarative Pipelines)

### Configuration
| Setting | Value |
|---|---|
| Name | MTA Transit - DLT Medallion Pipeline |
| Catalog | `mta_rtransit` |
| Target Schema | `dlt` (MANAGED) |
| Compute | Serverless, Photon enabled |
| Mode | Triggered (batch) |
| Channel | Preview |
| Management | DABs-managed (created on deploy) |

### Patterns Demonstrated
| Pattern | File | How |
|---|---|---|
| Streaming from Delta | `eventhub_raw.py`, `cosmos_events.py` | `spark.readStream.table()` from existing bronze |
| Append Flows | `fact_trip_delay_event.py` | `dp.create_streaming_table()` + `@dp.append_flow()` for multi-source fan-in |
| Expectations (WARN) | Bronze files | Log quality issues, keep all data |
| Expectations (DROP) | Silver fact | Drop invalid records |
| Expectations (FAIL) | Silver dim | Halt pipeline if dimension data is broken |
| Materialized Views | Gold files | Auto-recompute aggregations and joins |

### Serverless Limitations
- `cosmos.oltp.changeFeed` streaming NOT supported on serverless
- Azure Key Vault-backed secrets do NOT resolve via `spark.conf.get()` on serverless
- Workaround: regular pipeline (notebooks 03/04) handles raw ingestion, DLT reads from existing Delta tables

### Data Flow
```
mta_rtransit.bronze.gtfs_rt_events (EXTERNAL)
        | readStream
        v
bronze_cosmos_events (ST, 3 WARN)
        | Append Flow: from_cosmos_batch
        v
silver_fact_trip_delay_event (ST, 4 DROP) ----> gold_route_delay_kpi_daily (MV, 3 WARN)
        ^                                              |
        | Append Flow: from_eh_stream                  v
bronze_eventhub_raw (ST, 2 WARN)          gold_route_delay_kpi_enriched (MV, 1 WARN)
        ^                                              ^
        | readStream                                   | JOIN
mta_rtransit.bronze.eventhub_gtfs_raw     silver_dim_route (MV, 2 FAIL)
        (EXTERNAL)                                     ^
                                                       | spark.read
                                          mta_rtransit.silver.dim_route (EXTERNAL)
```

---

## Jobs (DABs-managed)

All jobs deploy to `/Shared/azure-databricks-transit-platform/prod/`.

| Job | Schedule | Notebooks |
|---|---|---|
| MTA Ingestion - Continuous API Poll | PAUSED | 02 |
| MTA Streaming - Event Hubs to Bronze | PAUSED | 04 |
| MTA Transit Pipeline - Daily Batch | Daily 6 AM ET | 03 -> 04a -> 05 -> 06 -> 07 -> 09 |
| MTA Pipeline Health Monitor | Every 6 hours | 08 |
| MTA Delta Table Maintenance - Weekly | Sunday 2 AM ET | 10 |
| MTA DLT Medallion Pipeline - Daily Refresh | Daily 7 AM ET | Pipeline task (incremental) |

### Daily Batch DAG
```
03_batch_cosmos_to_delta
  --> 04a_ingest_mta_route_ref
    --> 05_silver_fact_trip_delay_event
      --> 06_silver_dim_route
        --> 07_gold_route_delay_kpi_daily
          --> 09_gold_to_azure_sql
```

---

## CI/CD

### Deployment Flow
```
feature branch --> Pull Request --> main --> GitHub Actions --> databricks bundle deploy -t prod
```

### DABs Configuration (`databricks.yml`)
| Variable | Dev | Prod |
|---|---|---|
| `catalog` | `mta_rtransit_dev` | `mta_rtransit` |
| `cluster_id` | `0417-222320-e1j7vfs2` | same |
| Target root | Default (dev mode) | `/Shared/azure-databricks-transit-platform/prod/` |

### GitHub Secrets Required
- `DATABRICKS_HOST` — workspace URL
- `DATABRICKS_TOKEN` — personal access token

---

## Key Vault Secrets (scope: mta-kv)

| Secret | Service |
|---|---|
| `cosmos-endpoint`, `cosmos-key` | Cosmos DB |
| `eventhub-connection-string` | Event Hubs |
| `appinsights-connection-string` | Application Insights |
| `sql-server-host`, `sql-server-user`, `sql-server-password`, `sql-database-name` | Azure SQL |

---

## Azure SQL Database

Server: `mta-sql-eus.database.windows.net`
Database: `mta_analytics`

| Source (Delta) | Target (Azure SQL) | Mode |
|---|---|---|
| `gold.route_delay_kpi_daily` | `dbo.route_delay_kpi_daily` | Overwrite (daily) |
| `silver.dim_route` (active) | `dbo.dim_route` | Overwrite |
| `gold.route_delay_kpi_enriched` | `dbo.route_delay_kpi_enriched` | Overwrite |

---

## Table Maintenance (Notebook 10)

Runs weekly (Sunday 2 AM ET) against all 7 EXTERNAL tables. DLT managed tables are auto-optimized.

| Operation | Purpose |
|---|---|
| CLUSTER BY (liquid clustering) | Replaces legacy ZORDER — set once, OPTIMIZE auto-applies |
| OPTIMIZE | Compact small files + apply clustering incrementally |
| VACUUM (168h retention) | Remove stale files no longer in Delta log |
| ANALYZE TABLE | Refresh optimizer statistics for better query plans |

### Liquid Clustering Enabled
| Table | Cluster Keys |
|---|---|
| `bronze.gtfs_rt_events` | `route_id`, `ingested_at` |
| `bronze.eventhub_gtfs_raw` | `enqueued_time` |
| `silver.fact_trip_delay_event` | `route_id`, `event_ts` |

---

## Documentation

- **Blog Post**: `docs/MEDIUM_BLOG_POST.md` — full architecture walkthrough with Mermaid diagrams (for Medium publication)
- **README**: Professional README with Mermaid diagrams rendered natively by GitHub
- **PROJECT_CONTEXT.md**: This file — detailed project documentation for contributors

---

## Design Decisions

1. **External tables on ADLS** — full control over storage layout, survives catalog drops
2. **Watermark-based incremental load** — avoids re-reading entire Cosmos container
3. **Kafka protocol for Event Hubs** — no external Maven JARs, built into Spark
4. **DLT reads from existing Delta** — serverless can't access Key Vault or Cosmos directly
5. **Append Flows over UNION + MERGE** — native multi-source fan-in, DLT-managed dedup
6. **Expectations strategy** — WARN at bronze (keep everything), DROP at silver fact (clean data), FAIL at dimension (must be complete)
7. **Liquid clustering over ZORDER** — incremental, adaptive, no full-table rewrites
8. **DLT in separate managed schema** — `mta_rtransit.dlt` isolates managed tables from external ones
9. **Separate DLT job** — runs 1 hour after daily batch to ensure fresh bronze data
10. **Azure SQL export** — faster DirectQuery for Power BI vs hitting Delta directly

---

## Completed Phases

| Phase | Description |
|---|---|
| Phase 1 | Security and Observability (Key Vault, Application Insights) |
| Phase 2 | Analytics and BI (Gold layer, Genie space, AI/BI Dashboard, Power BI) |
| Phase 3 | CI/CD and DevOps (GitHub repo, DABs, GitHub Actions, dev/prod catalogs) |
| Phase 3.5 | Azure SQL (Gold to Azure SQL export via JDBC) |
| Phase 3.6 | DLT Pipeline (Lakeflow SDP with expectations, Append Flows, serverless) |
| Phase 3.7 | Table Optimization (OPTIMIZE, VACUUM, liquid clustering, ANALYZE) |
| Phase 3.8 | Workspace cleanup, professional README with Mermaid diagrams, dashboard to Git |
| Phase 3.9 | Medium blog post, documentation, cost shutdown |
| Phase 4 | ML and Advanced Analytics (upcoming — delay prediction, MLflow, Model Serving) |
