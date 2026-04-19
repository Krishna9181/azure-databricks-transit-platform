# MTA Real-Time Transit Pipeline

End-to-end Azure data engineering pipeline ingesting NYC MTA subway GTFS-RT feeds through a medallion architecture on Databricks with CI/CD, monitoring, DLT, and BI integration.

## Architecture

```
MTA GTFS-RT API
      |
      v
+-----+------+
|  Cosmos DB  |-----> Notebook 03 (batch) -----> Bronze Delta
+-----+------+
      |
      v
+-----+------+
| Event Hubs  |-----> Notebook 04 (stream) ----> Bronze Delta
+-------------+
                                                      |
                                                      v
                              +-----------+     +-----------+     +-----------+
                              |  Bronze   | --> |  Silver   | --> |   Gold    |
                              +-----------+     +-----------+     +-----------+
                                                      |
                                                      v
                              +---------------------------------------------------+
                              |  DLT Pipeline (Lakeflow Spark Declarative Pipelines)  |
                              |  Bronze ST --> Silver ST (Append Flows) --> Gold MV   |
                              |  Expectations: WARN / DROP / FAIL                     |
                              +---------------------------------------------------+
                                                      |
                                                      v
                                              +-------+--------+
                                              |  Azure SQL DB  | --> Power BI
                                              +----------------+
```

## Azure Services

| Service | Purpose |
|---------|---------|
| ADLS Gen2 | Delta Lake storage (external tables) |
| Cosmos DB | Raw event store from MTA API |
| Event Hubs | Real-time streaming ingestion (Kafka protocol) |
| Key Vault | Secrets management (scope: `mta-kv`) |
| Application Insights | Telemetry for API calls and micro-batches |
| Azure SQL Database | Gold layer serving for Power BI DirectQuery |
| GitHub Actions | CI/CD with Databricks Asset Bundles |

## Databricks Components

| Component | Purpose |
|-----------|---------|
| Delta Lake | Medallion architecture (bronze / silver / gold) |
| Unity Catalog | Governance, lineage, external + managed tables |
| Structured Streaming | Event Hubs to Bronze via Kafka protocol |
| Lakeflow SDP (DLT) | Declarative ETL with expectations, Append Flows |
| Jobs | 4 scheduled + 2 continuous (paused) |
| AI/BI Dashboard | Published operations dashboard |
| Genie Space | Self-service analytics for analysts |

## Notebooks

| # | Notebook | Layer | Purpose |
|---|----------|-------|---------|
| 01 | `setup_and_configs` | — | DDL for all tables, schemas, external locations |
| 02 | `mta_gtfs_rt_to_cosmos` | Ingest | Continuous API poll -> Cosmos DB + Event Hubs |
| 03 | `batch_cosmos_to_delta` | Bronze | Incremental Cosmos -> Delta (watermark-based) |
| 04 | `streaming_eventhubs_to_bronze` | Bronze | Continuous EH -> Delta (Kafka, foreachBatch) |
| 04a | `ingest_mta_route_ref` | Bronze | MTA GTFS static routes.txt -> dim_route_ref |
| 05 | `silver_fact_trip_delay_event` | Silver | Union both bronze, dedup, MERGE into fact |
| 06 | `silver_dim_route` | Silver | Route dimension from GTFS ref + fallback (SCD2) |
| 07 | `gold_route_delay_kpi_daily` | Gold | Daily KPIs per route + enriched BI view |
| 08 | `pipeline_health_monitor` | Ops | Freshness checks, job health, data quality |
| 09 | `gold_to_azure_sql` | Export | Gold -> Azure SQL Database via JDBC |
| 10 | `delta_table_maintenance` | Ops | OPTIMIZE, VACUUM, liquid clustering, ANALYZE |

## DLT Pipeline (Lakeflow Spark Declarative Pipelines)

Runs alongside the regular notebook pipeline. Reads from existing bronze Delta tables and applies declarative transformations with built-in data quality expectations.

| Dataset | Type | Expectations | Source |
|---------|------|-------------|--------|
| `bronze_eventhub_raw` | Streaming Table | 2 WARN | `bronze.eventhub_gtfs_raw` |
| `bronze_cosmos_events` | Streaming Table | 3 WARN | `bronze.gtfs_rt_events` |
| `silver_fact_trip_delay_event` | Streaming Table | 4 DROP | Append Flows from both bronze |
| `silver_dim_route` | Materialized View | 2 FAIL | `silver.dim_route` |
| `gold_route_delay_kpi_daily` | Materialized View | 3 WARN | Silver fact |
| `gold_route_delay_kpi_enriched` | Materialized View | 1 WARN | Gold KPI + Silver dim (join) |

Key patterns: `dp.create_streaming_table()` + `@dp.append_flow()` for multi-source fan-in, expectations at every layer, serverless compute.

## Jobs

| Job | Schedule | Type |
|-----|----------|------|
| Continuous API Poll | PAUSED | Continuous (notebook 02) |
| Event Hubs Streaming | PAUSED | Continuous (notebook 04) |
| Daily Batch Pipeline | 6:00 AM ET | DAG: 03 -> 04a -> 05 -> 06 -> 07 -> 09 |
| Pipeline Health Monitor | Every 6 hours | Notebook 08 |
| Delta Table Maintenance | Sunday 2 AM ET | Notebook 10 (SQL) |
| DLT Medallion Refresh | 7:00 AM ET | Pipeline task (incremental) |

## Unity Catalog

```
mta_rtransit (prod)                     mta_rtransit_dev (dev)
|                                       |
+-- bronze                              +-- bronze
|   +-- gtfs_rt_events                  |   +-- (same tables, managed storage)
|   +-- eventhub_gtfs_raw               |
|   +-- dim_route_ref                   +-- silver
|   +-- _cosmos_watermarks              |   +-- ...
|                                       |
+-- silver                              +-- gold
|   +-- fact_trip_delay_event           |   +-- ...
|   +-- dim_route                       |
|                                       +-- dlt
+-- gold                                    +-- (DLT managed tables)
|   +-- route_delay_kpi_daily
|   +-- route_delay_kpi_enriched (view)
|
+-- dlt
    +-- bronze_eventhub_raw (ST)
    +-- bronze_cosmos_events (ST)
    +-- silver_fact_trip_delay_event (ST)
    +-- silver_dim_route (MV)
    +-- gold_route_delay_kpi_daily (MV)
    +-- gold_route_delay_kpi_enriched (MV)
```

## Deployment

Uses Databricks Asset Bundles (DABs) with GitHub Actions CI/CD.

```
feature branch -> Pull Request -> main -> GitHub Actions -> databricks bundle deploy -t prod
```

Production assets deploy to `/Shared/azure-databricks-transit-platform/prod/`.

```bash
# Manual deployment (if needed)
databricks bundle deploy -t dev    # Development
databricks bundle deploy -t prod   # Production
```

## Project Structure

```
azure-databricks-transit-platform/
+-- .github/workflows/deploy.yml       # CI/CD pipeline
+-- databricks.yml                      # DABs configuration (jobs, pipeline, targets)
+-- PROJECT_CONTEXT.md                  # Detailed project documentation
+-- README.md
+-- .gitignore
|
+-- notebooks/
|   +-- 01_setup_and_configs.py
|   +-- 02_mta_gtfs_rt_to_cosmos.py
|   +-- 03_batch_cosmos_to_delta.py
|   +-- 04_streaming_eventhubs_to_bronze.py
|   +-- 04a_ingest_mta_route_ref.py
|   +-- 05_silver_fact_trip_delay_event.py
|   +-- 06_silver_dim_route.py
|   +-- 07_gold_route_delay_kpi_daily.py
|   +-- 08_pipeline_health_monitor.py
|   +-- 09_gold_to_azure_sql.py
|   +-- 10_delta_table_maintenance.py
|
+-- dlt/transformations/
    +-- eventhub_raw.py                 # Bronze ST (Event Hubs source)
    +-- cosmos_events.py                # Bronze ST (Cosmos source)
    +-- fact_trip_delay_event.py         # Silver ST (Append Flows)
    +-- dim_route.py                    # Silver MV (route dimension)
    +-- route_delay_kpi_daily.py        # Gold MV (KPI aggregation)
    +-- route_delay_kpi_enriched.py     # Gold MV (enriched join)
```
