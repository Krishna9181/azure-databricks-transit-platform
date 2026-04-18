# Azure Databricks Transit Platform

Real-time NYC subway transit data pipeline on Azure + Databricks.

## Architecture
```
MTA GTFS-RT API -> Cosmos DB / Event Hubs -> Bronze -> Silver -> Gold -> BI
```

### Azure Services
| Service | Purpose |
|---------|---------|
| ADLS Gen2 | Delta Lake storage |
| Cosmos DB | Interim document store |
| Event Hubs | Real-time message broker |
| Key Vault | Secret management |
| Application Insights | Telemetry & monitoring |

### Databricks Components
| Component | Purpose |
|-----------|---------|
| Delta Lake | Medallion architecture (bronze/silver/gold) |
| Structured Streaming | Event Hubs to Bronze (continuous) |
| Jobs | 2 continuous + 1 daily batch + 1 health monitor |
| AI/BI Dashboard | Published operations dashboard |
| Genie Space | Self-service analytics |
| Unity Catalog | Governance & metadata |

## Notebooks
| # | Notebook | Purpose |
|---|----------|---------|
| 01 | setup_and_configs | DDL for all tables |
| 02 | mta_gtfs_rt_to_cosmos | Continuous API poll -> Cosmos + Event Hubs |
| 03 | batch_cosmos_to_delta | Incremental Cosmos -> Bronze |
| 04 | streaming_eventhubs_to_bronze | Continuous EH -> Bronze |
| 04a | ingest_mta_route_ref | MTA GTFS routes.txt -> reference |
| 05 | silver_fact_trip_delay_event | Merge/dedup -> silver fact |
| 06 | silver_dim_route | Route dimension |
| 07 | gold_route_delay_kpi_daily | Daily KPIs + BI view |
| 08 | pipeline_health_monitor | Health checks |

## Jobs
| Job | Type | Schedule |
|-----|------|----------|
| Continuous API Poll | Continuous | Always-on |
| Event Hubs Streaming | Continuous | Always-on |
| Daily Batch Pipeline | Scheduled | 6:00 AM ET |
| Health Monitor | Scheduled | Every 6 hours |

## Deployment
```bash
databricks bundle deploy -t dev    # Development
databricks bundle deploy -t prod   # Production
```

## Catalog
```
mta_rtransit
+-- bronze (gtfs_rt_events, eventhub_gtfs_raw, dim_route_ref, _cosmos_watermarks)
+-- silver (fact_trip_delay_event, dim_route)
+-- gold   (route_delay_kpi_daily, route_delay_kpi_enriched, dim_date)
```
