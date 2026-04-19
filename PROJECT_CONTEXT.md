# MTA Real-Time Transit Pipeline — Project Context
**Last Updated**: April 19, 2026
**Owner**: saikrishnareddypoluri26@outlook.com
**Workspace**: https://adb-7405613547805720.0.azuredatabricks.net

---

## 🎯 Project Overview
End-to-end **Azure Data Engineering** portfolio project ingesting NYC MTA real-time subway/bus GTFS-RT feeds through a medallion architecture (Bronze → Silver → Gold) with CI/CD, monitoring, BI integration, and **Lakeflow Spark Declarative Pipelines (DLT)** for declarative ETL with data quality expectations.

---

## 🏗️ Architecture

```
MTA GTFS-RT API → Cosmos DB → Event Hubs → Databricks (Delta Lake) → Azure SQL → Power BI
                                              ↓
                                    Bronze → Silver → Gold
                                              ↓
                              DLT Pipeline (Expectations + Lineage)
                              Bronze ST → Silver ST (Append Flows) → Gold MV
```

### Two Pipeline Approaches (side by side)
| Aspect | Regular Pipeline (Notebooks 01–09) | DLT Pipeline (Lakeflow SDP) |
|---|---|---|
| **Orchestration** | Databricks Jobs (DAG) | DLT auto-resolves dependencies |
| **Ingestion** | Imperative (Python SDK, writeStream) | Declarative (@dp.table, readStream) |
| **Deduplication** | Window ROW_NUMBER + MERGE | Append Flows (multi-source fan-in) |
| **Data Quality** | Manual is_valid flag | Built-in expectations (WARN/DROP/FAIL) |
| **State Management** | Custom watermark table, checkpoints | DLT-managed checkpoints |
| **Lineage** | Implicit | Visual DAG + Unity Catalog lineage |

### Azure Services Used
| Service | Purpose |
|---|---|
| **Databricks** | Spark processing, Delta Lake, Unity Catalog, Jobs, DLT Pipelines, Genie, AI/BI Dashboard |
| **ADLS Gen2** | Storage (abfss://demo@mtartransitdl.dfs.core.windows.net) |
| **Cosmos DB** | Raw event store from MTA API |
| **Event Hubs** | Real-time streaming ingestion |
| **Key Vault** | `mta-gtfs-rt-kv` (secrets), scope `mta-kv` in Databricks |
| **Application Insights** | Telemetry & monitoring |
| **Azure SQL Database** | Gold layer serving for Power BI DirectQuery |
| **GitHub Actions** | CI/CD pipeline |

---

## 📁 Workspace Structure

```
mta-rtransit-pipeline/
├── PROJECT_CONTEXT.md
├── push_to_github.sh
│
├── 01_setup_and_configs.py              # Schema/table creation
├── 02_mta_gtfs_rt_to_cosmos.py          # API → Cosmos
├── 03_batch_cosmos_to_delta.py          # Cosmos → Bronze (batch)
├── 04_streaming_eventhubs_to_bronze.py  # EH → Bronze (streaming)
├── 04a_ingest_mta_route_ref.py          # Route reference data
├── 05_silver_fact_trip_delay_event.py   # Bronze → Silver fact
├── 06_silver_dim_route.py               # Silver dimension (SCD2)
├── 07_gold_route_delay_kpi_daily.py     # Silver → Gold KPIs
├── 08_pipeline_health_monitor.py        # Health checks every 6h
├── 09_gold_to_azure_sql.py              # Gold → Azure SQL (JDBC)
│
└── dlt/
    └── MTA Transit - DLT Medallion Pipeline/
        └── transformations/
            ├── eventhub_raw.py              # Bronze ST (EH Delta source)
            ├── cosmos_events.py             # Bronze ST (Cosmos Delta source)
            ├── fact_trip_delay_event.py      # Silver ST (Append Flows + expectations)
            ├── dim_route.py                 # Silver MV (route dimension)
            ├── route_delay_kpi_daily.py     # Gold MV (KPI aggregation)
            └── route_delay_kpi_enriched.py  # Gold MV (enriched join)
```

### GitHub Repository
**Repo**: https://github.com/Krishna9181/azure-databricks-transit-platform

---

## 🗄️ Unity Catalog Structure

### Catalogs
| Catalog | Environment | Storage |
|---|---|---|
| `mta_rtransit` | PROD | EXTERNAL (ADLS /mta_rtransit/) |
| `mta_rtransit_dev` | DEV | MANAGED (ADLS /mta_rtransit_dev/) |

### Schema & Tables — Regular Pipeline
| Layer | Table | Type | Description |
|---|---|---|---|
| **bronze** | `gtfs_rt_events` | Table | Raw GTFS-RT events from Cosmos |
| **bronze** | `eventhub_gtfs_raw` | Table | Raw events from Event Hubs stream |
| **bronze** | `dim_route_ref` | Table | MTA route reference data |
| **bronze** | `_cosmos_watermarks` | Table | Incremental load tracking |
| **silver** | `fact_trip_delay_event` | Table | Deduped/enriched trip delay events |
| **silver** | `dim_route` | Table | Route dimension (SCD Type 2) |
| **gold** | `route_delay_kpi_daily` | Table | Daily aggregated KPIs per route |
| **gold** | `route_delay_kpi_enriched` | View | KPIs joined with route names |
| **gold** | `dim_date` | View | Date dimension utility |

### Schema & Tables — DLT Pipeline (`mta_rtransit.dlt`)
| Dataset | Type | Rows | Source | Expectations |
|---|---|---|---|---|
| `bronze_eventhub_raw` | Streaming Table | 12,657 | `bronze.eventhub_gtfs_raw` (Delta) | 2 WARN |
| `bronze_cosmos_events` | Streaming Table | 45,339 | `bronze.gtfs_rt_events` (Delta) | 3 WARN |
| `silver_fact_trip_delay_event` | Streaming Table | 57,996 | Append Flows from both bronze | 4 DROP |
| `silver_dim_route` | Materialized View | 29 | `silver.dim_route` (external) | 2 FAIL |
| `gold_route_delay_kpi_daily` | Materialized View | 18 | Silver fact (batch read) | 3 WARN |
| `gold_route_delay_kpi_enriched` | Materialized View | 18 | Gold KPI + Silver dim (join) | 1 WARN |

**Storage**: DLT tables are MANAGED by Unity Catalog in `mta_rtransit.dlt` — completely isolated from existing EXTERNAL tables in bronze/silver/gold schemas.

---

## 🔄 Lakeflow Spark Declarative Pipeline (DLT)

### Pipeline Configuration
| Setting | Value |
|---|---|
| **Pipeline ID** | `4c26e9b6-49c0-42df-8b36-e7ebc446046d` |
| **Name** | MTA Transit - DLT Medallion Pipeline |
| **Catalog** | `mta_rtransit` |
| **Target Schema** | `dlt` (MANAGED tables) |
| **Compute** | Serverless |
| **Mode** | Triggered (batch) |
| **Photon** | Enabled |
| **Channel** | Preview |

### DLT Patterns Demonstrated
| Pattern | File | Description |
|---|---|---|
| **Kafka Streaming** | `eventhub_raw.py` | Event Hubs via Delta streaming read |
| **Change Feed** | `cosmos_events.py` | Cosmos DB via Delta streaming read |
| **Append Flows** | `fact_trip_delay_event.py` | Multi-source fan-in (`dp.create_streaming_table` + `@dp.append_flow`) — replaces UNION + MERGE |
| **Expectations (WARN)** | Bronze files | Log quality issues, keep all data |
| **Expectations (DROP)** | Silver fact | Drop invalid records before silver |
| **Expectations (FAIL)** | Silver dim | Halt pipeline if dimension is broken |
| **Materialized Views** | Gold files | Auto-recompute aggregations + joins |
| **Stream-batch hybrid** | Gold MVs | `spark.read` from streaming tables |

### Architecture Flow
```
mta_rtransit.bronze.gtfs_rt_events (EXTERNAL)
        │ readStream
        ▼
┌───────────────────────┐
│ bronze_cosmos_events  │ ST (3 WARN expectations)
└───────────┬───────────┘
            │ Append Flow: from_cosmos_batch
            ▼
┌───────────────────────────────┐     ┌──────────────────┐
│ silver_fact_trip_delay_event  │ ST  │ silver_dim_route  │ MV
│ 4 DROP expectations           │     │ 2 FAIL expectations│
│ + Append Flow: from_eh_stream │     └────────┬─────────┘
└───────────┬───────────────────┘              │
            │ spark.read                       │
            ▼                                  │
┌───────────────────────────────┐              │
│ gold_route_delay_kpi_daily    │ MV ←─────────┘ (JOIN)
│ 3 WARN expectations           │
└───────────┬───────────────────┘
            │ spark.read
            ▼
┌───────────────────────────────┐
│ gold_route_delay_kpi_enriched │ MV (1 WARN)
└───────────────────────────────┘

mta_rtransit.bronze.eventhub_gtfs_raw (EXTERNAL)
        │ readStream
        ▼
┌───────────────────────┐
│ bronze_eventhub_raw   │ ST (2 WARN expectations)
└───────────┬───────────┘
            │ Append Flow: from_eventhub_stream
            └──────────► silver_fact_trip_delay_event (above)
```

### Serverless Limitations (documented)
- `cosmos.oltp.changeFeed` streaming NOT supported — reads from existing Delta table instead
- Azure Key Vault-backed secrets do NOT resolve via `spark.conf.get()` — no direct Event Hubs/Cosmos ingestion
- Architecture: Regular pipeline handles raw ingestion → DLT handles transformation + quality layer

---

## ⚙️ Databricks Asset Bundles (DABs)

### Variables
| Variable | Dev | Prod |
|---|---|---|
| `catalog` | `mta_rtransit_dev` | `mta_rtransit` |
| `email_notifications` | saikrishnareddypoluri26@outlook.com | same |
| `cluster_id` | 0417-222320-e1j7vfs2 | same |

### Jobs (DAB-managed, UI_LOCKED)
| Job | ID | Schedule | Notebooks |
|---|---|---|---|
| MTA Ingestion - Continuous API Poll | 896486395513847 | PAUSED | 02 |
| MTA Streaming - Event Hubs to Bronze | 881210209845621 | PAUSED | 04 |
| MTA Transit Pipeline - Daily Batch | 623693707461938 | Daily 6AM ET | 03→04a→05→06→07→09 |
| MTA Pipeline Health Monitor | 774844392032409 | Every 6 hours | 08 |

### Daily Batch DAG
```
03_batch_cosmos_to_delta
  → 04a_ingest_mta_route_ref
    → 05_silver_fact_trip_delay_event
      → 06_silver_dim_route
        → 07_gold_route_delay_kpi_daily
          → 09_gold_to_azure_sql
```

### DLT Pipeline (separate, triggered independently)
```
bronze_cosmos_events ──┐
                       ├──► silver_fact_trip_delay_event ──► gold_route_delay_kpi_daily
bronze_eventhub_raw ───┘                                          │
                                                                  ▼
silver_dim_route ──────────────────────────────────► gold_route_delay_kpi_enriched
```

---

## 🔄 CI/CD Pipeline
**Trigger**: Push to `main` branch
**Workflow**: `.github/workflows/deploy.yml`
**Steps**: Checkout → Install Terraform → Install CLI → Validate bundle → Deploy to prod
**GitHub Secrets**: DATABRICKS_HOST, DATABRICKS_TOKEN

---

## 🔑 Key Vault Secrets (scope: mta-kv)
| Secret | Service |
|---|---|
| cosmos-endpoint, cosmos-key | Cosmos DB |
| eventhub-connection-string | Event Hubs |
| appinsights-connection-string | Application Insights |
| sql-server-host, sql-server-user, sql-server-password, sql-database-name | Azure SQL |

---

## 📊 Azure SQL Database
- **Server**: mta-sql-eus.database.windows.net
- **Database**: mta_analytics
- **Tables**: dbo.route_delay_kpi_daily, dbo.dim_route, dbo.route_delay_kpi_enriched
- **Purpose**: Power BI DirectQuery, app consumption

---

## ✅ Completed Phases
| Phase | Status | What |
|---|---|---|
| Phase 1: Security & Observability | ✅ | Key Vault, App Insights, secret rotation |
| Phase 2: Analytics & BI | ✅ | Gold layer, Genie space, AI/BI Dashboard, Power BI |
| Phase 3: CI/CD & DevOps | ✅ | GitHub repo, DABs, GitHub Actions, dev/prod catalogs |
| Phase 3.5: Azure SQL | ✅ | Gold → Azure SQL export via JDBC |
| Phase 3.6: DLT Pipeline | ✅ | Lakeflow SDP with expectations, Append Flows, multi-file structure, serverless |

## ⏳ Upcoming
| Phase | Plan |
|---|---|
| Phase 3.7: Table Optimization | OPTIMIZE, VACUUM, liquid clustering, ANALYZE |
| Phase 4: ML & Advanced Analytics | Delay prediction, MLflow, Model Serving |
| ADF Integration | Separate section (next week) |
| Microsoft Fabric | Second project |
| Palantir Foundry | Third project |

---

## 🛠️ Key Design Decisions
1. **Parameterized notebooks**: All use `catalog` widget for dev/prod switching
2. **Secrets in Key Vault**: No hardcoded credentials (fixed in notebook 02)
3. **SQL magic uses widget syntax**: `${catalog}` not `{catalog}` in %sql cells
4. **Full deploy, incremental apply**: DABs re-uploads all files but only updates changed resources
5. **SCD Type 2**: dim_route tracks route changes with effective_from/effective_to
6. **MERGE patterns**: Silver fact uses dedup + MERGE for idempotent processing
7. **DLT uses separate managed schema**: `mta_rtransit.dlt` — isolated from existing EXTERNAL tables on ADLS
8. **DLT Append Flows for multi-source merge**: Replaces UNION + MERGE with declarative fan-in pattern
9. **Expectations strategy**: WARN at bronze (keep all data), DROP at silver (clean data), FAIL at dimension (must be complete)
10. **DLT on serverless**: Direct Cosmos/EH ingestion not supported — DLT handles transformation + quality, regular pipeline handles raw ingestion
11. **New Python API**: DLT uses `from pyspark import pipelines as dp` (not legacy `import dlt`)
