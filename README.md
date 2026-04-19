# 🚇 MTA Real-Time Transit Pipeline

[![Azure](https://img.shields.io/badge/Azure-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com)
[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=for-the-badge&logo=databricks&logoColor=white)](https://delta.io)
[![GitHub Actions](https://img.shields.io/badge/CI/CD-GitHub_Actions-2088FF?style=for-the-badge&logo=githubactions&logoColor=white)](https://github.com/features/actions)
[![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com)

End-to-end **Azure Data Engineering** platform ingesting NYC MTA real-time subway feeds through a **medallion architecture** on Databricks — with CI/CD, declarative ETL (DLT), monitoring, and BI serving.

> Built to demonstrate production-grade integration across **9 Azure services**, **declarative + imperative pipelines**, and a complete **DevOps workflow** from code to dashboard.

---

## 🏗️ Architecture Overview

```mermaid
flowchart TB
    subgraph SOURCES["🌐 Data Sources"]
        MTA["🚇 MTA GTFS-RT API<br/><i>Real-time subway feeds</i>"]
        GTFS["📋 MTA GTFS Static<br/><i>routes.txt reference</i>"]
    end

    subgraph INGEST["☁️ Azure Ingestion Layer"]
        COSMOS[("🔵 Cosmos DB<br/><i>gtfs_rt_batch</i><br/><i>gtfs_rt_stream</i>")]
        EH["📨 Event Hubs<br/><i>Kafka protocol</i>"]
    end

    subgraph DATABRICKS["⚡ Databricks Lakehouse"]
        direction TB

        subgraph BRONZE["🥉 Bronze — Raw Landing"]
            B1["gtfs_rt_events"]
            B2["eventhub_gtfs_raw"]
            B3["dim_route_ref"]
        end

        subgraph SILVER["🥈 Silver — Cleaned & Conformed"]
            S1["fact_trip_delay_event<br/><i>deduped, validated</i>"]
            S2["dim_route<br/><i>SCD Type 2</i>"]
        end

        subgraph GOLD["🥇 Gold — Business KPIs"]
            G1["route_delay_kpi_daily<br/><i>per route, per day</i>"]
            G2["route_delay_kpi_enriched<br/><i>joined with route names</i>"]
        end

        subgraph DLT["🔷 DLT Pipeline — Declarative ETL"]
            D1["bronze_cosmos_events<br/><i>Streaming Table</i>"]
            D2["bronze_eventhub_raw<br/><i>Streaming Table</i>"]
            D3["silver_fact_trip_delay_event<br/><i>Append Flows + DROP</i>"]
            D4["silver_dim_route<br/><i>Materialized View + FAIL</i>"]
            D5["gold_route_delay_kpi_daily<br/><i>Materialized View</i>"]
            D6["gold_route_delay_kpi_enriched<br/><i>Materialized View</i>"]
        end
    end

    subgraph SERVING["📊 Serving & BI"]
        SQL[("🔷 Azure SQL DB<br/><i>mta_analytics</i>")]
        PBI["📊 Power BI<br/><i>DirectQuery</i>"]
        DASH["📈 AI/BI Dashboard<br/><i>Lakeview</i>"]
        GENIE["🤖 Genie Space<br/><i>Natural language</i>"]
    end

    subgraph OPS["⚙️ Operations"]
        KV["🔐 Key Vault"]
        AI["📡 App Insights"]
        GHA["🔄 GitHub Actions"]
    end

    MTA -->|"Protobuf"| COSMOS
    MTA -->|"JSON"| EH
    GTFS -->|"routes.txt"| B3
    COSMOS -->|"Notebook 03<br/>watermark"| B1
    EH -->|"Notebook 04<br/>Kafka stream"| B2
    B1 & B2 -->|"Notebook 05<br/>MERGE"| S1
    B3 -->|"Notebook 06<br/>SCD2"| S2
    S1 & S2 -->|"Notebook 07<br/>aggregate"| G1
    G1 & S2 -->|"JOIN"| G2
    G1 & G2 & S2 -->|"Notebook 09<br/>JDBC"| SQL
    SQL -->|"DirectQuery"| PBI
    G1 & S1 --> DASH
    G1 --> GENIE

    B1 -->|"readStream"| D1
    B2 -->|"readStream"| D2
    D1 -->|"Append Flow"| D3
    D2 -->|"Append Flow"| D3
    S2 -.->|"spark.read"| D4
    D3 -->|"aggregate"| D5
    D5 & D4 -->|"JOIN"| D6

    KV -.->|"secrets"| DATABRICKS
    AI -.->|"telemetry"| DATABRICKS
    GHA -->|"bundle deploy"| DATABRICKS

    style SOURCES fill:#e8f4fd,stroke:#0078D4
    style INGEST fill:#e8f4fd,stroke:#0078D4
    style DATABRICKS fill:#fff3e0,stroke:#FF3621
    style BRONZE fill:#f5e6cc,stroke:#cd7f32
    style SILVER fill:#e8e8e8,stroke:#808080
    style GOLD fill:#fff9c4,stroke:#ffd700
    style DLT fill:#e3f2fd,stroke:#1976d2
    style SERVING fill:#e8f5e9,stroke:#4caf50
    style OPS fill:#fce4ec,stroke:#e91e63
```

---

## 🔄 Data Flow — Regular Pipeline

The notebook pipeline runs daily at **6 AM ET** through a multi-task DAG job.

```mermaid
flowchart LR
    subgraph INGEST["Ingestion"]
        NB02["02 — API Poll<br/><i>Continuous</i>"]
    end

    subgraph BRONZE["Bronze"]
        NB03["03 — Cosmos → Delta<br/><i>Watermark-based</i>"]
        NB04["04 — Event Hubs → Delta<br/><i>Kafka streaming</i>"]
        NB04a["04a — Route Reference<br/><i>MTA GTFS static</i>"]
    end

    subgraph SILVER["Silver"]
        NB05["05 — Fact: Trip Delay<br/><i>Union + Dedup + MERGE</i>"]
        NB06["06 — Dim: Route<br/><i>SCD Type 2</i>"]
    end

    subgraph GOLD["Gold"]
        NB07["07 — KPI Daily<br/><i>Aggregate + BI View</i>"]
    end

    subgraph EXPORT["Export"]
        NB09["09 — Azure SQL<br/><i>JDBC overwrite</i>"]
    end

    NB03 --> NB05
    NB04a --> NB06
    NB05 --> NB07
    NB06 --> NB07
    NB07 --> NB09

    style INGEST fill:#e3f2fd,stroke:#1565c0
    style BRONZE fill:#f5e6cc,stroke:#cd7f32
    style SILVER fill:#e8e8e8,stroke:#808080
    style GOLD fill:#fff9c4,stroke:#ffd700
    style EXPORT fill:#e8f5e9,stroke:#4caf50
```

---

## 🔷 Data Flow — DLT Pipeline (Lakeflow Spark Declarative Pipelines)

Runs daily at **7 AM ET** (1 hour after the regular pipeline), on **serverless compute**. Reads from existing bronze Delta tables and applies declarative transformations with **data quality expectations**.

```mermaid
flowchart LR
    EXT_B1[("bronze.gtfs_rt_events<br/><i>EXTERNAL</i>")]
    EXT_B2[("bronze.eventhub_gtfs_raw<br/><i>EXTERNAL</i>")]
    EXT_S1[("silver.dim_route<br/><i>EXTERNAL</i>")]

    subgraph DLT["DLT Pipeline — mta_rtransit.dlt"]
        direction LR

        D1["bronze_cosmos_events<br/><b>Streaming Table</b><br/>✅ 3 WARN expectations"]
        D2["bronze_eventhub_raw<br/><b>Streaming Table</b><br/>✅ 2 WARN expectations"]
        D3["silver_fact_trip_delay_event<br/><b>Streaming Table</b><br/>🚫 4 DROP expectations<br/><i>Append Flows pattern</i>"]
        D4["silver_dim_route<br/><b>Materialized View</b><br/>🛑 2 FAIL expectations"]
        D5["gold_route_delay_kpi_daily<br/><b>Materialized View</b><br/>✅ 3 WARN expectations"]
        D6["gold_route_delay_kpi_enriched<br/><b>Materialized View</b><br/>✅ 1 WARN expectation"]

        D1 -->|"Append Flow<br/>from_cosmos_batch"| D3
        D2 -->|"Append Flow<br/>from_eh_stream"| D3
        D3 --> D5
        D4 --> D6
        D5 --> D6
    end

    EXT_B1 -->|"readStream"| D1
    EXT_B2 -->|"readStream"| D2
    EXT_S1 -->|"spark.read"| D4

    style DLT fill:#e3f2fd,stroke:#1976d2
    style D3 fill:#fff3e0,stroke:#ff9800
    style D4 fill:#fce4ec,stroke:#e91e63
```

### Expectations Strategy
| Layer | Action | Why |
|-------|--------|-----|
| **Bronze** | `WARN` | Keep all raw data, log quality issues for visibility |
| **Silver Fact** | `DROP` | Remove invalid records — clean data for aggregation |
| **Silver Dimension** | `FAIL` | Halt pipeline if dimension data is broken — downstream depends on it |
| **Gold** | `WARN` | Log anomalies but don't block KPI output |

---

## ⏰ Job Orchestration

```mermaid
gantt
    title Daily Schedule (ET timezone)
    dateFormat HH:mm
    axisFormat %H:%M

    section Continuous
    API Poll (PAUSED)           :crit, 00:00, 24h
    EH Streaming (PAUSED)       :crit, 00:00, 24h

    section Scheduled
    Table Maintenance (Sun)     :active, 02:00, 30min
    Daily Batch Pipeline        :active, 06:00, 45min
    DLT Refresh                 :active, 07:00, 15min
    Health Monitor              :done, 00:00, 10min
    Health Monitor              :done, 06:00, 10min
    Health Monitor              :done, 12:00, 10min
    Health Monitor              :done, 18:00, 10min
```

| Job | Schedule | What it does |
|-----|----------|-------------|
| **Daily Batch Pipeline** | 6:00 AM ET | `03 → 04a → 05 → 06 → 07 → 09` (full medallion refresh) |
| **DLT Refresh** | 7:00 AM ET | Incremental refresh of all DLT tables (serverless) |
| **Health Monitor** | Every 6 hours | Checks freshness, job health, data quality |
| **Table Maintenance** | Sunday 2 AM | OPTIMIZE, VACUUM, liquid clustering, ANALYZE |
| **API Poll** | PAUSED | Continuous MTA feed ingestion to Cosmos + Event Hubs |
| **EH Streaming** | PAUSED | Continuous Event Hubs to Bronze streaming |

---

## 🔧 Technology Stack

| Category | Technologies |
|----------|-------------|
| **Compute** | Databricks (Unity Catalog, Delta Lake, Structured Streaming, DLT, Photon) |
| **Storage** | ADLS Gen2 (external Delta tables), Cosmos DB (document store) |
| **Streaming** | Event Hubs (Kafka protocol), Structured Streaming (foreachBatch) |
| **ETL** | Lakeflow Spark Declarative Pipelines (Append Flows, expectations, MVs) |
| **Orchestration** | Databricks Jobs (multi-task DAG), GitHub Actions CI/CD |
| **Governance** | Unity Catalog (lineage, access control), Key Vault (secrets) |
| **Monitoring** | Application Insights (telemetry), Health Monitor notebook |
| **Serving** | Azure SQL Database (JDBC export), Power BI (DirectQuery) |
| **BI** | AI/BI Dashboard (Lakeview), Genie Space (natural language analytics) |
| **DevOps** | Databricks Asset Bundles, GitHub Actions, feature branch workflow |

---

## 📁 Project Structure

```
azure-databricks-transit-platform/
│
├── .github/workflows/
│   └── deploy.yml                          # CI/CD: main → bundle deploy -t prod
│
├── notebooks/
│   ├── 01_setup_and_configs.py             # DDL for all schemas and tables
│   ├── 02_mta_gtfs_rt_to_cosmos.py         # API → Cosmos DB + Event Hubs
│   ├── 03_batch_cosmos_to_delta.py         # Cosmos → Bronze (watermark incremental)
│   ├── 04_streaming_eventhubs_to_bronze.py # Event Hubs → Bronze (Kafka streaming)
│   ├── 04a_ingest_mta_route_ref.py         # MTA GTFS routes.txt → reference table
│   ├── 05_silver_fact_trip_delay_event.py  # Union + dedup + MERGE → Silver fact
│   ├── 06_silver_dim_route.py              # Route dimension with SCD Type 2
│   ├── 07_gold_route_delay_kpi_daily.py    # Daily KPIs per route + BI view
│   ├── 08_pipeline_health_monitor.py       # Freshness, job health, data quality
│   ├── 09_gold_to_azure_sql.py             # Gold → Azure SQL via JDBC
│   └── 10_delta_table_maintenance.py       # OPTIMIZE, VACUUM, liquid clustering
│
├── dlt/transformations/
│   ├── eventhub_raw.py                     # Bronze ST — Event Hubs source
│   ├── cosmos_events.py                    # Bronze ST — Cosmos source
│   ├── fact_trip_delay_event.py            # Silver ST — Append Flows pattern
│   ├── dim_route.py                        # Silver MV — route dimension
│   ├── route_delay_kpi_daily.py            # Gold MV — KPI aggregation
│   └── route_delay_kpi_enriched.py         # Gold MV — enriched join
│
├── dashboards/
│   └── mta_transit_pipeline_operations.lvdash.json
│
├── databricks.yml                          # DABs config (jobs, pipeline, dashboard)
├── PROJECT_CONTEXT.md                      # Detailed project documentation
└── README.md
```

---

## 🗄️ Unity Catalog

```
mta_rtransit (prod)
├── bronze
│   ├── gtfs_rt_events           ← Cosmos batch events
│   ├── eventhub_gtfs_raw        ← Event Hubs stream
│   ├── dim_route_ref            ← GTFS static reference
│   └── _cosmos_watermarks       ← Incremental load state
├── silver
│   ├── fact_trip_delay_event    ← Deduped delay facts
│   └── dim_route                ← Route dimension (SCD2)
├── gold
│   ├── route_delay_kpi_daily    ← Daily KPIs per route
│   └── route_delay_kpi_enriched ← KPIs + route names (VIEW)
└── dlt                          ← DLT managed tables
    ├── bronze_eventhub_raw
    ├── bronze_cosmos_events
    ├── silver_fact_trip_delay_event
    ├── silver_dim_route
    ├── gold_route_delay_kpi_daily
    └── gold_route_delay_kpi_enriched
```

---

## 🚀 Deployment

Uses **Databricks Asset Bundles** with **GitHub Actions** CI/CD.

```mermaid
flowchart LR
    DEV["🔧 Feature Branch<br/><i>develop & test</i>"]
    PR["📋 Pull Request<br/><i>code review</i>"]
    MAIN["✅ Main Branch<br/><i>merge</i>"]
    GHA["🔄 GitHub Actions<br/><i>bundle validate<br/>bundle deploy</i>"]
    PROD["🏭 Production<br/><i>/Shared/.../prod/</i>"]

    DEV --> PR --> MAIN --> GHA --> PROD

    style DEV fill:#fff3e0,stroke:#ff9800
    style PR fill:#e3f2fd,stroke:#1976d2
    style MAIN fill:#e8f5e9,stroke:#4caf50
    style GHA fill:#f3e5f5,stroke:#9c27b0
    style PROD fill:#e8f5e9,stroke:#2e7d32
```

```bash
# Manual deployment
databricks bundle deploy -t dev    # Development (personal workspace)
databricks bundle deploy -t prod   # Production (/Shared/)
```

---

## 🎯 Key Design Decisions

| # | Decision | Why |
|---|----------|-----|
| 1 | External tables on ADLS | Full control over storage, survives catalog drops |
| 2 | Watermark incremental load | Avoids re-reading entire Cosmos container each run |
| 3 | Kafka protocol for Event Hubs | Built into Spark — no external Maven JARs |
| 4 | DLT reads from existing Delta | Serverless can't access Key Vault or Cosmos directly |
| 5 | Append Flows over UNION + MERGE | Native multi-source fan-in, DLT-managed dedup |
| 6 | Liquid clustering over ZORDER | Incremental, adaptive, no full-table rewrites |
| 7 | Separate DLT managed schema | `dlt` schema isolates managed tables from external |
| 8 | DLT runs 1 hour after batch | Ensures fresh bronze data before transformation |
| 9 | Azure SQL for Power BI | Faster DirectQuery vs hitting Delta Lake directly |
| 10 | Health monitor every 6 hours | Catch staleness and job failures before users notice |

---

## 📅 Roadmap

- [x] Phase 1 — Security & Observability (Key Vault, App Insights)
- [x] Phase 2 — Analytics & BI (Gold layer, Genie, AI/BI Dashboard, Power BI)
- [x] Phase 3 — CI/CD & DevOps (GitHub, DABs, Actions, dev/prod catalogs)
- [x] Phase 3.5 — Azure SQL export (Gold → Azure SQL via JDBC)
- [x] Phase 3.6 — DLT Pipeline (expectations, Append Flows, serverless)
- [x] Phase 3.7 — Table Optimization (OPTIMIZE, VACUUM, liquid clustering)
- [ ] Phase 4 — ML & Advanced Analytics (delay prediction, MLflow, Model Serving)
- [ ] Phase 5 — Microsoft Fabric integration

---

## 📝 License

This project is for portfolio/educational purposes.
