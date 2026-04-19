# Databricks notebook source
# DBTITLE 1,Delta Table Maintenance
# MAGIC %md
# MAGIC # 10 — Delta Table Maintenance
# MAGIC **LIQUID CLUSTERING · OPTIMIZE · VACUUM · ANALYZE** for all EXTERNAL Delta tables in the MTA Transit pipeline.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Scope
# MAGIC | Layer | Tables | Storage |
# MAGIC |---|---|---|
# MAGIC | **Bronze** | `gtfs_rt_events`, `eventhub_gtfs_raw`, `dim_route_ref`, `_cosmos_watermarks` | EXTERNAL (ADLS) |
# MAGIC | **Silver** | `fact_trip_delay_event`, `dim_route` | EXTERNAL (ADLS) |
# MAGIC | **Gold** | `route_delay_kpi_daily` | EXTERNAL (ADLS) |
# MAGIC
# MAGIC ### Excluded
# MAGIC * **DLT managed tables** (`mta_rtransit.dlt.*`) — auto-optimized by Lakeflow Spark Declarative Pipelines
# MAGIC * **Views** (`route_delay_kpi_enriched`, `dim_date`) — no physical storage to optimize
# MAGIC
# MAGIC ### Operations (per table, in order)
# MAGIC 1. **CLUSTER BY** — enable liquid clustering on large tables (one-time, idempotent; replaces legacy ZORDER)
# MAGIC 2. **OPTIMIZE** — compact files + apply liquid clustering incrementally (only unclustered files rewritten)
# MAGIC 3. **VACUUM** — remove stale files older than 7 days (168 hours, Delta default)
# MAGIC 4. **ANALYZE TABLE** — compute statistics for query optimizer
# MAGIC
# MAGIC ### Why Liquid Clustering over ZORDER?
# MAGIC | | ZORDER (legacy) | Liquid Clustering (modern) |
# MAGIC |---|---|---|
# MAGIC | Setup | Specify columns at every OPTIMIZE | Set once via `ALTER TABLE ... CLUSTER BY` |
# MAGIC | Rewrite | Full file rewrite each time | Incremental — only unclustered files |
# MAGIC | Change keys | Requires full data rewrite | Change anytime, no rewrite |
# MAGIC | DBR support | All versions | GA since DBR 13.3+ |
# MAGIC
# MAGIC ### Suggested Schedule
# MAGIC Run **weekly** (e.g., Sunday 2 AM ET) after the daily batch job has completed.
# MAGIC Add as the last task in the weekly maintenance job or as a standalone scheduled job.

# COMMAND ----------

# DBTITLE 1,Visual Guide to Delta Table Maintenance
# MAGIC %md
# MAGIC ## Visual Guide: OPTIMIZE, VACUUM, ZORDER & Liquid Clustering
# MAGIC
# MAGIC Using our **`bronze.gtfs_rt_events`** table as the example throughout.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 1. The Small Files Problem (why we need OPTIMIZE)
# MAGIC
# MAGIC Every time notebook `03_batch_cosmos_to_delta` runs a MERGE, Delta creates new small Parquet files:
# MAGIC
# MAGIC ```
# MAGIC 📁 bronze/gtfs_rt_events/              (ADLS)
# MAGIC ├── part-00001.parquet   (2 MB)   ← Day 1 batch
# MAGIC ├── part-00002.parquet   (1 MB)   ← Day 1 batch
# MAGIC ├── part-00003.parquet   (3 MB)   ← Day 2 batch
# MAGIC ├── part-00004.parquet   (0.5 MB) ← Day 2 batch
# MAGIC ├── part-00005.parquet   (2 MB)   ← Day 3 batch
# MAGIC ├── part-00006.parquet   (1 MB)   ← Day 3 batch
# MAGIC ├── part-00007.parquet   (0.8 MB) ← Day 4 batch
# MAGIC ├── part-00008.parquet   (1.5 MB) ← Day 4 batch
# MAGIC └── ... 50+ tiny files after a month
# MAGIC ```
# MAGIC
# MAGIC **Problem:** Each query must open ALL 50+ files → slow reads, high I/O overhead.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 2. OPTIMIZE — Compact Small Files
# MAGIC
# MAGIC ```sql
# MAGIC OPTIMIZE mta_rtransit.bronze.gtfs_rt_events;
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC BEFORE (8 small files, 12 MB total)        AFTER (1 large file, 12 MB total)
# MAGIC ┌──────────┐ ┌──────────┐                 ┌─────────────────────────────────┐
# MAGIC │ part-001 │ │ part-002 │                 │                                 │
# MAGIC │  2 MB    │ │  1 MB    │                 │     part-00001.parquet          │
# MAGIC ├──────────┤ ├──────────┤                 │                                 │
# MAGIC │ part-003 │ │ part-004 │   ───────►      │         12 MB                   │
# MAGIC │  3 MB    │ │  0.5 MB  │                 │     (all data merged into       │
# MAGIC ├──────────┤ ├──────────┤                 │      one optimal-sized file)    │
# MAGIC │ part-005 │ │ part-006 │                 │                                 │
# MAGIC │  2 MB    │ │  1 MB    │                 └─────────────────────────────────┘
# MAGIC ├──────────┤ ├──────────┤                 ✅ 1 file to open instead of 8
# MAGIC │ part-007 │ │ part-008 │                 ✅ Fewer I/O calls = faster queries
# MAGIC │  0.8 MB  │ │  1.5 MB  │                 ✅ Better compression ratio
# MAGIC └──────────┘ └──────────┘
# MAGIC ```
# MAGIC
# MAGIC **Key point:** Data is the SAME — just fewer, larger files. Like consolidating 8 small boxes into 1 big box.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 3. VACUUM — Clean Up Stale Files
# MAGIC
# MAGIC After OPTIMIZE, the old small files still exist on ADLS (for time travel):
# MAGIC
# MAGIC ```
# MAGIC 📁 bronze/gtfs_rt_events/
# MAGIC ├── part-00001.parquet  (NEW - 12 MB)  ✅ Active (in Delta log)
# MAGIC ├── part-00001.parquet  (OLD - 2 MB)   ❌ Stale (not in Delta log)
# MAGIC ├── part-00002.parquet  (OLD - 1 MB)   ❌ Stale
# MAGIC ├── part-00003.parquet  (OLD - 3 MB)   ❌ Stale
# MAGIC ├── part-00004.parquet  (OLD - 0.5 MB) ❌ Stale
# MAGIC ├── ...more stale files...
# MAGIC └── Total ADLS storage: 24 MB  (12 MB useful + 12 MB garbage!)
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC VACUUM mta_rtransit.bronze.gtfs_rt_events RETAIN 168 HOURS;
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC BEFORE VACUUM                              AFTER VACUUM
# MAGIC 📁 24 MB on ADLS                           📁 12 MB on ADLS
# MAGIC ┌──────────────────────┐                   ┌──────────────────────┐
# MAGIC │ ✅ New optimized file │                   │ ✅ New optimized file │
# MAGIC │ ❌ Old part-001      │   ───────►        │                      │
# MAGIC │ ❌ Old part-002      │   (deletes files  │  Only active files   │
# MAGIC │ ❌ Old part-003      │   older than      │  remain on ADLS      │
# MAGIC │ ❌ Old part-004      │   7 days)         │                      │
# MAGIC │ ❌ ...               │                   └──────────────────────┘
# MAGIC └──────────────────────┘                   ✅ 50% storage savings!
# MAGIC ```
# MAGIC
# MAGIC **Key point:** VACUUM saves storage costs. The 168-hour retention means you can still time-travel within the last 7 days.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 4. ZORDER (Legacy) — Organize Data Layout by Column
# MAGIC
# MAGIC Without ZORDER, data is stored in arrival order (random layout):
# MAGIC
# MAGIC ```
# MAGIC                    File 1           File 2           File 3
# MAGIC                 ┌───────────┐   ┌───────────┐   ┌───────────┐
# MAGIC   route_id      │ A, 7, 1   │   │ 4, A, 6   │   │ 1, 7, A   │
# MAGIC                 │ 6, 4, 7   │   │ 1, 7, A   │   │ 4, 6, 1   │
# MAGIC                 └───────────┘   └───────────┘   └───────────┘
# MAGIC
# MAGIC   Query: WHERE route_id = 'A'
# MAGIC   → Must scan ALL 3 files (A is scattered everywhere)
# MAGIC   → Reads: 3 files ❌
# MAGIC ```
# MAGIC
# MAGIC After ZORDER BY (route_id):
# MAGIC
# MAGIC ```sql
# MAGIC OPTIMIZE mta_rtransit.bronze.gtfs_rt_events ZORDER BY (route_id);
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC                    File 1           File 2           File 3
# MAGIC                 ┌───────────┐   ┌───────────┐   ┌───────────┐
# MAGIC   route_id      │ A, A, A   │   │ 1, 1, 4   │   │ 6, 7, 7   │
# MAGIC                 │ A, A, 1   │   │ 4, 4, 6   │   │ 7, 7, 7   │
# MAGIC                 └───────────┘   └───────────┘   └───────────┘
# MAGIC                  min=A max=A     min=1 max=6     min=6 max=7
# MAGIC
# MAGIC   Query: WHERE route_id = 'A'
# MAGIC   → File stats say: A is only in File 1!
# MAGIC   → Reads: 1 file ✅  (skipped 2 files = 66% less I/O)
# MAGIC ```
# MAGIC
# MAGIC **Problem with ZORDER:**
# MAGIC - Must specify columns at EVERY `OPTIMIZE` run
# MAGIC - Rewrites ALL files every time (even already-sorted ones)
# MAGIC - Can't change columns without full rewrite
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### 5. Liquid Clustering (Modern) — Smart, Incremental ZORDER
# MAGIC
# MAGIC ```sql
# MAGIC -- Set once (one-time, idempotent)
# MAGIC ALTER TABLE mta_rtransit.bronze.gtfs_rt_events CLUSTER BY (route_id, ingested_at);
# MAGIC
# MAGIC -- Then just run OPTIMIZE — clustering happens automatically
# MAGIC OPTIMIZE mta_rtransit.bronze.gtfs_rt_events;
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC   Day 1: Initial OPTIMIZE                  Day 2: New data arrives
# MAGIC   ┌─────────────────────────┐              ┌─────────────────────────┐
# MAGIC   │ File 1: route A, Apr 1  │ ✅ Clustered │ File 1: route A, Apr 1  │ ✅ Already done
# MAGIC   │ File 2: route A, Apr 2  │ ✅ Clustered │ File 2: route A, Apr 2  │ ✅ Already done
# MAGIC   │ File 3: route 1, Apr 1  │ ✅ Clustered │ File 3: route 1, Apr 1  │ ✅ Already done
# MAGIC   │ File 4: route 7, Apr 1  │ ✅ Clustered │ File 4: route 7, Apr 1  │ ✅ Already done
# MAGIC   └─────────────────────────┘              │ File 5: route A, Apr 3  │ 🆕 Unclustered
# MAGIC                                            │ File 6: route 7, Apr 3  │ 🆕 Unclustered
# MAGIC                                            └─────────────────────────┘
# MAGIC
# MAGIC   Day 2: OPTIMIZE runs again
# MAGIC   ┌─────────────────────────┐
# MAGIC   │ File 1: route A, Apr 1  │ ⏭️  SKIPPED (already clustered)
# MAGIC   │ File 2: route A, Apr 2  │ ⏭️  SKIPPED
# MAGIC   │ File 3: route 1, Apr 1  │ ⏭️  SKIPPED
# MAGIC   │ File 4: route 7, Apr 1  │ ⏭️  SKIPPED
# MAGIC   │ File 5: route A, Apr 2-3│ ✅ Reclustered (merged new data)
# MAGIC   │ File 6: route 7, Apr 3  │ ✅ Reclustered
# MAGIC   └─────────────────────────┘
# MAGIC   Only 2 files rewritten instead of 6! ───► Much faster on large tables
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Summary Comparison
# MAGIC
# MAGIC ```
# MAGIC   ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC   │                    What each operation does                        │
# MAGIC   ├──────────────────┬──────────────────────────────────────────────────┤
# MAGIC   │  OPTIMIZE        │  Compact small files → fewer large files        │
# MAGIC   │                  │  (like defragmenting a hard drive)              │
# MAGIC   ├──────────────────┼──────────────────────────────────────────────────┤
# MAGIC   │  VACUUM          │  Delete stale files from storage                │
# MAGIC   │                  │  (like emptying the recycle bin)                │
# MAGIC   ├──────────────────┼──────────────────────────────────────────────────┤
# MAGIC   │  ZORDER          │  Sort data by columns during OPTIMIZE           │
# MAGIC   │  (legacy)        │  (like alphabetizing ALL books every time)      │
# MAGIC   ├──────────────────┼──────────────────────────────────────────────────┤
# MAGIC   │  LIQUID CLUSTER  │  Smart ZORDER — only sorts NEW books            │
# MAGIC   │  (modern)        │  (set once, runs incrementally forever)         │
# MAGIC   ├──────────────────┼──────────────────────────────────────────────────┤
# MAGIC   │  ANALYZE TABLE   │  Compute statistics for query optimizer         │
# MAGIC   │                  │  (like updating the library catalog index)      │
# MAGIC   └──────────────────┴──────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Parameters
# MAGIC %sql
# MAGIC -- Parameter: catalog name (switch between mta_rtransit / mta_rtransit_dev)
# MAGIC CREATE WIDGET TEXT catalog DEFAULT 'mta_rtransit';

# COMMAND ----------

# DBTITLE 1,Maintenance function
# MAGIC %sql
# MAGIC -- ---------------------------------------------------------------
# MAGIC -- LIQUID CLUSTERING (one-time, idempotent)
# MAGIC -- Replaces legacy ZORDER — set once, OPTIMIZE auto-applies.
# MAGIC -- Only needed on large, frequently filtered tables.
# MAGIC -- Small tables (dim_route_ref, _cosmos_watermarks, dim_route,
# MAGIC -- route_delay_kpi_daily) skip clustering — plain OPTIMIZE suffices.
# MAGIC -- ---------------------------------------------------------------
# MAGIC
# MAGIC ALTER TABLE ${catalog}.bronze.gtfs_rt_events       CLUSTER BY (route_id, ingested_at);
# MAGIC ALTER TABLE ${catalog}.bronze.eventhub_gtfs_raw     CLUSTER BY (enqueued_time);
# MAGIC ALTER TABLE ${catalog}.silver.fact_trip_delay_event CLUSTER BY (route_id, event_ts);

# COMMAND ----------

# DBTITLE 1,Table configuration
# MAGIC %sql
# MAGIC -- ---------------------------------------------------------------
# MAGIC -- OPTIMIZE: compact small files + apply liquid clustering.
# MAGIC -- Tables with CLUSTER BY get incremental clustering automatically.
# MAGIC -- Small tables just get file compaction.
# MAGIC -- ---------------------------------------------------------------
# MAGIC
# MAGIC -- Bronze
# MAGIC OPTIMIZE ${catalog}.bronze.gtfs_rt_events;
# MAGIC OPTIMIZE ${catalog}.bronze.eventhub_gtfs_raw;
# MAGIC OPTIMIZE ${catalog}.bronze.dim_route_ref;
# MAGIC OPTIMIZE ${catalog}.bronze._cosmos_watermarks;
# MAGIC
# MAGIC -- Silver
# MAGIC OPTIMIZE ${catalog}.silver.fact_trip_delay_event;
# MAGIC OPTIMIZE ${catalog}.silver.dim_route;
# MAGIC
# MAGIC -- Gold
# MAGIC OPTIMIZE ${catalog}.gold.route_delay_kpi_daily;

# COMMAND ----------

# DBTITLE 1,Run maintenance on all tables
# MAGIC %sql
# MAGIC -- ---------------------------------------------------------------
# MAGIC -- VACUUM: remove files no longer referenced by the Delta log.
# MAGIC -- Default retention: 168 hours (7 days).
# MAGIC -- Safety check is ON — will not delete files within retention.
# MAGIC -- ---------------------------------------------------------------
# MAGIC
# MAGIC -- Bronze
# MAGIC VACUUM ${catalog}.bronze.gtfs_rt_events        RETAIN 168 HOURS;
# MAGIC VACUUM ${catalog}.bronze.eventhub_gtfs_raw      RETAIN 168 HOURS;
# MAGIC VACUUM ${catalog}.bronze.dim_route_ref          RETAIN 168 HOURS;
# MAGIC VACUUM ${catalog}.bronze._cosmos_watermarks      RETAIN 168 HOURS;
# MAGIC
# MAGIC -- Silver
# MAGIC VACUUM ${catalog}.silver.fact_trip_delay_event   RETAIN 168 HOURS;
# MAGIC VACUUM ${catalog}.silver.dim_route               RETAIN 168 HOURS;
# MAGIC
# MAGIC -- Gold
# MAGIC VACUUM ${catalog}.gold.route_delay_kpi_daily     RETAIN 168 HOURS;

# COMMAND ----------

# DBTITLE 1,Summary report
# MAGIC %sql
# MAGIC -- ---------------------------------------------------------------
# MAGIC -- ANALYZE TABLE: refresh optimizer statistics for better
# MAGIC -- query plans (predicate pushdown, join reordering).
# MAGIC -- ---------------------------------------------------------------
# MAGIC
# MAGIC -- Bronze
# MAGIC ANALYZE TABLE ${catalog}.bronze.gtfs_rt_events        COMPUTE STATISTICS;
# MAGIC ANALYZE TABLE ${catalog}.bronze.eventhub_gtfs_raw      COMPUTE STATISTICS;
# MAGIC ANALYZE TABLE ${catalog}.bronze.dim_route_ref          COMPUTE STATISTICS;
# MAGIC ANALYZE TABLE ${catalog}.bronze._cosmos_watermarks      COMPUTE STATISTICS;
# MAGIC
# MAGIC -- Silver
# MAGIC ANALYZE TABLE ${catalog}.silver.fact_trip_delay_event   COMPUTE STATISTICS;
# MAGIC ANALYZE TABLE ${catalog}.silver.dim_route               COMPUTE STATISTICS;
# MAGIC
# MAGIC -- Gold
# MAGIC ANALYZE TABLE ${catalog}.gold.route_delay_kpi_daily     COMPUTE STATISTICS;

# COMMAND ----------

# DBTITLE 1,Table detail after maintenance
# ---------------------------------------------------------------
# Post-maintenance summary: file counts, sizes, clustering state.
# (Python needed because DESCRIBE DETAIL can't be used as subquery)
# ---------------------------------------------------------------
from functools import reduce
import pyspark.sql.functions as F

catalog = dbutils.widgets.get("catalog")

tables = [
    ("bronze", "gtfs_rt_events"),
    ("bronze", "eventhub_gtfs_raw"),
    ("bronze", "dim_route_ref"),
    ("bronze", "_cosmos_watermarks"),
    ("silver", "fact_trip_delay_event"),
    ("silver", "dim_route"),
    ("gold",   "route_delay_kpi_daily"),
]

dfs = []
for schema, table in tables:
    fqn = f"`{catalog}`.`{schema}`.`{table}`"
    try:
        df = (
            spark.sql(f"DESCRIBE DETAIL {fqn}")
            .select(
                F.lit(f"{schema}.{table}").alias("table"),
                "format",
                "numFiles",
                F.round(F.col("sizeInBytes") / 1048576, 2).alias("size_mb"),
                "clusteringColumns",
                "lastModified",
            )
        )
        dfs.append(df)
    except Exception as e:
        print(f"  \u26a0\ufe0f {fqn}: {e}")

if dfs:
    display(reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), dfs))
