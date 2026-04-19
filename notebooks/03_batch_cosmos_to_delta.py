# Databricks notebook source
# -- Catalog parameter (set by DABs or default to dev) --
dbutils.widgets.text("catalog", "mta_rtransit_dev")
catalog = dbutils.widgets.get("catalog")
print(f"Using catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 03 — Scheduled batch: Cosmos DB → Delta (incremental via `_ts` watermark)
# MAGIC
# MAGIC **Before:** `read_all_items()` re-read the **entire** `gtfs_rt_batch` container every run (huge, redundant Bronze appends).
# MAGIC
# MAGIC **Now:** Each run queries Cosmos for documents with **`_ts > last_watermark`** (only new/changed docs since the last successful load). The watermark is persisted in a small Delta table so it survives across scheduled runs.
# MAGIC
# MAGIC **How it works:**
# MAGIC 1. Read the last-processed `_ts` from the watermark table
# MAGIC 2. Query Cosmos: `SELECT * FROM c WHERE c._ts > {last_ts}`
# MAGIC 3. Flatten & append to **`bronze.gtfs_rt_events`**
# MAGIC 4. Update the watermark with the new max `_ts`
# MAGIC
# MAGIC **First run:** Watermark defaults to 0 → pulls all documents (full backfill). Subsequent runs pull only changes.
# MAGIC
# MAGIC **Full replay:** Delete the watermark row (or reset `last_ts` to 0) and re-run.

# COMMAND ----------

# MAGIC %pip install --quiet azure-cosmos opencensus-ext-azure

# COMMAND ----------

# ── Secrets from Azure Key Vault (scope: mta-kv) ──
COSMOS_ENDPOINT = dbutils.secrets.get("mta-kv", "cosmos-endpoint")
COSMOS_KEY      = dbutils.secrets.get("mta-kv", "cosmos-key")

COSMOS_DATABASE  = "gtfs"
COSMOS_CONTAINER = "gtfs_rt_batch"

BRONZE_TABLE = f"{catalog}.bronze.gtfs_rt_events"

# Delta table to persist the incremental watermark across runs
WATERMARK_TABLE = f"{catalog}.bronze._cosmos_watermarks"
WATERMARK_KEY   = "gtfs_rt_batch"   # row identifier in the watermark table

# COMMAND ----------

# DBTITLE 1,App Insights setup
import logging
from opencensus.ext.azure.log_exporter import AzureEventHandler

APPINSIGHTS_CONN = dbutils.secrets.get("mta-kv", "appinsights-connection-string")

logger = logging.getLogger("mta_batch_ingest")
logger.setLevel(logging.INFO)

# AzureEventHandler → customEvents table (distinct event names)
# AzureLogHandler → traces table (everything looks the same)
if not any(isinstance(h, AzureEventHandler) for h in logger.handlers):
    handler = AzureEventHandler(connection_string=APPINSIGHTS_CONN)
    logger.addHandler(handler)

print("✓ Application Insights configured → customEvents table (batch)")

# COMMAND ----------

from pyspark.sql import functions as F
from azure.cosmos import CosmosClient
import json, pandas as pd, time

# ── 1. Ensure watermark table exists ──
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
        source_key STRING,
        last_ts    LONG,
        updated_at TIMESTAMP
    ) USING DELTA
""")

# ── 2. Read last-processed Cosmos _ts ──
wm = spark.sql(f"""
    SELECT last_ts FROM {WATERMARK_TABLE}
    WHERE source_key = '{WATERMARK_KEY}'
""").collect()
last_ts = wm[0]["last_ts"] if wm else 0
print(f"Watermark: _ts = {last_ts}")

# ── 3. Query Cosmos for docs newer than watermark ──
t0 = time.time()
client    = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY)
container = client.get_database_client(COSMOS_DATABASE).get_container_client(COSMOS_CONTAINER)

query = f"SELECT * FROM c WHERE c._ts > {last_ts}" if last_ts > 0 else "SELECT * FROM c"
docs  = list(container.query_items(query=query, enable_cross_partition_query=True))
cosmos_latency_ms = (time.time() - t0) * 1000
print(f"Fetched {len(docs)} new/changed documents from Cosmos")

# ── Telemetry: Cosmos query ──
logger.info("batch_cosmos_query", extra={
    "custom_dimensions": {
        "documents_fetched": len(docs),
        "cosmos_latency_ms": round(cosmos_latency_ms, 1),
        "watermark_from": last_ts,
        "container": COSMOS_CONTAINER,
    }
})

if not docs:
    print("Nothing new — skipping append.")
    logger.info("batch_cycle_skip", extra={
        "custom_dimensions": {"reason": "no_new_documents", "watermark": last_ts}
    })
else:
    new_max_ts = max(d.get("_ts", 0) for d in docs)

    # ── 4. Convert to Spark DataFrame ──
    pdf     = pd.DataFrame({"value": [json.dumps(d) for d in docs]})
    json_df = spark.createDataFrame(pdf)
    schema_str = (
        spark.range(1)
        .select(F.schema_of_json(F.lit(json.dumps(docs[0]))))
        .collect()[0][0]
    )
    raw = json_df.select(F.from_json(F.col("value"), schema_str).alias("d")).select("d.*")

    # ── 5. Flatten (handles both envelope and flat doc shapes) ──
    _cols = set(raw.columns)
    flat = (
        raw.select(
            F.col("id"),
            F.col("route_id"),
            F.coalesce(
                F.col("metadata.source_system"),
                F.col("source") if "source" in _cols else F.lit(None),
                F.lit("mta_gtfs_rt"),
            ).alias("source"),
            F.coalesce(
                F.col("payload.feed_entity_id"),
                F.col("feed_entity_id") if "feed_entity_id" in _cols else F.lit(None),
            ).alias("feed_entity_id"),
            F.coalesce(
                F.col("payload.trip_id"),
                F.col("trip_id") if "trip_id" in _cols else F.lit(None),
            ).alias("trip_id"),
            F.coalesce(
                F.col("payload.trip_update_timestamp").cast("long"),
                F.col("trip_update_timestamp").cast("long") if "trip_update_timestamp" in _cols else F.lit(None).cast("long"),
            ).alias("trip_update_timestamp"),
            F.coalesce(
                F.col("payload.first_stop_delay_sec").cast("int"),
                F.col("first_stop_delay_sec").cast("int") if "first_stop_delay_sec" in _cols else F.lit(None).cast("int"),
            ).alias("first_stop_delay_sec"),
            F.coalesce(
                F.col("metadata.ingest_batch_id"),
                F.col("ingest_batch_id") if "ingest_batch_id" in _cols else F.lit(None),
            ).alias("ingest_batch_id"),
            F.coalesce(
                F.to_timestamp(F.col("metadata.ingested_at")),
                F.to_timestamp(F.col("ingested_at")) if "ingested_at" in _cols else F.lit(None).cast("timestamp"),
            ).alias("ingested_at"),
        )
        .withColumn("lake_ingest_ts", F.current_timestamp())
    )

    flat = flat.select(
        "id", "route_id", "source", "feed_entity_id", "trip_id",
        "trip_update_timestamp", "first_stop_delay_sec",
        "ingest_batch_id", "ingested_at", "lake_ingest_ts",
    )

    cnt = flat.count()

    # ── Telemetry: route-level breakdown ──
    route_counts = flat.groupBy("route_id").count().collect()
    route_breakdown = {row["route_id"]: row["count"] for row in route_counts}
    for route_id, doc_count in route_breakdown.items():
        logger.info("batch_route_ingest", extra={
            "custom_dimensions": {
                "route_id": route_id,
                "document_count": doc_count,
                "pipeline": "batch_cosmos_to_delta",
            }
        })
    print(f"Route breakdown: {route_breakdown}")

    flat.write.mode("append").insertInto(BRONZE_TABLE)

    # ── 6. Update watermark ──
    spark.sql(f"""
        MERGE INTO {WATERMARK_TABLE} AS t
        USING (SELECT '{WATERMARK_KEY}' AS source_key, {new_max_ts} AS last_ts, current_timestamp() AS updated_at) AS s
        ON t.source_key = s.source_key
        WHEN MATCHED THEN UPDATE SET t.last_ts = s.last_ts, t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT *
    """)

    # ── Telemetry: batch cycle complete ──
    total_ms = (time.time() - t0) * 1000
    logger.info("batch_cycle_complete", extra={
        "custom_dimensions": {
            "documents_total": cnt,
            "distinct_routes": len(route_breakdown),
            "route_breakdown": json.dumps(route_breakdown),
            "watermark_from": last_ts,
            "watermark_to": new_max_ts,
            "total_latency_ms": round(total_ms, 1),
        }
    })
    print(f"Incremental load complete: {cnt} docs → {BRONZE_TABLE} | watermark _ts={new_max_ts}")

# COMMAND ----------

# DBTITLE 1,Flush App Insights telemetry
# Force flush App Insights telemetry before notebook exits
# opencensus AzureLogHandler batches events in a background thread —
# without explicit flush, short-lived batch jobs lose all telemetry.
import time
for h in logger.handlers:
    if hasattr(h, 'flush'):
        h.flush()
time.sleep(5)  # Allow background export thread to complete
print("✓ App Insights telemetry flushed")

# COMMAND ----------

# DBTITLE 1,Verify Bronze rows
# MAGIC %sql
# MAGIC -- Verify Bronze table
# MAGIC SELECT * FROM ${catalog}.bronze.gtfs_rt_events
# MAGIC ORDER BY lake_ingest_ts DESC
# MAGIC LIMIT 20

# COMMAND ----------

# DBTITLE 1,Check watermark state
# MAGIC %sql
# MAGIC -- Check current watermark
# MAGIC SELECT * FROM ${catalog}.bronze._cosmos_watermarks
