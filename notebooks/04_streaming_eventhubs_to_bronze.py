# Databricks notebook source

# -- Catalog parameter (set by DABs or default to dev) --
dbutils.widgets.text("catalog", "mta_rtransit_dev")
catalog = dbutils.widgets.get("catalog")
print(f"Using catalog: {catalog}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 04 — Streaming: Event Hubs → `bronze.eventhub_gtfs_raw`
# MAGIC Consumes JSON bodies produced by **`01`** (same canonical document per message).
# MAGIC **Batch** path uses **`03`** from Cosmos — this path is **streaming-only** from Event Hubs.
# MAGIC
# MAGIC **Protocol:** Kafka-compatible endpoint (built-in `kafka` format — no external Maven JARs needed).
# MAGIC **Checkpoint:** `abfss://...` under your registered storage.

# COMMAND ----------

# DBTITLE 1,Install telemetry package
# MAGIC %pip install --quiet opencensus-ext-azure

# COMMAND ----------

# DBTITLE 1,App Insights setup
import logging
from opencensus.ext.azure.log_exporter import AzureEventHandler

APPINSIGHTS_CONN = dbutils.secrets.get("mta-kv", "appinsights-connection-string")

logger = logging.getLogger("mta_stream_ingest")
logger.setLevel(logging.INFO)

# AzureEventHandler → customEvents table (distinct event names)
if not any(isinstance(h, AzureEventHandler) for h in logger.handlers):
    handler = AzureEventHandler(connection_string=APPINSIGHTS_CONN)
    logger.addHandler(handler)

def flush_telemetry():
    """Call after each micro-batch to ensure telemetry is exported."""
    for h in logger.handlers:
        if hasattr(h, 'flush'):
            h.flush()

print("✓ Application Insights configured → customEvents table (streaming)")

# COMMAND ----------

# ── Secrets from Azure Key Vault (scope: mta-kv) ──
EVENTHUB_CONN = dbutils.secrets.get("mta-kv", "eventhub-connection-string")

# v3: fresh checkpoint for foreachBatch sink (incompatible with v2 writeStream.toTable checkpoint)
CHECKPOINT = f"abfss://demo@adlsgen2deportfolioeus.dfs.core.windows.net/{catalog}/checkpoints/eh_gtfs_raw_v3"
BRONZE_EH_TABLE = f"{catalog}.bronze.eventhub_gtfs_raw"

# COMMAND ----------

import re
from pyspark.sql import functions as F

# ── Derive Kafka config from the existing Event Hubs connection string ──
_namespace = re.search(r"sb://([^/]+)", EVENTHUB_CONN).group(1)
_topic     = re.search(r"EntityPath=([^;]+)", EVENTHUB_CONN).group(1)

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers":  f"{_namespace}:9093",
    "subscribe":                _topic,
    "kafka.sasl.mechanism":     "PLAIN",
    "kafka.security.protocol":  "SASL_SSL",
    "kafka.sasl.jaas.config":   (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="$ConnectionString" password="{EVENTHUB_CONN}";'
    ),
    "startingOffsets":          "earliest",
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
}

raw_stream = spark.readStream.format("kafka").options(**KAFKA_OPTIONS).load()

# Map Kafka schema → same column names expected by downstream cells
parsed = raw_stream.select(
    F.col("offset").cast("long").alias("sequence_number"),
    F.col("offset").cast("string").alias("offset"),
    F.col("timestamp").alias("enqueued_time"),
    F.decode(F.col("value"), "UTF-8").alias("payload"),
    F.current_timestamp().alias("bronze_ingest_ts"),
)

# COMMAND ----------

import json
import time

def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    t0 = time.time()
    cnt = batch_df.count()

    # Parse route_id from payload JSON for route-level telemetry
    route_df = batch_df.select(
        F.get_json_object("payload", "$.route_id").alias("route_id")
    ).groupBy("route_id").count().collect()
    route_breakdown = {row["route_id"] or "UNKNOWN": row["count"] for row in route_df}

    # Write to Delta table
    batch_df.write.mode("append").insertInto(BRONZE_EH_TABLE)

    batch_ms = (time.time() - t0) * 1000

    # Telemetry: route-level breakdown
    for route_id, doc_count in route_breakdown.items():
        logger.info("stream_route_ingest", extra={
            "custom_dimensions": {
                "route_id": route_id,
                "document_count": doc_count,
                "micro_batch_id": batch_id,
                "pipeline": "streaming_eh_to_bronze",
            }
        })

    # Telemetry: micro-batch summary
    logger.info("stream_batch_complete", extra={
        "custom_dimensions": {
            "micro_batch_id": batch_id,
            "messages_total": cnt,
            "distinct_routes": len(route_breakdown),
            "route_breakdown": json.dumps(route_breakdown),
            "batch_latency_ms": round(batch_ms, 1),
        }
    })
    flush_telemetry()
    print(f"Batch {batch_id}: {cnt} msgs, {len(route_breakdown)} routes, {batch_ms:.0f}ms")

query = (
    parsed
    .writeStream
    .foreachBatch(process_batch)
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ${catalog}.bronze.eventhub_gtfs_raw

# COMMAND ----------

