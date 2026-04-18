# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ## 05 — Silver: `fact_trip_delay_event`
# MAGIC Merges **batch** (`bronze.gtfs_rt_events`) and **streaming** (`bronze.eventhub_gtfs_raw`) bronze
# MAGIC into a single deduplicated fact table with data-quality flags.
# MAGIC
# MAGIC **Logic:**
# MAGIC 1. Read both bronze sources; parse the EH raw JSON payload
# MAGIC 2. Union into a common schema
# MAGIC 3. Deduplicate on document `id` (same event can arrive via both paths)
# MAGIC 4. Derive `event_ts`, generate surrogate key, set `is_valid` flag
# MAGIC 5. `MERGE` into `silver.fact_trip_delay_event` (idempotent — safe to re-run)

# COMMAND ----------

# DBTITLE 1,Config
# ── Table references ──
BRONZE_BATCH  = "mta_rtransit.bronze.gtfs_rt_events"
BRONZE_EH     = "mta_rtransit.bronze.eventhub_gtfs_raw"
SILVER_TABLE  = "mta_rtransit.silver.fact_trip_delay_event"

# COMMAND ----------

# DBTITLE 1,Read & union both bronze sources
from pyspark.sql import functions as F

# ── 1. Batch bronze (already flattened by notebook 03) ──
batch = spark.table(BRONZE_BATCH).select(
    F.col("id"),
    F.col("route_id"),
    F.col("trip_id"),
    F.col("trip_update_timestamp"),
    F.col("first_stop_delay_sec"),
    F.col("ingest_batch_id"),
    F.col("ingested_at"),
    F.lit("batch").alias("_source"),
)

# ── 2. Streaming bronze — parse the canonical JSON envelope ──
eh_raw = spark.table(BRONZE_EH)

eh_parsed = eh_raw.select(
    F.get_json_object("payload", "$.id").alias("id"),
    F.get_json_object("payload", "$.route_id").alias("route_id"),
    F.get_json_object("payload", "$.payload.trip_id").alias("trip_id"),
    F.get_json_object("payload", "$.payload.trip_update_timestamp").cast("long").alias("trip_update_timestamp"),
    F.get_json_object("payload", "$.payload.first_stop_delay_sec").cast("int").alias("first_stop_delay_sec"),
    F.get_json_object("payload", "$.metadata.ingest_batch_id").alias("ingest_batch_id"),
    F.to_timestamp(F.get_json_object("payload", "$.metadata.ingested_at")).alias("ingested_at"),
    F.lit("event_hub").alias("_source"),
)

# ── 3. Union both paths ──
unioned = batch.unionByName(eh_parsed)
print(f"Batch rows:  {batch.count()}")
print(f"EH rows:     {eh_parsed.count()}")
print(f"Union total: {unioned.count()}")

# COMMAND ----------

# DBTITLE 1,Deduplicate across sources
from pyspark.sql import Window

# ── 4. Deduplicate on document id ──
#   Same event can land via both Cosmos (batch) and Event Hubs (stream).
#   Keep the freshest ingestion per id (prefer batch source if tied).
w = Window.partitionBy("id").orderBy(
    F.desc("ingested_at"),
    F.when(F.col("_source") == "batch", 0).otherwise(1),  # batch wins ties
)

deduped = (
    unioned
    .withColumn("_rn", F.row_number().over(w))
    .filter(F.col("_rn") == 1)
    .drop("_rn", "_source")
)

print(f"After dedup: {deduped.count()} unique events")

# COMMAND ----------

# DBTITLE 1,Transform to silver schema
# ── 5. Transform to silver schema ──
silver_df = deduped.select(
    # Surrogate key — reuse the stable doc-id hash
    F.col("id").alias("event_sk"),

    F.col("route_id"),
    F.col("trip_id"),

    # Coalesce event time: epoch trip_update_timestamp → ingested_at fallback
    F.coalesce(
        F.from_unixtime(F.col("trip_update_timestamp")),
        F.col("ingested_at"),
    ).cast("timestamp").alias("event_ts"),

    F.col("first_stop_delay_sec").alias("delay_sec"),
    F.col("ingest_batch_id"),

    # Data-quality flag
    (
        F.col("route_id").isNotNull()
        & F.col("trip_id").isNotNull()
        & F.col("ingested_at").isNotNull()
    ).alias("is_valid"),
)

display(silver_df.limit(10))

# COMMAND ----------

# DBTITLE 1,MERGE into silver table
# ── 6. MERGE into silver (idempotent upsert on surrogate key) ──
silver_df.createOrReplaceTempView("_silver_staging")

merge_result = spark.sql(f"""
    MERGE INTO {SILVER_TABLE} AS t
    USING _silver_staging AS s
    ON t.event_sk = s.event_sk
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# Report
cnt = spark.table(SILVER_TABLE).count()
print(f"MERGE complete → {SILVER_TABLE} now has {cnt} rows")

# COMMAND ----------

# DBTITLE 1,Verify silver fact by route
# MAGIC %sql
# MAGIC -- Quick verification
# MAGIC SELECT route_id,
# MAGIC        COUNT(*)            AS events,
# MAGIC        SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) AS valid_events,
# MAGIC        AVG(delay_sec)      AS avg_delay_sec,
# MAGIC        MIN(event_ts)        AS earliest,
# MAGIC        MAX(event_ts)        AS latest
# MAGIC FROM   mta_rtransit.silver.fact_trip_delay_event
# MAGIC GROUP  BY route_id
# MAGIC ORDER  BY events DESC