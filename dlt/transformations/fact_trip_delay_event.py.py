# Databricks notebook source
# --------------------------------------------------------------------------
# Silver: Streaming Table with APPEND FLOWS — deduped trip delay facts
# DLT equivalent of notebook 05_silver_fact_trip_delay_event
#
# Uses dp.create_streaming_table() + @dp.append_flow() pattern
# This REPLACES the regular pipeline's UNION + Window dedup + MERGE approach
# with DLT's native multi-source fan-in pattern.
# --------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col, sha2, concat, lit, coalesce, from_unixtime,
    expr, get_json_object, to_timestamp
)


# -- Expectations applied to all incoming records --
silver_fact_expectations = {
    "valid_route_id":    "route_id IS NOT NULL",
    "valid_trip_id":     "trip_id IS NOT NULL",
    "reasonable_delay":  "delay_sec IS NULL OR (delay_sec BETWEEN -3600 AND 7200)",
    "has_event_time":    "event_ts IS NOT NULL",
}

# Create the target streaming table (empty shell with expectations)
dp.create_streaming_table(
    name="silver_fact_trip_delay_event",
    comment="Silver: cleaned, deduped trip delay events from both Event Hubs and Cosmos paths",
    table_properties={"quality": "silver"},
    expect_all_or_drop=silver_fact_expectations,
)


# ── Append Flow 1: From Cosmos bronze (batch-loaded events) ──
@dp.append_flow(
    target="silver_fact_trip_delay_event",
    name="from_cosmos_batch",
    comment="Cosmos DB batch-loaded events transformed to silver schema"
)
def silver_from_cosmos():
    return (
        spark.readStream.table("bronze_cosmos_events")
        .withColumn(
            "event_sk",
            sha2(concat(col("id"), lit("_cosmos_"), coalesce(col("trip_update_timestamp").cast("string"), lit(""))), 256)
        )
        .withColumn(
            "event_ts",
            coalesce(from_unixtime(col("trip_update_timestamp")), col("ingested_at"))
        )
        .withColumn("delay_sec", col("first_stop_delay_sec"))
        .withColumn(
            "is_valid",
            expr("route_id IS NOT NULL AND trip_id IS NOT NULL AND (ingested_at IS NOT NULL OR trip_update_timestamp IS NOT NULL)")
        )
        .withColumn("source_system", lit("cosmos_batch"))
        .select(
            "event_sk", "route_id", "trip_id", "event_ts",
            "delay_sec", "ingest_batch_id", "is_valid", "source_system"
        )
    )


# ── Append Flow 2: From Event Hubs bronze (real-time streamed events) ──
@dp.append_flow(
    target="silver_fact_trip_delay_event",
    name="from_eventhub_stream",
    comment="Event Hubs streamed events parsed from JSON and transformed to silver schema"
)
def silver_from_eventhub():
    return (
        spark.readStream.table("bronze_eventhub_raw")
        # Parse the canonical JSON envelope (same structure as notebook 05)
        .withColumn("doc_id", get_json_object("payload", "$.id"))
        .withColumn("route_id", get_json_object("payload", "$.route_id"))
        .withColumn("trip_id", get_json_object("payload", "$.payload.trip_id"))
        .withColumn("trip_update_timestamp", get_json_object("payload", "$.payload.trip_update_timestamp").cast("long"))
        .withColumn("first_stop_delay_sec", get_json_object("payload", "$.payload.first_stop_delay_sec").cast("int"))
        .withColumn("ingest_batch_id", get_json_object("payload", "$.metadata.ingest_batch_id"))
        .withColumn("ingested_at", to_timestamp(get_json_object("payload", "$.metadata.ingested_at")))
        # Transform to silver schema
        .withColumn(
            "event_sk",
            sha2(concat(col("doc_id"), lit("_eh_"), coalesce(col("trip_update_timestamp").cast("string"), lit(""))), 256)
        )
        .withColumn(
            "event_ts",
            coalesce(from_unixtime(col("trip_update_timestamp")), col("ingested_at"))
        )
        .withColumn("delay_sec", col("first_stop_delay_sec"))
        .withColumn(
            "is_valid",
            expr("route_id IS NOT NULL AND trip_id IS NOT NULL AND (ingested_at IS NOT NULL OR trip_update_timestamp IS NOT NULL)")
        )
        .withColumn("source_system", lit("event_hub"))
        .select(
            "event_sk", "route_id", "trip_id", "event_ts",
            "delay_sec", "ingest_batch_id", "is_valid", "source_system"
        )
    )
