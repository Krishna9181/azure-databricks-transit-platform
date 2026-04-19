# Databricks notebook source
# --------------------------------------------------------------------------
# Bronze: Event Hubs raw events (streamed from existing Delta table)
# DLT equivalent of notebook 04_streaming_eventhubs_to_bronze
#
# NOTE: On serverless pipelines, Azure Key Vault-backed secrets do not
# resolve. Direct Kafka ingestion from Event Hubs requires the connection
# string secret. Instead, we stream incrementally from the existing EH
# bronze Delta table (populated by notebook 04).
#
# Architecture:
#   Regular pipeline (notebook 04): Event Hubs → Bronze Delta (raw)
#   DLT pipeline (this file):       Bronze Delta → Silver → Gold (quality)
# --------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.table(
    name="bronze_eventhub_raw",
    comment="Bronze: raw Event Hubs GTFS-RT events (streamed incrementally from existing Delta table)",
    table_properties={"quality": "bronze"}
)
@dp.expect("has_payload", "payload IS NOT NULL")
@dp.expect("has_enqueued_time", "enqueued_time IS NOT NULL")
def bronze_eventhub_raw():
    return (
        spark.readStream.table("mta_rtransit.bronze.eventhub_gtfs_raw")
        .withColumn("processing_ts", current_timestamp())
    )
