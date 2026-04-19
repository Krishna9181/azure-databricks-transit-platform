# Databricks notebook source
# --------------------------------------------------------------------------
# Bronze: Cosmos DB events (streamed from existing Delta table)
# DLT equivalent of notebook 03_batch_cosmos_to_delta
#
# NOTE: cosmos.oltp.changeFeed is NOT supported on serverless clusters.
# Instead, we stream incrementally from the existing external Bronze Delta
# table (populated by notebook 03). DLT tracks its own checkpoint — only
# processes new rows added since the last pipeline run.
#
# The regular batch pipeline (notebook 03) handles:
#   Cosmos DB → Bronze Delta (watermark-based incremental)
# This DLT pipeline handles:
#   Bronze Delta → Silver → Gold (with expectations + lineage)
# --------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.table(
    name="bronze_cosmos_events",
    comment="Bronze: GTFS-RT events from Cosmos DB (streamed incrementally from existing Delta table)",
    table_properties={"quality": "bronze"}
)
@dp.expect("valid_id", "id IS NOT NULL")
@dp.expect("valid_route_id", "route_id IS NOT NULL")
@dp.expect("has_timestamp", "trip_update_timestamp IS NOT NULL OR ingested_at IS NOT NULL")
def bronze_cosmos_events():
    return (
        spark.readStream.table("mta_rtransit.bronze.gtfs_rt_events")
        .withColumn("processing_ts", current_timestamp())
    )
