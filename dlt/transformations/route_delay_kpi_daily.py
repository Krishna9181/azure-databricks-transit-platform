# Databricks notebook source
# --------------------------------------------------------------------------
# Gold: Daily delay KPIs per route (Materialized View)
# DLT equivalent of notebook 07_gold_route_delay_kpi_daily
# No manual MERGE needed — MV auto-recomputes when silver data changes
# --------------------------------------------------------------------------
from pyspark import pipelines as dp
from pyspark.sql.functions import col, date_format, avg, count, percentile_approx


gold_kpi_expectations = {
    "valid_kpi_date":      "kpi_date IS NOT NULL",
    "valid_route":         "route_id IS NOT NULL",
    "positive_trip_count": "trip_update_cnt > 0",
}


@dp.materialized_view(
    name="gold_route_delay_kpi_daily",
    comment="Gold: daily aggregated delay KPIs per route (auto-recomputes from silver)",
    table_properties={"quality": "gold"}
)
@dp.expect_all(gold_kpi_expectations)
def gold_route_delay_kpi_daily():
    return (
        spark.read.table("silver_fact_trip_delay_event")
        .filter("is_valid = true")
        .withColumn("kpi_date", date_format("event_ts", "yyyy-MM-dd").cast("date"))
        .groupBy("route_id", "kpi_date")
        .agg(
            count("*").alias("trip_update_cnt"),
            avg("delay_sec").alias("avg_delay_sec"),
            percentile_approx(col("delay_sec"), 0.95).alias("p95_delay_sec")
        )
    )
