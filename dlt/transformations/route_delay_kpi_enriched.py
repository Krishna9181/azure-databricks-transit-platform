# Databricks notebook source
# --------------------------------------------------------------------------
# Gold: Enriched KPIs with route names (Materialized View)
# DLT equivalent of the CREATE VIEW in notebook 07
# Persisted as MV instead of a view — faster for Power BI DirectQuery
# --------------------------------------------------------------------------
from pyspark import pipelines as dp


@dp.materialized_view(
    name="gold_route_delay_kpi_enriched",
    comment="Gold: daily delay KPIs enriched with route dimension for BI",
    table_properties={"quality": "gold"}
)
@dp.expect("has_route_name", "route_short_name IS NOT NULL")
def gold_route_delay_kpi_enriched():
    kpi = spark.read.table("gold_route_delay_kpi_daily")
    routes = spark.read.table("silver_dim_route")
    return (
        kpi.join(routes, "route_id", "left")
        .select(
            "kpi_date",
            kpi["route_id"],
            "route_short_name",
            "route_long_name",
            "route_type",
            "trip_update_cnt",
            "avg_delay_sec",
            "p95_delay_sec"
        )
    )
