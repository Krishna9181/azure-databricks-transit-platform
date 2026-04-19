# Databricks notebook source
# --------------------------------------------------------------------------
# Silver: Active route dimension (Materialized View)
# DLT equivalent of notebook 06_silver_dim_route
# --------------------------------------------------------------------------
from pyspark import pipelines as dp


@dp.materialized_view(
    name="silver_dim_route",
    comment="Silver: active route dimension from existing SCD2 table",
    table_properties={"quality": "silver"}
)
@dp.expect_or_fail("valid_route_id", "route_id IS NOT NULL")
@dp.expect_or_fail("has_route_name", "route_short_name IS NOT NULL OR route_long_name IS NOT NULL")
def silver_dim_route():
    return (
        spark.read.table("mta_rtransit.silver.dim_route")
        .select(
            "route_id", "route_short_name", "route_long_name",
            "route_type", "effective_from", "effective_to"
        )
        .filter("effective_to IS NULL")  # active routes only
    )
