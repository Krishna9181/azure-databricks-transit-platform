# Databricks notebook source

# -- Catalog parameter (set by DABs or default to dev) --
dbutils.widgets.text("catalog", "mta_rtransit_dev")
catalog = dbutils.widgets.get("catalog")
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ## 07 — Gold: `route_delay_kpi_daily`
# MAGIC Daily KPI mart aggregating trip-update events **per route** for BI and dashboarding.
# MAGIC
# MAGIC **Grain:** one row per `(kpi_date, route_id)`
# MAGIC
# MAGIC | KPI | Definition |
# MAGIC |-----|------------|
# MAGIC | `trip_update_cnt` | Number of valid trip-update events that day |
# MAGIC | `avg_delay_sec` | Mean first-stop delay (NULL when feed omits delay) |
# MAGIC | `p95_delay_sec` | 95th-percentile delay (NULL when no delay data) |
# MAGIC
# MAGIC **Source:** `silver.fact_trip_delay_event` (only `is_valid = true` rows).
# MAGIC
# MAGIC **Enrichment:** An additional view `gold.route_delay_kpi_enriched` joins
# MAGIC with `silver.dim_route` to add human-readable route names for dashboards.
# MAGIC
# MAGIC **Idempotent:** Uses `MERGE` on `(kpi_date, route_id)` — safe to re-run.

# COMMAND ----------

# DBTITLE 1,Config
# ── Table references ──
SILVER_FACT  = f"{catalog}.silver.fact_trip_delay_event"
SILVER_DIM   = f"{catalog}.silver.dim_route"
GOLD_TABLE   = f"{catalog}.gold.route_delay_kpi_daily"

# COMMAND ----------

# DBTITLE 1,Read valid silver events
from pyspark.sql import functions as F

# ── 1. Read valid silver events ──
fact = (
    spark.table(SILVER_FACT)
    .filter(F.col("is_valid") == True)
    .withColumn("kpi_date", F.col("event_ts").cast("date"))
)

total_valid = fact.count()
date_range = fact.select(F.min("kpi_date"), F.max("kpi_date")).collect()[0]
print(f"Valid events: {total_valid}")
print(f"Date range:   {date_range[0]}  →  {date_range[1]}")

# COMMAND ----------

# DBTITLE 1,Aggregate daily KPIs per route
# ── 2. Aggregate daily KPIs per route ──
gold_df = (
    fact
    .groupBy("kpi_date", "route_id")
    .agg(
        F.count("*").alias("trip_update_cnt"),
        F.avg("delay_sec").alias("avg_delay_sec"),
        F.percentile_approx("delay_sec", 0.95).cast("double").alias("p95_delay_sec"),
    )
)

print(f"Gold rows to write: {gold_df.count()} (date × route combos)")
display(gold_df.orderBy("kpi_date", F.desc("trip_update_cnt")))

# COMMAND ----------

# DBTITLE 1,MERGE into gold table
# ── 3. MERGE into gold table (idempotent upsert) ──
gold_df.createOrReplaceTempView("_gold_staging")

spark.sql(f"""
    MERGE INTO {GOLD_TABLE} AS t
    USING _gold_staging AS s
    ON  t.kpi_date = s.kpi_date
    AND t.route_id = s.route_id
    WHEN MATCHED THEN UPDATE SET
        t.trip_update_cnt = s.trip_update_cnt,
        t.avg_delay_sec   = s.avg_delay_sec,
        t.p95_delay_sec   = s.p95_delay_sec
    WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.table(GOLD_TABLE).count()
print(f"MERGE complete → {GOLD_TABLE} now has {cnt} rows")

# COMMAND ----------

# DBTITLE 1,Create enriched BI view
# ── 4. Create enriched view for BI / dashboards ──
#   Joins gold KPIs with dim_route for human-readable route names
spark.sql(f"""
    CREATE OR REPLACE VIEW {catalog}.gold.route_delay_kpi_enriched AS
    SELECT
        k.kpi_date,
        k.route_id,
        d.route_short_name,
        d.route_long_name,
        d.route_type,
        k.trip_update_cnt,
        k.avg_delay_sec,
        k.p95_delay_sec
    FROM {GOLD_TABLE} k
    LEFT JOIN {SILVER_DIM} d
        ON k.route_id = d.route_id
       AND d.effective_to IS NULL   -- current version of the route
""")

print(f"View created: {catalog}.gold.route_delay_kpi_enriched")

# COMMAND ----------

# DBTITLE 1,Verify enriched gold KPIs
# MAGIC %sql
# MAGIC -- ── Verify: gold KPI table with route names ──
# MAGIC SELECT kpi_date,
# MAGIC        route_id,
# MAGIC        route_short_name,
# MAGIC        route_long_name,
# MAGIC        trip_update_cnt,
# MAGIC        avg_delay_sec,
# MAGIC        p95_delay_sec
# MAGIC FROM   {catalog}.gold.route_delay_kpi_enriched
# MAGIC ORDER  BY kpi_date, trip_update_cnt DESC

# COMMAND ----------

# DBTITLE 1,Daily summary across all routes
# MAGIC %sql
# MAGIC -- ── Summary: daily totals across all routes ──
# MAGIC SELECT kpi_date,
# MAGIC        COUNT(DISTINCT route_id)   AS routes_active,
# MAGIC        SUM(trip_update_cnt)       AS total_trip_updates,
# MAGIC        ROUND(AVG(avg_delay_sec), 1) AS overall_avg_delay_sec
# MAGIC FROM   {catalog}.gold.route_delay_kpi_daily
# MAGIC GROUP  BY kpi_date
# MAGIC ORDER  BY kpi_date DESC