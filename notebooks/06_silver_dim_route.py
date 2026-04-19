# Databricks notebook source

# -- Catalog parameter (set by DABs or default to dev) --
dbutils.widgets.text("catalog", "mta_rtransit_dev")
catalog = dbutils.widgets.get("catalog")
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ## 06 — Silver: `dim_route`
# MAGIC Builds the route dimension from the **bronze reference table** (`dim_route_ref`),
# MAGIC which is populated by notebook `04a` from the MTA GTFS static `routes.txt` feed.
# MAGIC
# MAGIC **Priority chain:**
# MAGIC 1. `dim_route_ref` (latest snapshot) — official GTFS route attributes
# MAGIC 2. Hardcoded MTA mapping — fallback for routes in fact data but not in the reference file
# MAGIC 3. Route ID as short name + `UNKNOWN` type — last resort for completely unknown routes
# MAGIC
# MAGIC Maintains `effective_from` / `effective_to` for SCD-style tracking.

# COMMAND ----------

# DBTITLE 1,Config
# ── Table references ──
BRONZE_REF    = f"{catalog}.bronze.dim_route_ref"
BRONZE_FACT   = f"{catalog}.bronze.gtfs_rt_events"
SILVER_TABLE  = f"{catalog}.silver.dim_route"

# COMMAND ----------

# DBTITLE 1,MTA route reference mapping
from pyspark.sql import functions as F

# ── Hardcoded MTA route mapping (FALLBACK ONLY) ──
# Used for routes that appear in fact data but are missing from
# bronze.dim_route_ref (e.g. special/express variants like 7X).
# Once 04a populates dim_route_ref, this is rarely needed.
MTA_ROUTES_FALLBACK = {
    "1":  ("1",  "Broadway – 7th Ave Local",       "SUBWAY"),
    "2":  ("2",  "7th Ave Express",                "SUBWAY"),
    "3":  ("3",  "7th Ave Express",                "SUBWAY"),
    "4":  ("4",  "Lexington Ave Express",          "SUBWAY"),
    "5":  ("5",  "Lexington Ave Express",          "SUBWAY"),
    "6":  ("6",  "Lexington Ave Local",            "SUBWAY"),
    "7":  ("7",  "Flushing Local",                 "SUBWAY"),
    "7X": ("7X", "Flushing Express",               "SUBWAY"),
    "A":  ("A",  "8th Ave Express",                "SUBWAY"),
    "B":  ("B",  "6th Ave Express",                "SUBWAY"),
    "C":  ("C",  "8th Ave Local",                  "SUBWAY"),
    "D":  ("D",  "6th Ave Express",                "SUBWAY"),
    "E":  ("E",  "8th Ave Local",                  "SUBWAY"),
    "F":  ("F",  "6th Ave Local",                  "SUBWAY"),
    "G":  ("G",  "Brooklyn–Queens Crosstown",      "SUBWAY"),
    "J":  ("J",  "Nassau St Local",                "SUBWAY"),
    "L":  ("L",  "14th St–Canarsie",               "SUBWAY"),
    "M":  ("M",  "6th Ave Local",                  "SUBWAY"),
    "N":  ("N",  "Broadway Express",               "SUBWAY"),
    "Q":  ("Q",  "Broadway Express",               "SUBWAY"),
    "R":  ("R",  "Broadway Local",                 "SUBWAY"),
    "W":  ("W",  "Broadway Local",                 "SUBWAY"),
    "Z":  ("Z",  "Nassau St Express",              "SUBWAY"),
    "GS": ("S",  "42nd St Shuttle",                "SUBWAY"),
    "FS": ("S",  "Franklin Ave Shuttle",           "SUBWAY"),
    "H":  ("S",  "Rockaway Park Shuttle",          "SUBWAY"),
    "SI": ("SIR","Staten Island Railway",          "RAILWAY"),
}

fallback_df = spark.createDataFrame(
    [(k, v[0], v[1], v[2]) for k, v in MTA_ROUTES_FALLBACK.items()],
    ["route_id", "route_short_name", "route_long_name", "route_type"],
)
print(f"Fallback mapping loaded: {fallback_df.count()} routes")

# COMMAND ----------

# DBTITLE 1,Extract distinct routes
from pyspark.sql import Window

ref_count = spark.table(BRONZE_REF).count()
print(f"bronze.dim_route_ref rows: {ref_count}")

if ref_count > 0:
    # ── PRIMARY: parse latest snapshot from dim_route_ref ──
    w = Window.partitionBy("route_id").orderBy(F.desc("snapshot_date"))
    ref_routes = (
        spark.table(BRONZE_REF)
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("route_id"),
            F.get_json_object("payload", "$.route_short_name").alias("route_short_name"),
            F.get_json_object("payload", "$.route_long_name").alias("route_long_name"),
            # GTFS route_type: 1=Subway, 2=Rail, 3=Bus
            F.when(F.get_json_object("payload", "$.route_type") == "1", "SUBWAY")
             .when(F.get_json_object("payload", "$.route_type") == "2", "RAILWAY")
             .when(F.get_json_object("payload", "$.route_type") == "3", "BUS")
             .otherwise("OTHER")
             .alias("route_type"),
        )
    )

    # FALLBACK: routes in fact data but NOT in the reference file
    fact_only = (
        spark.table(BRONZE_FACT)
        .select("route_id").distinct()
        .filter(F.col("route_id").isNotNull())
        .join(ref_routes.select("route_id"), on="route_id", how="left_anti")
        .join(fallback_df, on="route_id", how="left")
        .withColumn("route_short_name", F.coalesce(F.col("route_short_name"), F.col("route_id")))
        .withColumn("route_long_name",  F.coalesce(F.col("route_long_name"),  F.lit(None)))
        .withColumn("route_type",       F.coalesce(F.col("route_type"),       F.lit("UNKNOWN")))
        .select("route_id", "route_short_name", "route_long_name", "route_type")
    )

    route_source = ref_routes.unionByName(fact_only)
    print(f"Routes: {ref_routes.count()} from ref + {fact_only.count()} fact-only = {route_source.count()} total")

else:
    # ── NO REFERENCE DATA: derive from fact + hardcoded mapping ──
    route_source = (
        spark.table(BRONZE_FACT)
        .select("route_id").distinct()
        .filter(F.col("route_id").isNotNull())
        .join(fallback_df, on="route_id", how="left")
        .withColumn("route_short_name", F.coalesce(F.col("route_short_name"), F.col("route_id")))
        .withColumn("route_long_name",  F.coalesce(F.col("route_long_name"),  F.lit(None)))
        .withColumn("route_type",       F.coalesce(F.col("route_type"),       F.lit("UNKNOWN")))
        .select("route_id", "route_short_name", "route_long_name", "route_type")
    )
    print(f"dim_route_ref empty — derived {route_source.count()} routes from fact + fallback")

# COMMAND ----------

# DBTITLE 1,Enrich routes with metadata
# ── Add effective dates (route attributes already resolved above) ──

earliest = (
    spark.table(BRONZE_FACT)
    .groupBy("route_id")
    .agg(F.min(F.col("ingested_at")).cast("date").alias("effective_from"))
)

dim_route_df = (
    route_source
    .join(earliest, on="route_id", how="left")
    .withColumn("effective_from", F.coalesce(F.col("effective_from"), F.current_date()))
    .withColumn("effective_to", F.lit(None).cast("date"))   # NULL = active
    .select(
        "route_id", "route_short_name", "route_long_name",
        "route_type", "effective_from", "effective_to",
    )
)

display(dim_route_df)

# COMMAND ----------

# DBTITLE 1,MERGE into silver dim_route
# ── MERGE into silver (upsert on route_id) ──
dim_route_df.createOrReplaceTempView("_dim_route_staging")

spark.sql(f"""
    MERGE INTO {SILVER_TABLE} AS t
    USING _dim_route_staging AS s
    ON t.route_id = s.route_id
    WHEN MATCHED THEN UPDATE SET
        t.route_short_name = s.route_short_name,
        t.route_long_name  = s.route_long_name,
        t.route_type       = s.route_type,
        t.effective_from   = s.effective_from,
        t.effective_to     = s.effective_to
    WHEN NOT MATCHED THEN INSERT *
""")

cnt = spark.table(SILVER_TABLE).count()
print(f"MERGE complete → {SILVER_TABLE} now has {cnt} routes")

# COMMAND ----------

# DBTITLE 1,Verify silver dim_route
# MAGIC %sql
# MAGIC -- Verify dim_route
# MAGIC SELECT * FROM {catalog}.silver.dim_route
# MAGIC ORDER BY route_id