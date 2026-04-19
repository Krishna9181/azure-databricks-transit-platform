# Databricks notebook source

# -- Catalog parameter (set by DABs or default to dev) --
dbutils.widgets.text("catalog", "mta_rtransit_dev")
catalog = dbutils.widgets.get("catalog")
print(f"Using catalog: {catalog}")

# COMMAND ----------

# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ## 04a — Ingest MTA route reference (`routes.txt` → Bronze)
# MAGIC Downloads the **MTA GTFS static feed** (subway), extracts `routes.txt`, and
# MAGIC lands each route as a JSON snapshot in `bronze.dim_route_ref`.
# MAGIC
# MAGIC **Source:** `http://web.mta.info/developers/data/nyct/subway/google_transit.zip`
# MAGIC
# MAGIC **Schedule:** Weekly (routes rarely change). Each run creates a new
# MAGIC `snapshot_date` partition so historical snapshots are preserved.
# MAGIC
# MAGIC **Downstream:** Notebook `06_silver_dim_route` reads the latest snapshot
# MAGIC to build `silver.dim_route`.

# COMMAND ----------

# DBTITLE 1,Config
BRONZE_TABLE = f"{catalog}.bronze.dim_route_ref"
GTFS_ZIP_URL = "http://web.mta.info/developers/data/nyct/subway/google_transit.zip"

# COMMAND ----------

# DBTITLE 1,Download & parse routes.txt
import requests, zipfile, io, csv, json
from datetime import datetime, timezone

# ── 1. Download MTA GTFS static ZIP ──
print(f"Downloading {GTFS_ZIP_URL} ...")
resp = requests.get(GTFS_ZIP_URL, timeout=60)
resp.raise_for_status()
print(f"Downloaded {len(resp.content) / 1024:.0f} KB")

# ── 2. Extract routes.txt from the ZIP ──
z = zipfile.ZipFile(io.BytesIO(resp.content))
print(f"Files in ZIP: {z.namelist()}")

routes_csv = z.read("routes.txt").decode("utf-8-sig")  # BOM-safe
reader = csv.DictReader(io.StringIO(routes_csv))
routes = list(reader)
print(f"Parsed {len(routes)} routes from routes.txt")

# Preview first route
print(f"Sample: {json.dumps(routes[0], indent=2)}")

# COMMAND ----------

# DBTITLE 1,Build bronze DataFrame
from pyspark.sql import functions as F
import pandas as pd

now = datetime.now(timezone.utc)
batch_id = f"ref-{now.strftime('%Y%m%d%H%M%S')}"
snapshot_date = now.date()

# ── 3. Convert to bronze.dim_route_ref schema ──
rows = []
for r in routes:
    rows.append({
        "route_id":       r["route_id"],
        "snapshot_date":  str(snapshot_date),
        "payload":        json.dumps(r),          # full GTFS record as JSON
        "ingest_batch_id": batch_id,
        "ingested_at":    now.isoformat(),
    })

pdf = pd.DataFrame(rows)
df = spark.createDataFrame(pdf).select(
    F.col("route_id"),
    F.col("snapshot_date").cast("date"),
    F.col("payload"),
    F.col("ingest_batch_id"),
    F.to_timestamp("ingested_at").alias("ingested_at"),
)

print(f"Snapshot date: {snapshot_date} | Batch: {batch_id} | Rows: {df.count()}")
display(df)

# COMMAND ----------

# DBTITLE 1,Append to bronze.dim_route_ref
# ── 4. Append snapshot to bronze (skip if today's snapshot already exists) ──
existing = spark.sql(f"""
    SELECT COUNT(*) AS cnt
    FROM   {BRONZE_TABLE}
    WHERE  snapshot_date = '{snapshot_date}'
""").collect()[0]["cnt"]

if existing > 0:
    print(f"Snapshot for {snapshot_date} already exists ({existing} rows) — skipping.")
else:
    df.write.mode("append").insertInto(BRONZE_TABLE)
    total = spark.table(BRONZE_TABLE).count()
    print(f"Appended {df.count()} routes → {BRONZE_TABLE} (total rows now: {total})")

# COMMAND ----------

# DBTITLE 1,Verify latest snapshot
# MAGIC %sql
# MAGIC -- Verify: latest snapshot in dim_route_ref
# MAGIC SELECT route_id,
# MAGIC        snapshot_date,
# MAGIC        get_json_object(payload, '$.route_short_name') AS short_name,
# MAGIC        get_json_object(payload, '$.route_long_name')  AS long_name,
# MAGIC        get_json_object(payload, '$.route_type')        AS gtfs_type
# MAGIC FROM   ${catalog}.bronze.dim_route_ref
# MAGIC WHERE  snapshot_date = (SELECT MAX(snapshot_date) FROM ${catalog}.bronze.dim_route_ref)
# MAGIC ORDER  BY route_id