# Databricks notebook source
# DBTITLE 1,Catalog Parameter
# -- Catalog parameter (set by DABs or default to dev) --
dbutils.widgets.text("catalog", "mta_rtransit_dev")
catalog = dbutils.widgets.get("catalog")
print(f"Using catalog: {catalog}")

# COMMAND ----------

# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ## 09 — Gold → Azure SQL Database
# MAGIC
# MAGIC Exports gold-layer KPI data and route dimension to **Azure SQL Database** for:
# MAGIC - **Power BI DirectQuery** — faster queries than hitting Delta Lake directly
# MAGIC - **Application consumption** — REST APIs, web apps can query SQL directly
# MAGIC
# MAGIC **Tables exported:**
# MAGIC | Source (Delta) | Target (Azure SQL) | Mode |
# MAGIC |---|---|---|
# MAGIC | `gold.route_delay_kpi_daily` | `dbo.route_delay_kpi_daily` | Overwrite (daily) |
# MAGIC | `silver.dim_route` (active only) | `dbo.dim_route` | Overwrite |
# MAGIC | `gold.route_delay_kpi_enriched` | `dbo.route_delay_kpi_enriched` | Overwrite |

# COMMAND ----------

# DBTITLE 1,Configuration — JDBC connection
# ── Azure SQL connection (secrets from Key Vault) ──
sql_host = dbutils.secrets.get("mta-kv", "sql-server-host")
sql_user = dbutils.secrets.get("mta-kv", "sql-server-user")
sql_password = dbutils.secrets.get("mta-kv", "sql-server-password")
sql_database = dbutils.secrets.get("mta-kv", "sql-database-name")

JDBC_URL = (
    f"jdbc:sqlserver://{sql_host}:1433;"
    f"database={sql_database};"
    f"encrypt=true;"
    f"trustServerCertificate=false;"
    f"hostNameInCertificate=*.database.windows.net;"
    f"loginTimeout=30;"
)

JDBC_PROPERTIES = {
    "user": sql_user,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

print(f"✓ JDBC configured → {sql_host} / {sql_database}")

# COMMAND ----------

# DBTITLE 1,Export gold.route_delay_kpi_daily
# ── 1. Export daily KPIs ──
kpi_df = spark.table(f"{catalog}.gold.route_delay_kpi_daily")

kpi_df.write \
    .format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", "dbo.route_delay_kpi_daily") \
    .option("user", JDBC_PROPERTIES["user"]) \
    .option("password", JDBC_PROPERTIES["password"]) \
    .option("driver", JDBC_PROPERTIES["driver"]) \
    .mode("overwrite") \
    .save()

print(f"✓ dbo.route_delay_kpi_daily — {kpi_df.count()} rows exported")

# COMMAND ----------

# DBTITLE 1,Export silver.dim_route (active only)
# ── 2. Export active routes only ──
from pyspark.sql import functions as F

route_df = spark.table(f"{catalog}.silver.dim_route") \
    .filter(F.col("effective_to").isNull())

route_df.write \
    .format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", "dbo.dim_route") \
    .option("user", JDBC_PROPERTIES["user"]) \
    .option("password", JDBC_PROPERTIES["password"]) \
    .option("driver", JDBC_PROPERTIES["driver"]) \
    .mode("overwrite") \
    .save()

print(f"✓ dbo.dim_route — {route_df.count()} rows exported (active routes only)")

# COMMAND ----------

# DBTITLE 1,Export gold.route_delay_kpi_enriched
# ── 3. Export enriched KPI view (joins KPIs + route names) ──
enriched_df = spark.table(f"{catalog}.gold.route_delay_kpi_enriched")

enriched_df.write \
    .format("jdbc") \
    .option("url", JDBC_URL) \
    .option("dbtable", "dbo.route_delay_kpi_enriched") \
    .option("user", JDBC_PROPERTIES["user"]) \
    .option("password", JDBC_PROPERTIES["password"]) \
    .option("driver", JDBC_PROPERTIES["driver"]) \
    .mode("overwrite") \
    .save()

print(f"✓ dbo.route_delay_kpi_enriched — {enriched_df.count()} rows exported")

# COMMAND ----------

# DBTITLE 1,Verification — read back from Azure SQL
# ── Verify data landed in Azure SQL ──
tables = ["dbo.route_delay_kpi_daily", "dbo.dim_route", "dbo.route_delay_kpi_enriched"]
total_rows = 0

print("Azure SQL Verification:")
print("=" * 50)
for table in tables:
    df = spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", table) \
        .option("user", JDBC_PROPERTIES["user"]) \
        .option("password", JDBC_PROPERTIES["password"]) \
        .option("driver", JDBC_PROPERTIES["driver"]) \
        .load()
    count = df.count()
    total_rows += count
    print(f"  ✓ {table}: {count:,} rows")

print(f"\n✓ Azure SQL export complete: {len(tables)} tables, {total_rows:,} total rows")
