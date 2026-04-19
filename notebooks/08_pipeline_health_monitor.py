# Databricks notebook source

# -- Catalog parameter (set by DABs or default to dev) --
dbutils.widgets.text("catalog", "mta_rtransit_dev")
catalog = dbutils.widgets.get("catalog")
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC ## 08 — Pipeline Health Monitor
# MAGIC Checks data freshness, row counts, job run history, and data quality across all medallion layers.
# MAGIC Raises alerts when data is stale or quality drops.

# COMMAND ----------

# DBTITLE 1,Config
from pyspark.sql import functions as F
from datetime import datetime, timezone
import json

TABLES = {
    "bronze.gtfs_rt_events": {"ts_col": "lake_ingest_ts", "stale_hours": 24},
    "bronze.eventhub_gtfs_raw": {"ts_col": "bronze_ingest_ts", "stale_hours": 2},
    "bronze.dim_route_ref": {"ts_col": "ingested_at", "stale_hours": 168},
    "silver.fact_trip_delay_event": {"ts_col": "event_ts", "stale_hours": 24},
    "gold.route_delay_kpi_daily": {"ts_col": "kpi_date", "stale_hours": 48},
}
CATALOG = catalog  # Uses widget parameter

# COMMAND ----------

# DBTITLE 1,Table freshness & row counts
from datetime import timedelta

now = datetime.now(timezone.utc)
results = []

for table_name, cfg in TABLES.items():
    fqn = f"{CATALOG}.{table_name}"
    ts_col = cfg["ts_col"]
    stale_hours = cfg["stale_hours"]
    
    row = spark.sql(f"""
        SELECT COUNT(*) AS row_count,
               MAX({ts_col}) AS latest_ts
        FROM {fqn}
    """).collect()[0]
    
    row_count = row["row_count"]
    latest_ts = row["latest_ts"]
    
    if latest_ts is not None:
        if hasattr(latest_ts, 'timestamp'):
            age_hours = (now - latest_ts.replace(tzinfo=timezone.utc)).total_seconds() / 3600
        else:
            from datetime import date
            age_hours = (now.date() - latest_ts).total_seconds() / 3600 if hasattr(latest_ts, '__sub__') else 999
    else:
        age_hours = 999
    
    is_stale = age_hours > stale_hours
    status = "🔴 STALE" if is_stale else "✅ OK"
    
    results.append({
        "table": table_name,
        "rows": row_count,
        "latest": str(latest_ts)[:19] if latest_ts else "EMPTY",
        "age_hours": round(age_hours, 1),
        "threshold_hours": stale_hours,
        "status": status
    })
    print(f"{status}  {table_name:45s}  rows={row_count:>8,}  latest={str(latest_ts)[:19] if latest_ts else 'EMPTY':>22s}  age={age_hours:.1f}h (threshold: {stale_hours}h)")

stale_count = sum(1 for r in results if "STALE" in r["status"])
print(f"\n{'='*80}")
if stale_count > 0:
    print(f"⚠️  {stale_count} table(s) are STALE — check pipeline jobs!")
else:
    print(f"✅ All {len(results)} tables are fresh.")

# COMMAND ----------

# DBTITLE 1,Job run health (last 24h)
from databricks.sdk import WorkspaceClient
from datetime import timedelta

w = WorkspaceClient()

JOBS = {
    182375670422584: "Continuous API Poll",
    968403858233947: "Continuous EH Streaming",
    46942749106650: "Daily Batch Pipeline",
}

cutoff_ms = int((now - timedelta(hours=24)).timestamp() * 1000)

print(f"Job runs in the last 24 hours:\n{'='*80}")
for job_id, job_name in JOBS.items():
    try:
        runs = w.api_client.do("GET", "/api/2.1/jobs/runs/list", query={
            "job_id": job_id, "limit": 10, "start_time_from": cutoff_ms
        })
        run_list = runs.get("runs", [])
        
        total = len(run_list)
        failed = sum(1 for r in run_list if r.get("state", {}).get("result_state") == "FAILED")
        succeeded = sum(1 for r in run_list if r.get("state", {}).get("result_state") == "SUCCESS")
        running = sum(1 for r in run_list if r.get("state", {}).get("life_cycle_state") == "RUNNING")
        cancelled = sum(1 for r in run_list if r.get("state", {}).get("result_state") == "CANCELED")
        
        status = "🔴 FAILURES" if failed > 0 else ("🟡 NO RUNS" if total == 0 else "✅ HEALTHY")
        print(f"{status}  {job_name:35s}  runs={total}  success={succeeded}  failed={failed}  running={running}  cancelled={cancelled}")
    except Exception as e:
        print(f"🔴 ERROR  {job_name:35s}  {e}")

# COMMAND ----------

# DBTITLE 1,Silver data quality
quality = spark.sql(f"""
    SELECT 
        COUNT(*) AS total_events,
        SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) AS valid_events,
        SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END) AS invalid_events,
        ROUND(100.0 * SUM(CASE WHEN is_valid THEN 1 ELSE 0 END) / COUNT(*), 2) AS valid_pct,
        COUNT(DISTINCT route_id) AS distinct_routes,
        SUM(CASE WHEN delay_sec IS NOT NULL THEN 1 ELSE 0 END) AS has_delay_cnt,
        SUM(CASE WHEN event_ts IS NULL THEN 1 ELSE 0 END) AS null_event_ts
    FROM {catalog}.silver.fact_trip_delay_event
""").collect()[0]

print("Silver Fact Data Quality:")
print(f"{'='*50}")
print(f"  Total events:      {quality['total_events']:,}")
print(f"  Valid events:      {quality['valid_events']:,} ({quality['valid_pct']}%)")
print(f"  Invalid events:    {quality['invalid_events']:,}")
print(f"  Distinct routes:   {quality['distinct_routes']}")
print(f"  Has delay data:    {quality['has_delay_cnt']:,}")
print(f"  Null event_ts:     {quality['null_event_ts']:,}")

if quality['valid_pct'] < 95:
    print(f"\n⚠️  Data quality below 95% threshold ({quality['valid_pct']}%)")
else:
    print(f"\n✅ Data quality is healthy ({quality['valid_pct']}%)")

# COMMAND ----------

# DBTITLE 1,Table history (recent operations)
# MAGIC %sql
# MAGIC -- Recent Delta operations across key tables
# MAGIC SELECT 'bronze.gtfs_rt_events' AS table_name, version, timestamp, operation, 
# MAGIC        operationMetrics.numOutputRows AS rows_written
# MAGIC FROM (DESCRIBE HISTORY {catalog}.bronze.gtfs_rt_events LIMIT 5)
# MAGIC UNION ALL
# MAGIC SELECT 'silver.fact_trip_delay_event', version, timestamp, operation, 
# MAGIC        operationMetrics.numOutputRows
# MAGIC FROM (DESCRIBE HISTORY {catalog}.silver.fact_trip_delay_event LIMIT 5)
# MAGIC UNION ALL
# MAGIC SELECT 'gold.route_delay_kpi_daily', version, timestamp, operation, 
# MAGIC        operationMetrics.numOutputRows
# MAGIC FROM (DESCRIBE HISTORY {catalog}.gold.route_delay_kpi_daily LIMIT 5)
# MAGIC ORDER BY timestamp DESC

# COMMAND ----------

# DBTITLE 1,Overall health summary
print("\n" + "="*80)
print("  MTA TRANSIT PIPELINE — HEALTH REPORT")
print(f"  Generated: {now.strftime('%Y-%m-%d %H:%M:%S UTC')}")
print("="*80)
print(f"\n  📊 Tables:       {len(results) - stale_count}/{len(results)} fresh")
failed_jobs = sum(1 for j in JOBS if any(r.get('state',{}).get('result_state') == 'FAILED' for r in w.api_client.do('GET', '/api/2.1/jobs/runs/list', query={'job_id': j, 'limit': 5, 'start_time_from': cutoff_ms}).get('runs', [])))
print(f"  🔧 Jobs:         {len(JOBS) - failed_jobs}/{len(JOBS)} healthy (24h)")
print(f"  ✅ Data Quality:  {quality['valid_pct']}% valid")
print(f"  📈 Silver Events: {quality['total_events']:,}")
print(f"  🚇 Routes:       {quality['distinct_routes']}")

if stale_count == 0 and failed_jobs == 0 and quality['valid_pct'] >= 95:
    print(f"\n  ✅ OVERALL STATUS: HEALTHY")
else:
    issues = []
    if stale_count > 0: issues.append(f"{stale_count} stale table(s)")
    if failed_jobs > 0: issues.append(f"{failed_jobs} failed job(s)")
    if quality['valid_pct'] < 95: issues.append(f"quality at {quality['valid_pct']}%")
    print(f"\n  ⚠️  OVERALL STATUS: NEEDS ATTENTION — {', '.join(issues)}")