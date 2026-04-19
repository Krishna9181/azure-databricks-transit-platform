-- Databricks notebook source
-- =============================================================================
-- MTA medallion — Unity Catalog DDL (external Delta tables only)
-- =============================================================================
--
-- BEFORE YOU RUN:
-- 1. Replace ALL placeholders in paths:
--      <container>          → your ADLS Gen2 container name
--      <storage_account>    → your storage account name (no .dfs.core.windows.net)
-- 2. In Unity Catalog: register an **external location** that covers this prefix
--      (Catalog admin → External data → External locations), e.g.:
--      abfss://demo@adlsgen2deportfolioeus.dfs.core.windows.net/${catalog}/
--    Grant **CREATE EXTERNAL TABLE** / **WRITE** as required for your principal.
-- 3. Catalog **mta_rtransit** must already exist with a valid storage root (UI or
--    CREATE CATALOG ... MANAGED LOCATION).
--
-- Folder layout (one Delta table root per path — standard for external Delta):
--   .../mta_rtransit/bronze/gtfs_rt_events
--   .../mta_rtransit/bronze/eventhub_gtfs_raw
--   .../mta_rtransit/bronze/dim_route_ref
--   .../mta_rtransit/silver/fact_trip_delay_event
--   .../mta_rtransit/silver/dim_route
--   .../mta_rtransit/gold/route_delay_kpi_daily
--
-- Streaming checkpoints (notebooks): use another prefix under the same container,
-- e.g. .../mta_rtransit/checkpoints/stream_gtfs_rt (not created by this script).
--
-- =============================================================================

-- Catalog parameter (set by DABs or default to dev)
CREATE WIDGET TEXT catalog DEFAULT "mta_rtransit_dev";

-- Schemas ---------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS ${catalog}.bronze COMMENT 'Raw / immutable landing';
CREATE SCHEMA IF NOT EXISTS ${catalog}.silver COMMENT 'Conformed, deduped, typed';
CREATE SCHEMA IF NOT EXISTS ${catalog}.gold COMMENT 'Marts & KPIs for BI / Genie';

-- -----------------------------------------------------------------------------
-- Bronze — raw fact stream
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.gtfs_rt_events (
  id STRING COMMENT 'Stable doc id (hash)',
  route_id STRING,
  source STRING COMMENT 'e.g. mta_gtfs_rt',
  feed_entity_id STRING,
  trip_id STRING,
  trip_update_timestamp BIGINT COMMENT 'Epoch seconds from feed when present',
  first_stop_delay_sec INT,
  ingest_batch_id STRING,
  ingested_at TIMESTAMP,
  lake_ingest_ts TIMESTAMP COMMENT 'When row landed in Delta (set in notebook)'
)
USING DELTA
LOCATION 'abfss://demo@adlsgen2deportfolioeus.dfs.core.windows.net/${catalog}/bronze/gtfs_rt_events'
COMMENT 'Bronze: high-churn trip updates';

-- -----------------------------------------------------------------------------
-- Bronze — raw Event Hubs capture
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.eventhub_gtfs_raw (
  sequence_number BIGINT,
  offset STRING,
  enqueued_time TIMESTAMP,
  payload STRING COMMENT 'Raw JSON body from Event Hubs',
  bronze_ingest_ts TIMESTAMP
)
USING DELTA
LOCATION 'abfss://demo@adlsgen2deportfolioeus.dfs.core.windows.net/${catalog}/bronze/eventhub_gtfs_raw'
COMMENT 'Bronze: append-only Event Hubs messages';

-- -----------------------------------------------------------------------------
-- Bronze — route / reference snapshots
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.bronze.dim_route_ref (
  route_id STRING,
  snapshot_date DATE,
  payload STRING COMMENT 'JSON snapshot as text',
  ingest_batch_id STRING,
  ingested_at TIMESTAMP
)
USING DELTA
LOCATION 'abfss://demo@adlsgen2deportfolioeus.dfs.core.windows.net/${catalog}/bronze/dim_route_ref'
COMMENT 'Bronze: route reference snapshots';

-- -----------------------------------------------------------------------------
-- Silver — fact trip-delay events
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.silver.fact_trip_delay_event (
  event_sk STRING COMMENT 'Surrogate key',
  route_id STRING NOT NULL,
  trip_id STRING,
  event_ts TIMESTAMP COMMENT 'Coalesced event time',
  delay_sec INT,
  ingest_batch_id STRING,
  is_valid BOOLEAN
)
USING DELTA
LOCATION 'abfss://demo@adlsgen2deportfolioeus.dfs.core.windows.net/${catalog}/silver/fact_trip_delay_event'
COMMENT 'Silver: one row per logical trip update event';

-- -----------------------------------------------------------------------------
-- Silver — route dimension
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.silver.dim_route (
  route_id STRING NOT NULL,
  route_short_name STRING,
  route_long_name STRING,
  route_type STRING,
  effective_from DATE,
  effective_to DATE
)
USING DELTA
LOCATION 'abfss://demo@adlsgen2deportfolioeus.dfs.core.windows.net/${catalog}/silver/dim_route'
COMMENT 'Silver: route attributes';

-- -----------------------------------------------------------------------------
-- Gold — daily KPI mart (grain: route + day)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ${catalog}.gold.route_delay_kpi_daily (
  kpi_date DATE NOT NULL,
  route_id STRING NOT NULL,
  trip_update_cnt BIGINT,
  avg_delay_sec DOUBLE,
  p95_delay_sec DOUBLE
)
USING DELTA
LOCATION 'abfss://demo@adlsgen2deportfolioeus.dfs.core.windows.net/${catalog}/gold/route_delay_kpi_daily'
COMMENT 'Gold: daily delay KPIs by route';

-- -----------------------------------------------------------------------------
-- Grants (optional)
-- -----------------------------------------------------------------------------
-- GRANT USE SCHEMA ON SCHEMA ${catalog}.gold TO `analysts`;
-- GRANT SELECT ON TABLE ${catalog}.gold.route_delay_kpi_daily TO `analysts`;


-- COMMAND ----------

