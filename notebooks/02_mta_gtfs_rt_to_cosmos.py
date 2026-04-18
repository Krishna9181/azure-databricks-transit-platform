# Databricks notebook source
# MAGIC %md
# MAGIC ## 01 — MTA API → segmented sinks
# MAGIC
# MAGIC - **Batch routes** → Cosmos **`gtfs_rt_batch`** only *(Databricks **`03`** reads this)*.
# MAGIC - **Stream routes** → Cosmos **`gtfs_rt_stream`** **and** JSON messages to **Event Hubs** *(**`04`** consumes EH)*.
# MAGIC
# MAGIC Tune **`SEGMENT_MODE`** / **`STREAM_ROUTE_IDS`** below. Cluster needs internet + Cosmos + EH send rules.

# COMMAND ----------

# MAGIC %pip install --quiet requests protobuf gtfs-realtime-bindings azure-cosmos azure-eventhub opencensus-ext-azure

# COMMAND ----------

COSMOS_URI = dbutils.secrets.get("mta-kv", "cosmos-endpoint")
COSMOS_KEY = dbutils.secrets.get("mta-kv", "cosmos-key")
COSMOS_DATABASE = "gtfs"

CONTAINER_BATCH = "gtfs_rt_batch"
CONTAINER_STREAM = "gtfs_rt_stream"

MTA_FEED_URL = (
    "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs"
)

# "explicit" | "letter_to_stream" | "numeric_to_batch"
SEGMENT_MODE = "letter_to_stream"
STREAM_ROUTE_IDS = frozenset()

EVENTHUB_CONNECTION_STRING = dbutils.secrets.get("mta-kv", "eventhub-connection-string")
EVENTHUB_NAME = None

# COMMAND ----------

import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Any

import requests
from azure.cosmos import CosmosClient, PartitionKey
from azure.eventhub import EventData, EventHubProducerClient
from google.transit import gtfs_realtime_pb2 as gtfs_rt

# COMMAND ----------

# Long-running poll (Job / interactive cluster). False = single fetch + exit.
CONTINUOUS = True
INGEST_INTERVAL_SEC = 30.0

COSMOS_RU = 400

# COMMAND ----------

# DBTITLE 1,App Insights setup
# ── Application Insights telemetry ──
import logging
from opencensus.ext.azure.log_exporter import AzureEventHandler

APPINSIGHTS_CONN = dbutils.secrets.get("mta-kv", "appinsights-connection-string")

logger = logging.getLogger("mta_ingest")
logger.setLevel(logging.INFO)

# AzureEventHandler → customEvents table (distinct event names)
if not any(isinstance(h, AzureEventHandler) for h in logger.handlers):
    handler = AzureEventHandler(connection_string=APPINSIGHTS_CONN)
    logger.addHandler(handler)

def flush_telemetry():
    """Call after each ingest cycle to ensure telemetry is exported."""
    for h in logger.handlers:
        if hasattr(h, 'flush'):
            h.flush()

print("✓ Application Insights configured → customEvents table (API poll)")

# COMMAND ----------

import hashlib
import json
import time
import traceback
from datetime import datetime, timezone
from typing import Any

import requests
from azure.cosmos import CosmosClient, PartitionKey
from azure.eventhub import EventData, EventHubProducerClient
from google.transit import gtfs_realtime_pb2 as gtfs_rt


def first_stop_delay_seconds(tu: gtfs_rt.TripUpdate) -> int | None:
    """Delay may live on StopTimeUpdate (older protos) or arrival/departure StopTimeEvent (GTFS-R v2+)."""
    if not tu.stop_time_update:
        return None
    first = tu.stop_time_update[0]
    names = {f.name for f in first.DESCRIPTOR.fields}
    if "delay" in names:
        try:
            if first.HasField("delay"):
                return int(first.delay)
        except ValueError:
            pass
    for slot in ("arrival", "departure"):
        if slot not in names:
            continue
        try:
            if not first.HasField(slot):
                continue
        except ValueError:
            continue
        ev = getattr(first, slot)
        ev_names = {f.name for f in ev.DESCRIPTOR.fields}
        if "delay" in ev_names and ev.HasField("delay"):
            return int(ev.delay)
    return None


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_stream_route(route_id: str | None) -> bool:
    """True → stream container + Event Hub; False → batch container only."""
    rid = (route_id or "UNKNOWN").strip().upper()
    if SEGMENT_MODE == "explicit":
        return rid in STREAM_ROUTE_IDS
    if SEGMENT_MODE == "letter_to_stream":
        return bool(rid and rid[0].isalpha())
    if SEGMENT_MODE == "numeric_to_batch":
        return not (rid and rid[0].isdigit())
    if SEGMENT_MODE == "numeric_to_stream":
        return bool(rid and rid[0].isdigit())
    return False


def fetch_feed(url: str, timeout_s: float = 30.0) -> bytes:
    t0 = time.time()
    r = requests.get(
        url,
        headers={
            "User-Agent": "mta-databricks-ingest/1.0",
            "Accept": "application/x-protobuf",
        },
        timeout=timeout_s,
    )
    r.raise_for_status()
    api_ms = (time.time() - t0) * 1000
    # ── Telemetry: API request latency ──
    logger.info("api_request", extra={
        "custom_dimensions": {
            "url": url,
            "latency_ms": round(api_ms, 1),
            "status_code": r.status_code,
            "response_bytes": len(r.content),
        }
    })
    return r.content


def trip_updates_to_docs(
    feed_bytes: bytes, ingest_batch_id: str
) -> list[dict[str, Any]]:
    msg = gtfs_rt.FeedMessage()
    msg.ParseFromString(feed_bytes)
    rows: list[dict[str, Any]] = []
    ingested = utc_now_iso()
    for entity in msg.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        trip = tu.trip
        route_id = trip.route_id or "UNKNOWN"
        trip_id = trip.trip_id or ""
        delay = first_stop_delay_seconds(tu)
        doc_id = hashlib.sha256(
            f"{trip_id}|{tu.timestamp}|{entity.id}".encode()
        ).hexdigest()[:32]
        stream = is_stream_route(route_id)
        rows.append(
            {
                "id": doc_id,
                "route_id": route_id,
                "metadata": {
                    "schema_version": "1.0",
                    "source_system": "mta_gtfs_rt",
                    "event_type": "TripUpdate",
                    "ingest_batch_id": ingest_batch_id,
                    "ingested_at": ingested,
                    "segment": "stream" if stream else "batch",
                    "segment_mode": SEGMENT_MODE,
                    "cosmos_container": CONTAINER_STREAM
                    if stream
                    else CONTAINER_BATCH,
                    "emit_to_event_hub": stream,
                },
                "payload": {
                    "feed_entity_id": entity.id,
                    "trip_id": trip_id,
                    "trip_update_timestamp": int(tu.timestamp)
                    if tu.timestamp
                    else None,
                    "first_stop_delay_sec": delay,
                },
            }
        )
    return rows


def send_eventhub(docs: list[dict[str, Any]]) -> int:
    if not docs:
        return 0
    if EVENTHUB_NAME:
        producer = EventHubProducerClient.from_connection_string(
            EVENTHUB_CONNECTION_STRING,
            eventhub_name=EVENTHUB_NAME,
        )
    else:
        producer = EventHubProducerClient.from_connection_string(
            EVENTHUB_CONNECTION_STRING
        )
    sent = 0
    batch_eh = producer.create_batch()
    try:
        for doc in docs:
            body = json.dumps(doc, ensure_ascii=False)
            ev = EventData(body)
            try:
                batch_eh.add(ev)
                sent += 1
            except ValueError:
                producer.send_batch(batch_eh)
                batch_eh = producer.create_batch()
                batch_eh.add(ev)
                sent += 1
        producer.send_batch(batch_eh)
    finally:
        producer.close()
    return sent


client = CosmosClient(COSMOS_URI, credential=COSMOS_KEY)
database = client.create_database_if_not_exists(id=COSMOS_DATABASE)

ctr_batch = database.create_container_if_not_exists(
    id=CONTAINER_BATCH,
    partition_key=PartitionKey(path="/route_id"),
    offer_throughput=COSMOS_RU,
)
ctr_stream = database.create_container_if_not_exists(
    id=CONTAINER_STREAM,
    partition_key=PartitionKey(path="/route_id"),
    offer_throughput=COSMOS_RU,
)


def run_ingest_cycle() -> dict[str, Any]:
    batch_id = f"batch-{int(time.time())}"
    raw = fetch_feed(MTA_FEED_URL)
    docs = trip_updates_to_docs(raw, batch_id)

    counts = {"cosmos_batch": 0, "cosmos_stream": 0}
    stream_docs: list[dict[str, Any]] = []

    for doc in docs:
        if doc["metadata"]["segment"] == "stream":
            ctr_stream.upsert_item(doc)
            counts["cosmos_stream"] += 1
            stream_docs.append(doc)
        else:
            ctr_batch.upsert_item(doc)
            counts["cosmos_batch"] += 1

    eh_sent = send_eventhub(stream_docs)
    return {
        "database": COSMOS_DATABASE,
        "containers": {"batch": CONTAINER_BATCH, "stream": CONTAINER_STREAM},
        "counts": counts,
        "event_hub_messages": eh_sent,
        "documents_total": len(docs),
        "batch_id": batch_id,
        "segment_mode": SEGMENT_MODE,
    }


if not CONTINUOUS:
    print(json.dumps(run_ingest_cycle(), indent=2))
else:
    print(
        f"[mta_ingest] continuous: interval={INGEST_INTERVAL_SEC}s feed={MTA_FEED_URL}",
        flush=True,
    )
    while True:
        try:
            t0 = time.time()
            out = run_ingest_cycle()
            cycle_ms = (time.time() - t0) * 1000
            # ── Telemetry: ingest cycle success ──
            logger.info("ingest_cycle_success", extra={
                "custom_dimensions": {
                    "cycle_latency_ms": round(cycle_ms, 1),
                    "documents_total": out["documents_total"],
                    "cosmos_batch": out["counts"]["cosmos_batch"],
                    "cosmos_stream": out["counts"]["cosmos_stream"],
                    "event_hub_messages": out["event_hub_messages"],
                    "batch_id": out["batch_id"],
                    "segment_mode": out["segment_mode"],
                }
            })
            flush_telemetry()
            print(json.dumps(out), flush=True)
        except Exception as e:
            # ── Telemetry: ingest cycle failure ──
            logger.exception("ingest_cycle_failed", extra={
                "custom_dimensions": {"error": str(e)}
            })
            flush_telemetry()
            print("[mta_ingest] cycle failed; sleeping then retry", flush=True)
            traceback.print_exc()
        time.sleep(INGEST_INTERVAL_SEC)

# COMMAND ----------

