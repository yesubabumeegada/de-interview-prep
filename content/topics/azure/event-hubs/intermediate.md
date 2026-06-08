---
title: "Event Hubs — Intermediate"
topic: azure
subtopic: event-hubs
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [azure, event-hubs, kafka, checkpointing, capture, schema-registry, producer]
---

# Event Hubs — Intermediate

## Producer and Consumer with Azure SDK

```python
import asyncio
from azure.eventhub.aio import EventHubProducerClient, EventHubConsumerClient
from azure.eventhub import EventData
from azure.identity.aio import DefaultAzureCredential

NAMESPACE   = "mynamespace.servicebus.windows.net"
EVENTHUB    = "orders"
CONSUMER_GRP = "databricks-consumer"

# ── PRODUCER ──────────────────────────────────────────────────────────────────
async def send_order_batch():
    credential = DefaultAzureCredential()
    async with EventHubProducerClient(
        fully_qualified_namespace=NAMESPACE,
        eventhub_name=EVENTHUB,
        credential=credential
    ) as producer:
        # Send a batch (most efficient — single network round-trip)
        batch = await producer.create_batch(partition_key="customer_123")
        orders = [
            {"order_id": 101, "amount": 99.99,  "region": "US"},
            {"order_id": 102, "amount": 249.50, "region": "EU"},
            {"order_id": 103, "amount": 15.00,  "region": "US"},
        ]
        for order in orders:
            import json
            batch.add(EventData(json.dumps(order)))
        await producer.send_batch(batch)
        print(f"Sent batch of {len(orders)} events")

# Partition key routing:
#   partition_key="customer_123" → hash → same partition every time
#   Guarantees ordering for same customer_id
#   If partition_key omitted: round-robin across all partitions

# ── CONSUMER (checkpoint store in ADLS) ──────────────────────────────────────
async def consume_orders():
    from azure.eventhub.extensions.checkpointstoreaio import BlobCheckpointStore

    checkpoint_store = BlobCheckpointStore(
        blob_account_url="https://myaccount.blob.core.windows.net",
        container_name="checkpoints",
        credential=DefaultAzureCredential()
    )

    async def on_event(partition_context, event):
        data = json.loads(event.body_as_str())
        print(f"Partition {partition_context.partition_id}: {data}")
        # Checkpoint every event (or batch for efficiency)
        await partition_context.update_checkpoint(event)

    async with EventHubConsumerClient(
        fully_qualified_namespace=NAMESPACE,
        eventhub_name=EVENTHUB,
        consumer_group=CONSUMER_GRP,
        checkpoint_store=checkpoint_store,   # persist offset to Blob
        credential=DefaultAzureCredential()
    ) as consumer:
        await consumer.receive(on_event=on_event, starting_position="-1")

asyncio.run(send_order_batch())
asyncio.run(consume_orders())
```

---

## Event Hubs Capture (Auto-Archive to ADLS)

```
Event Hubs Capture: automatically archive raw events to ADLS Gen2 or Blob Storage
No code required — configure in Event Hub settings

Capture settings:
  Time window:  1–15 minutes (buffer time before writing a file)
  Size window:  10–500 MB (write when buffer reaches size)
  Whichever triggers first causes a file to be written

Output format: Apache Avro (schema embedded in file)
Path pattern (customizable):
  {Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}.avro
  Example: mynamespace/orders/0/2024/01/15/14/30/00.avro

Benefits:
  Zero code: instant ADLS archival enabled in portal
  Exactly-once delivery to ADLS (Capture guarantees no duplicates)
  Multiple consumers (real-time ASA + capture to ADLS simultaneously)
  Low latency: 1-minute file frequency = data in ADLS within 2 min of arrival

Read captured Avro files in Databricks:
  df = spark.read.format("avro") \
      .load("abfss://raw@account.dfs.core.windows.net/mynamespace/orders/*/2024/01/15/*/*.avro")
  
  # Each Avro file contains: SequenceNumber, Offset, EnqueuedTimeUtc, SystemProperties, Body
  # Body is the raw event bytes → cast to string → parse JSON
  df_parsed = df \
      .withColumn("event", F.from_json(df.Body.cast("string"), event_schema)) \
      .select("event.*", "EnqueuedTimeUtc", "SequenceNumber")

Cost of Capture:
  Storage: normal ADLS pricing per GB
  Capture: $0.10/hour per throughput unit (Standard tier)
  For 10 TU namespace: +$1/hour = ~$720/month for capture feature
```

---

## Schema Registry

```python
# Event Hubs Schema Registry: centralized schema management for Avro/JSON Schema

# Benefits:
#   Producers register schema once → validated at publish time
#   Consumers fetch schema by ID → no schema embedded per message (smaller messages)
#   Schema evolution: backward/forward compatibility enforced at registry level

from azure.schemaregistry import SchemaRegistryClient
from azure.schemaregistry.encoder.avroencoder import AvroEncoder
from azure.identity import DefaultAzureCredential

# Order Avro schema
ORDER_SCHEMA = """{
    "type": "record",
    "name": "Order",
    "namespace": "com.mycompany.ecommerce",
    "fields": [
        {"name": "order_id",    "type": "long"},
        {"name": "customer_id", "type": "int"},
        {"name": "amount",      "type": "double"},
        {"name": "order_date",  "type": "string"},
        {"name": "region",      "type": "string"}
    ]
}"""

credential = DefaultAzureCredential()

# Producer: encode with schema (schema ID embedded in message, not full schema)
encoder = AvroEncoder(
    client=SchemaRegistryClient(
        fully_qualified_namespace="mynamespace.servicebus.windows.net",
        credential=credential
    ),
    group_name="ecommerce-schemas",
    auto_register=True   # register schema if not exists
)

order_event = {"order_id": 101, "customer_id": 42, "amount": 99.99,
               "order_date": "2024-01-15", "region": "US"}

# Encode: includes schema ID (4 bytes) + Avro-encoded payload
encoded = encoder.encode(order_event, schema=ORDER_SCHEMA)
# Message is ~50 bytes vs ~200 bytes for JSON (75% smaller)

# Consumer: decode using schema from registry
decoder = AvroEncoder(
    client=SchemaRegistryClient(...),
    group_name="ecommerce-schemas"
)
decoded = decoder.decode(encoded)
# {"order_id": 101, "customer_id": 42, ...}

# Schema compatibility groups:
#   Backward:  new schema can read old data (safe: add optional fields)
#   Forward:   old schema can read new data (remove fields with defaults)
#   Full:      both backward and forward compatible
```

---

## Interview Tips

> **Tip 1:** "How do you ensure ordering guarantees with Event Hubs?" — Event Hubs guarantees ordering *within a partition only*. To ensure all events for the same entity (same customer, same order) are processed in order: always use a partition key (`partition_key = customer_id`). This hashes the key consistently to the same partition. Events from different customers may arrive interleaved (on different partitions) — that's fine as long as per-customer ordering is maintained within a partition. For global ordering across all events: you'd need a single partition, but this eliminates parallelism (not recommended at scale).

> **Tip 2:** "What's the purpose of a checkpoint in Event Hubs consumers?" — A checkpoint records the last successfully processed event offset for each partition. On consumer restart, it resumes from the checkpoint rather than re-reading all history. Without checkpoints: restart = re-process from beginning (or `earliest`). With Blob-backed checkpoint store: offset persists across consumer restarts, VM reboots, and auto-scaling. Checkpoint frequency trade-off: checkpoint every event = safe but slow (I/O per event); checkpoint every N events = faster but N events reprocessed on restart (at-least-once).

> **Tip 3:** "What is Event Hubs Capture and how does it differ from a consumer writing to ADLS?" — Capture is a built-in feature that automatically archives events to ADLS/Blob at a configured frequency (time or size window). A custom consumer writing to ADLS has these differences: (1) Capture provides exactly-once delivery (no duplicates in ADLS), while a custom consumer needs deduplication logic. (2) Capture works in parallel with other consumers (same events go to both ADLS and ASA simultaneously). (3) Capture writes Avro format with metadata — a custom consumer can choose format. (4) Capture adds cost ($0.10/TU-hour) but saves engineering effort.
