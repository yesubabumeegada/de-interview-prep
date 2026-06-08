---
title: "Event Hubs — Real World"
topic: azure
subtopic: event-hubs
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [azure, event-hubs, production, ingestion, kafka-migration, monitoring]
---

# Event Hubs — Real World

## Pattern 1: Multi-Consumer Fan-Out Architecture

```
Event Hubs as the single ingestion point, multiple consumers reading independently

Architecture:
  Producer apps → Event Hub: orders (32 partitions, 20 TUs)
  
  Consumer group 1: asa-consumer → Azure Stream Analytics
    Purpose: real-time fraud detection, Power BI live dashboard
    Lag tolerance: < 30 seconds
    
  Consumer group 2: databricks-consumer → Databricks Structured Streaming
    Purpose: Bronze Delta table, medallion ETL pipeline
    Lag tolerance: < 5 minutes (micro-batch every 2 min)
    
  Consumer group 3: capture-consumer → Event Hubs Capture → ADLS
    Purpose: raw event archive for replay and audit
    No code needed — enabled as a feature
    File format: Avro with full message metadata
    
  Consumer group 4: elk-consumer → Logstash → Elasticsearch
    Purpose: operational log search for engineering team
    Lag tolerance: < 2 minutes

Each consumer group checkpoints independently in Blob Storage.
If Databricks job crashes: it resumes from its own checkpoint.
ASA continues unaffected (separate consumer group, separate offset).

Event Hubs Capture settings:
  Time window: 5 minutes
  Size window: 100 MB
  Path: raw/orders/{Namespace}/{EventHub}/{PartitionId}/{Year}/{Month}/{Day}/{Hour}/{Minute}/{Second}.avro
  
Monitoring:
  Azure Monitor alert: consumer lag > 100,000 events for 10 minutes → escalate
  Alert: IncomingMessages drop to 0 for 5 min → producer health check
  Alert: ThrottledRequests > 0 → TU limit hit, auto-inflate triggered
```

---

## Pattern 2: Kafka Application Migration to Event Hubs

```python
# Zero-code migration: Kafka app → Event Hubs (Kafka protocol compatible)

# Original Kafka producer config (before migration):
KAFKA_CONFIG_BEFORE = {
    'bootstrap.servers': 'kafka-broker-1:9092,kafka-broker-2:9092',
    'security.protocol': 'PLAINTEXT',
    'client.id': 'order-producer'
}

# After migration to Event Hubs (change ONLY these 3 lines):
KAFKA_CONFIG_AFTER = {
    'bootstrap.servers': 'mynamespace.servicebus.windows.net:9093',  # ← Event Hubs endpoint
    'security.protocol': 'SASL_SSL',                                   # ← TLS
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '$ConnectionString',
    'sasl.password': 'Endpoint=sb://mynamespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=<key>',
    'client.id': 'order-producer'
    # All other config (compression, batch.size, linger.ms) stays identical
}

# Better: use Managed Identity instead of connection string
# Requires azure-identity library and SASL OAuth BEARER mechanism

KAFKA_CONFIG_MSI = {
    'bootstrap.servers': 'mynamespace.servicebus.windows.net:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'OAUTHBEARER',
    'sasl.oauthbearer.config': f'grant_type=client_credentials '
                               f'scope=https://eventhubs.azure.net/.default '
                               f'client_id={CLIENT_ID} '
                               f'client_secret={CLIENT_SECRET} '
                               f'tenant_id={TENANT_ID}',
}

# Migration validation checklist:
# 1. Create Event Hub with same partition count as Kafka topic
# 2. Update bootstrap.servers + auth config in all producer/consumer apps
# 3. Test with shadow traffic (write to both Kafka and Event Hubs for 1 week)
# 4. Validate: consumer group offsets advance correctly, no message loss
# 5. Cutover: stop Kafka producers, ensure all inflight events consumed, switch
# 6. Decommission Kafka cluster after 2-week parallel observation
```

---

## Pattern 3: Monitoring Event Hubs with Azure Monitor

```python
# Key Event Hubs metrics and KQL queries for operational health

# Metric 1: Consumer Lag per consumer group
# Azure Monitor doesn't expose consumer lag natively
# Use: az eventhubs eventhub consumer-group show --name asa-consumer
# Or: custom Azure Function that reads partition offset metadata every minute

from azure.eventhub import EventHubConsumerClient

def get_consumer_lag(namespace: str, eventhub: str, consumer_group: str, credential) -> dict:
    """Calculate consumer lag across all partitions."""
    client = EventHubConsumerClient(
        fully_qualified_namespace=namespace,
        eventhub_name=eventhub,
        consumer_group=consumer_group,
        credential=credential
    )
    
    lag_per_partition = {}
    partition_ids = client.get_partition_ids()
    
    for partition_id in partition_ids:
        props = client.get_partition_properties(partition_id)
        last_enqueued = props["last_enqueued_sequence_number"]
        
        # Consumer checkpoint (from Blob checkpoint store):
        # checkpoint_offset = read from blob: container/eventhub/consumer_group/partition_id
        # lag = last_enqueued - checkpoint_sequence_number
        # (simplified — actual implementation reads Blob checkpoint)
        
        lag_per_partition[partition_id] = {
            "last_enqueued_sequence": last_enqueued,
            "last_enqueued_time": props["last_enqueued_time"].isoformat()
        }
    
    return lag_per_partition

# KQL alert for Event Hubs throttling:
# AzureMetrics
# | where ResourceProvider == "MICROSOFT.EVENTHUB"
# | where MetricName == "ThrottledRequests"
# | summarize total_throttled = sum(Total) by bin(TimeGenerated, 5m)
# | where total_throttled > 0
# | project TimeGenerated, total_throttled

# Action: if throttled → auto-inflate TUs (if enabled)
# Or: Azure Automation runbook to increase TUs via REST API
```

---

## Interview Tips

> **Tip 1:** "How do you handle poison messages (bad events that crash your consumer) in Event Hubs?" — Event Hubs has no built-in dead-letter queue (unlike Service Bus). Strategies: (a) wrap message processing in try/except, log failures to a "poison events" Delta table or Blob (include partition, offset, raw bytes, error message), continue processing; (b) use Event Hubs Capture as a safety net — the raw Avro archive allows replaying specific events after fixing the consumer; (c) for Databricks streaming, use `badRecordsPath` option in `cloudFiles` or write corrupt records to a quarantine table with `@dlt.expect_all_or_drop`. Never crash the consumer on a single bad event — it blocks all subsequent events in that partition.

> **Tip 2:** "A consumer group is falling behind (growing lag). How do you scale up?" — For Databricks Structured Streaming: add more executor nodes to the cluster (auto-scaling), reduce `trigger` interval, or increase `maxOffsetsPerTrigger`. For ASA: increase SUs (no restart needed) or add `PARTITION BY PartitionId` to the query for parallel processing. For custom consumer: add consumer instances (up to the number of partitions — beyond that, instances idle). Check if the bottleneck is reading (add parallelism) or processing (optimize the processing logic). Also check if Event Hubs is throttling: ThrottledRequests > 0 means adding consumer instances won't help — increase TUs first.

> **Tip 3:** "What's the maximum message size in Event Hubs and how do you handle larger payloads?" — Standard and Premium: 1 MB max per event. For larger payloads: (a) Claim Check pattern — store large payload in ADLS Blob, send only the Blob URL + metadata as the event (consumers fetch from Blob); (b) Compress payload (Gzip/LZ4) before sending — JSON usually compresses 5-10×, bringing 5MB event to <1MB; (c) Split large events into multiple events with a correlation ID (consumer reassembles). The Claim Check pattern is most common: event contains only references, storage handles the large data.
