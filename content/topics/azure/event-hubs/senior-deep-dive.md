---
title: "Event Hubs — Senior Deep Dive"
topic: azure
subtopic: event-hubs
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [azure, event-hubs, kafka, geo-replication, private-link, federation, performance]
---

# Event Hubs — Senior Deep Dive

## Event Hubs Internal Architecture

```
Physical architecture:

Namespace → mapped to Azure Service Bus cluster
Event Hub  → logical entity (like Kafka topic)
Partition  → append-only log on cluster storage (Azure Storage-backed)
  
Storage model:
  Each partition: persistent append log in Azure storage (durable, replicated)
  Retention: events stored for configured duration (1h–90 days)
  Offset: sequential byte offset into partition log
  Sequence number: monotonically increasing integer per partition

Producer flow:
  1. Producer sends EventData batch → TLS to Event Hubs gateway
  2. Gateway routes to broker responsible for the target partition (based on partition key hash)
  3. Broker appends to partition log (Azure storage write)
  4. Acknowledgment sent to producer
  5. In parallel: replication to geo-paired region (if Geo-DR enabled)

Consumer flow:
  1. Consumer subscribes with consumer group + partition assignment
  2. Sends FETCH request with offset
  3. Broker returns events from offset position
  4. Consumer processes events, advances checkpoint

Throughput Units (TU) — Standard tier:
  1 TU = 1 MB/sec ingress OR 1,000 events/sec ingress (whichever is lower)
           2 MB/sec egress  OR 2,000 events/sec egress
  Auto-inflate: automatically increases TUs when approaching limit
  
  Capacity planning:
    Peak ingress: 500 MB/sec → 500 TUs
    Peak egress: 3 consumers × 500 MB/sec = 1,500 MB/sec → 750 TUs
    Always size for egress (typically 2-3× ingress for multiple consumers)
```

---

## Geo-Disaster Recovery and Active-Active Replication

```
Geo-DR (passive replication — metadata only):
  Primary namespace → alias (e.g., mycompany.servicebus.windows.net)
  Secondary namespace: manually created in paired region
  
  What replicates: entity configuration (Event Hub names, consumer groups, properties)
  What does NOT replicate: actual event data
  
  Failover: when primary is down → initiate failover → alias points to secondary
  RPO for event data: events NOT replicated → secondary starts from empty log
  Use case: business continuity for new events (historical data not preserved)
  Limitation: not for disaster recovery of existing events

Active-Active replication (Event Hubs Federation):
  Two (or more) namespaces in different regions
  Replication task: reads from one namespace, writes to the other (and vice versa)
  Azure Functions-based: custom function reads consumer group, writes to remote namespace
  
  Federation pattern:
    Primary (East US):   producer → Event Hub A → local consumers
    Secondary (West US): Event Hub B → local consumers
    Replication task (Azure Functions):
      East→West: reads from EH-A consumer group "federation" → writes to EH-B
      West→East: reads from EH-B consumer group "federation" → writes to EH-A
  
  RPO: near-zero (lag depends on replication function frequency, ~seconds)
  Cost: Azure Functions invocations + egress bandwidth (cross-region data transfer)
  Use case: multi-region active serving of the same events

Event Hubs Geo Replication (Preview — Premium/Dedicated):
  Microsoft-managed active-passive replication with event data included
  Unlike manual Geo-DR: actual events replicated to secondary
  RPO: minutes (async replication)
  GA timeline: 2024-2025 (check current Azure docs for status)
```

---

## Private Endpoints and Network Security

```
Production network security for Event Hubs:

Option 1: IP firewall rules (basic)
  Allow list: specific IP ranges or Azure service bypass
  Portal: Namespace → Networking → Firewalls and virtual networks
  Limitation: still uses public DNS (nameresolution)

Option 2: Private Endpoints (recommended)
  Create private endpoint in your VNet → Event Hubs gets private IP
  DNS: mynamespace.servicebus.windows.net → 10.0.1.4 (private IP via Private DNS Zone)
  All traffic: producer/consumer → private IP → Event Hubs (never public internet)
  
  Setup:
  az network private-endpoint create \
    --name pe-eventhubs-prod \
    --resource-group rg-data \
    --vnet-name vnet-data \
    --subnet subnet-private \
    --private-connection-resource-id /subscriptions/.../namespaces/mynamespace \
    --group-id namespace \
    --connection-name eventhubs-connection

  Private DNS Zone: privatelink.servicebus.windows.net
  Register CNAME: mynamespace.servicebus.windows.net → mynamespace.privatelink.servicebus.windows.net

Authentication (in priority order):
  1. Managed Identity (preferred): no credentials, auto-rotated tokens
     Role: Azure Event Hubs Data Sender (producer), Azure Event Hubs Data Receiver (consumer)
  2. Service Principal with client secret (acceptable)
  3. Shared Access Signature (SAS): time-limited, avoid for long-running services
  4. Connection string: legacy, contains key — avoid in code, use Key Vault reference

Defender for IoT / Event Hubs:
  Microsoft Defender for IoT: monitors Event Hubs for anomalous traffic patterns
  Custom: Azure Monitor alert on unexpected spike in IncomingMessages count
```

---

## Performance Tuning and Partitioning Strategy

```python
# Performance best practices for high-throughput producers

from azure.eventhub import EventHubProducerClient, EventData, EventDataBatch

# Best practice: batch sends (not one event at a time)
async def high_throughput_producer(events: list, namespace: str, eventhub: str):
    """Send events in maximum-size batches for best throughput."""
    async with EventHubProducerClient(
        fully_qualified_namespace=namespace,
        eventhub_name=eventhub,
        credential=DefaultAzureCredential()
    ) as producer:
        batch = await producer.create_batch()
        sent_count = 0
        
        for event_dict in events:
            event = EventData(json.dumps(event_dict))
            try:
                batch.add(event)
            except ValueError:
                # Batch full (1MB limit) → send and start new batch
                await producer.send_batch(batch)
                sent_count += len(batch)
                batch = await producer.create_batch()
                batch.add(event)
        
        if len(batch) > 0:
            await producer.send_batch(batch)
            sent_count += len(batch)
        
        print(f"Sent {sent_count} events in batches")

# Partition strategy:
# Option A: partition_key (consistent routing, ordering guaranteed)
#   Use when: same entity's events must be ordered (customer_id, device_id)
#   Cost: uneven partition load if keys are not uniformly distributed

# Option B: round-robin (no partition_key)
#   Use when: events are independent, maximize throughput
#   Cost: no ordering guarantee

# Option C: explicit partition_id (advanced control)
#   Use when: consumer-partition affinity required (A/B testing, sharded processing)
#   Cost: must manage partition assignment in code

# Partition count sizing:
# Rule: partition_count >= max_concurrent_consumers
# Event Hubs Standard: max 32 partitions
# If you need more: use Premium (100 partitions) or Dedicated (no limit)
# Example: 10 Databricks workers reading in parallel → need 10 partitions minimum

# Throughput calculation:
# Target: 500 MB/sec ingress
# Standard TUs: 500 TUs × $0.03/TU-hr × 24 × 30 = $10,800/month
# Premium PUs: 500MB/sec / (0.1 GB/sec per PU) = 5 PUs × $0.08/PU-hr × 24 × 30 = $288/month
# Conclusion: Premium tier is drastically cheaper for high throughput!
```

---

## Interview Tips

> **Tip 1:** "How does Event Hubs handle back-pressure from slow consumers?" — Event Hubs is a pull-based system — consumers fetch events at their own pace. There is no back-pressure from Event Hubs to producers (producers can always write as long as TU capacity allows). Slow consumers simply fall behind — their offset stays behind while new events continue appending. This is visible as "consumer lag" (events in partition minus last committed offset). If a consumer is consistently behind, it will eventually fall behind the retention window and lose events. Solution: scale the consumer (more threads, more worker nodes), or increase Event Hubs retention to allow more catch-up time.

> **Tip 2:** "What's Event Hubs Federation and when would you use it?" — Federation is active-active replication between Event Hubs namespaces in different regions using Azure Functions as the replication task. Unlike Geo-DR (which only replicates namespace metadata, not events), Federation replicates the actual event stream. Use cases: (a) multi-region latency optimization (producers write to nearest region, events replicated to all regions), (b) disaster recovery with full event data (RPO ~ seconds instead of "all events since failover"), (c) consolidation (aggregate events from multiple regions into one central namespace for global analytics). Cost: Azure Functions compute + cross-region egress bandwidth.

> **Tip 3:** "When should you use Event Hubs Premium over Standard tier?" — Standard tier is sufficient for: moderate throughput (<100 MB/sec), up to 32 partitions, 7-day retention. Choose Premium when: (a) throughput >100 MB/sec (Premium is drastically cheaper per MB at scale), (b) retention >7 days (up to 90 days in Premium), (c) need Schema Registry (included in Premium, add-on for Standard), (d) compliance requiring dedicated compute isolation, (e) large consumer count (Premium supports 100+ consumer groups vs Standard's 20). The pricing crossover: above ~50 TUs, Premium PUs become cheaper per MB/sec.
