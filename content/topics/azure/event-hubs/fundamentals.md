---
title: "Event Hubs — Fundamentals"
topic: azure
subtopic: event-hubs
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [azure, event-hubs, kafka, streaming, ingestion, partitions]
---

# Event Hubs — Fundamentals

## What Is Azure Event Hubs?

Azure Event Hubs is a **fully managed, real-time data streaming and event ingestion service**. It acts as the "front door" for event pipelines — applications publish events to Event Hubs, and multiple consumers read those events independently.

```
Event Hubs = managed Apache Kafka (compatible API)

Analogy: Event Hubs is like a distributed log
  Producers:  applications, IoT devices, services → publish events
  Event Hubs: stores events ordered per partition (retain for up to 90 days)
  Consumers:  ASA, Databricks, ADF, custom apps → read at their own pace
  
Key characteristics:
  Fully managed:     no ZooKeeper, no broker management
  Kafka compatible:  native Kafka protocol support (port 9093)
  High throughput:   millions of events/sec, petabytes/day
  Multiple consumers: each consumer group reads events independently (fan-out)
  Retention:         1 to 90 days (Standard), unlimited (Premium/Dedicated)
  
Comparison to Azure Service Bus:
  Event Hubs: high-throughput streaming (billions/day), log-style, no per-message ACK
  Service Bus: transactional messaging, per-message settlement, queuing semantics
  Use Event Hubs for: analytics, telemetry, log aggregation
  Use Service Bus for: order processing, work queues, pub/sub topics
```

---

## Core Concepts

```
Namespace: container for multiple Event Hubs (like a cluster)
  URL: mynamespace.servicebus.windows.net
  One namespace per environment (dev/prod) is typical

Event Hub: named stream within the namespace (like a Kafka topic)
  Example: orders, clickstream, iot-telemetry, audit-logs

Partition:
  Each Event Hub is divided into N partitions (1 to 32, set at creation)
  Events with same partition key → same partition → ordered per partition
  Different partitions: independent, parallel reading
  More partitions = more parallel consumers = higher throughput
  Cannot change partition count after creation (plan ahead)

Consumer Group:
  Logical view of an Event Hub for a consumer application
  Each consumer group maintains its own checkpoint (offset)
  Multiple consumer groups on one Event Hub = fan-out (all get same events)
  Default: $Default consumer group (exists automatically)
  Best practice: create dedicated consumer group per consumer

Event:
  Max size: 1 MB (Standard), 1 MB (Premium)
  Properties: body, user properties (key-value metadata), system properties
  Ordering: guaranteed within a partition only
  Sequence number: monotonically increasing per partition
```

---

## Tiers and Sizing

```
Standard tier:
  Throughput Units (TUs): 1 TU = 1 MB/sec ingress, 2 MB/sec egress
  Max TUs: 20 (auto-inflate to 40)
  Retention: 1-7 days
  Consumer groups: up to 20
  Cost: $0.015/million events + $0.03/TU-hour
  Use for: most production workloads

Premium tier:
  Processing Units (PUs): 1 PU = 1 vCPU + 2GB RAM (dedicated cluster slice)
  Retention: up to 90 days
  Schema Registry included
  Better isolation (dedicated compute slice)
  Cost: ~$0.08/PU-hour
  Use for: compliance requirements, long retention, schema governance

Dedicated tier:
  Entire Event Hubs cluster dedicated to you
  Unlimited retention (Blob Storage backed)
  Up to 200 MB/sec per Capacity Unit
  Cost: ~$6,900/month per Capacity Unit
  Use for: largest workloads (petabytes/day), BYOS (Bring Your Own Storage)

Partitions and TUs:
  1 TU = 1 MB/sec ingress across all partitions
  For 100 MB/sec: need 100 TUs
  Partition count: set to expected number of parallel consumers
  Rule: partitions ≥ concurrent consumer instances
```

---

## Kafka Protocol Compatibility

```python
# Event Hubs supports Kafka protocol (port 9093) — no code changes needed for Kafka apps

# Standard Kafka producer pointing to Event Hubs:
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'mynamespace.servicebus.windows.net:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism':    'PLAIN',
    'sasl.username':     '$ConnectionString',
    'sasl.password':     '<Event Hubs connection string>',
    'client.id':         'my-producer'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err:
        print(f'Delivery failed: {err}')

producer.produce(
    topic='orders',           # Event Hub name = Kafka topic name
    key='customer_123',       # partition key
    value='{"order_id":42, "amount": 99.99}',
    callback=delivery_report
)
producer.flush()

# Standard Kafka consumer:
from confluent_kafka import Consumer

consumer_conf = {
    **conf,
    'group.id':           'my-consumer-group',    # = Event Hubs consumer group
    'auto.offset.reset':  'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['orders'])

while True:
    msg = consumer.poll(timeout=1.0)
    if msg and not msg.error():
        print(f"Received: {msg.value().decode('utf-8')}")
# Works with existing Kafka code — just change bootstrap.servers
```

---

## Interview Tips

> **Tip 1:** "What's the difference between Event Hubs and Kafka?" — They're almost the same: Event Hubs implements the Kafka wire protocol, so Kafka clients work with Event Hubs without code changes. Key differences: Event Hubs is fully managed (no cluster administration), has tighter Azure integration (Managed Identity auth, Private Link, Azure Monitor), and caps partition count at 32 (Kafka clusters have no hard limit). Kafka gives more control: custom compaction, tiered storage configs, complex ACLs. Use Event Hubs if you're Azure-first and don't want cluster management. Use Kafka (HDInsight or Confluent) if you need Kafka ecosystem tools (Kafka Streams, KSQL, Kafka Connect).

> **Tip 2:** "Why create a dedicated consumer group per consumer application?" — Each consumer group has its own independent offset (checkpoint). If Application A (Databricks ETL) and Application B (ASA fraud detection) share the $Default consumer group, they compete for partitions — one application's progress affects the other. With separate consumer groups: Databricks reads at its own pace (may be 5 minutes behind), ASA reads at its own pace (real-time), both reading the same events from Event Hubs independently. Never use $Default in production — always create named consumer groups.

> **Tip 3:** "Can you change the partition count after creating an Event Hub?" — No — partition count is set at creation time and is immutable. This is a critical design decision. Under-provision: you'll hit throughput limits per partition. Over-provision: you pay for unused partitions (minor cost). Rule: set partitions = expected number of parallel consumer instances × 2 (headroom). For IoT with 50 devices: 32 partitions is fine. For 10 consumer threads: 32 is enough. For future scale: choose the highest supported count (32 for Standard, more for Premium/Dedicated).
