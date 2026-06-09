---
title: "Streaming Architecture Patterns — Intermediate"
topic: real-time-streaming
subtopic: streaming-architecture
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [streaming, architecture, design, kafka, flink, scaling, fault-tolerance, monitoring]
---

# Streaming Architecture Patterns — Intermediate

## Multi-Tier Streaming Architecture

```
Production streaming architecture for a large-scale system:

Tier 1: Ingestion
  Web/Mobile/IoT → Kafka (raw events)
  Database changes → Debezium → Kafka (CDC events)
  APIs → Kafka (integration events)
  
  Kafka cluster: 9 brokers, 300+ partitions, replication factor 3
  Throughput: 500 MB/sec
  Retention: 7 days (for replay capability)

Tier 2: Stream Processing
  Kafka → [Flink / Spark Structured Streaming] → Kafka or Delta Lake
  
  Jobs:
    1. Bronze ingestion: raw → Bronze Delta Lake (no transformation, fast)
    2. Validation/enrichment: Bronze Kafka → Silver Kafka/Delta (parse, validate, enrich)
    3. Real-time aggregations: Silver → Gold Delta (windowed, stateful)
    4. Alerting: Silver → Kafka alerts (immediate rule-based)
  
  Each job is independently deployable and scalable
  Jobs connected via Kafka topics (loose coupling, replayable)

Tier 3: Serving
  Delta Lake (Gold): batch SQL queries (Athena, Spark SQL, Synapse)
  Redis: real-time lookups (< 5ms per key)
  Elasticsearch: full-text search + near-real-time analytics
  Kafka: event forwarding to downstream services
  API: REST endpoints reading from Redis/Delta

Tier 4: Monitoring
  Prometheus + Grafana: Kafka lag, Flink metrics, throughput
  PagerDuty: alerts on SLA violations
  Data Quality: row counts, late events, schema validation alerts

Design principles:
  - Each tier loosely coupled (connected via Kafka or object storage)
  - Any tier can fail and recover without losing data (idempotent writes, checkpointing)
  - Each tier scales independently (Flink parallelism, Kafka partition count, Delta OPTIMIZE)
```

---

## Kafka Topic Design for Production

```
Topic design: critical architecture decision (hard to change after)

Granularity:
  Too coarse (one topic for everything): consumers must filter out irrelevant events
  Too fine (one topic per entity per event): operational overhead (hundreds of topics)
  
  Recommended: one topic per logical entity type
    orders-events (all order lifecycle events)
    user-events (all user actions)
    payment-events (all payment events)
    → Within topic: consumers filter by event_type field

Partition strategy:
  Goal: even distribution, ordering where needed, enough parallelism
  
  Partition by business key (most common):
    orders-events: partition by order_id → all events for same order are ordered
    user-events:   partition by user_id → all events for same user are ordered
    
  Partition count:
    Start: max expected consumer parallelism × 2 (for future growth)
    Example: plan for 32 Flink tasks → 64 partitions
    Rule: never less than the number of intended consumer instances
    Cost: each partition = overhead (memory, file handles)
    Max: 10,000 partitions per cluster is a practical limit
    
  Replication factor:
    Development: 1 (no redundancy, don't care about data loss)
    Production:  3 (survive 1 broker failure without data loss)
    Critical:    5 (survive 2 broker failures)
    
  Retention:
    Raw events: 7 days (allows consumer catch-up + replay)
    Processed: 3 days (shorter, already checkpointed to Delta)
    Audit logs: 90 days (compliance)

Topic naming convention:
  {environment}.{domain}.{entity}.{version}
  prod.ecommerce.orders.v1
  prod.payments.transactions.v2
  dev.ecommerce.orders.v1

  Benefits: easy filtering, namespace isolation per environment/domain

Schema management:
  All topics: Avro format + Schema Registry (Confluent)
  Compatibility: FORWARD (new fields must be optional with defaults)
  Version strategy: schema version in Registry, NOT in topic name
    (topic names are stable; schema evolves transparently via Registry)
```

---

## Fault Tolerance Design

```
Fault tolerance layers:

1. Kafka (source):
   Replication factor = 3 (survives 1 broker failure)
   min.insync.replicas = 2 (producer gets ack from 2 replicas before confirming)
   acks = all (producer waits for all ISR replicas to acknowledge)
   
   Recovery: if broker fails, partition leader election < 30 seconds
   Data loss: none (3 replicas, min 2 in-sync required)

2. Stream processor (Flink):
   Checkpointing every 60 seconds to S3/ADLS
   Recovery: restart from last checkpoint, reprocess ~60 seconds of events
   Exactly-once: Kafka source offsets + sink 2PC (Iceberg, Kafka)
   
   High availability: JobManager HA with ZooKeeper/KRaft standby
   TaskManager failure: replaced by Kubernetes (restart pod, restore from checkpoint)
   
   Parallelism: 16+ instances per operator (single node failure = job continues)

3. Sinks:
   Delta Lake: ACID transactions, data never corrupted by partial writes
   Kafka: replication factor = 3 (output events durable)
   Redis: sentinel or cluster mode (primary + replicas)
   
4. Monitoring and alerting:
   Kafka consumer lag > 5 minutes → page on-call
   Flink checkpoint failure 3× in a row → page on-call
   DLQ messages > 100/hour → alert
   End-to-end latency > 2× SLA → alert

5. Regional failover:
   Kafka: MirrorMaker 2 (replicates topics to DR region)
   Flink: separate cluster in DR region (hot standby reading from replicated Kafka)
   Delta Lake: cross-region S3 replication
   
   RTO (Recovery Time Objective): 5 minutes (failover to DR)
   RPO (Recovery Point Objective): 1 minute (checkpoint frequency)
```

---

## Scaling Strategies

```
Scaling stream processing:

1. Horizontal scaling (add more instances):
   Flink: increase parallelism (more TaskManagers → more tasks → more CPU)
   Spark: add executor nodes (Databricks: increase cluster size)
   Kafka consumers: add consumer group members (up to partition count)
   
   Bottleneck: Kafka partition count limits consumer parallelism
   Fix: increase partition count (Kafka allows adding partitions, not reducing)
   Note: adding partitions redistributes data only for new messages
         existing messages stay in original partitions

2. Vertical scaling (bigger machines):
   RocksDB-backed state: benefit from more RAM (block cache)
   Photon (Databricks): benefit from more CPU cores for vectorized execution
   Network I/O bound: bigger network cards, placement groups
   
   Preferred: horizontal (more resilient than single large machine)

3. Backpressure handling:
   Kafka max.offsets.per.trigger (Spark): limit batch size
   Flink: automatic backpressure propagation to source (slows Kafka poll)
   Rate limiting: token bucket at source (limit events/sec ingested)
   
   Backpressure signals:
     Flink: outPoolUsage > 80% per operator
     Spark: batch duration > trigger interval
     Kafka: consumer lag growing

4. State scaling:
   Problem: state too large for heap memory
   Solution: RocksDB state backend (disk-based)
   Scale state: more nodes with local NVMe SSDs
   State redistribution: savepoint → change parallelism → restore (automatic key redistribution)

5. Multi-region scaling:
   Kafka: add brokers in multiple AZs (within-region HA)
   Cross-region: Kafka MirrorMaker + Flink in each region
   Trade-off: cross-region bandwidth cost vs. local processing latency
```

---

## Interview Tips

> **Tip 1:** "How do you design a streaming architecture that can handle a 10× traffic spike?" — Defense in depth: (a) Kafka absorbs spikes: producers write at full speed, Kafka buffers. Consumers process at their max rate. Consumer lag grows during spike, drains after; (b) Autoscaling: Kubernetes HPA on Flink TaskManagers based on CPU (or consumer lag via KEDA); (c) `maxOffsetsPerTrigger` (Spark) or `maxBatchSize` limits per-batch load; (d) Pre-scale: for known events (product launches, Black Friday), pre-scale to 2-3× expected load 30 minutes before; (e) Circuit breaker on sink: if downstream DB is overwhelmed, queue in Kafka (don't block the pipeline). Key: the streaming buffer (Kafka) is the shock absorber for traffic spikes.

> **Tip 2:** "What is KEDA and how does it help with streaming auto-scaling?" — KEDA (Kubernetes Event-Driven Autoscaler) scales Kubernetes pods based on external metrics, including Kafka consumer lag. Configure: `ScaledObject` targeting Kafka topic → consumer group lag. When lag > threshold: KEDA increases pod count. When lag = 0: KEDA scales down to 0 (or min pods). This enables cost-efficient auto-scaling: Flink job processes events at full throughput only when needed. Works with Spark on Kubernetes similarly. Critical configuration: don't scale down faster than it takes to redistribute state — set `stabilizationWindowSeconds` to prevent thrashing. Pair with Flink savepoints on scale-down to preserve state cleanly.

> **Tip 3:** "How do you monitor end-to-end latency in a streaming pipeline?" — Embed an event timestamp at the source and measure at each stage. Method: add a `kafka_produced_at` field when producing to Kafka. Each downstream stage computes `stage_latency = current_time - kafka_produced_at`. Write latency metrics to Prometheus. Alert if p99 latency > SLA. For complex pipelines: use distributed tracing (OpenTelemetry) — each event carries a trace context header through Kafka, Flink, and to the sink. Trace visualization shows where latency accumulates. Synthetic canary events: publish a "heartbeat" event every 30 seconds; measure how long it takes to appear in the final sink. If heartbeat latency exceeds SLA → pipeline is falling behind.
