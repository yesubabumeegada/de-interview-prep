---
title: "Streaming Architecture Patterns — Senior Deep Dive"
topic: real-time-streaming
subtopic: streaming-architecture
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [streaming, architecture, design, kafka, flink, cost-optimization, observability, production]
---

# Streaming Architecture Patterns — Senior Deep Dive

## Technology Selection Framework

```
Choosing the right streaming stack:

Decision tree:

1. Latency requirement:
   < 10ms:    Kafka Streams (in-process, no network hops)
              Flink (optimized, local state)
   10-100ms:  Flink DataStream API
   100ms-1s:  Flink SQL, Spark Structured Streaming (continuous mode)
   1-60s:     Spark Structured Streaming (micro-batch)
   1-60min:   Spark batch with trigger(availableNow)

2. State complexity:
   None:      Any (Kafka Streams, Flink, Spark, ASA)
   Simple (count, sum):   Any with windows
   Rich stateful (complex sessions, timers):  Flink (best)
   Large state (> 10 GB): Flink + RocksDB
   
3. Ecosystem:
   AWS-centric:      Kinesis + KDA (managed Flink) or MSK + Flink on EMR
   Azure-centric:    Event Hubs + Databricks (Spark SS) or Azure Stream Analytics
   GCP-centric:      Pub/Sub + Dataflow (Apache Beam) or Bigtable + Flink on GKE
   Multi-cloud:      Kafka + Flink (portable, self-managed) or Confluent Cloud + Flink

4. Team expertise:
   SQL-heavy team:        Flink SQL, ksqlDB, Azure Stream Analytics
   Java/Scala team:       Flink DataStream API, Kafka Streams
   Python/PySpark team:   Spark Structured Streaming, Flink Table API (Python)
   No streaming expertise: Managed services (KDA, Dataflow, Azure Stream Analytics)

5. Cost model:
   Always-on, high throughput:  Flink on Kubernetes (EC2 cost, efficient)
   Variable, bursty:            Spark on Databricks (serverless, pay per DBU)
   Fully managed, AWS:          KDA (per KPU-hour, higher but no ops)
   
Framework recommendation matrix:
  Use case                    Recommended Stack
  ─────────────────────────── ─────────────────────────────────────
  Sub-second fraud detection  Flink (DataStream API + RocksDB)
  Real-time dashboard         Spark SS (trigger=1min) → Delta → Power BI
  CDC to data lake            Debezium + Flink SQL → Iceberg
  IoT sensor analytics        Flink SQL (session windows, aggregations)
  Microservice integration    Kafka Streams (embedded, no cluster needed)
  Multi-cloud streaming       Flink on Kubernetes (portable)
  AWS-native, simple rules    Kinesis + ASA (fully managed)
```

---

## Streaming Data Mesh Architecture

```
Data Mesh: decentralized data ownership by domain teams
Streaming extension: each domain owns its event streams

Domain-oriented Kafka organization:
  orders.domain.events (owned by Order team)
  payments.domain.events (owned by Payment team)
  inventory.domain.events (owned by Inventory team)
  users.domain.events (owned by User team)

Governance:
  Central:    Schema Registry (common, enforced compatibility)
              Kafka ACLs (producers can only write to their domain topics)
              Data Catalog (Purview/Atlas: discover all event streams)
  Domain:     Topic design, partition count, retention
              Consumer SLAs (how long will domain support consumers?)
              Schema version management

Cross-domain streaming:
  Order team produces to: orders.domain.events
  Analytics team consumes: orders.domain.events (read-only, separate consumer group)
  Payment team reacts:     orders.domain.events → internal payment processing
  
  No direct cross-domain API calls: loose coupling via Kafka

Event contracts:
  Domain teams publish an "event contract":
    - Topic name and format
    - Schema (Avro, registered in Schema Registry)
    - SLA: "ORDER_PLACED events published within 500ms of DB commit"
    - Retention: "7 days minimum"
    - Breaking changes: "30-day deprecation notice"
  Consumed by: analytics, ML, other microservices
  
  Contract enforcement:
    Schema Registry: blocks schema breaking changes automatically
    Monitoring: alert if event rate drops (SLA violation detection)
    Canary events: synthetic events to validate pipeline health

Data Mesh + Medallion:
  Each domain: Bronze (raw events) → Silver (validated) → Gold (domain aggregations)
  Cross-domain Gold tables: shared via Delta Sharing or Iceberg REST Catalog
  No cross-domain DB queries: data shared as read-optimized files (not live DB access)
```

---

## Cost Optimization for Streaming

```
Major cost drivers in streaming:

1. Compute (stream processor):
   Flink on Kubernetes: EC2 cost
   Databricks: DBU + EC2 cost (DBU premium for spark runtime)
   KDA: per KPU-hour ($0.11/KPU-hr)
   
   Optimization:
     Spot instances (Flink on K8s): 60-80% savings
       Risk: instance preemption → job restarts from checkpoint (brief delay)
       Mitigation: checkpoint every 30s, driver on on-demand, workers on spot
     
     Right-size parallelism: don't over-provision
       Measure: actual CPU utilization per TaskManager (target 70-80%)
       Reduce: parallelism from 32 → 16 if CPU < 40% → 50% cost reduction
     
     Trigger(availableNow) vs continuous:
       Continuous (always-on): 24/7 cluster running → $3,000/month (32 tasks)
       Scheduled (every 15 min): cluster runs 10 min/hour → $300/month (90% savings)
       When to use scheduled: if < 15 min latency is acceptable

2. Kafka / Kinesis:
   Kafka (MSK/Confluent Cloud): per broker/hour + storage
     Optimization: right-size retention (7 days instead of 30 → 4× storage savings)
     Compact topics where applicable (keyed topics with full state)
     
   Kinesis: per shard/hour + per PUT API call
     KPL aggregation: pack 10 records into 1 KDS record → 90% PUT call cost savings
     Enhanced Fan-Out: only when needed (adds $0.015/shard-hour per consumer)
     Auto-scaling: reduce shards during off-peak

3. Storage:
   S3 (Delta Lake): $0.023/GB/month (us-east-1)
     OPTIMIZE + VACUUM: remove small files and old snapshots → 50-80% storage savings
     Lifecycle policy: archive old partitions to Glacier ($0.004/GB) → 83% savings
     
4. Cost monitoring:
   Tag all streaming resources: team, project, environment
   AWS Cost Explorer: per-tag breakdown monthly
   Alert: if stream processing cost > budget + 20%
   Dashboards: cost per event processed (normalize by throughput)
   
Cost optimization roadmap:
  Month 1: Baseline — measure cost per component
  Month 2: Right-size — reduce over-provisioned clusters
  Month 3: Spot instances — Flink workers on spot
  Month 4: Retention tuning — reduce Kafka/S3 retention
  Month 5: Scheduled triggers — convert batch-style jobs from always-on
  Expected savings: 40-60% reduction from baseline
```

---

## Observability for Streaming Systems

```
Three pillars: Metrics, Logs, Traces

METRICS (what's happening — time-series data):

  Kafka metrics (exported via JMX or Prometheus JMX exporter):
    kafka.server.BrokerTopicMetrics.BytesInPerSec        → write throughput
    kafka.server.BrokerTopicMetrics.BytesOutPerSec       → read throughput
    kafka.consumer.ConsumerFetchManagerMetrics.records-lag → consumer lag (critical!)
    kafka.log.LogSize                                    → disk usage per topic

  Flink metrics (exported via Prometheus reporter):
    flink.taskmanager.job.task.operator.numRecordsInPerSecond → input throughput
    flink.taskmanager.job.task.operator.numRecordsOutPerSecond → output throughput
    flink.jobmanager.job.numberOfFailedCheckpoints            → checkpoint health
    flink.taskmanager.job.task.backPressuredTimeMsPerSecond   → backpressure indicator
    flink.taskmanager.memory.heap.used                        → memory pressure

  Business metrics (emit from your code):
    events_processed_total (counter)
    late_events_dropped_total (counter)
    fraud_alerts_emitted_total (counter)
    processing_latency_ms (histogram — p50, p95, p99)

LOGS (what went wrong — structured events):
  Level: ERROR for failures, WARNING for degraded performance, INFO for milestones
  Format: JSON (structured, queryable in Elasticsearch/CloudWatch Logs Insights)
  Include: job_name, operator_name, partition_id, batch_id, event_time, error_type
  
  Avoid: logging every record (flooding; use sampling for DEBUG)
  Log: DLQ events (full payload + error reason), checkpoint failures, state size warnings

TRACES (end-to-end flow):
  OpenTelemetry: instrument Kafka producer/consumer with trace context
  Trace context propagated through: Kafka header → Flink operator → Kafka sink
  Visualize in: Jaeger or Zipkin
  Use for: tracing why a specific event is delayed or missing

Alerting strategy:
  P0 (page immediately):
    Consumer lag > 30 minutes (approaching retention boundary risk)
    Kafka broker down (data loss risk)
    Flink job failed + not restarted within 5 minutes
    Checkpoint failures > 3 consecutive
    
  P1 (alert on-call within 15 min):
    Consumer lag > 10 minutes (SLA at risk)
    DLQ messages > 1000/hour (spike in processing errors)
    Processing latency p99 > 2× SLA
    
  P2 (notification, no immediate action):
    State size > 80% of capacity
    Checkpoint duration > 5 minutes
    Late event rate > 10%
```

---

## Interview Tips

> **Tip 1:** "How do you choose between Flink and Spark Structured Streaming for a new project?" — Ask four questions: (1) What's the latency SLA? Flink: ~10-100ms; Spark SS: ~100ms-minutes. (2) How complex is the state? Flink: native support for complex stateful logic (timers, CEP, custom state). Spark: good for window aggregations, less flexible for arbitrary state. (3) What's the team's background? Java/Scala shop → Flink DataStream API. Python/PySpark shop → Spark SS. SQL-focused → both work (Flink SQL is excellent). (4) What's the cloud platform? Azure → Databricks (Spark SS) integrates better. AWS → KDA (Flink) or MSK + Spark on EMR. On-prem Kubernetes → Flink (Flink Kubernetes Operator). In 80% of cases, either works — the deciding factor is team expertise.

> **Tip 2:** "How do you design a streaming pipeline to be testable?" — Testing strategy with three levels: (1) Unit tests: test individual operators using Flink's `ProcessFunctionTestHarness` or mock Kafka with `EmbeddedKafkaCluster`. Inject synthetic events, advance watermarks, verify outputs. (2) Integration tests: use `MiniCluster` (Flink) or `SparkSession.builder.master("local")` with `MemoryStream`. Full end-to-end processing with synthetic data. (3) Shadow/canary tests: run new job version in parallel with production, consuming same Kafka topics, writing to shadow Delta tables. Compare outputs for 1-2 weeks before cutover. Include: late-data test (events with timestamp 30 min old), DLQ test (invalid JSON), empty-batch test (trigger with no events), burst test (1000 events at once).

> **Tip 3:** "What is the most common mistake you see in streaming architecture design?" — The most common mistake: designing the streaming job as a monolith (all logic in one Flink/Spark job, writing directly to the final sink). This creates: (a) no ability to replay individual stages (if Silver logic has a bug, you can't replay Silver without replaying Bronze); (b) all-or-nothing failures (one bug in the Gold aggregation takes down the entire pipeline from raw ingest to analytics); (c) no independent scaling (the stage that needs more CPU can't scale without scaling everything). Better pattern: decompose into small, independent jobs connected by Kafka topics or Delta Lake tables. Each job: one responsibility, one source, one or two sinks. Failure in one job doesn't affect others. Replay is per-stage. Cost scales per workload independently.

## ⚡ Cheat Sheet

**Streaming fundamentals**
```
Event time:    when the event actually occurred (on the device)
Processing time: when the system processes it (can be much later)
Ingestion time: when it arrives at the message broker
Watermark:     max expected event time lag — defines when a window closes
Late data:     events arriving after the watermark → handled by allowedLateness or drop
```

**Apache Flink key concepts**
```java
// Keyed stream + window + aggregate
stream.keyBy(event -> event.userId)
      .window(TumblingEventTimeWindows.of(Time.minutes(5)))
      .aggregate(new RevenueAggregator());

// Watermark strategy
WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withTimestampAssigner((event, ts) -> event.eventTimeMs);
```

**Spark Structured Streaming**
```python
# Read from Kafka
stream = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "orders") \
    .load()

# Window aggregation
from pyspark.sql.functions import window, col
agg = stream \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(window("event_time", "5 minutes"), "region") \
    .sum("amount")

# Write to Delta (trigger: every 1 min or micro-batch)
agg.writeStream.format("delta").trigger(processingTime="1 minute") \
    .outputMode("append").option("checkpointLocation", "/chk/orders").start()
```

**Window types**
| Window | Description | Use case |
|---|---|---|
| Tumbling | Fixed non-overlapping | Hourly totals |
| Sliding | Fixed size, moves by slide interval | 5-min avg, every 1 min |
| Session | Gap-based (closes after inactivity) | User sessions |
| Global | Accumulates all events | Running total |

**Exactly-once semantics**
```
Source: idempotent read (Kafka offset tracking)
Processing: checkpointing (Flink) or write-ahead log (Spark)
Sink: idempotent write (Delta MERGE, upsert) or transactional sink
Kafka → Flink/Spark → Delta = exactly-once end-to-end (with checkpointing)
```

**CDC streaming (Debezium → Kafka → Lakehouse)**
```
1. Debezium captures MySQL/Postgres binlog → Kafka topic (op: c/u/d/r)
2. Flink/Spark reads Kafka topic
3. MERGE INTO Delta/Iceberg table:
   INSERT on c, UPDATE on u, DELETE on d
4. Result: real-time replicated lakehouse table
```

**Kinesis key operations**
```python
import boto3
kinesis = boto3.client('kinesis', region_name='us-east-1')
# Put record
kinesis.put_record(StreamName='orders', Data=json.dumps(event).encode(), PartitionKey=order_id)
# Get shard iterator
it = kinesis.get_shard_iterator(StreamName='orders', ShardId='shardId-000000000000',
                                 ShardIteratorType='LATEST')['ShardIterator']
# Read records
records = kinesis.get_records(ShardIterator=it, Limit=100)['Records']
```

**Stateful processing patterns**
```
Running total:    keyed state (ValueState[Double])
Sessionization:   keyed + timer-based (clear state after N seconds inactivity)
Pattern detection: CEP (Flink Complex Event Processing) — detect A then B within 5 min
Deduplication:    keyed state stores seen event IDs (with TTL for cleanup)
```

**Key interview points**
- Checkpointing: Flink snapshots operator state to S3/HDFS for fault tolerance
- Backpressure: slow downstream = upstream stops reading Kafka = natural flow control
- Parallelism = Kafka partitions: each Flink/Spark task reads one partition
- Streaming vs micro-batch: Flink = true streaming (event-by-event); Spark = micro-batch (more latency, simpler)
