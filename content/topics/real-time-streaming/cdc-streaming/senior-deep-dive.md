---
title: "CDC Streaming (Debezium) — Senior Deep Dive"
topic: real-time-streaming
subtopic: cdc-streaming
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [cdc, debezium, kafka, exactly-once, distributed-transactions, schema-registry, production]
---

# CDC Streaming (Debezium) — Senior Deep Dive

## CDC Exactly-Once Semantics

```
Debezium delivery guarantee: AT-LEAST-ONCE

Failure scenarios:
  1. Connector publishes to Kafka but crashes before committing offset
     → On restart: re-reads same binlog position → duplicate events in Kafka
     
  2. Kafka producer retries: network error → producer retries → duplicate message
  
  3. Connector crash mid-batch: some events published, offset not committed
     → On restart: entire batch re-published → duplicates

Making CDC pipelines effectively exactly-once:

Option 1: Idempotent consumer (most practical)
  Consumer writes using UPSERT with natural primary key:
    INSERT INTO target (order_id, status, amount, updated_at)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (order_id) DO UPDATE
    SET status = EXCLUDED.status,
        amount = EXCLUDED.amount,
        updated_at = EXCLUDED.updated_at
    WHERE EXCLUDED.updated_at > target.updated_at  -- only update if newer
  
  Duplicate INSERT events: UPSERT → no change (idempotent)
  Duplicate UPDATE events: UPSERT → same result (idempotent)
  Duplicate DELETE events: second delete is a no-op (idempotent)
  
  Key: include ts_ms (event timestamp from binlog) in the WHERE clause
       to prevent older CDC events from overwriting newer ones

Option 2: Binlog position deduplication
  Track last-seen (binlog_file, binlog_pos) per connector in external store
  On receive: if pos <= last_seen → skip (duplicate)
  
  Problem: DynamoDB round-trip per event (latency overhead)
  Use case: when UPSERT is not available (append-only sinks)

Option 3: Kafka transactions + 2PC
  Producer: Debezium → Kafka (transactional, exactly-once to Kafka)
  Configure: enable.idempotence=true, acks=all, transactional.id=unique-per-connector
  Consumer: Kafka → Flink (exactly-once via Flink checkpointing + Flink-Kafka 2PC)
  Sink: Flink → Iceberg (2PC via Iceberg commit)
  
  Result: exactly-once from MySQL binlog → Iceberg table
  Cost: ~20% throughput reduction (2PC overhead)
  Use case: financial data where duplicate records cause billing errors

Debezium configuration for exactly-once to Kafka:
  "producer.override.enable.idempotence": "true"
  "producer.override.acks": "all"
  "producer.override.max.in.flight.requests.per.connection": "1"
```

---

## Advanced Debezium Transformations (SMTs)

```json
// Single Message Transforms (SMTs): transform events in-flight within Kafka Connect

// 1. Route based on operation: only forward inserts and updates, drop deletes
{
  "transforms": "filterDeletes",
  "transforms.filterDeletes.type": "io.debezium.transforms.Filter",
  "transforms.filterDeletes.condition": "value.op != 'd'"
}

// 2. Extract only the 'after' field (for append-only sinks that don't need before)
{
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.drop.tombstones": "false",
  "transforms.unwrap.delete.handling.mode": "rewrite",  // include __deleted field
  "transforms.unwrap.add.fields": "op,table,ts_ms",      // add metadata fields
  "transforms.unwrap.add.headers": "op"
}
// After unwrap: message = flat row (no before/after nesting)
// {"order_id": 1, "status": "shipped", "amount": 100.0, "__op": "u", "__ts_ms": 123456}

// 3. Mask sensitive fields (PII) before publishing to Kafka
{
  "transforms": "maskPII",
  "transforms.maskPII.type": "org.apache.kafka.connect.transforms.MaskField$Value",
  "transforms.maskPII.fields": "email,ssn,phone_number",
  "transforms.maskPII.replacement": "***REDACTED***"
}

// 4. Add partition by date for time-series data
{
  "transforms": "addPartition",
  "transforms.addPartition.type": "org.apache.kafka.connect.transforms.InsertField$Value",
  "transforms.addPartition.static.field": "kafka_ingested_at",
  "transforms.addPartition.static.value": "${timestamp}"
}

// 5. Route to different topics by table
{
  "transforms": "router",
  "transforms.router.type": "io.debezium.transforms.ByLogicalTableRouter",
  "transforms.router.topic.regex": "([^.]+)\\.([^.]+)\\.([^.]+)",
  "transforms.router.topic.replacement": "cdc-$3"  // → topic = "cdc-orders", "cdc-customers"
}
```

---

## Multi-Database CDC Architecture

```
Production pattern: capture changes from multiple source databases

  MySQL (orders)      → Debezium MySQL connector → kafka: myserver.ecommerce.*
  PostgreSQL (users)  → Debezium PG connector   → kafka: pg.users.*
  MongoDB (catalog)   → Debezium Mongo connector → kafka: mongo.catalog.*
  SQL Server (finance)→ Debezium MSSQL connector → kafka: mssql.finance.*

  All changes → Kafka → consumers:
    Flink (real-time analytics)
    Spark Structured Streaming (bronze → silver medallion)
    Elasticsearch (full-text search sync)
    Data Warehouse (Snowflake, Redshift, BigQuery)

Kafka Connect cluster sizing for multi-database CDC:
  Workers: 3 Kafka Connect workers (HA)
  Each connector: 1 task (CDC connectors are single-threaded)
  4 connectors × 1 task = 4 tasks across 3 workers (distributed automatically)
  
  Memory: 2 GB per worker (connector overhead + Kafka producer buffers)
  CPU: low (I/O bound: reading DB logs, writing to Kafka)
  
  Monitoring:
    connector-status = RUNNING (not FAILED or PAUSED)
    kafka.consumer.records-lag = how far behind the connector is
    source-record-write-rate = events/sec being published
    
  Alert: connector status != RUNNING → immediate page

Schema Registry: single registry for all CDC events
  One schema per (topic, subject) = (connector.db.table, connector.db.table-value)
  All consumers use same schema registry → consistent deserialization
  Schema compatibility policy: FORWARD (new fields added with defaults)

Topic naming convention:
  {env}.{database}.{table}
  prod.ecommerce.orders
  prod.users.accounts
  Benefit: easy filtering (subscribe to prod.ecommerce.* for all ecommerce tables)
```

---

## Debezium vs Native SQL CDC

```
Compare Debezium (Kafka Connect) vs native CDC:

MySQL:      Debezium binlog reader  vs  AWS DMS (managed, to RDS/Redshift)
PostgreSQL: Debezium WAL            vs  pglogical, Postgres logical replication
SQL Server: Debezium log reader     vs  SQL Server CDC (built-in, for T-SQL consumers)

Debezium advantages:
  - Events in Kafka: durable, replayable, multiple consumers
  - Unified format: same event structure regardless of DB type
  - Rich ecosystem: SMTs, Schema Registry, Kafka Connect plugins
  - Open source, community support, battle-tested at scale

Debezium disadvantages:
  - Operational complexity: Kafka Connect cluster management
  - Additional latency: DB → Debezium → Kafka → consumer (vs direct DB read)
  - Replication slot bloat (PostgreSQL): must monitor and manage

Native CDC alternatives:
  AWS DMS (Database Migration Service):
    - Fully managed, minimal setup
    - Target: Aurora, RDS, Redshift, S3
    - Limitation: delivers to 1 target (no fan-out like Kafka)
    - Use: simple replication to one AWS service, teams without Kafka expertise
  
  Airbyte / Fivetran:
    - SaaS CDC platforms, fully managed
    - 300+ connectors (not just databases)
    - Limitation: not for real-time streaming (near-real-time, typically 5-30 min)
    - Use: analytics pipelines where < 1 hour latency is acceptable

Decision framework:
  Need < 1 second latency + multiple consumers? → Debezium + Kafka
  Need < 30 minutes latency + managed service? → Airbyte/Fivetran
  Single AWS target, fully managed? → AWS DMS
  On-prem, SQL Server CDC only? → SQL Server built-in CDC (sp_cdc_enable_table)
```

---

## Interview Tips

> **Tip 1:** "How do you handle a large table (1 billion rows) initial snapshot without impacting production?" — Large table snapshot strategies: (a) Point Debezium at a read replica (not primary) for the initial snapshot. Configure `database.hostname` to replica address. After snapshot completes, the connector naturally switches to reading the primary's binlog. This eliminates all snapshot load from production; (b) Use `snapshot.select.statement.overrides` to add `LIMIT`/pagination for chunk-by-chunk snapshot (Debezium 1.9+: chunked snapshots); (c) Manual pre-load: load historical data to the target using bulk export (mysqldump + LOAD DATA), then configure Debezium with `snapshot.mode=schema_only` to skip the snapshot and start streaming from the current binlog position. The target has historical data from the bulk load + streaming changes from Debezium.

> **Tip 2:** "What is the role of Kafka topic compaction in CDC pipelines, and should you enable it?" — Log compaction keeps only the latest message per key (same as the latest state of each row). For CDC topics: enables downstream consumers to rebuild the full table state by replaying only the compacted topic (one message per primary key = current state). Enables: restore a new downstream system from a compacted topic (no need for full snapshot). Trade-offs: compacted topics don't preserve history (only latest state), so point-in-time analysis of "all changes to order 123" won't work. Recommendation: maintain two topics: (a) change-log topic (no compaction, 7-day retention): for streaming consumers that need all changes; (b) snapshot topic (compacted, indefinite retention): for bootstrapping new consumers. Debezium's Outbox Router can write to both.

> **Tip 3:** "How do you ensure CDC events are processed in order when there are multiple consumers and multiple partitions?" — Within a single Kafka partition: order is guaranteed (Debezium publishes changes for the same row to the same partition via primary key as partition key). Across partitions: no ordering guarantee. For ordering per row: ensure all changes to the same row go to the same partition (Debezium uses the row's primary key as the Kafka message key → same key always → same partition via consistent hashing). Consumer side: process partitions independently (each partition's consumer handles one shard of rows). For global ordering across all rows: not feasible at scale without a single-partition topic (throughput bottleneck). In practice: ordering per row is sufficient for UPSERT patterns — each message has the full after-state, so processing order within a row doesn't matter as long as you use `WHERE updated_at > target.updated_at`.

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
