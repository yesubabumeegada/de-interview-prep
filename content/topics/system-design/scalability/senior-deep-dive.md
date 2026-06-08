---
title: "Scalability & Partitioning — Senior Deep Dive"
topic: system-design
subtopic: scalability
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [system-design, scalability, cap-theorem, hot-partition, backpressure, tiered-storage]
---

# Scalability & Partitioning — Senior Deep Dive

## CAP Theorem for Data Engineers

Every distributed system can only guarantee two of three properties:

```
C — Consistency:    All reads see the latest write
A — Availability:   Every request gets a response (even if not latest data)
P — Partition tolerance: System works even if network partitions occur

Network partitions ALWAYS happen in real systems → must choose CP or AP:

CP (Consistency + Partition tolerance):
  - System rejects reads/writes during partition (error rather than stale data)
  - Examples: Zookeeper, HBase, etcd, MongoDB (with majority write concern)
  - Use for: leader election, distributed locks, configuration management

AP (Availability + Partition tolerance):
  - System returns best-effort data during partition (may be stale)
  - Examples: Cassandra, DynamoDB, CouchDB
  - Use for: user profiles, shopping carts, session data, analytics

CA (Consistency + Availability):
  - Only possible in single-node systems (no partition possible)
  - Examples: traditional RDBMS (PostgreSQL, MySQL) on single node
  - Not relevant for distributed systems

Practical implication for DE:
  - Kafka: AP (brokers serve stale data during partition)
  - Delta Lake / ACID DW: CP (writes blocked during network issues)
  - Cassandra: AP with tunable consistency (QUORUM = more CP, ONE = more AP)
```

---

## Hot Partition Problem and Solutions

A hot partition = one partition receives disproportionately more writes/reads:

```python
# Detection
# Kafka: check partition lag per partition-consumer pair
# kafka-consumer-groups.sh --describe --group my-group
# Partition 0: LAG=0, Partition 1: LAG=0, Partition 2: LAG=2,000,000 ← HOT

# Common hot partition causes:
# 1. Bad partition key (e.g., ALL orders for user_id=1 go to partition 0)
# 2. Celebrity problem: one key has 1000× more events than average
# 3. Time-based key: all writes go to "today" partition

# Fix 1: Compound key (add suffix)
# Instead of key=user_id, use key=user_id + "_" + (ts % 10)
# Spreads one user across 10 partitions; aggregate at consumer side

# Fix 2: Random routing for write-heavy keys
def get_partition_key(user_id: str, event: dict) -> str:
    if is_celebrity_user(user_id):  # pre-computed hot user list
        return f"{user_id}_{random.randint(0, 9)}"  # shard across 10 partitions
    return user_id

# Fix 3: Two-phase aggregation
# Phase 1: partial aggregation per partition (local count)
# Phase 2: merge partial results (global count)
# Avoids sending all data for one key to single reducer

# Fix 4: Delta Lake data skipping for hot regions
# Add a bloom filter on the hot column
spark.sql("""
    ALTER TABLE orders SET TBLPROPERTIES (
        'delta.dataSkippingNumIndexedCols' = '5',
        'delta.bloomFilter.columns' = 'user_id',
        'delta.bloomFilter.fpp' = '0.1'
    )
""")
```

---

## Backpressure and Flow Control

When producers generate data faster than consumers can process it:

```
Without backpressure:
  Producer → [queue fills up] → [queue overflow] → data loss or OOM

Backpressure mechanisms:
  1. Blocking queues: producer blocks when queue full (TCP-style flow control)
     - Simple; works in single-process
     - Risk: deadlock if producer and consumer share thread pool

  2. Reactive backpressure (Reactive Streams / Project Reactor):
     Consumer signals to producer: "I can handle N more items"
     Producer slows down to match consumer rate
     Used by: Akka Streams, Spring WebFlux, RxJava

  3. Kafka: natural backpressure
     - Consumers pull at their own rate (Kafka is a pull-based system)
     - No push pressure; consumer just processes when ready
     - Risk: growing consumer lag (data is safe but processing falls behind)

  4. Spark Structured Streaming rate limiting:
     .option("maxOffsetsPerTrigger", "100000")  # process max 100K events per batch
     Prevents one slow run from cascading into OOM

  5. AWS SQS message visibility timeout:
     If consumer doesn't ack within timeout, message becomes visible again
     Natural retry + backpressure (slow consumer = message stays in queue)

Monitoring backpressure:
  Kafka consumer lag: > 1M events = consumer falling behind; scale consumers
  Spark: inputRowsPerSecond vs processedRowsPerSecond (in StreamingQuery.lastProgress)
  Queue depth: CloudWatch SQS NumberOfMessageVisible metric
```

---

## Tiered Storage for Scale

```
Tiered storage: keep hot data on fast/expensive storage; move cold data to cheap storage

Cloud data lake tiers:
  Hot:   S3 Standard        / GCS Standard      — frequent access, $0.023/GB
  Warm:  S3 Standard-IA     / GCS Nearline       — access < monthly, $0.0125/GB
  Cold:  S3 Glacier Instant / GCS Coldline       — access < quarterly, $0.004/GB
  Archive: S3 Glacier Deep  / GCS Archive        — access < yearly, $0.00099/GB

Lifecycle policy (S3 example):
  - 0 days:  S3 Standard (hot queries, last 30 days)
  - 30 days: transition to S3 Standard-IA
  - 90 days: transition to S3 Glacier Instant Retrieval
  - 365 days: transition to S3 Glacier Deep Archive

Kafka tiered storage (2.8+ / Confluent):
  - Hot: local broker disk (last N hours, fast consumer lag catch-up)
  - Cold: S3/GCS (longer retention at low cost, slightly higher latency)
  - Transparent: consumers don't change; broker fetches from S3 when needed

ClickHouse tiered storage:
  - Hot: NVMe SSD (recent data, frequent queries)
  - Cold: HDD or S3 (historical data, rare queries)
  - Config: TTL policy moves data to cold tier after N days
```

---

## Interview Tips

> **Tip 1:** "How would you scale a system from 1TB to 100TB of daily data?" — Step 1: move from single-node to distributed compute (Spark instead of pandas). Step 2: partition data by query pattern (date + region) to enable partition pruning. Step 3: use columnar storage (Parquet/Delta) with compression. Step 4: introduce tiered storage — keep only 90 days hot, archive older data. Step 5: separate compute from storage (cloud data lake + Snowflake/Databricks). Step 6: add caching layer for hot query patterns (materialized views, Redis for lookup tables).

> **Tip 2:** "What happens when a Kafka consumer group falls behind?" — Consumer lag grows. This is not data loss (Kafka retains messages per retention policy). Resolution: (1) check if consumers are slow (CPU, GC, downstream bottleneck) or if producer volume spiked, (2) scale consumer instances (up to # of partitions), (3) increase consumer batch size for throughput. If lag is unacceptable: set `maxOffsetsPerTrigger` in Spark to limit catch-up speed and prevent OOM during burst processing.

> **Tip 3:** "How do you handle a 10TB table that needs to be rebuilt from scratch?" — Use parallel partition rebuild: instead of one job reprocessing the full table, split by partition (e.g., by month) and run N parallel Spark jobs, each rebuilding one month. Use swap pattern: write to a temp table, validate, then atomic rename. On Snowflake/BigQuery: `CREATE TABLE new_orders AS SELECT ... FROM source` is fully parallel (DW handles it). On S3/Delta: write to a temp prefix, validate row counts, then rename atomically.
