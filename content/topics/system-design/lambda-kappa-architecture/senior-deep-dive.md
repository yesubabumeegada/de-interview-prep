---
title: "Lambda & Kappa Architecture — Senior Deep Dive"
topic: system-design
subtopic: lambda-kappa-architecture
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [system-design, lambda-architecture, kappa-architecture, lakehouse, delta-lake, streaming]
---

# Lambda & Kappa Architecture — Senior Deep Dive

## Lambda Architecture: When It Still Makes Sense

```
Lambda is often dismissed as "legacy" but has valid use cases:

Use Lambda when:
  1. ML model training requires full historical data:
     - Training job: batch over all historical events (Spark on S3)
     - Inference serving: streaming job (low latency predictions)
     - These are inherently different: no unification possible

  2. Complex aggregations that are cheaper in batch:
     - Exact distinct counts over 10 billion events → HyperLogLog in streaming,
       exact in batch; business requires exact → batch wins

  3. Regulatory recomputation:
     - Tax/financial reports must be recomputed from authoritative source
     - Speed layer for ops; batch layer for official reports

  4. Legacy infrastructure migration:
     - Existing Hadoop/Hive batch system + new Kafka streaming layer
     - Can't migrate everything at once; Lambda bridges the gap

Why Lambda became unpopular:
  - Dual codebase maintenance (batch SQL ≠ streaming DSL)
  - Spark can do both streaming AND batch (same API)
  - Delta Lake / Iceberg enable streaming writes → batch reads
  - Cloud storage is cheap: no need to delete the "speed layer" after batch
```

---

## Lakehouse as the Modern Synthesis

```
Lakehouse pattern solves Lambda's dual codebase problem:

Traditional Lambda:
  Batch:    Hive SQL  → Hadoop HDFS views
  Streaming: Kafka Streams → Redis/Druid speed views
  Serving:  custom merge logic

Lakehouse (Lambda without the complexity):
  All writes → Delta Lake / Iceberg table (ACID, time-travel)
  Streaming: Spark Structured Streaming → Delta (micro-batch, every 30s)
  Batch:     Spark batch → same Delta table (reprocess historical)
  Serving:   BI tools query Delta directly (no merge logic!)
  Time-travel: SELECT * FROM orders TIMESTAMP AS OF '2024-01-15 14:00'

How Delta Lake enables this:
  - ACID transactions: streaming write + batch read from same table are consistent
  - Transaction log (_delta_log): tracks every change (insert/update/delete)
  - Time-travel: any previous version of the table is queryable via log
  - Schema evolution: columns added without breaking readers
  - Compaction (OPTIMIZE): merge small streaming files into large batch-friendly files
```

---

## Event Sourcing + CQRS for Data Engineering

```
Event Sourcing: store events (facts), not current state
  Traditional: UPDATE orders SET status = 'SHIPPED' WHERE id = 1
  Event Sourcing: INSERT INTO order_events (type='SHIPPED', order_id=1, ts=now)
  
  Reconstruct state: SELECT * FROM order_events WHERE order_id=1 ORDER BY ts
  → replay events to get current + any historical state

CQRS (Command Query Responsibility Segregation):
  Write path (Command): validate → produce event → Kafka
  Read path (Query): materialized views updated by consuming events

Applied to DE architecture:
  Write:  service events → Kafka → raw event store (append-only Bronze)
  Read:   stream processing → materialize views per query pattern
    View A: current order status (keyed state, compacted Kafka topic)
    View B: daily revenue aggregation (windowed aggregation → Delta)
    View C: customer 360 (joined materialized view → Snowflake)
  
  Benefit: each read view optimized for its query pattern
  No single model forced to serve all needs
  
  Reprocessing = replay events → rebuild any view from scratch
  Source of truth = event log (Kafka/S3) not the materialized views
```

---

## Streaming SQL Maturity (Flink SQL vs Spark SQL)

```sql
-- Modern stream processing: write SQL, not code

-- Flink SQL example: streaming revenue aggregation
CREATE TABLE orders (
  order_id   BIGINT,
  customer_id VARCHAR(50),
  region     VARCHAR(20),
  amount     DECIMAL(10,2),
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'format' = 'json'
);

CREATE TABLE hourly_revenue (
  window_start TIMESTAMP,
  window_end   TIMESTAMP,
  region       VARCHAR(20),
  total_revenue DECIMAL(12,2)
) WITH (
  'connector' = 'delta',
  'path' = 's3://bucket/delta/hourly_revenue'
);

-- Streaming SQL: same syntax as batch SQL!
INSERT INTO hourly_revenue
SELECT
  TUMBLE_START(event_time, INTERVAL '1' HOUR),
  TUMBLE_END(event_time, INTERVAL '1' HOUR),
  region,
  SUM(amount)
FROM orders
GROUP BY TUMBLE(event_time, INTERVAL '1' HOUR), region;

-- Flink SQL vs Spark SQL streaming comparison:
-- Flink: true streaming (processes event-by-event), lower latency, more SQL coverage
-- Spark: micro-batch (processes in batches), easier to debug, better integration with Delta
-- Both: support windowing, watermarks, UPSERT/MERGE to Delta/Iceberg
```

---

## Interview Tips

> **Tip 1:** "How does the Lakehouse pattern improve on Lambda Architecture?" — Lambda requires two separate processing systems (batch + streaming) with duplicate business logic. The Lakehouse uses one system (e.g., Spark) writing to one storage layer (Delta Lake / Iceberg). Delta's ACID transactions allow streaming writes and batch reads from the same table. Reprocessing is just running a batch Spark job that overwrites specific partitions. No serving layer merge needed. Same SQL works for historical backfill and real-time streaming.

> **Tip 2:** "What is event sourcing and how does it relate to data engineering?" — Event sourcing stores all state changes as immutable events rather than overwriting current state. The event log (Kafka/S3) is the source of truth; current state is derived by replaying events. For DE: this means the raw event log in Bronze is authoritative. Any downstream table (Silver, Gold) can be rebuilt by replaying events. This is exactly the Kappa architecture principle: streaming is just event sourcing with materialized views.

> **Tip 3:** "When would you choose Flink over Spark Structured Streaming?" — Flink for: true event-at-a-time processing (latency < 100ms), complex event processing (CEP patterns), Kafka exactly-once with transactional sinks, fine-grained state management (RocksDB backend). Spark for: teams already know Spark SQL, tight integration with Delta Lake and the broader Spark ecosystem, ML pipelines (MLlib co-location), larger community and more managed offerings (Databricks). For most enterprise DE workloads > 1-second latency tolerance: both work; Spark is simpler operationally.

## ⚡ Cheat Sheet

**System design framework (DE interviews)**
```
1. Clarify requirements: batch or streaming? latency SLA? scale (rows/day)?
2. Define data flow: source → ingest → transform → serve → consume
3. Choose storage: DW (structured), Data Lake (raw), Lakehouse (both)
4. Choose compute: Spark/Flink for scale; dbt for SQL transforms; Airflow for orchestration
5. Define SLAs: freshness (15 min? 1 hr?), uptime (99.9%?), cost budget
6. Address failure modes: what breaks? how do you detect and recover?
```

**Lambda vs Kappa architecture**
```
Lambda:
  Batch layer:  reprocesses all historical data on a schedule (accurate)
  Speed layer:  processes recent data in real-time (approximate)
  Serving:      merges batch + speed views for queries
  Problem:      two codebases for same logic; complex to maintain

Kappa:
  Streaming only:  one pipeline handles both real-time and reprocessing
  Reprocessing:    replay Kafka from beginning with new consumer group
  Advantage:       single codebase; simpler ops
  Requirement:     Kafka retention must cover reprocessing window
```

**Scalability patterns**
```
Horizontal partitioning:  Kafka partitions, HDFS blocks, table partitions
Data skipping:            Z-ordering, bloom filters, min/max statistics
Push down:                predicates + projections to storage layer
Caching:                  result cache (Snowflake, Databricks SQL), Redis for lookups
Async processing:         decouple ingestion from transformation via message queue
```

**Fault tolerance patterns**
```
Idempotency:     safe to re-run; same output for same input
Checkpointing:   Flink/Spark saves progress; restart from last checkpoint
Dead letter:     failed records go to DLQ for inspection and replay
Circuit breaker: stop pipeline on repeated failures; alert before resuming
Retry with backoff: exponential backoff + jitter for transient failures
Exactly-once:    Kafka + Flink + Delta = end-to-end exactly-once
```

**Cost optimization levers**
```
Compute:
  - Spot/preemptible instances (60-80% cheaper; need checkpointing)
  - Auto-suspend warehouses (pay only when active)
  - Right-size: XL warehouse for batch; S for ad hoc
Storage:
  - Partition + vacuum old snapshots
  - Lifecycle policies: S3 IA after 30 days, Glacier after 1 year
  - Compression: ZSTD > Snappy (better ratio, acceptable CPU cost)
Query:
  - Columnar reads (never SELECT *)
  - Materialized views for expensive repeated aggregations
  - Result cache (Snowflake caches identical queries for 24h)
```

**Data warehouse design checklist**
```
□ Star schema with conformed dimensions
□ Surrogate keys on all dimensions
□ Fact table: numeric measures + FK references only
□ SCD2 on slowly changing dimensions
□ Partition on query predicate (date, region)
□ Cluster/Z-order on high-cardinality filter columns
□ Row counts + DQ checks at each medallion layer boundary
□ Freshness SLA defined and monitored for each gold table
□ Data lineage captured (dbt docs, OpenLineage)
□ Access control: role-based + column masking for PII
```

**Trade-off framework**
```
Latency vs throughput:    streaming (low latency, lower throughput) vs batch (high throughput, higher latency)
Consistency vs availability: strong consistency (slower, single writer) vs eventual (faster, multi-write)
Cost vs freshness:        real-time = expensive compute; hourly batch = cheap; choose based on business SLA
Simplicity vs flexibility: managed service (easy ops) vs self-managed (full control, higher ops burden)
Storage vs compute:       pre-aggregate (storage cost, fast queries) vs compute on demand (fresh data, slower)
```
