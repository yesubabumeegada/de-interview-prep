---
title: "Pipeline Design Patterns — Senior Deep Dive"
topic: system-design
subtopic: pipeline-design-patterns
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [system-design, pipeline, event-driven, saga, exactly-once, schema-evolution]
---

# Pipeline Design Patterns — Senior Deep Dive

## Exactly-Once Semantics

A pipeline processes each event exactly once — no duplicates, no losses. The three delivery guarantees:

```
At-most-once:   may lose data; never duplicates  (fire and forget)
At-least-once:  may duplicate; never loses data  (retry on failure)
Exactly-once:   no loss, no duplicate            (hardest to achieve)

Exactly-once requires:
1. Idempotent writes (same input → same output, safe to retry)
2. Transactional offset commits (read + write + commit atomically)

Kafka exactly-once (EOS):
  - Producer: enable.idempotence=true + transactional.id
  - Consumer+writer: read → process → write → commit in ONE transaction
  - Only works between Kafka → Kafka or Kafka → transactional store

Spark Structured Streaming + Delta Lake (EOS):
  - Delta Lake's ACID transactions make offset + data write atomic
  - Checkpoint: Spark writes offsets AND data atomically to Delta
  - On replay: deduplication via transaction log ensures exactly-once
```

```python
# Spark Structured Streaming + Delta (exactly-once end-to-end)
query = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe", "orders")
        .load()
    .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", "s3://bucket/checkpoints/orders")
        .trigger(processingTime="1 minute")
        .start("s3://bucket/delta/orders")
)
# Delta's transaction log records: "processed offsets 0-999 → written 1000 rows"
# On crash-restart: offsets 0-999 already committed → skip; resume from 1000
```

---

## Schema Evolution Strategies

```
Strategy 1: Schema Registry (Kafka / Avro)
  - Schemas versioned and stored centrally
  - Producers register schema before publishing
  - Consumers pull schema by ID from registry
  - Compatibility modes:
    BACKWARD: new schema can read old data (add fields with defaults)
    FORWARD:  old schema can read new data (consumers tolerate new fields)
    FULL:     both directions (safest)

Strategy 2: Schema-on-Read (Data Lake)
  - Store raw bytes (JSON/Parquet)
  - Apply schema at read time
  - Flexible but can break downstream transforms silently

Strategy 3: Explicit versioning
  - topic.v1, topic.v2 — separate topics per schema version
  - Consumers migrate independently to new version
  - Clean separation; operational overhead of running both

Strategy 4: Delta Lake / Iceberg schema evolution
  - ALTER TABLE ADD COLUMN / RENAME COLUMN without rewriting data
  - Old files still valid (column reads as NULL for old partitions)
  - New files include new column
  - Zero downtime schema changes
```

---

## Event-Driven Pipeline Architecture

```
Traditional polling: pipeline polls source every N minutes — wasteful and adds latency

Event-driven: source emits an event → pipeline triggers immediately

Components:
  Producers: services emit events to Kafka/SNS/EventBridge
  Router: topic/queue routes events to correct consumers
  Consumers: pipelines triggered by events, process and emit new events

Benefits:
  - Low latency (seconds vs minutes)
  - Decoupled: producers don't know consumers
  - Backpressure: consumers process at their own rate
  - Replayability: Kafka retains events (configurable retention)

Example flow:
  checkout-service → orders.created (Kafka)
    → inventory-pipeline: update stock levels
    → analytics-pipeline: update metrics
    → notification-service: send confirmation email
  (all 3 consume independently from the same topic)
```

---

## Saga Pattern for Distributed Data Pipelines

When a pipeline spans multiple services/databases, maintaining consistency without distributed transactions:

```
Saga = sequence of local transactions, each publishing an event for the next step
On failure: compensating transactions undo previous steps

Example: Order processing saga
  1. Reserve inventory    → publish "inventory_reserved"
  2. Charge payment       → publish "payment_charged"
  3. Create fulfillment   → publish "fulfillment_created"
  4. Commit order         → done

Failure handling:
  If step 3 fails: publish "fulfillment_failed"
    → compensation: refund payment → release inventory

Choreography saga (no central coordinator):
  Each service listens for events and emits events — decentralized
  Pro: simple, no SPOF
  Con: hard to trace; complex failure flows

Orchestration saga (central coordinator = Airflow/Step Functions):
  A workflow engine calls each service in sequence
  Tracks state centrally; handles compensations
  Pro: easy to monitor, debug, retry
  Con: orchestrator becomes SPOF
```

---

## Pipeline Observability

```python
# Instrument every pipeline with:
# 1. Row count assertions between stages
# 2. Null rate checks on critical columns
# 3. Freshness monitoring
# 4. Processing latency tracking

import time
from dataclasses import dataclass

@dataclass
class PipelineMetrics:
    pipeline: str
    stage: str
    rows_in: int
    rows_out: int
    null_rate: float
    duration_s: float
    execution_date: str

def emit_metrics(m: PipelineMetrics):
    # Push to Prometheus, Datadog, CloudWatch, etc.
    metrics.gauge(f"pipeline.{m.pipeline}.{m.stage}.rows_out", m.rows_out)
    metrics.gauge(f"pipeline.{m.pipeline}.{m.stage}.null_rate", m.null_rate)
    metrics.gauge(f"pipeline.{m.pipeline}.{m.stage}.duration_s", m.duration_s)

    if m.null_rate > 0.01:  # >1% nulls: alert
        alert(f"High null rate in {m.pipeline}/{m.stage}: {m.null_rate:.1%}")
    if m.rows_out == 0:
        alert(f"Zero rows output in {m.pipeline}/{m.stage} for {m.execution_date}")

# Row count lineage: track rows through each transformation
def transform_with_metrics(df, pipeline, stage, execution_date):
    rows_in = df.count()
    t0 = time.time()
    result = apply_transform(df)
    duration = time.time() - t0
    rows_out = result.count()

    critical_cols = ["order_id", "amount_usd", "customer_id"]
    null_rate = result.filter(
        any(col(c).isNull() for c in critical_cols)
    ).count() / max(rows_out, 1)

    emit_metrics(PipelineMetrics(pipeline, stage, rows_in, rows_out, null_rate, duration, execution_date))
    return result
```

---

## Interview Tips

> **Tip 1:** "How do you achieve exactly-once processing?" — Combine idempotent writes with transactional offset commits. In Kafka → Delta Lake: Spark Structured Streaming writes offsets + data atomically using Delta's transaction log. If the job restarts mid-batch, it replays from the last committed checkpoint and overwrites the partial write — no duplicates. True end-to-end exactly-once also requires the sink to support idempotency (Delta, BigQuery, Snowflake support this; basic HDFS does not).

> **Tip 2:** "How would you design a pipeline that handles schema changes from the source?" — Use Avro + Schema Registry for Kafka pipelines. Set compatibility mode to BACKWARD so consumers can read old messages when new fields are added. For data lake storage, use Delta Lake or Iceberg — both support column additions without rewriting files. Add schema drift detection in the pipeline (compare incoming schema to registered schema; alert on unexpected changes before propagating them downstream).

> **Tip 3:** "How do you monitor data pipelines in production?" — Four pillars: (1) Freshness — is data arriving on time? Alert if a table hasn't been updated in >25 hours. (2) Volume — is row count within expected range? Alert on >20% deviation from the 7-day average. (3) Quality — are null rates, duplicate rates, referential integrity checks passing? (4) Latency — processing time per stage. Use Airflow/Prefect for orchestration monitoring; Great Expectations/Soda for quality; Prometheus/Grafana or Datadog for pipeline metrics.

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
