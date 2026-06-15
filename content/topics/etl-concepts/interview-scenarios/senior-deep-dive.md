---
title: "ETL Interview Questions: Full System Design, Fault Tolerance, and Exactly-Once Semantics"
description: "Senior ETL interview walkthrough — full ingestion-to-serving system design, failure handling, replay, and exactly-once delivery semantics."
content_type: study_material
topic: etl-concepts
subtopic: interview-scenarios
layer: senior-deep-dive
difficulty_level: senior
tags: [system-design, exactly-once, fault-tolerance, replay, ingestion, ETL-architecture, senior-interview]
---

# ETL Interview Questions: Senior Deep Dive

## What Interviewers Expect at the Senior Level

For senior DE roles (5+ years), ETL interview questions are less about "what is X" and more about:

- **System design:** "Design a pipeline to ingest 10M events/day from 50 microservices"
- **Trade-off reasoning:** "Why would you choose Flink over Spark Streaming here?"
- **Failure mode analysis:** "What happens if this step fails? How does the system recover?"
- **Exactly-once guarantees:** "How do you prevent double-counting in your aggregation pipeline?"
- **Scaling:** "This worked at 1M/day — how does your design change at 1B/day?"

---

## Full ETL System Design Walkthrough

### Prompt

"Design an ETL system to ingest order events from 20 microservices, transform them into a unified orders fact table, and serve them to both a real-time fraud scoring system and a daily analytics dashboard. Handle 50M events per day, with a freshness requirement of 5 minutes for fraud and 2 hours for analytics."

### How to Structure Your Answer

Senior interviewers want you to walk through the design methodically. Use this structure:

1. **Clarify requirements** (2 minutes)
2. **Sketch the high-level architecture** (3 minutes)
3. **Deep-dive each layer** (10 minutes)
4. **Address failure modes** (3 minutes)
5. **Discuss trade-offs and alternatives** (2 minutes)

---

### Step 1: Clarify Requirements

Ask before designing:
- Event schema: Is there a common schema across microservices, or does each have its own?
- Delivery guarantees: At-least-once or exactly-once from the microservices?
- Duplicate events: Can the same order_id arrive multiple times?
- Deletes: Do we need to handle order cancellations (hard deletes)?
- Compliance: Are there PII fields that need masking?
- Reprocessing: How far back do we need to replay if a bug is found?

---

### Step 2: High-Level Architecture

```
┌────────────────────────────────────────────────────────────────────────┐
│  INGESTION                                                             │
│  20 Microservices → Kafka (partitioned by order_id)                   │
│  Schema Registry (Avro, backward compatible)                           │
└────────────────────┬───────────────────────────────────────────────────┘
                     │
         ┌───────────┴───────────┐
         ▼                       ▼
┌────────────────┐      ┌────────────────────────────────┐
│  SPEED LAYER   │      │  BATCH LAYER                   │
│  (Flink)       │      │  (Spark on Databricks)         │
│  5-min window  │      │  Hourly micro-batch             │
│  aggregates    │      │  Full dedup + schema enforcement│
└────────┬───────┘      └──────────────┬─────────────────┘
         │                             │
         ▼                             ▼
┌────────────────┐      ┌────────────────────────────────┐
│  Feature Store │      │  Delta Lake (Bronze/Silver/Gold)│
│  (Redis)       │      │  orders_fact (Gold)             │
│  Fraud scoring │      │  Analytics dashboard            │
└────────────────┘      └────────────────────────────────┘
```

---

### Step 3: Layer-by-Layer Design

#### Ingestion Layer: Kafka

```
Kafka Topics:
  orders.raw         — all microservices publish here
  orders.dlq         — failed/undeserializable events
  orders.corrections — late correction events (cancellations, refunds)

Kafka Configuration:
  Partitions: 100 (parallelism for 50M events/day ≈ 580/sec peak)
  Replication factor: 3
  Retention: 7 days (allows replay up to 7 days)
  Compression: Snappy (30-50% size reduction)
  Max message size: 1MB

Partition key: order_id
  → All events for the same order go to the same partition
  → Preserves event ordering per order
```

#### Speed Layer: Flink for Fraud Features

```java
// Flink: compute 5-minute order velocity per customer
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Enable checkpointing for fault tolerance (exactly-once)
env.enableCheckpointing(30_000);  // 30-second checkpoint interval
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

DataStream<OrderEvent> orders = env
    .fromSource(kafkaSource,
                WatermarkStrategy.<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                    .withTimestampAssigner((e, ts) -> e.getEventTimeMs()),
                "Kafka Orders");

// 5-minute tumbling window: order velocity per customer
orders
    .keyBy(OrderEvent::getCustomerId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .allowedLateness(Time.seconds(30))
    .aggregate(new OrderVelocityAggregator())
    .addSink(new RedisSink("velocity:{customer_id}:{window_start}"));
```

#### Batch Layer: Spark on Databricks

```python
# Hourly Spark job: full dedup, schema enforcement, Delta write

from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

def run_batch_pipeline(batch_hour: str):
    # 1. Read raw events from Kafka (or S3 staging)
    raw_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKERS) \
        .option("subscribe", "orders.raw") \
        .option("startingOffsets", get_offsets_for_hour(batch_hour)) \
        .option("endingOffsets", get_offsets_for_hour_end(batch_hour)) \
        .load()
    
    # 2. Deserialize Avro with schema registry
    orders_df = raw_df.select(
        F.from_avro(F.col("value"), schema_str).alias("data")
    ).select("data.*")
    
    # 3. Deduplicate (same order_id may arrive from multiple microservices)
    deduped_df = orders_df \
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("order_id").orderBy(F.desc("event_time"))
        )) \
        .filter("rn = 1") \
        .drop("rn")
    
    # 4. Apply transformations and business rules
    transformed_df = deduped_df \
        .withColumn("amount_usd", standardize_currency("amount", "currency")) \
        .withColumn("customer_tier", classify_customer("customer_id")) \
        .withColumn("_loaded_at", F.current_timestamp())
    
    # 5. Idempotent MERGE into Delta Lake Gold layer
    DeltaTable.forPath(spark, "/data/gold/orders_fact") \
        .alias("target") \
        .merge(transformed_df.alias("source"), "target.order_id = source.order_id") \
        .whenMatchedUpdateAll(condition="source.event_time > target.event_time") \
        .whenNotMatchedInsertAll() \
        .execute()
```

---

### Step 4: Handling Failures

#### Scenario A: Kafka Consumer Group Falls Behind

```
Symptom: Consumer lag grows, freshness degrades
Root cause: Flink or Spark job processing too slowly

Detection:
  - Kafka consumer lag metrics in Grafana
  - Alert when lag > 100K messages

Response:
  1. Scale out Flink task managers (more parallelism)
  2. If persistent: increase Kafka partitions (requires consumer restart)
  3. Enable Flink backpressure handling to avoid OOM
```

#### Scenario B: Batch Job Fails Midway Through MERGE

```
Problem: MERGE ran for 45 minutes, failed at 90% due to OOM.
Risk: Delta Lake table may be in a partially updated state.

Why it's safe:
  - Delta Lake MERGE is atomic (ACID)
  - Failed transaction is rolled back automatically
  - Re-running the job processes the same hour window (idempotent)

Recovery:
  1. Fix the OOM issue (reduce shuffle partitions, add memory)
  2. Re-trigger the same batch_hour parameter
  3. Job re-reads the same Kafka offsets and re-runs MERGE
  4. Delta MERGE deduplicates against existing data via order_id key
```

#### Scenario C: Schema Breaking Change from a Microservice

```
Symptom: Avro deserialization fails, events go to DLQ
Alert: DLQ message count > threshold

Response:
  1. Halt the affected microservice's Kafka producer (or add passthrough)
  2. Identify the schema change (check Schema Registry)
  3. Update the Flink/Spark schema mapping to handle new schema
  4. Replay events from DLQ after fix is deployed
  5. Post-mortem: enforce schema compatibility checks in microservice CI/CD
```

---

### Step 5: Exactly-Once Semantics

**The challenge:** Kafka guarantees at-least-once delivery by default. How do we prevent double-counting?

#### At-Least-Once vs. Exactly-Once

```
At-least-once: Every event is processed at least once, but may be processed twice.
Exactly-once: Every event is processed exactly once, even under failure and retry.
```

#### Flink Exactly-Once with Kafka Transactions

```java
// Flink Kafka sink with exactly-once via two-phase commit
KafkaSink<AggregatedVelocity> sink = KafkaSink.<AggregatedVelocity>builder()
    .setBootstrapServers(KAFKA_BROKERS)
    .setRecordSerializer(new VelocitySerializer())
    .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)  // Two-phase commit
    .setTransactionalIdPrefix("velocity-producer")
    .build();
```

Flink's exactly-once works via **distributed snapshots (checkpoints):**

1. Flink takes a checkpoint: records current Kafka offsets + state
2. If the job crashes and recovers, it restores from checkpoint
3. Kafka sink uses transactions: uncommitted writes are rolled back on recovery
4. Downstream consumer reads only committed messages

**Cost:** Exactly-once has ~10-30% performance overhead vs. at-least-once.

#### Deduplication as an Alternative to Exactly-Once

For the batch layer, true exactly-once is hard to implement end-to-end. A simpler approach:

```sql
-- Idempotent MERGE: even if run twice, produces the same result
-- Because order_id is the unique key, duplicate events are handled
MERGE INTO orders_fact USING orders_staging
ON orders_fact.order_id = orders_staging.order_id
-- Idempotency guarantees: same as exactly-once for our use case
```

This is "effectively-once" — at-least-once delivery + idempotent writes = correct final result.

---

## Trade-Offs Discussion: What Interviewers Want to Hear

### Flink vs. Spark Streaming

| Factor | Apache Flink | Spark Structured Streaming |
|--------|-------------|--------------------------|
| Latency | True event-by-event (ms) | Micro-batch (1s-1min) |
| State management | Rich, scalable state | Limited state |
| Exactly-once | Native via checkpoints | With Delta + idempotency |
| Learning curve | Steep | Lower (SQL + DataFrame API) |
| Ecosystem | Java/Scala native | Spark ecosystem |
| Use case | Fraud, real-time scoring | Near-real-time analytics |

**Answer:** "For fraud (5-min SLO), Flink's true streaming with sub-second latency is the right choice. For analytics (2-hour SLO), Spark Structured Streaming micro-batch on Databricks is simpler to operate and sufficient for the SLO."

### Lambda vs. Kappa Trade-Off for This Design

The design above is Lambda (dual speed + batch layers). A senior interviewer may push back:

> "Why not just use Kappa — one Flink job for everything?"

**Good answer:**
> "Kappa simplifies operations — one codebase, one processing engine. The challenge here is that the batch layer does complex deduplication and multi-source joining that would require keeping significant state in Flink, which is expensive. For the fraud use case, Kappa works well. For the analytics fact table with 50M+ records requiring full dedup, the batch layer's MERGE into Delta is more cost-effective. A production decision would depend on whether the team can maintain Flink state at this scale, and what the Kafka retention policy allows for reprocessing."

---

## Key Senior-Level Points to Always Cover

1. **Checkpointing:** How does the system recover from failure without data loss or duplication?
2. **Idempotency:** Why is the MERGE/overwrite pattern safe to re-run?
3. **Schema evolution:** What happens when a source schema changes?
4. **Monitoring:** What metrics do you track? What are your alerting thresholds?
5. **Reprocessing:** How do you replay historical data if you find a bug in the transformation?
6. **Cost:** What are the most expensive parts of this design, and how would you optimize them?
