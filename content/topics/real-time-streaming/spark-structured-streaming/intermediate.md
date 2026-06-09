---
title: "Spark Structured Streaming — Intermediate"
topic: real-time-streaming
subtopic: spark-structured-streaming
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [spark, structured-streaming, delta-lake, kafka, stateful, joins, foreachbatch]
---

# Spark Structured Streaming — Intermediate

## Stateful Operations: Stream-Stream Joins

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Stream-Stream join: join two Kafka streams
# Use case: match orders with their payments

orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("user_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("event_time", TimestampType())
])

payments_schema = StructType([
    StructField("order_id", StringType()),
    StructField("payment_method", StringType()),
    StructField("payment_time", TimestampType())
])

orders = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders").load() \
    .select(from_json(col("value").cast("string"), orders_schema).alias("d")).select("d.*") \
    .withWatermark("event_time", "10 minutes")

payments = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "payments").load() \
    .select(from_json(col("value").cast("string"), payments_schema).alias("d")).select("d.*") \
    .withWatermark("payment_time", "10 minutes")

# Stream-stream join with time constraint
# Without time constraint: state grows unboundedly (Spark buffers all orders waiting for payment)
# With constraint: Spark can drop state after the window passes
joined = orders.join(
    payments,
    expr("""
        orders.order_id = payments.order_id AND
        payment_time >= event_time AND
        payment_time <= event_time + interval 1 hour
    """),  # payment must arrive within 1 hour of order
    "leftOuter"    # outer join: include unmatched orders (payment=null)
)

joined.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "s3://bucket/checkpoints/order-payments/") \
    .start("s3://bucket/delta/order-payments/")
```

---

## ForeachBatch: Custom Sink Logic

```python
# ForeachBatch: process each micro-batch as a regular batch DataFrame
# Use for: complex merge logic, multiple sinks, non-streaming sink connectors

from delta.tables import DeltaTable

def upsert_to_delta(batch_df, batch_id):
    """
    Upsert streaming data into Delta Lake (merge = insert or update).
    ForeachBatch gives you the full batch DataFrame to manipulate.
    """
    # Deduplicate within batch (Kafka can deliver duplicates within a batch)
    deduped = batch_df.dropDuplicates(["order_id"])
    
    # Check if Delta table exists
    if DeltaTable.isDeltaTable(spark, "s3://bucket/delta/orders/"):
        delta_table = DeltaTable.forPath(spark, "s3://bucket/delta/orders/")
        
        # Merge: upsert based on order_id
        delta_table.alias("existing") \
            .merge(
                deduped.alias("incoming"),
                "existing.order_id = incoming.order_id"
            ) \
            .whenMatchedUpdate(set={
                "status":     "incoming.status",
                "updated_at": "incoming.updated_at",
                "amount":     "incoming.amount"
            }) \
            .whenNotMatchedInsertAll() \
            .execute()
    else:
        # First batch: create the table
        deduped.write.format("delta").mode("overwrite") \
            .save("s3://bucket/delta/orders/")

# Use foreachBatch
query = orders.writeStream \
    .foreachBatch(upsert_to_delta) \
    .option("checkpointLocation", "s3://bucket/checkpoints/orders-merge/") \
    .trigger(processingTime="1 minute") \
    .start()

# ForeachBatch: write to MULTIPLE sinks from one stream
def multi_sink(batch_df, batch_id):
    """Write same data to Delta (for analytics) and PostgreSQL (for operations)."""
    batch_df.persist()  # cache in memory — avoid recomputing for each sink
    
    # Sink 1: Delta Lake
    batch_df.write.format("delta").mode("append").save("s3://bucket/delta/events/")
    
    # Sink 2: PostgreSQL (JDBC)
    batch_df.filter(col("priority") == "high") \
        .write.format("jdbc") \
        .option("url", "jdbc:postgresql://pg:5432/ops") \
        .option("dbtable", "high_priority_events") \
        .option("user", "dbuser").option("password", "dbpass") \
        .mode("append").save()
    
    # Sink 3: alert count to console
    count = batch_df.filter(col("is_fraud") == True).count()
    if count > 0:
        print(f"Batch {batch_id}: {count} fraud events detected!")
    
    batch_df.unpersist()

stream.writeStream.foreachBatch(multi_sink) \
    .option("checkpointLocation", "s3://bucket/checkpoints/multi/") \
    .start()
```

---

## Stream-Table Join (Lookup Enrichment)

```python
# Stream-Table join: enrich streaming events with a static/slow-changing table
# Use case: join clickstream events with user profile table

# Static dimension (batch read)
user_profiles = spark.read.format("delta") \
    .load("s3://bucket/delta/user-profiles/") \
    .select("user_id", "tier", "country", "signup_date")

# Streaming events
events = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "clickstream").load() \
    .select(from_json(col("value").cast("string"), event_schema).alias("e")).select("e.*")

# Join streaming with static (non-watermarked join — table acts as lookup)
enriched = events.join(
    broadcast(user_profiles),  # broadcast small table to all partitions
    "user_id",
    "left"
)

# Problem: user_profiles is read ONCE at job start → stale after profile updates
# Solution: refresh user_profiles in foreachBatch:
def enrich_with_refresh(batch_df, batch_id):
    # Re-read user profiles every batch (or every N batches)
    if batch_id % 10 == 0:  # refresh every 10 batches
        profiles = spark.read.format("delta").load("s3://bucket/delta/user-profiles/")
        broadcast_profiles = broadcast(profiles)
    
    enriched = batch_df.join(broadcast_profiles, "user_id", "left")
    enriched.write.format("delta").mode("append").save("s3://bucket/delta/enriched-events/")

events.writeStream.foreachBatch(enrich_with_refresh) \
    .option("checkpointLocation", "s3://bucket/checkpoints/enrich/").start()
```

---

## Kafka to Delta Lake Production Pattern

```python
# Production: Kafka → Bronze (raw) → Silver (parsed+deduplicated) → Gold (aggregated)

# BRONZE: raw Kafka bytes, no transformation
bronze_query = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \   # don't fail if Kafka topic cleaned up
    .load() \
    .select(
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("value").cast("string").alias("raw_value"),
        current_timestamp().alias("ingested_at")
    ) \
    .writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://bucket/checkpoints/bronze/transactions/") \
    .partitionBy("date")  \   # partition by date for efficient queries
    .trigger(processingTime="30 seconds") \
    .start("s3://bucket/delta/bronze/transactions/")

# SILVER: parse, validate, deduplicate
silver_schema = StructType([...])

def bronze_to_silver(batch_df, batch_id):
    parsed = batch_df \
        .select(from_json("raw_value", silver_schema).alias("d"), "kafka_timestamp") \
        .select("d.*", "kafka_timestamp") \
        .filter(col("transaction_id").isNotNull()) \   # drop invalid records
        .filter(col("amount") > 0) \
        .dropDuplicates(["transaction_id"])            # exactly-once dedup
    
    DeltaTable.forPath(spark, "s3://bucket/delta/silver/transactions/") \
        .alias("t") \
        .merge(parsed.alias("s"), "t.transaction_id = s.transaction_id") \
        .whenNotMatchedInsertAll() \
        .execute()

silver_query = spark.readStream.format("delta") \
    .load("s3://bucket/delta/bronze/transactions/") \
    .writeStream \
    .foreachBatch(bronze_to_silver) \
    .option("checkpointLocation", "s3://bucket/checkpoints/silver/transactions/") \
    .trigger(processingTime="1 minute") \
    .start()

bronze_query.awaitTermination()
silver_query.awaitTermination()
```

---

## Interview Tips

> **Tip 1:** "How does idempotency work in Spark Structured Streaming with ForeachBatch?" — Spark guarantees at-least-once processing in foreachBatch (a batch may be replayed on failure). To achieve exactly-once: make the foreachBatch function idempotent. For Delta Lake: use `merge` with primary key (re-running with same data produces same result). For JDBC: use `INSERT ... ON CONFLICT DO NOTHING` or `UPSERT`. For REST APIs: include an idempotency key derived from batch_id + record_id. The `batch_id` is stable across retries (same batch ID is replayed on failure), so it can serve as an idempotency key for external systems.

> **Tip 2:** "What causes stream-stream joins to have unbounded state growth, and how do you fix it?" — Without a time constraint in the join condition, Spark must buffer all records from both streams indefinitely (waiting for a potential match). Fix: add a time boundary in the join condition (`payment_time <= event_time + interval 1 hour`) AND add watermarks to both streams. Spark then drops state for join pairs where both watermarks have advanced past the time window. The state size becomes bounded by: (max event rate) × (join time window). Monitor state size via `StreamingQuery.lastProgress()` → `stateOperators[0].memoryUsedBytes`.

> **Tip 3:** "When would you choose Spark Structured Streaming over Flink for streaming?" — Choose Spark Structured Streaming when: (a) your team is already on Databricks/Spark — same language, same APIs, easy integration with Delta Lake; (b) you need ML model scoring in the stream (MLlib integration); (c) you need interactive analytics alongside streaming (same Delta table for streaming writes and ad-hoc SQL queries); (d) you use `trigger(availableNow=True)` for near-real-time batch processing (cheaper than always-on streaming). Choose Flink when: latency must be <100ms, you need complex CEP (Complex Event Processing), or rich stateful processing with custom timers.
