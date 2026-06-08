---
title: "Lambda & Kappa Architecture — Intermediate"
topic: system-design
subtopic: lambda-kappa-architecture
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [system-design, lambda-architecture, kappa-architecture, streaming, reprocessing]
---

# Lambda & Kappa Architecture — Intermediate

## Lambda Architecture Implementation

### Batch Layer (Spark)
```python
# Batch layer: reprocess all data nightly, produce accurate batch views
# Spark job reading from raw data lake

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, to_date

spark = SparkSession.builder.appName("BatchLayer").getOrCreate()

# Read all historical events from data lake
events = spark.read.parquet("s3://data-lake/raw/orders/")

# Compute daily revenue by region (batch view)
daily_revenue_batch = (
    events
    .withColumn("order_date", to_date(col("order_timestamp")))
    .groupBy("order_date", "region")
    .agg(spark_sum("amount").alias("total_revenue"),
         spark_sum("quantity").alias("units_sold"))
)

# Write batch view to serving layer
daily_revenue_batch.write.mode("overwrite").saveAsTable("serving.batch_daily_revenue")
# This overwrites the entire batch view — always fully recomputed (accurate)
```

### Speed Layer (Spark Structured Streaming)
```python
# Speed layer: process only recent events (since last batch run)
# Results are approximate (may miss late-arriving events)

recent_revenue = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "orders")
        .option("startingOffsets", "latest")  # only process new events
        .load()
    .withColumn("order_date", to_date(col("order_timestamp")))
    .groupBy("order_date", "region")
    .agg(spark_sum("amount").alias("total_revenue"))
    .writeStream
        .format("memory")   # or Delta, Redis
        .queryName("speed_daily_revenue")
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
)
```

### Serving Layer (merge)
```python
# Serving layer: query both, merge for final result
def query_revenue(start_date: str, end_date: str):
    # Batch view: yesterday and earlier (accurate)
    batch_df = spark.sql(f"""
        SELECT order_date, region, total_revenue
        FROM serving.batch_daily_revenue
        WHERE order_date BETWEEN '{start_date}' AND DATE_SUB(CURRENT_DATE, 1)
    """)
    
    # Speed view: today's data (approximate, low latency)
    speed_df = spark.sql("""
        SELECT order_date, region, total_revenue
        FROM speed_daily_revenue
        WHERE order_date = CURRENT_DATE
    """)
    
    return batch_df.union(speed_df)
```

---

## Kappa Architecture Implementation

### Kafka as Immutable Event Log
```
Kafka setup for Kappa Architecture:
  - Retention: infinite (or very long: 1-2 years)
  - Compaction: log compaction for key-based CDC topics
  - Partitioning: by event_type or entity_id for parallelism

Topic design:
  orders.raw:    all order events, RF=3, retention=1 year
  orders.enriched: post-processing output (can be regenerated)
  orders.agg:    pre-aggregated views (regenerated on reprocessing)

Why Kafka as source of truth:
  - Immutable log: can't accidentally delete; replay any time
  - Ordering: events in partition are ordered (process in correct sequence)
  - Parallelism: multiple partitions = parallel reprocessing
```

### Kappa Reprocessing Pattern
```python
# When business logic changes: reprocess from beginning of Kafka

# Step 1: Deploy v2 job reading from offset 0
v2_job = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "orders.raw")
        .option("startingOffsets", "earliest")  # read from the very beginning!
        .load()
    .transform(new_business_logic_v2)  # updated logic
    .writeStream
        .format("delta")
        .option("checkpointLocation", "s3://checkpoints/orders_v2")
        .start("s3://delta/orders_enriched_v2")  # separate output table
)

# Step 2: Monitor v2 progress — wait until it catches up to current time
# Spark UI → Streaming tab → "inputRowsPerSecond" approaches 0 = caught up

# Step 3: Switch serving layer
# Update BI tool / application to read from orders_enriched_v2

# Step 4: Clean up
# Stop v1 job, drop v1 output table after N days
```

---

## Modern Streaming-First Architecture (Beyond Kappa)

```
Limitations of classic Lambda/Kappa:
  - Kafka-only: what if data comes from databases, APIs?
  - No SQL: streaming logic must be written in code (complex)
  - No time-travel: can't query "what was the state at 2pm yesterday?"

Modern stack addresses these:
  1. Sources: Debezium CDC → Kafka (database changes as events)
  2. Processing: Apache Flink (SQL API) OR Spark Structured Streaming
  3. Storage: Delta Lake / Apache Iceberg (time-travel, ACID, streaming + batch)
  4. Query: same table serves streaming writes and batch analytical reads

Delta Lake as the unification layer:
  - Streaming job writes: spark.writeStream.format("delta")...
  - Batch job reads: spark.read.format("delta")...
  - Time travel: SELECT * FROM orders VERSION AS OF 7 (7 commits ago)
  - Both use the same table → no separate batch/speed views to merge!

This is the "Lakehouse" pattern: combines Lambda accuracy with Kappa simplicity
```

---

## Windowing in Stream Processing

```python
# Stream processing must handle time carefully

# Tumbling windows: fixed, non-overlapping (e.g., revenue per hour)
from pyspark.sql.functions import window

hourly_revenue = (
    df.withWatermark("event_time", "10 minutes")  # tolerate 10-min late data
    .groupBy(window(col("event_time"), "1 hour"), col("region"))
    .agg(spark_sum("amount"))
)

# Sliding windows: overlapping (e.g., 1-hour window, slides every 15 min)
sliding = df.groupBy(
    window(col("event_time"), "1 hour", "15 minutes"),  # window size, slide interval
    col("region")
).agg(spark_sum("amount"))

# Session windows: variable-length, based on activity gaps
# Close window when no events for N minutes
# Spark: no built-in session windows → use stateful processing
# Flink: natively supported

# Watermarks (late data handling):
# Watermark = how long to wait for late-arriving events before closing a window
# .withWatermark("event_time", "30 minutes") = wait up to 30 min for late events
# Events arriving > 30 min late: DROPPED (counted as late)
# Tradeoff: larger watermark → more accurate but higher latency
```

---

## Interview Tips

> **Tip 1:** "How does Kappa Architecture handle reprocessing?" — Deploy a new streaming job that reads from Kafka offset 0 (the beginning of the topic). The new job processes historical events at high throughput (no rate limiting from incoming new events). It writes to a new output table. When it catches up to real-time, switch the serving layer to the new table and shut down the old job. The key: Kafka must retain events long enough for reprocessing (months/years of retention).

> **Tip 2:** "What is a watermark in stream processing?" — A watermark defines how late an event can arrive and still be included in the correct time window. For example, a 10-minute watermark means: wait 10 minutes after a window closes before finalizing its results. Events arriving >10 minutes late are dropped. Setting the watermark too low: late events are dropped, results are slightly inaccurate. Too high: results are delayed. Choose based on expected latency variance in your event sources.

> **Tip 3:** "What are the limitations of Kappa Architecture?" — (1) Kafka storage cost: long retention for all event types can be expensive. (2) Complex stateful logic is harder in streaming than batch (session windows, complex aggregations). (3) Not all sources are naturally event-driven (some are database snapshots). (4) Debugging streaming jobs is harder than batch. Modern solution: Lakehouse (Delta Lake/Iceberg) with streaming writes and batch reads from the same table — same simplicity as Kappa but with easier time-travel and history.
