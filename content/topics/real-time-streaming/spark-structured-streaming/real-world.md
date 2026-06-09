---
title: "Spark Structured Streaming — Real World"
topic: real-time-streaming
subtopic: spark-structured-streaming
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [spark, structured-streaming, delta-lake, production, kafka, databricks]
---

# Spark Structured Streaming — Real World

## Pattern 1: Kafka to Delta Medallion Pipeline

```python
# Production: Bronze → Silver → Gold streaming pipeline on Databricks
# Each layer is a separate streaming query

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("MedallionStreaming").getOrCreate()

# ===== BRONZE: raw ingest from Kafka =====
bronze_query = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "raw-events")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "200000")
    .option("failOnDataLoss", "false")
    .load()
    .select(
        col("topic"),
        col("partition").cast("int"),
        col("offset").cast("long"),
        col("timestamp").alias("kafka_ts"),
        col("key").cast("string").alias("event_key"),
        col("value").cast("string").alias("raw_json"),
        current_timestamp().alias("bronze_ingested_at"),
        to_date(col("timestamp")).alias("date")   # partition column
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .partitionBy("date")
    .option("checkpointLocation", "abfss://checkpoints@account.dfs.core.windows.net/bronze/events/")
    .trigger(processingTime="30 seconds")
    .start("abfss://bronze@account.dfs.core.windows.net/events/")
)

# ===== SILVER: parse, validate, deduplicate =====
event_schema = StructType([
    StructField("event_id",   StringType()),
    StructField("user_id",    StringType()),
    StructField("event_type", StringType()),
    StructField("properties", MapType(StringType(), StringType())),
    StructField("event_time", TimestampType()),
    StructField("session_id", StringType())
])

def bronze_to_silver(batch_df, batch_id):
    # Parse JSON
    parsed = batch_df.select(
        from_json("raw_json", event_schema).alias("e"),
        "bronze_ingested_at"
    ).select("e.*", "bronze_ingested_at")
    
    # Validate: drop nulls in required fields
    valid = parsed.filter(
        col("event_id").isNotNull() &
        col("user_id").isNotNull() &
        col("event_type").isin(["click", "view", "purchase", "search"])
    )
    
    # Route invalid to DLQ
    invalid = parsed.subtract(valid)
    if not invalid.isEmpty():
        invalid.write.format("delta").mode("append") \
            .save("abfss://silver@account.dfs.core.windows.net/dlq/events/")
    
    # Deduplicate on event_id (exactly-once)
    deduped = valid.dropDuplicates(["event_id"])
    
    # Upsert into Silver Delta table
    silver_table = DeltaTable.forPath(
        spark, "abfss://silver@account.dfs.core.windows.net/events/")
    silver_table.alias("t").merge(
        deduped.alias("s"),
        "t.event_id = s.event_id"
    ).whenNotMatchedInsertAll().execute()

silver_query = (
    spark.readStream
    .format("delta")
    .load("abfss://bronze@account.dfs.core.windows.net/events/")
    .writeStream
    .foreachBatch(bronze_to_silver)
    .option("checkpointLocation", "abfss://checkpoints@account.dfs.core.windows.net/silver/events/")
    .trigger(processingTime="1 minute")
    .start()
)

# ===== GOLD: 5-minute purchase aggregation =====
gold_query = (
    spark.readStream
    .format("delta")
    .load("abfss://silver@account.dfs.core.windows.net/events/")
    .filter(col("event_type") == "purchase")
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window("event_time", "5 minutes"),
        "user_id"
    )
    .agg(
        count("*").alias("purchase_count"),
        sum(col("properties")["amount"].cast("double")).alias("total_amount")
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "user_id", "purchase_count", "total_amount"
    )
    .writeStream
    .outputMode("append")   # emit when watermark advances past window end
    .format("delta")
    .option("checkpointLocation", "abfss://checkpoints@account.dfs.core.windows.net/gold/purchases/")
    .trigger(processingTime="5 minutes")
    .start("abfss://gold@account.dfs.core.windows.net/purchase-aggregates/")
)

bronze_query.awaitTermination()
silver_query.awaitTermination()
gold_query.awaitTermination()
```

---

## Pattern 2: Real-Time Anomaly Detection

```python
# Pattern: stream IoT sensor data, detect anomalies using rolling statistics

from pyspark.sql.functions import *

sensor_schema = StructType([
    StructField("device_id",    StringType()),
    StructField("metric",       StringType()),
    StructField("value",        DoubleType()),
    StructField("event_time",   TimestampType())
])

sensors = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "iot-sensors").load() \
    .select(from_json(col("value").cast("string"), sensor_schema).alias("d")).select("d.*") \
    .withWatermark("event_time", "2 minutes")

# Rolling 10-minute statistics per device per metric
stats = sensors \
    .groupBy(
        window("event_time", "10 minutes", "1 minute"),  # hopping: 10-min window, 1-min slide
        "device_id", "metric"
    ) \
    .agg(
        avg("value").alias("avg_value"),
        stddev("value").alias("std_value"),
        min("value").alias("min_value"),
        max("value").alias("max_value"),
        count("*").alias("reading_count")
    )

def detect_and_alert(batch_df, batch_id):
    """Detect outliers: reading > avg + 3*stddev or < avg - 3*stddev."""
    
    # Load recent stats from last window
    recent_stats = batch_df.withColumn(
        "upper_bound", col("avg_value") + 3 * col("std_value")
    ).withColumn(
        "lower_bound", col("avg_value") - 3 * col("std_value")
    )
    
    # Join current readings with stats to find anomalies
    # (In practice: maintain stats state and join against latest readings)
    recent_stats.write.format("delta").mode("append") \
        .save("s3://bucket/delta/sensor-stats/")
    
    # Emit alerts for anomalous devices
    anomalies = recent_stats.filter(
        (col("max_value") > col("upper_bound")) |
        (col("min_value") < col("lower_bound"))
    ).withColumn("alert_time", current_timestamp())
    
    if not anomalies.isEmpty():
        # Write to alerts Delta table
        anomalies.write.format("delta").mode("append") \
            .save("s3://bucket/delta/sensor-alerts/")
        
        # Publish alert count to Kafka for downstream consumers
        anomalies.select(
            to_json(struct("device_id", "metric", "avg_value", "max_value", "alert_time"))
                .alias("value")
        ).write.format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "sensor-alerts") \
            .save()

stats.writeStream.foreachBatch(detect_and_alert) \
    .option("checkpointLocation", "s3://bucket/checkpoints/sensor-anomaly/") \
    .trigger(processingTime="1 minute") \
    .start().awaitTermination()
```

---

## Pattern 3: Streaming Deduplication

```python
# Kafka at-least-once delivery: same event may arrive multiple times
# Common causes: producer retry, consumer group rebalance, network retries
# Solution: streaming deduplication using Delta Lake merge

def streaming_dedup(batch_df, batch_id):
    """
    Idempotent streaming write with deduplication.
    Uses Delta merge with unique event_id as primary key.
    """
    # Step 1: Deduplicate WITHIN the current batch (Kafka may duplicate in same batch)
    deduped_batch = batch_df \
        .dropDuplicates(["event_id"]) \
        .filter(col("event_id").isNotNull())
    
    # Step 2: Deduplicate AGAINST history (Delta merge = only insert if not seen before)
    target = DeltaTable.forPath(spark, "s3://bucket/delta/events-deduped/")
    
    target.alias("hist").merge(
        deduped_batch.alias("new"),
        "hist.event_id = new.event_id"
    ) \
    .whenNotMatchedInsertAll() \
    .execute()
    
    # Metrics
    print(f"Batch {batch_id}: {deduped_batch.count()} unique events written")

# Optimization: maintain a "seen IDs" bloom filter in state
# Instead of full merge, use bloom filter for fast membership check
# (custom with mapGroupsWithState + Python bloom-filter library)

raw_events.writeStream \
    .foreachBatch(streaming_dedup) \
    .option("checkpointLocation", "s3://bucket/checkpoints/dedup/") \
    .trigger(processingTime="1 minute") \
    .start().awaitTermination()
```

---

## Interview Tips

> **Tip 1:** "How do you handle a Kafka topic being deleted or retention expiring while a streaming job is paused?" — Set `.option("failOnDataLoss", "false")` to skip missing offsets instead of failing. Set `.option("startingOffsets", "latest")` on restart if you're OK skipping the gap. For production: monitor consumer lag via Kafka admin APIs — alert if lag > X minutes of data. Best practice: set Kafka retention to at least 2× your maximum expected job pause time. For critical pipelines: use Kafka topic with 7-day retention and Structured Streaming with `failOnDataLoss=false`. After restart, log how many offsets were skipped for audit purposes.

> **Tip 2:** "What monitoring do you set up for a production Spark Structured Streaming job?" — Key metrics from `query.lastProgress`: `inputRowsPerSecond` (source throughput), `processedRowsPerSecond` (processing throughput), `triggerExecution.batchTriggerDeltaMs` (batch duration), `stateOperators[].memoryUsedBytes` (state growth), `eventTime.watermark` (lag indicator). Set alerts: batch duration > 2× trigger interval → performance issue; state memory > 80% executor heap → state explosion; `inputRowsPerSecond` drops to 0 → source connectivity issue. Use Databricks Delta Live Tables monitoring UI or push metrics to Prometheus via a `StreamingQueryListener`.

> **Tip 3:** "How do you migrate a batch Spark job to streaming without rewriting from scratch?" — Spark Structured Streaming's biggest advantage: batch and streaming use the same API. Migration path: (a) Wrap existing batch logic in `foreachBatch(batch_df, batch_id)` — your existing code runs unchanged on each micro-batch; (b) Replace batch `spark.read` with `spark.readStream` for the source; (c) Replace batch `.write` with `.writeStream.foreachBatch(your_function)`; (d) Add checkpoint location. This works for 80% of batch jobs. Exceptions: global sorts, full-table scans, `collect()` calls in the batch that return data to the driver — these don't work in streaming (replace with window aggregations or approximate methods).
