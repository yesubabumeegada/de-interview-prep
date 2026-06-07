---
title: "Structured Streaming on Databricks - Real-World Production Examples"
topic: databricks
subtopic: structured-streaming
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, structured-streaming, production, patterns, kafka, monitoring]
---

# Structured Streaming on Databricks — Real-World Production Examples

## Pattern 1: Kafka to Delta Medallion (End-to-End)

```python
from pyspark.sql.functions import *

# BRONZE: Raw Kafka events → Delta (append-only)
kafka_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-cluster:9092")
    .option("subscribe", "user-events")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", "200000")
    .load()
)

bronze = (kafka_stream
    .selectExpr(
        "CAST(key AS STRING) as event_key",
        "CAST(value AS STRING) as raw_json",
        "topic", "partition", "offset", "timestamp as kafka_timestamp"
    )
    .withColumn("_ingested_at", current_timestamp())
)

(bronze.writeStream
    .option("checkpointLocation", "/checkpoints/bronze-events/")
    .trigger(processingTime="10 seconds")
    .toTable("production.bronze.user_events")
)

# SILVER: Parse, validate, dedup (streaming from bronze Delta table)
silver_source = spark.readStream.table("production.bronze.user_events")

schema = "event_id STRING, user_id STRING, event_type STRING, amount DOUBLE, event_ts STRING, properties MAP<STRING, STRING>"

silver = (silver_source
    .select(from_json(col("raw_json"), schema).alias("data"), "_ingested_at")
    .select("data.*", "_ingested_at")
    .withColumn("event_time", to_timestamp(col("event_ts")))
    .filter(col("event_id").isNotNull())
    .withWatermark("event_time", "1 hour")
    .dropDuplicatesWithinWatermark(["event_id"])
)

(silver.writeStream
    .option("checkpointLocation", "/checkpoints/silver-events/")
    .trigger(processingTime="30 seconds")
    .toTable("production.silver.user_events")
)

# GOLD: Windowed aggregations (streaming from silver)
gold_source = spark.readStream.table("production.silver.user_events")

gold = (gold_source
    .withWatermark("event_time", "15 minutes")
    .groupBy(
        window("event_time", "5 minutes"),
        "event_type"
    )
    .agg(
        count("*").alias("event_count"),
        sum("amount").alias("total_amount"),
        approx_count_distinct("user_id").alias("unique_users"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "event_type", "event_count", "total_amount", "unique_users",
    )
)

(gold.writeStream
    .outputMode("update")
    .option("checkpointLocation", "/checkpoints/gold-event-metrics/")
    .trigger(processingTime="1 minute")
    .toTable("production.gold.event_metrics_5min")
)
```

---

## Pattern 2: CDC Pipeline (Database Replication)

```python
# Replicate PostgreSQL changes to Delta Lake via Kafka (Debezium)

cdc_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "cdc.public.orders,cdc.public.customers")
    .option("startingOffsets", "earliest")
    .load()
)

# Parse Debezium CDC format
parsed_cdc = (cdc_stream
    .selectExpr("CAST(value AS STRING) as json", "topic")
    .select(
        from_json(col("json"), cdc_schema).alias("payload"),
        col("topic")
    )
    .select(
        col("payload.after.*"),
        col("payload.op").alias("cdc_op"),
        col("payload.ts_ms").alias("cdc_timestamp"),
        col("topic"),
    )
)

# MERGE into silver (current state) using foreachBatch
def apply_cdc_changes(batch_df, batch_id):
    """Apply CDC operations to maintain current state."""
    # Split by table
    orders = batch_df.filter(col("topic") == "cdc.public.orders")
    customers = batch_df.filter(col("topic") == "cdc.public.customers")
    
    if orders.count() > 0:
        orders.createOrReplaceTempView("orders_updates")
        spark.sql("""
            MERGE INTO production.silver.orders t
            USING (SELECT * FROM orders_updates WHERE cdc_op != 'd') s
            ON t.order_id = s.order_id
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        # Handle deletes
        spark.sql("""
            DELETE FROM production.silver.orders 
            WHERE order_id IN (SELECT order_id FROM orders_updates WHERE cdc_op = 'd')
        """)
    
    # Similar for customers...

(parsed_cdc.writeStream
    .foreachBatch(apply_cdc_changes)
    .option("checkpointLocation", "/checkpoints/cdc-merge/")
    .trigger(processingTime="30 seconds")
    .start()
    .awaitTermination()
)
```

---

## Pattern 3: Real-Time Alerting Pipeline

```python
# Detect anomalies in real-time and trigger alerts

events = spark.readStream.table("production.silver.user_events")

# Calculate rolling metrics per user (last 5 minutes)
user_metrics = (events
    .withWatermark("event_time", "10 minutes")
    .groupBy(
        window("event_time", "5 minutes"),
        "user_id"
    )
    .agg(
        count("*").alias("event_count"),
        sum("amount").alias("total_amount"),
    )
)

# Detect anomalies (spending >$10K in 5 minutes)
alerts = (user_metrics
    .filter(col("total_amount") > 10000)
    .select(
        col("user_id"),
        col("total_amount"),
        col("window.start").alias("alert_window_start"),
        current_timestamp().alias("alert_generated_at"),
        lit("HIGH_SPEND_ALERT").alias("alert_type"),
    )
)

# Write alerts to a Delta table (consumed by notification service)
(alerts.writeStream
    .outputMode("update")
    .option("checkpointLocation", "/checkpoints/alerts/")
    .trigger(processingTime="30 seconds")
    .toTable("production.alerts.spending_alerts")
)

# Separate process reads alerts table and sends notifications:
# Notification service polls production.alerts.spending_alerts every 30s
# Sends Slack/PagerDuty/email based on alert_type
```

---

## Pattern 4: Multi-Stream Coordination

```python
# Run multiple streams on a single cluster (cost-efficient)

streams = []

# Stream 1: Orders ingestion
stream1 = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load("s3://lake/landing/orders/")
    .writeStream
    .queryName("ingest_orders")
    .option("checkpointLocation", "/checkpoints/orders/")
    .trigger(processingTime="30 seconds")
    .toTable("production.bronze.orders")
)
streams.append(stream1)

# Stream 2: Events ingestion
stream2 = (spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load("s3://lake/landing/events/")
    .writeStream
    .queryName("ingest_events")
    .option("checkpointLocation", "/checkpoints/events/")
    .trigger(processingTime="30 seconds")
    .toTable("production.bronze.events")
)
streams.append(stream2)

# Stream 3: Silver transformation
stream3 = (spark.readStream.table("production.bronze.orders")
    .filter(col("order_id").isNotNull())
    .writeStream
    .queryName("silver_orders")
    .option("checkpointLocation", "/checkpoints/silver-orders/")
    .trigger(processingTime="1 minute")
    .toTable("production.silver.orders")
)
streams.append(stream3)

# Wait for all streams (they run concurrently on the same cluster)
for stream in streams:
    stream.awaitTermination()

# Benefits: one cluster runs 3 streams (shared resources, lower cost)
# Risk: one stream failure doesn't stop others (independent checkpoints)
# Monitoring: spark.streams.active shows all running queries
```

---

## Pattern 5: Streaming Pipeline Monitoring

```python
import time
from datetime import datetime

class StreamingMonitor:
    """Monitor streaming query health and alert on issues."""
    
    def __init__(self, alert_fn):
        self.alert = alert_fn
        self.baselines = {}
    
    def check_health(self):
        """Run every 5 minutes to check all active streams."""
        for query in spark.streams.active:
            progress = query.lastProgress
            if not progress:
                continue
            
            name = query.name
            batch_duration = progress.get("batchDuration", 0)
            input_rows = progress.get("numInputRows", 0)
            
            # Alert 1: Stream stopped unexpectedly
            # (Can't detect here since stopped streams aren't in .active)
            
            # Alert 2: Batch duration exceeding trigger interval (falling behind)
            trigger_ms = self._get_trigger_interval_ms(progress)
            if batch_duration > trigger_ms * 2:
                self.alert(f"FALLING BEHIND: {name} batch took {batch_duration}ms (trigger: {trigger_ms}ms)")
            
            # Alert 3: Zero input rows for extended period (source may be dead)
            if input_rows == 0:
                self.baselines.setdefault(name, {"zero_count": 0})
                self.baselines[name]["zero_count"] += 1
                if self.baselines[name]["zero_count"] > 10:  # 10 consecutive empty batches
                    self.alert(f"NO DATA: {name} has received 0 rows for {self.baselines[name]['zero_count']} consecutive batches")
            else:
                if name in self.baselines:
                    self.baselines[name]["zero_count"] = 0
    
    def generate_report(self) -> dict:
        """Summary of all streaming queries."""
        report = {}
        for query in spark.streams.active:
            p = query.lastProgress
            if p:
                report[query.name] = {
                    "status": "ACTIVE",
                    "input_rows_per_sec": p.get("inputRowsPerSecond", 0),
                    "processed_rows_per_sec": p.get("processedRowsPerSecond", 0),
                    "batch_duration_ms": p.get("batchDuration", 0),
                    "state_rows": sum(
                        s.get("numRowsTotal", 0) 
                        for s in p.get("stateOperators", [])
                    ),
                }
        return report
```

---

## Interview Tips

> **Tip 1:** "Design a real-time event processing pipeline" — Kafka → Bronze (raw JSON, append-only, Auto Loader or Kafka source) → Silver (parse, validate, dedup with watermark) → Gold (windowed aggregations). Each layer is a separate streaming query with its own checkpoint. End-to-end latency: 30-60 seconds depending on trigger intervals.

> **Tip 2:** "How do you handle CDC in streaming?" — Read Debezium CDC from Kafka (Avro/JSON format with before/after/op fields). Use foreachBatch to MERGE changes into silver table (insert → INSERT, update → UPDATE, delete → DELETE). The checkpoint ensures exactly-once: if a batch retries, MERGE is idempotent (matched rows update, no duplicates).

> **Tip 3:** "How do you monitor streaming in production?" — Check: batch duration vs trigger interval (falling behind?), input rate vs processing rate (backpressure?), state size growth (unbounded?), zero-input batches (source dead?). Use StreamingQueryListener for metrics emission. Alert if: stream stops, batch duration > 2× trigger, or zero input for >10 consecutive batches.
