---
title: "PySpark Structured Streaming - Real World Patterns"
topic: pyspark
subtopic: structured-streaming
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [pyspark, structured-streaming, kafka, delta-lake, deduplication, monitoring, production]
---

# PySpark Structured Streaming — Real-World Patterns

## Pattern 1: Kafka to Delta Lake Streaming Pipeline

**Problem:** Ingest 500K events/second from Kafka, parse, validate, and write to Delta Lake with schema enforcement and exactly-once guarantees.

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from delta.tables import DeltaTable

spark = (SparkSession.builder
    .appName("KafkaToDelta")
    .config("spark.sql.streaming.kafka.maxOffsetsPerTrigger", "500000")
    .config("spark.sql.shuffle.partitions", "100")
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
    .getOrCreate())

# Schema for incoming events
event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("payload", MapType(StringType(), StringType())),
    StructField("timestamp", LongType(), False),
])

# Read from Kafka with consumer tuning
raw_stream = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "broker1:9092,broker2:9092,broker3:9092")
    .option("subscribe", "user_events")
    .option("startingOffsets", "latest")
    .option("maxOffsetsPerTrigger", 500000)
    .option("kafka.max.partition.fetch.bytes", "10485760")
    .option("kafka.fetch.max.bytes", "52428800")
    .option("failOnDataLoss", "false")
    .load())

# Parse and validate
parsed = (raw_stream
    .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS kafka_ts")
    .select(
        F.from_json("json_str", event_schema).alias("event"),
        "kafka_ts"
    )
    .select("event.*", "kafka_ts")
    .withColumn("event_time", F.from_unixtime(F.col("timestamp") / 1000).cast("timestamp"))
    .withColumn("event_date", F.to_date("event_time"))
    .withColumn("processing_time", F.current_timestamp())
    # Data quality filters
    .filter(F.col("event_id").isNotNull())
    .filter(F.col("user_id").isNotNull())
    .filter(F.col("event_time").isNotNull())
    .filter(F.col("event_time") > "2020-01-01")  # Reject clearly invalid timestamps
)

# Write to Delta Lake with partitioning
query = (parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://checkpoints/kafka-to-delta/user-events/")
    .option("mergeSchema", "true")
    .partitionBy("event_date")
    .trigger(processingTime="30 seconds")
    .start("s3://data-lake/delta/user_events/"))

query.awaitTermination()
```

---

## Pattern 2: Real-Time Aggregations (5-Minute Windows)

**Problem:** Compute real-time metrics per service: request count, error rate, p99 latency — updated every 30 seconds with 15-minute late data tolerance.

```python
from pyspark.sql import functions as F

# Metrics schema
metrics_schema = StructType([
    StructField("service", StringType()),
    StructField("endpoint", StringType()),
    StructField("status_code", IntegerType()),
    StructField("latency_ms", DoubleType()),
    StructField("timestamp", TimestampType()),
])

# Read metrics stream
metrics_stream = (spark.readStream
    .format("kafka")
    .option("subscribe", "service_metrics")
    .load()
    .select(F.from_json(F.col("value").cast("string"), metrics_schema).alias("m"))
    .select("m.*")
    .withWatermark("timestamp", "15 minutes")
)

# 5-minute window aggregations
windowed_metrics = (metrics_stream
    .groupBy(
        F.window("timestamp", "5 minutes"),
        "service",
        "endpoint",
    )
    .agg(
        F.count("*").alias("request_count"),
        F.count(F.when(F.col("status_code") >= 500, True)).alias("error_count"),
        F.expr("percentile_approx(latency_ms, 0.99)").alias("p99_latency"),
        F.expr("percentile_approx(latency_ms, 0.50)").alias("p50_latency"),
        F.avg("latency_ms").alias("avg_latency"),
        F.max("latency_ms").alias("max_latency"),
    )
    .withColumn("error_rate", F.col("error_count") / F.col("request_count"))
)

# Write to Delta for dashboards + alerting
def write_metrics_batch(batch_df, batch_id):
    """Write to Delta and trigger alerts."""
    # Write to Delta
    batch_df.write.mode("append").format("delta").save("s3://analytics/service_metrics/")
    
    # Check for alerting conditions
    alerts = batch_df.filter(
        (F.col("error_rate") > 0.05) |  # 5% error rate
        (F.col("p99_latency") > 5000)    # 5 second p99
    )
    
    if alerts.count() > 0:
        alert_data = alerts.collect()
        for row in alert_data:
            send_pagerduty_alert(
                service=row.service,
                message=f"High error rate: {row.error_rate:.2%}, P99: {row.p99_latency:.0f}ms"
            )

query = (windowed_metrics.writeStream
    .foreachBatch(write_metrics_batch)
    .option("checkpointLocation", "s3://checkpoints/service-metrics/")
    .trigger(processingTime="30 seconds")
    .start())
```

---

## Pattern 3: Streaming Deduplication

**Problem:** Upstream systems occasionally send duplicate events. Deduplicate within a 1-hour window to prevent double-counting in downstream analytics.

```python
from pyspark.sql import functions as F

# Read events with potential duplicates
raw_events = (spark.readStream
    .format("kafka")
    .option("subscribe", "events")
    .load()
    .select(F.from_json(F.col("value").cast("string"), event_schema).alias("e"))
    .select("e.*")
    .withColumn("event_time", F.col("timestamp").cast("timestamp"))
)

# Strategy 1: Built-in dropDuplicates with watermark
deduped_builtin = (raw_events
    .withWatermark("event_time", "1 hour")
    .dropDuplicates(["event_id", "event_time"])
)

# Strategy 2: foreachBatch with Delta MERGE for stronger dedup guarantees
def dedup_with_delta_merge(batch_df, batch_id):
    """Deduplicate using Delta Lake's MERGE for exactly-once semantics."""
    if batch_df.isEmpty():
        return
    
    target_path = "s3://data-lake/delta/deduped_events/"
    
    # Check if target exists
    try:
        target = DeltaTable.forPath(spark, target_path)
    except Exception:
        # First batch — create the table
        batch_df.write.format("delta").partitionBy("event_date").save(target_path)
        return
    
    # MERGE: only insert if event_id doesn't already exist
    (target.alias("t")
        .merge(
            batch_df.alias("s"),
            """t.event_id = s.event_id 
               AND t.event_date = s.event_date"""
        )
        .whenNotMatchedInsertAll()
        .execute())

query = (raw_events
    .withColumn("event_date", F.to_date("event_time"))
    .writeStream
    .foreachBatch(dedup_with_delta_merge)
    .option("checkpointLocation", "s3://checkpoints/dedup-events/")
    .trigger(processingTime="1 minute")
    .start())
```

### Deduplication Strategy Comparison

| Strategy | Window | State | Guarantees | Throughput |
|----------|--------|-------|-----------|-----------|
| `dropDuplicates` | Watermark-bounded | In-memory state | Best-effort within window | High |
| Delta MERGE | Unbounded (table) | Delta transaction log | Exact deduplication | Medium |
| Bloom filter | Configurable | Compact probabilistic | Approximate | Very high |

---

## Pattern 4: Stream Monitoring and Recovery

**Problem:** Production streaming jobs need automated monitoring, alerting, and self-healing recovery.

```python
import time
import json
from threading import Thread

class StreamingMonitor:
    """Monitor streaming queries and handle failures."""
    
    def __init__(self, spark, alert_callback):
        self.spark = spark
        self.alert_callback = alert_callback
        self.query_configs = {}
    
    def register_query(self, query, name, restart_func, max_restarts=3):
        self.query_configs[query.id] = {
            "name": name,
            "query": query,
            "restart_func": restart_func,
            "max_restarts": max_restarts,
            "restart_count": 0,
            "last_restart": None,
        }
    
    def check_health(self):
        """Check all registered queries for issues."""
        for query_id, config in self.query_configs.items():
            query = config["query"]
            
            # Check if query is still active
            if not query.isActive:
                exception = query.exception()
                self.alert_callback(
                    severity="critical",
                    message=f"Query {config['name']} stopped: {exception}"
                )
                self._attempt_restart(config)
                continue
            
            # Check processing latency
            progress = query.lastProgress
            if progress:
                batch_duration = progress.get("batchDuration", 0)
                trigger_interval = progress.get("triggerExecution", {})
                input_rows = progress.get("numInputRows", 0)
                
                # Alert on processing lag
                if batch_duration > 60000:  # > 60 seconds per batch
                    self.alert_callback(
                        severity="warning",
                        message=f"Query {config['name']} batch took {batch_duration}ms"
                    )
                
                # Alert on zero throughput
                if input_rows == 0:
                    self.alert_callback(
                        severity="info",
                        message=f"Query {config['name']} processed 0 rows"
                    )
    
    def _attempt_restart(self, config):
        """Attempt to restart a failed query with backoff."""
        if config["restart_count"] >= config["max_restarts"]:
            self.alert_callback(
                severity="critical",
                message=f"Query {config['name']} exceeded max restarts"
            )
            return
        
        config["restart_count"] += 1
        backoff = 2 ** config["restart_count"] * 30  # Exponential backoff
        
        time.sleep(backoff)
        new_query = config["restart_func"]()
        config["query"] = new_query
        config["last_restart"] = time.time()
    
    def start_monitoring(self, interval_seconds=30):
        """Start monitoring loop in background thread."""
        def monitor_loop():
            while True:
                self.check_health()
                time.sleep(interval_seconds)
        
        thread = Thread(target=monitor_loop, daemon=True)
        thread.start()

# Usage
monitor = StreamingMonitor(spark, alert_callback=send_alert)

def start_events_query():
    return (events.writeStream
        .format("delta")
        .option("checkpointLocation", "s3://checkpoints/events/")
        .start("s3://data-lake/events/"))

query = start_events_query()
monitor.register_query(query, "events_pipeline", start_events_query)
monitor.start_monitoring()
```

---

## Interview Tips

> **Tip 1:** "Design a Kafka to Delta Lake pipeline." — "Key decisions: trigger interval (balance latency vs cost), partition strategy (by date for time-series), checkpoint location (durable storage separate from data), and error handling (dead-letter queue for unparseable messages). Use maxOffsetsPerTrigger for backpressure, failOnDataLoss=false for resilience, and Delta's ACID guarantees for exactly-once semantics."

> **Tip 2:** "How do you handle duplicate events in streaming?" — "Three approaches by strength: dropDuplicates with watermark for bounded in-memory dedup (good for most cases), Delta MERGE for exact deduplication against the full table (strongest but slower), or Bloom filters for high-throughput approximate dedup. Choice depends on whether you need exact guarantees and your throughput requirements. Always pair with a unique event ID from the source."

> **Tip 3:** "How do you monitor streaming jobs in production?" — "Track three metrics: processing latency (batch duration vs trigger interval), input rate (rows per second), and state size (for stateful queries). Alert when batch duration exceeds trigger interval (falling behind), when throughput drops to zero (source issue), or when state size grows unexpectedly. Implement auto-restart with exponential backoff and maximum retry limits. Use the StreamingQueryListener API for programmatic monitoring."
