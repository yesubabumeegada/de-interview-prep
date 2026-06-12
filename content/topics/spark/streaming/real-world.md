---
title: "Spark Streaming — Real World"
topic: spark
subtopic: streaming
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [spark, structured-streaming, production, lag, recovery, exactly-once, real-world]
---

# Spark Streaming — Real World

## War Story: Watermark Too Tight, Events Silently Dropped

**Scenario:** A fraud detection pipeline joined a click stream with an impression stream. The team set a 5-minute watermark on both. After 2 weeks, data scientists reported that ~8% of click-impression pairs were missing in analysis.

**Root cause:**
```
Mobile app clicks were often delayed (offline mode, poor connectivity)
Actual click delay distribution:
  50th percentile: 30 seconds
  95th percentile: 8 minutes
  99th percentile: 22 minutes

Watermark: 5 minutes → dropped all events > 5 min late
8% of events had delay > 5 min → silently discarded (no error!)
```

**Fix:**
```python
# Measure actual late-arrival distribution BEFORE setting watermark
# Look at: event_time vs Kafka message timestamp
liveness_df = raw_stream.withColumn("delay_sec",
    F.unix_timestamp(F.col("kafka_ts")) - F.unix_timestamp(F.col("event_time")))
liveness_df.groupBy(F.percentile_approx("delay_sec", [0.5, 0.9, 0.95, 0.99])).show()

# Set watermark at P99 + buffer:
impressions.withWatermark("impression_time", "30 minutes")
clicks.withWatermark("click_time", "30 minutes")

# Accept the tradeoff: higher watermark = more memory state held
# Memory = (events/second) × (watermark_seconds) × (avg_event_bytes)
# 10K events/sec × 1800s × 500 bytes = ~9 GB state — plan executor memory accordingly
```

---

## War Story: Checkpoint Corruption on EMR Spot Termination

**Scenario:** Streaming job on EMR used Spot instances for cost savings. After a Spot termination event, the job restarted but threw `StreamingQueryException: Error reading streaming offsets` and couldn't recover.

**Root cause:**
```
EMR Spot interrupted mid-write to S3 checkpoint
Partial checkpoint files written:
  offsets/42  - complete (planned offsets)
  commits/41  - complete (last good batch)
  state/      - partial (some state files missing or truncated)

Spark couldn't reconstruct state from partial files
```

**Fix:**
```python
# 1. Use S3A with strong consistency (S3 Consistency since 2020 — use SDK 1.x+ on EMR 5.34+)
spark.conf.set("spark.hadoop.fs.s3a.path.style.access", "true")

# 2. Use HDFS on EMR for checkpoint (more reliable than S3 for streaming)
checkpoint_path = "hdfs:///checkpoints/fraud-detect/"

# 3. For mission-critical: use On-Demand for driver + core, Spot for task nodes only
# spark.executor.instances.ondemand = 2  (core nodes)
# remainder on Spot

# 4. Implement checkpoint recovery procedure:
# On corrupt checkpoint: delete checkpoint, restart from latest committed Kafka offset
# Find committed offset from Kafka:
from kafka import KafkaConsumer
consumer = KafkaConsumer(bootstrap_servers="broker:9092")
committed = consumer.committed(TopicPartition("events", 0))

# Restart with startingOffsets:
spark.readStream.format("kafka") \
    .option("startingOffsets",
        f'{{"events": {{"0": {committed}}}}}') \
    .load()
```

---

## Production Streaming Checklist

```python
production_streaming_config = {
    # Performance
    "spark.sql.shuffle.partitions": "20",        # lower for streaming (not 200!)
    "spark.default.parallelism": "20",
    "spark.sql.adaptive.enabled": "true",

    # Kafka source
    "maxOffsetsPerTrigger": "100000",            # limit per-batch reads
    "kafka.consumer.max.poll.records": "500",
    "fetchOffset.numRetries": "3",

    # State
    "spark.sql.streaming.stateStore.providerClass":
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",

    # Reliability
    "spark.task.maxFailures": "10",              # more retries for streaming
    "spark.streaming.stopGracefullyOnShutdown": "true",

    # Monitoring
    "spark.sql.streaming.metricsEnabled": "true",
}

# Always:
# - Set checkpointLocation to durable storage (HDFS/Delta)
# - Set watermark on ALL stateful operations
# - Test recovery: kill job mid-batch, verify restart is clean
# - Monitor: Kafka consumer lag, batchDuration, stateRows
```

---

## Interview Tips

> **Tip 1:** "How do you decide the right watermark duration?" — Measure the P99 of event_time vs arrival_time delay on real data. Set the watermark to P99 + 20% buffer. Accept that events beyond the watermark are silently dropped — this is a business decision, not just a technical one. For fraud/billing use cases, a high watermark (30-60 min) with more state memory is worth it. For real-time dashboards, a tighter watermark (5 min) is acceptable since approximate counts are fine.

> **Tip 2:** "How would you handle a streaming job that can't recover from a bad checkpoint?" — Delete the checkpoint directory and restart from the latest committed Kafka offset. Retrieve the committed offset for each partition from Kafka's `__consumer_offsets` topic or AdminClient. Restart with `startingOffsets` set to those offsets. Accept that the batch that was in-flight is replayed — ensure the sink is idempotent (Delta merge or upsert) so replayed data doesn't cause duplicates.
