---
title: "Spark Streaming — Intermediate"
topic: spark
subtopic: streaming
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [spark, structured-streaming, stateful, mapGroupsWithState, flatMapGroupsWithState, session-windows, stream-stream-join]
---

# Spark Streaming — Intermediate

## Stateful Processing: mapGroupsWithState

`mapGroupsWithState` lets you maintain custom state per group across micro-batches:

```python
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import *

# Define state schema
state_schema = StructType([
    StructField("event_count", LongType()),
    StructField("total_amount", DoubleType()),
    StructField("last_seen", TimestampType()),
])

# Define output schema
output_schema = StructType([
    StructField("customer_id", StringType()),
    StructField("session_count", LongType()),
    StructField("session_revenue", DoubleType()),
])

def update_customer_state(customer_id, events, state: GroupState):
    if state.hasTimedOut:
        # Session expired — emit final result
        current = state.get
        state.remove()
        yield (customer_id, current.event_count, current.total_amount)
        return

    current = state.getOption or (0, 0.0, None)
    count, total, last_seen = current

    for event in events:
        count += 1
        total += event.amount
        last_seen = event.event_time

    state.update((count, total, last_seen))
    state.setTimeoutDuration("10 minutes")   # expire if no events for 10 min

    yield (customer_id, count, total)

result = (parsed
    .withWatermark("event_time", "30 minutes")
    .groupBy("customer_id")
    .applyInPandasWithState(
        update_customer_state,
        output_schema,
        state_schema,
        "Update",
        GroupStateTimeout.ProcessingTimeTimeout,
    ))
```

---

## flatMapGroupsWithState

`flatMapGroupsWithState` allows emitting zero or more output rows per group per batch — useful for session detection, anomaly detection, and complex event processing:

```python
# Session windowing: group events into sessions separated by >30 min inactivity
def detect_sessions(user_id, events, state: GroupState):
    events_list = sorted(events, key=lambda e: e.event_time)

    if state.hasTimedOut:
        # Emit completed session
        s = state.get
        yield (user_id, s.session_id, s.start_time, s.end_time, s.event_count)
        state.remove()
        return

    # Process new events
    for event in events_list:
        if not state.exists:
            state.update(Session(
                session_id=generate_uuid(),
                start_time=event.event_time,
                end_time=event.event_time,
                event_count=1
            ))
        else:
            s = state.get
            if (event.event_time - s.end_time).seconds > 1800:
                # Gap > 30 min — emit old session, start new
                yield (user_id, s.session_id, s.start_time, s.end_time, s.event_count)
                state.update(Session(...))
            else:
                state.update(s._replace(end_time=event.event_time,
                                        event_count=s.event_count + 1))

    state.setTimeoutDuration("30 minutes")
```

---

## Window Types

```python
# 1. Tumbling window: non-overlapping fixed-size windows
F.window(F.col("event_time"), "10 minutes")
# [10:00, 10:10), [10:10, 10:20), ...

# 2. Sliding window: overlapping windows
F.window(F.col("event_time"), "10 minutes", "5 minutes")
# window size=10m, slide=5m
# [10:00, 10:10), [10:05, 10:15), [10:10, 10:20), ...

# 3. Session window (Spark 3.2+): dynamic size based on inactivity gap
from pyspark.sql.functions import session_window
F.session_window(F.col("event_time"), "5 minutes")  # gap timeout = 5 min
# Groups events separated by < 5 min gap into the same session

# Example: count events per user per session
session_counts = (parsed
    .withWatermark("event_time", "20 minutes")
    .groupBy(F.col("user_id"), F.session_window(F.col("event_time"), "10 minutes"))
    .count()
)
```

---

## Stream-Stream Joins

Join two streaming DataFrames — both sides buffer events until a match is found:

```python
# Stream-stream join requires watermarks on both sides
impressions = spark.readStream.format("kafka") \
    .option("subscribe", "ad_impressions").load() \
    .select(F.from_json(...).alias("data")).select("data.*") \
    .withWatermark("impression_time", "2 hours")

clicks = spark.readStream.format("kafka") \
    .option("subscribe", "ad_clicks").load() \
    .select(F.from_json(...).alias("data")).select("data.*") \
    .withWatermark("click_time", "3 hours")

# Join with time range constraint (required for bounded state)
result = impressions.join(
    clicks,
    (impressions.ad_id == clicks.ad_id) &
    (clicks.click_time >= impressions.impression_time) &
    (clicks.click_time <= impressions.impression_time + F.expr("INTERVAL 1 HOUR")),
    how="leftOuter"
)
```

**State management in stream-stream joins:**
- Impressions are buffered waiting for matching clicks
- Clicks are buffered waiting for matching impressions
- Watermarks determine when buffered rows can be dropped (join window expired)
- Without time constraint: state grows without bound!

---

## Stream-Batch Join

```python
# Static (batch) DataFrame joined with a stream:
# Static side is broadcast or replicated to all executors
# Batch data is refreshed at query start (or on each trigger if using .load())

country_codes = spark.read.parquet("s3://bucket/dim/country_codes/")  # batch

enriched = parsed.join(
    country_codes,
    on="country_code",
    how="left"
)
# country_codes is read once at query start, broadcast to all executors
# This is efficient and doesn't require watermarks
```

---

## Monitoring Streaming Queries

```python
# Query progress — called after each trigger
query = stream.writeStream.format("console").start()

# Programmatic access to metrics
import time
for _ in range(10):
    progress = query.lastProgress
    if progress:
        print(f"Batch: {progress['batchId']}")
        print(f"Input rows/sec: {progress['inputRowsPerSecond']:.0f}")
        print(f"Process rows/sec: {progress['processedRowsPerSecond']:.0f}")
        print(f"Duration (ms): {progress['durationMs']}")
        print(f"State rows: {progress.get('stateOperators', [{}])[0].get('numRowsTotal', 0)}")
    time.sleep(10)

# Spark UI → Structured Streaming tab:
# - Input rate, process rate
# - Batch duration trend
# - State size over time (growing = memory leak in state logic!)
```

---

## foreachBatch for Custom Sinks

```python
def write_to_multiple_sinks(batch_df, batch_id):
    # batch_df is a regular DataFrame — full batch API available
    batch_df.cache()  # cache since we're writing to multiple places

    # Write to Delta Lake
    batch_df.write.format("delta").mode("append").save("s3://bucket/delta/events/")

    # Write summary to Postgres
    (batch_df
        .groupBy("region")
        .agg(F.sum("amount").alias("revenue"))
        .write.format("jdbc")
        .option("url", "jdbc:postgresql://host/db")
        .option("dbtable", "regional_summary")
        .mode("append")
        .save())

    batch_df.unpersist()

query = stream.writeStream \
    .foreachBatch(write_to_multiple_sinks) \
    .option("checkpointLocation", "checkpoint/") \
    .trigger(processingTime="1 minute") \
    .start()
```

---

## Interview Tips

> **Tip 1:** "How does stream-stream join work and what are the constraints?" — Both streams buffer incoming events. When a new event arrives on one side, Spark looks for matches in the other side's buffer. The join requires time range constraints (e.g., click must arrive within 1 hour of impression) to bound how long events stay in the buffer. Both sides also need watermarks so Spark knows when buffered events can safely be dropped. Without these constraints, the state buffer grows without bound and the job eventually runs out of memory.

> **Tip 2:** "What's the difference between mapGroupsWithState and flatMapGroupsWithState?" — `mapGroupsWithState` emits exactly one output row per group per batch. `flatMapGroupsWithState` emits zero or more rows per group per batch. The flat variant is more powerful: you can output nothing (state accumulating), one row, or multiple rows in a single batch. Use flatMap for session detection, anomaly alerts (emit only when threshold crossed), or complex event processing where output count isn't 1:1 with batches.

> **Tip 3:** "How do you handle exactly-once delivery in Structured Streaming?" — End-to-end exactly-once requires: (1) checkpointing to store committed source offsets and state, (2) idempotent or transactional sinks. Kafka-to-Kafka and Kafka-to-Delta are exactly-once out of the box. Custom sinks via `foreachBatch` need idempotent logic (e.g., upsert by batch_id). The checkpoint records the last committed batch; on restart, Spark replays from that point — but duplicate writes to non-idempotent sinks (simple appends) can still happen.
