---
title: "Watermarks & Windows — Intermediate"
topic: real-time-streaming
subtopic: watermarks-and-windows
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [streaming, watermarks, windows, late-data, triggers, allowedLateness, flink, spark]
---

# Watermarks & Windows — Intermediate

## Watermark Strategies in Flink

```java
import org.apache.flink.api.common.eventtime.*;
import java.time.Duration;

// Strategy 1: Bounded Out-of-Orderness (most common)
// Assumes events arrive within a bounded delay
WatermarkStrategy<Order> boundedOOO = WatermarkStrategy
    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withTimestampAssigner((order, ts) -> order.getEventTimeMs());

// Strategy 2: Monotonous timestamps (events arrive in order)
// Watermark = timestamp of latest event - 0 delay
WatermarkStrategy<Order> monotonous = WatermarkStrategy
    .<Order>forMonotonousTimestamps()
    .withTimestampAssigner((order, ts) -> order.getEventTimeMs());

// Strategy 3: Custom watermark generator (complex logic)
WatermarkStrategy<Order> custom = WatermarkStrategy
    .forGenerator(ctx -> new WatermarkGenerator<Order>() {
        
        private long maxSeenTimestamp = Long.MIN_VALUE;
        private static final long MAX_LAG_MS = 60_000L;  // 1 minute
        
        @Override
        public void onEvent(Order event, long eventTimestamp, WatermarkOutput output) {
            // Update max seen timestamp
            maxSeenTimestamp = Math.max(maxSeenTimestamp, event.getEventTimeMs());
            // Don't emit watermark here — emit on periodic pulse
        }
        
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // Emit watermark every 200ms (default auto watermark interval)
            if (maxSeenTimestamp != Long.MIN_VALUE) {
                output.emitWatermark(new Watermark(maxSeenTimestamp - MAX_LAG_MS));
            }
        }
    })
    .withTimestampAssigner((order, ts) -> order.getEventTimeMs());

// Per-partition watermarks (prevent slow partition from blocking all windows)
// Kafka source: assign timestamps per Kafka partition
// Watermark = min(watermark across all assigned partitions)
// Problem: if partition 5 has no events for 10 minutes → global watermark stalls
// Solution: idle partition timeout
WatermarkStrategy<Order> withIdleDetection = WatermarkStrategy
    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withTimestampAssigner((order, ts) -> order.getEventTimeMs())
    .withIdleness(Duration.ofMinutes(2));  // treat partition as idle after 2 min silence
// Idle partition excluded from watermark calculation → other partitions can advance
```

---

## Triggers and Window Firing

```java
/*
 Window triggers: control WHEN a window fires (emits results)
 Default: fire when watermark passes window end (EventTimeTrigger)
 
 Custom triggers allow:
   - Early firing (show intermediate results before window completes)
   - Late firing (update results when late events arrive)
   - Count-based firing (fire every N records)
*/

import org.apache.flink.streaming.api.windowing.triggers.*;

// Default: fire once when watermark > window end
.window(TumblingEventTimeWindows.of(Time.minutes(1)))
.trigger(EventTimeTrigger.create())  // this is the default

// Custom trigger: fire every 30 seconds OR when watermark passes window end
// Enables "early" preview of partial results while waiting for watermark
.window(TumblingEventTimeWindows.of(Time.minutes(5)))
.trigger(new EarlyFiringEventTimeTrigger(30_000))  // custom trigger

// Flink's built-in composite trigger (since 1.18):
// Fire every 30 seconds (early) + when window ends (final)
.window(TumblingEventTimeWindows.of(Time.minutes(5)))
.trigger(Trigger.TRIGGER_ON_EVERY_WINDOW_AND_PROCESSING_TIME(Time.seconds(30)))

// Example custom trigger:
public class EarlyFiringEventTimeTrigger extends Trigger<Object, TimeWindow> {
    private final long earlyFireIntervalMs;
    
    @Override
    public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx)
            throws Exception {
        // Register event-time timer for window end
        ctx.registerEventTimeTimer(window.maxTimestamp());
        // Register processing-time timer for early firing
        long nextEarlyFire = ctx.getCurrentProcessingTime() + earlyFireIntervalMs;
        ctx.registerProcessingTimeTimer(nextEarlyFire);
        return TriggerResult.CONTINUE;  // don't fire yet
    }
    
    @Override
    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
        // Window end reached → fire FINAL result
        return time == window.maxTimestamp() ? TriggerResult.FIRE_AND_PURGE : TriggerResult.CONTINUE;
    }
    
    @Override
    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
            throws Exception {
        // Processing-time timer → fire EARLY result (don't purge — keep state)
        // Re-register for next early firing
        ctx.registerProcessingTimeTimer(time + earlyFireIntervalMs);
        return TriggerResult.FIRE;  // fire but keep state (not PURGE)
    }
}
```

---

## AllowedLateness and Side Outputs

```java
/*
 allowedLateness: how long after watermark passes window end to keep window state
 
 Timeline (1-minute window [10:00, 10:01)):
   10:01:00: watermark reaches 10:01 → window fires (emitted with all records so far)
   10:01:30: late event arrives with event_time=10:00:45
             → without allowedLateness: DROPPED
             → with allowedLateness(30s): window re-fires with updated result
   10:01:30: window state cleared (30s elapsed since watermark passed 10:01)
*/

OutputTag<Order> lateOrderTag = new OutputTag<Order>("late-orders"){};

SingleOutputStreamOperator<WindowResult> mainStream = orders
    .keyBy(Order::getCategory)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .allowedLateness(Time.seconds(30))   // keep window state 30s after watermark
    .sideOutputLateData(lateOrderTag)    // truly late events → side output
    .aggregate(new CountAndSumAggregator(), new ResultWindowFunction());

// Main stream: final window results (watermark passed + allowedLateness expired)
mainStream.addSink(analyticsDB);

// Side output: truly late events (arrived after allowedLateness expired)
DataStream<Order> lateOrders = mainStream.getSideOutput(lateOrderTag);
lateOrders.addSink(dlqSink);  // send to DLQ for manual investigation

// In the downstream: update query must handle re-fired windows (update mode)
// Best: sink with UPSERT on (category, window_start, window_end)
// Window re-fires → UPDATE counts in sink → final result is accurate
```

---

## Window Aggregation Patterns in Spark

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Pattern 1: Tumbling window with append mode (most common)
orders = spark.readStream.format("delta") \
    .load("s3://bucket/delta/silver/orders/") \
    .withWatermark("event_time", "5 minutes")

# Append mode: row emitted only when window is final (watermark passed window end)
# No partial results — each row appears exactly once
result = orders.groupBy(
    window("event_time", "1 hour"),
    "category"
).agg(
    count("*").alias("order_count"),
    sum("amount").alias("total_amount"),
    avg("amount").alias("avg_order_value"),
    approx_count_distinct("user_id").alias("unique_users")  # HLL-based distinct count
)

result.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "s3://bucket/ckpt/hourly-orders/") \
    .start("s3://bucket/delta/gold/hourly-orders/")

# Pattern 2: Sliding window for moving average
# 5-minute window computed every 1 minute (4 windows overlap)
# Each event contributes to 5 windows (window_size / slide_interval)
response_times = spark.readStream.format("kafka") \
    .option("subscribe", "api-latency").load() \
    .select(from_json(col("value").cast("string"), latency_schema).alias("d")).select("d.*") \
    .withWatermark("event_time", "2 minutes")

moving_avg = response_times.groupBy(
    window("event_time", "5 minutes", "1 minute"),  # 5-min window, 1-min slide
    "endpoint"
).agg(
    avg("response_ms").alias("avg_response_ms"),
    percentile_approx("response_ms", 0.95).alias("p95_response_ms"),
    count("*").alias("request_count")
)

moving_avg.writeStream \
    .outputMode("append") \  # emit when watermark advances past window end
    .format("delta") \
    .option("checkpointLocation", "s3://bucket/ckpt/moving-avg/") \
    .start("s3://bucket/delta/gold/api-latency-moving-avg/")

# Pattern 3: Count-based window (emit every N events per key)
# Not natively supported in Spark SQL windows
# Use foreachBatch with stateful tracking:
def count_based_window(batch_df, batch_id):
    """Emit result when accumulation reaches 1000 events per key."""
    # Read current counts from Delta
    # Add new batch counts
    # Emit keys that crossed 1000
    pass  # use mapGroupsWithState for this in production
```

---

## Interview Tips

> **Tip 1:** "What is the global watermark in Spark and Flink when you have multiple sources?" — In Flink: when a job has multiple sources (e.g., Kafka partitions), each partition tracks its own watermark. The global watermark = min(all partition watermarks). This means the slowest partition controls when windows fire. Problem: if one Kafka partition receives no events, its watermark stalls, blocking all windows. Solution: idle partition detection (`withIdleness(Duration.ofMinutes(2))` in Flink) — after 2 minutes of silence, that partition is excluded from the min watermark calculation. In Spark: when joining two streams with different watermarks, the global watermark = min(watermark1, watermark2).

> **Tip 2:** "What's the difference between `FIRE` and `FIRE_AND_PURGE` in Flink triggers?" — `FIRE`: emit the window results but keep all window state (records still in state). The window can fire again later (e.g., when more records arrive). Used for early/intermediate results with subsequent updates. `FIRE_AND_PURGE`: emit results AND delete all window state. The window cannot update after this. This is the default behavior for `EventTimeTrigger` when the watermark passes the window end — the window is done, no more records expected, state cleaned up. Use `FIRE` for multi-fire windows (early + late results), `FIRE_AND_PURGE` for final results only.

> **Tip 3:** "How do you handle a situation where event timestamps in the data are unreliable (e.g., device clocks are wrong)?" — Unreliable event timestamps are a real problem with IoT devices and mobile apps. Options: (a) Use Kafka ingestion timestamp instead of event timestamp (use `withTimestampAssigner((msg, ts) -> msg.getKafkaTimestamp())` — Kafka timestamps when it received the record, which is reliable). Trade-off: you're measuring arrival time, not event time; (b) Validate event timestamps: filter out events where `abs(event_time - kafka_time) > 10 minutes` → route to DLQ for investigation; (c) Use bounded out-of-orderness with a wide window (5+ minutes) to absorb clock drift; (d) Hybrid: use event time when it's within bounds, fall back to Kafka time otherwise.
