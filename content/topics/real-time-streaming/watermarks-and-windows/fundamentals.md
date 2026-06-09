---
title: "Watermarks & Windows — Fundamentals"
topic: real-time-streaming
subtopic: watermarks-and-windows
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [streaming, watermarks, windows, event-time, tumbling, hopping, session, flink, spark]
---

# Watermarks & Windows — Fundamentals

## Why Event Time and Watermarks Matter

```
The problem:
  Network delay, device clock drift, mobile batching → events arrive out of order
  
  Example (orders):
  Event time:    10:00  10:01  10:02  10:00  10:03  10:01
  Arrival time:  10:02  10:03  10:03  10:04  10:05  10:06
                                             ↑
                                       This 10:00 event arrived 4 minutes late!

If we aggregate by ARRIVAL time (processing time):
  10:00-10:01 window: 0 events (10:00 order not yet arrived)
  10:02-10:03 window: 3 events (orders from 10:00, 10:01, 10:02 arrive together)
  → INCORRECT: 10:00-10:01 window shows 0 orders, 10:02-10:03 shows 3

If we aggregate by EVENT TIME (correct approach):
  10:00-10:01 window: 3 events (both 10:00 orders + 10:01 order)
  10:02-10:03 window: 2 events (10:02 + 10:03)
  → CORRECT: windows reflect when events actually happened

Watermark:
  How does the engine know when a window is "complete"?
  = track the maximum event_time seen minus a tolerance

  Watermark = max(event_time) - tolerance
  
  When watermark > window_end: the window is closed and emitted
  Events arriving AFTER the watermark has passed their window: LATE (dropped or handled)
```

---

## Window Types

```
1. Tumbling Window
   Fixed size, non-overlapping, no gaps
   Example: count orders per minute
   
   ├─────────┤├─────────┤├─────────┤
   [10:00-10:01][10:01-10:02][10:02-10:03]
   
   Each event belongs to EXACTLY ONE window
   Use: hourly/daily aggregations, periodic reporting

2. Sliding (Hopping) Window
   Fixed size, overlapping, slide interval < window size
   Example: moving average of last 5 minutes, computed every 1 minute
   
   ├─────────────────┤
       ├─────────────────┤
           ├─────────────────┤
   window=5min, slide=1min
   
   Each event may belong to MULTIPLE windows (window/slide count)
   Use: moving averages, trend detection, anomaly detection
   
3. Session Window
   Dynamic size, gap-based (window extends while events keep arriving)
   Example: user session = all events within 30-min inactivity gap
   
   ├────────────┤  ├──────────┤   ├─────────┤
   [login..clicks] [gap>30min] [new session]
   
   Sessions per user vary in length
   Use: user session analytics, IoT device activity windows

4. Global Window
   All events in one window (no time boundary)
   Only useful with custom triggers
   Example: process all events since job start
   Use: rare; usually combined with custom Flink triggers
```

---

## Window Implementation (Flink)

```java
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.time.Time;

// 1. Tumbling Event-Time Window
DataStream<AggResult> tumblingResult = orders
    .keyBy(Order::getCategory)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .aggregate(new CountAndSumAggregator());

// 2. Sliding Event-Time Window (5-min window, slide every 1 min)
DataStream<MovingAvg> slidingResult = metrics
    .keyBy(Metric::getServiceId)
    .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
    .aggregate(new AverageAggregator());

// 3. Session Window (30-min gap)
DataStream<Session> sessionResult = events
    .keyBy(Event::getUserId)
    .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
    .apply(new SessionWindowFunction());

// 4. Processing-time tumbling window (simpler, no watermark needed)
DataStream<AggResult> processingTimeResult = orders
    .keyBy(Order::getCategory)
    .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
    .aggregate(new CountAndSumAggregator());
```

---

## Window Implementation (Spark)

```python
from pyspark.sql.functions import window, count, sum as spark_sum, avg

# Read streaming data with event-time and watermark
events = spark.readStream.format("kafka") \
    .option("subscribe", "events").load() \
    .select(from_json(col("value").cast("string"), schema).alias("d")).select("d.*") \
    .withWatermark("event_time", "5 minutes")  # 5-minute tolerance for late events

# 1. Tumbling window (1-minute, non-overlapping)
tumbling = events \
    .groupBy(
        window("event_time", "1 minute"),   # windowDuration
        "category"
    ) \
    .agg(count("*").alias("count"), spark_sum("amount").alias("total"))

# 2. Sliding window (5-minute window, 1-minute slide)
sliding = events \
    .groupBy(
        window("event_time", "5 minutes", "1 minute"),  # windowDuration, slideDuration
        "service_id"
    ) \
    .agg(avg("response_ms").alias("avg_latency"))

# 3. Session window — NOT natively supported in Spark Structured Streaming
# Workaround: use foreachBatch with stateful logic (mapGroupsWithState)
# Or: use Flink (natively supports session windows)

# Write to Delta (append mode: emit each window exactly once after watermark passes)
tumbling.writeStream \
    .outputMode("append") \
    .format("delta") \
    .option("checkpointLocation", "s3://bucket/ckpt/tumbling/") \
    .start("s3://bucket/delta/category-counts/")
```

---

## Watermark Configuration Guide

```
How to set the right watermark tolerance:

1. Measure your actual event delay:
   Monitor: p99(kafka_timestamp - event_time) per source
   This tells you: 99% of events arrive within N seconds of their event time
   
   Example measurements:
     Desktop web: p99 = 2 seconds
     iOS app:     p99 = 45 seconds (batch uploads when app goes foreground)
     Android app: p99 = 90 seconds
     IoT sensors: p99 = 15 seconds

2. Set watermark to p99 of delay (or p999 for high accuracy):
   Multiple sources → use max delay:
   withWatermark("event_time", "2 minutes")  // covers 99% of mobile events
   
3. Trade-offs:
   Larger watermark:  more complete windows, lower late event rate, higher latency
   Smaller watermark: faster output, more late events dropped
   
   Latency impact:
     Watermark "2 minutes" = windows fire 2 minutes later than event time
     Example: 10:00-10:01 window fires when max(event_time) reaches 10:03
     Dashboard shows 10:00-10:01 data at approximately 10:03

4. Monitor late event rate:
   Alert if > 5% of events are arriving after watermark
   → Increase watermark tolerance
   
5. Handle truly late events:
   Flink: sideOutputLateData(lateTag) → route to DLQ
   Spark: foreachBatch → check if record falls outside watermark → route to DLQ
```

---

## Interview Tips

> **Tip 1:** "What is the difference between event time, processing time, and ingestion time?" — Event time: when the event actually occurred (timestamp in the data payload). Processing time: wall-clock time when the streaming engine processes the record (e.g., when Flink's operator runs). Ingestion time: when the record entered the messaging system (Kafka timestamp). For analytics, always use event time — it gives correct results even when the pipeline is restarted or falls behind. Processing time is only appropriate when approximate results are acceptable and ordering/late data doesn't matter (e.g., monitoring dashboards).

> **Tip 2:** "What happens to a tumbling window if no events arrive in that time period?" — In event-time processing: the window may never fire if the watermark doesn't advance past the window end time. If events stop arriving, the watermark stops advancing, and pending windows never emit. This is correct behavior — if no data arrived for the 10:05-10:06 window, Flink correctly produces no output (rather than emitting a zero count at the wrong time). For dashboards that need zero counts in empty windows: either use processing-time windows (always fire at the wall-clock interval), or emit explicit "empty" records, or query the sink with `COALESCE(count, 0)` for missing time windows.

> **Tip 3:** "When would you use a session window vs a tumbling window?" — Use tumbling windows when you need fixed-time aggregations (counts per minute, hourly totals) where the time boundary is business-meaningful. Use session windows when you need to group events by user activity — a session ends when the user is inactive for N minutes, and a new session begins with the next event. Session windows are variable length (a session may be 30 seconds or 2 hours depending on activity). Common use cases: user engagement analytics, e-commerce session analysis, IoT device activity windows. Session windows require state per key (current session start/last event time) — use Flink's native session windows or Spark's `mapGroupsWithState` for this.
