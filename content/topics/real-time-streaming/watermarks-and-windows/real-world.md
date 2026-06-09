---
title: "Watermarks & Windows — Real World"
topic: real-time-streaming
subtopic: watermarks-and-windows
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [streaming, watermarks, windows, production, flink, spark, event-time]
---

# Watermarks & Windows — Real World

## Pattern 1: Multi-Source Watermark Reconciliation

```java
/*
 Production pattern: merge events from multiple sources with different latency profiles
 - Web events: arrive within 2 seconds
 - Mobile app events: arrive within 90 seconds (batched upload)
 - IoT sensors: arrive within 30 seconds
 
 Problem: if we use a single watermark for all sources,
          the slowest source (mobile, 90s) controls the global watermark
          → web events wait 90 seconds before their windows fire
 
 Solution: separate watermarks per source, then union
*/

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(8);
env.enableCheckpointing(60_000);

// Web events: tight watermark (2 seconds)
WatermarkStrategy<Event> webWM = WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
    .withTimestampAssigner((e, ts) -> e.getEventTimeMs())
    .withIdleness(Duration.ofMinutes(5));

// Mobile events: loose watermark (90 seconds)
WatermarkStrategy<Event> mobileWM = WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(90))
    .withTimestampAssigner((e, ts) -> e.getEventTimeMs())
    .withIdleness(Duration.ofMinutes(10));

// IoT events: medium watermark (30 seconds)
WatermarkStrategy<Event> iotWM = WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(30))
    .withTimestampAssigner((e, ts) -> e.getEventTimeMs())
    .withIdleness(Duration.ofMinutes(5));

// Sources
DataStream<Event> webEvents = env.fromSource(webKafkaSource, webWM, "Web Source")
    .filter(e -> e.getSource().equals("web"));

DataStream<Event> mobileEvents = env.fromSource(mobileKafkaSource, mobileWM, "Mobile Source")
    .filter(e -> e.getSource().equals("mobile"));

DataStream<Event> iotEvents = env.fromSource(iotKafkaSource, iotWM, "IoT Source")
    .filter(e -> e.getSource().equals("iot"));

// Union: global watermark = min(web watermark, mobile watermark, iot watermark)
// NOTE: global watermark = 90s lag (controlled by mobile)
// Alternative: process each source separately, write to different tables
DataStream<Event> allEvents = webEvents.union(mobileEvents, iotEvents);

// Window with the loosest tolerance (cover all sources)
allEvents
    .keyBy(Event::getUserId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .aggregate(new EventAggregator())
    .addSink(analyticsSink);

// Better approach: separate aggregations per source type
webEvents
    .keyBy(Event::getUserId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .aggregate(new EventAggregator())
    .addSink(webAnalyticsSink);

mobileEvents
    .keyBy(Event::getUserId)
    .window(TumblingEventTimeWindows.of(Time.minutes(5)))
    .aggregate(new EventAggregator())
    .addSink(mobileAnalyticsSink);
```

---

## Pattern 2: Sliding Window for SLA Monitoring

```python
# Production: monitor API response time SLA compliance
# Alert if p99 > 500ms over a 5-minute sliding window

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("SLAMonitor").getOrCreate()

api_schema = StructType([
    StructField("request_id",   StringType()),
    StructField("endpoint",     StringType()),
    StructField("response_ms",  IntegerType()),
    StructField("status_code",  IntegerType()),
    StructField("event_time",   TimestampType())
])

requests = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "api-requests") \
    .option("maxOffsetsPerTrigger", "50000") \
    .load() \
    .select(from_json(col("value").cast("string"), api_schema).alias("d")).select("d.*") \
    .withWatermark("event_time", "2 minutes")  # API events are very fresh

# Sliding window: 5-minute window, recomputed every 1 minute
sla_stats = requests \
    .filter(col("status_code") < 500) \  # exclude server errors from latency calc
    .groupBy(
        window("event_time", "5 minutes", "1 minute"),
        "endpoint"
    ) \
    .agg(
        count("*").alias("request_count"),
        avg("response_ms").alias("avg_response_ms"),
        percentile_approx("response_ms", 0.50).alias("p50_ms"),
        percentile_approx("response_ms", 0.95).alias("p95_ms"),
        percentile_approx("response_ms", 0.99).alias("p99_ms"),
        sum(when(col("response_ms") > 500, 1).otherwise(0)).alias("slow_requests"),
        (sum(when(col("response_ms") > 500, 1).otherwise(0)) * 100.0 / count("*"))
            .alias("slow_pct")
    ) \
    .withColumn("sla_breach", col("p99_ms") > 500)

def emit_alerts(batch_df, batch_id):
    """Write stats to Delta and emit alerts for SLA breaches."""
    # Write all stats
    batch_df.write.format("delta").mode("append") \
        .save("s3://bucket/delta/gold/sla-stats/")
    
    # Alert on breaches
    breaches = batch_df.filter(col("sla_breach") == True) \
        .select("window", "endpoint", "p99_ms", "slow_pct")
    
    if not breaches.isEmpty():
        breach_list = breaches.collect()
        for b in breach_list:
            send_alert(
                f"SLA BREACH: {b.endpoint} p99={b.p99_ms}ms ({b.slow_pct:.1f}% slow)",
                channel="pagerduty"
            )
    
    # Maintenance: optimize Delta table every hour
    if batch_id % 60 == 0:
        spark.sql("OPTIMIZE delta.`s3://bucket/delta/gold/sla-stats/`")

sla_stats.writeStream \
    .foreachBatch(emit_alerts) \
    .option("checkpointLocation", "s3://bucket/ckpt/sla-stats/") \
    .trigger(processingTime="1 minute") \
    .start().awaitTermination()
```

---

## Pattern 3: Session Analytics

```java
/*
 Production: user session analytics for e-commerce
 Session = user activity within 30-minute inactivity gap
 Track: session duration, pages viewed, conversion (did they purchase?)
*/

public class SessionAnalyticsJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);
        env.enableCheckpointing(60_000);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        
        WatermarkStrategy<UserEvent> wm = WatermarkStrategy
            .<UserEvent>forBoundedOutOfOrderness(Duration.ofMinutes(5))
            .withTimestampAssigner((e, ts) -> e.getEventTimeMs())
            .withIdleness(Duration.ofMinutes(15));
        
        DataStream<UserEvent> events = env
            .addSource(new FlinkKinesisConsumer<>("user-events", new UserEventDeserializer(), props))
            .assignTimestampsAndWatermarks(wm);
        
        // Session window: 30-minute gap
        DataStream<SessionSummary> sessions = events
            .keyBy(UserEvent::getUserId)
            .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
            .aggregate(new SessionAggregator(), new SessionWindowFn());
        
        // Write session summaries
        sessions.addSink(icebergSink);
        
        // Additional: detect abandoned carts (session ended without purchase)
        sessions
            .filter(s -> s.getCartItems() > 0 && !s.isPurchased())
            .filter(s -> s.getTotalCartValue() > 50.0)  // significant cart value
            .addSink(abandonedCartSink);  // trigger follow-up email
        
        env.execute("Session Analytics");
    }
}

class SessionAggregator implements AggregateFunction<UserEvent, SessionAccumulator, SessionAccumulator> {
    @Override
    public SessionAccumulator createAccumulator() {
        return new SessionAccumulator();
    }
    
    @Override
    public SessionAccumulator add(UserEvent event, SessionAccumulator acc) {
        acc.pageViews++;
        if (event.getType().equals("purchase")) {
            acc.purchased = true;
            acc.revenue += event.getAmount();
        }
        if (event.getType().equals("add_to_cart")) {
            acc.cartItems++;
            acc.totalCartValue += event.getAmount();
        }
        acc.minTime = Math.min(acc.minTime, event.getEventTimeMs());
        acc.maxTime = Math.max(acc.maxTime, event.getEventTimeMs());
        return acc;
    }
    
    @Override
    public SessionAccumulator getResult(SessionAccumulator acc) { return acc; }
    
    @Override
    public SessionAccumulator merge(SessionAccumulator a, SessionAccumulator b) {
        // Session windows can merge (when adjacent sessions merge due to late event filling gap)
        a.pageViews += b.pageViews;
        a.purchased |= b.purchased;
        a.revenue += b.revenue;
        a.cartItems += b.cartItems;
        a.totalCartValue += b.totalCartValue;
        a.minTime = Math.min(a.minTime, b.minTime);
        a.maxTime = Math.max(a.maxTime, b.maxTime);
        return a;
    }
}

class SessionWindowFn extends ProcessWindowFunction<SessionAccumulator, SessionSummary, String, TimeWindow> {
    @Override
    public void process(String userId, Context ctx, Iterable<SessionAccumulator> accumulators, 
                        Collector<SessionSummary> out) {
        SessionAccumulator acc = accumulators.iterator().next();
        out.collect(new SessionSummary(
            userId,
            ctx.window().getStart(),
            ctx.window().getEnd(),
            (ctx.window().getEnd() - ctx.window().getStart()) / 1000L,  // duration_seconds
            acc.pageViews, acc.purchased, acc.revenue, acc.cartItems, acc.totalCartValue
        ));
    }
}
```

---

## Interview Tips

> **Tip 1:** "What happens when a session window receives a very late event that bridges two previously separate sessions?" — A late event with a timestamp that falls within the gap between two sessions causes the sessions to merge. Flink handles this automatically: the two session window states are merged (using the `merge()` method of the `AggregateFunction`), resulting in one larger session. The merged session has the earliest start time from both and the latest end time from both. This is correct behavior — the late event proves the user was active during what appeared to be a gap. The merged session is then emitted as a single (larger) session result.

> **Tip 2:** "How do you choose between `percentile_approx` and exact `percentile` in streaming aggregations?" — Exact percentile (`percentile`) requires keeping all values in state for each window (O(N) state per window per key). For a sliding 5-minute window with 1 million requests per minute: 5 million values × 8 bytes × number of endpoints = huge state. `percentile_approx` uses TDigest or GK sketch (O(1/ε²) state, where ε is the relative error). At ε=0.01 (1% error), state is ~10KB per group — orders of magnitude smaller. For SLA monitoring: 1% error on p99 is acceptable (499ms vs 500ms threshold). Use exact percentile only when regulatory or contractual requirements demand exact values and state size is bounded.

> **Tip 3:** "How do you handle daylight saving time (DST) transitions in event-time windows?" — Event timestamps should always be in UTC internally — never local time. All streaming engines (Flink, Spark) work with milliseconds-since-epoch (UTC). DST transitions don't affect UTC timestamps. The issue arises only in reporting: converting UTC window boundaries to local time for display. Handle this at the presentation layer: store window_start and window_end in UTC, display converted to user's timezone using the reporting tool's timezone conversion (e.g., `CONVERT_TZ()` in MySQL, `AT TIME ZONE` in Spark/Presto). If your source data uses local timestamps: convert to UTC at ingestion time using the known timezone, before applying watermarks.
