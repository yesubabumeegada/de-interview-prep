---
title: "Watermarks & Windows — Senior Deep Dive"
topic: real-time-streaming
subtopic: watermarks-and-windows
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [streaming, watermarks, windows, chandy-lamport, flink-internals, state-cleanup, production]
---

# Watermarks & Windows — Senior Deep Dive

## Watermark Propagation Internals

```
How watermarks flow through a Flink job:

1. Source generates watermarks (per partition):
   KafkaSource: one watermark per Kafka partition
   Within source operator: watermark = min(all assigned partition watermarks)
   
2. Watermark propagates downstream through operators:
   Each operator receives watermarks from all upstream inputs
   Operator's output watermark = min(all input watermarks)
   → ensures downstream always has the most conservative (slowest) view

3. Union operator:
   output watermark = min(watermark_stream_A, watermark_stream_B)
   → the slower stream controls the global watermark
   
4. KeyBy + Window:
   After keyBy: same partition, same watermark propagation
   Window operator fires when: watermark >= window_end_time + 0 (for EventTimeTrigger)

5. Watermark alignment (Flink 1.15+):
   Problem: Kafka source with 32 partitions, one partition far ahead in time
   → That one partition's events must wait for others (reorder buffer grows)
   Solution: watermark alignment — cap how far ahead any single partition's
   watermark can be relative to the global watermark (alignment group max drift)
   
   Configuration:
   WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5))
       .withWatermarkAlignment("alignment-group-1", Duration.ofSeconds(20), Duration.ofSeconds(1))
   // No partition's watermark can be > 20s ahead of the slowest partition
   // The fast partition's source pauses reading to let others catch up

Checkpoint barrier alignment (Chandy-Lamport):
  When checkpoint barrier reaches an operator:
    All input channels must receive the barrier before the operator snapshots state
    Channels that already sent the barrier: buffered in their input queue (blocked)
    → "barrier alignment" = waiting for all inputs to deliver the barrier
    → This is one source of checkpoint duration/latency (especially with skewed inputs)
  
  Unaligned checkpoints (Flink 1.11+):
    Operator snapshots state immediately when FIRST barrier arrives
    In-flight records in input queues are included in the snapshot
    Benefit: no blocking → lower checkpoint duration for high-throughput jobs
    Trade-off: larger checkpoint size (includes in-flight data)
```

---

## State Management for Windows

```java
/*
 Window state lifecycle:
 
 1. Window state created when first event in window arrives
 2. State updated as more events arrive
 3. Window fires when trigger fires (watermark or processing-time)
 4. State PURGED when:
    - FIRE_AND_PURGE trigger
    - Watermark passes window_end + allowedLateness
    - State TTL expires
 
 Window state backend considerations:
   Small windows (< 5 min), low cardinality keys:
     HeapStateBackend is fine (state fits in JVM heap)
   Large windows (hours), high cardinality keys (millions of users):
     RocksDB state backend required (state exceeds heap)
   
 State size estimation:
   Example: count distinct users per category per 1-hour window
   - 100 categories × 1 window = 100 window states
   - Each state: bloom filter (10KB) or HashSet (32 bytes × 1M users = 32MB)
   - Total: 100 × 32MB = 3.2GB per window
   - Overlapping windows: × slide_factor
   
 Reduce state size:
   Use aggregating functions (sum, count, avg) instead of collecting all records
   COUNT(*): O(1) state (just a long)
   SUM:      O(1) state
   DISTINCT: O(users) state — use HyperLogLog (approx_count_distinct) for bounded state
*/

// Efficient aggregation: AggregateFunction reduces state to accumulator type
public class OrderAggregator implements AggregateFunction<Order, AggAccumulator, AggResult> {
    
    @Override
    public AggAccumulator createAccumulator() {
        return new AggAccumulator(0, 0.0);  // count=0, sum=0.0
    }
    
    @Override
    public AggAccumulator add(Order order, AggAccumulator acc) {
        // Only accumulator stored in state — not the full order record
        return new AggAccumulator(acc.count + 1, acc.sum + order.getAmount());
    }
    
    @Override
    public AggResult getResult(AggAccumulator acc) {
        return new AggResult(acc.count, acc.sum, acc.count > 0 ? acc.sum / acc.count : 0);
    }
    
    @Override
    public AggAccumulator merge(AggAccumulator a, AggAccumulator b) {
        // For session windows: merge accumulators when sessions are merged
        return new AggAccumulator(a.count + b.count, a.sum + b.sum);
    }
}

// vs INEFFICIENT: collecting all orders in state
// .reduce((a, b) -> {a.addOrder(b); return a;})
// Stores ALL orders in state → O(N) memory per window per key
```

---

## Watermark Debugging and Monitoring

```java
/*
 Debugging watermark issues:
 
 Symptom 1: Windows never fire
   Cause: watermark not advancing (idle source, no events, slow partition)
   Debug:
     Flink Web UI → Job → click source operator → "Current Watermark" metric
     If watermark = Long.MIN_VALUE → source never received events
     If watermark stalled at a specific time → one Kafka partition is empty
   
   Fix: add idle detection on sources
   .withIdleness(Duration.ofMinutes(2))

 Symptom 2: Windows fire with incorrect counts (too low)
   Cause: late events dropped (watermark too tight)
   Debug:
     Add late event side output, monitor count
     Check distribution of (event_time - kafka_timestamp) lag
   Fix: increase watermark tolerance

 Symptom 3: Checkpoints failing / slow
   Cause: barrier alignment waiting for slow partition
   Debug:
     Flink Web UI → Checkpoints → click failed checkpoint
     Look at "Alignment Duration" per operator (high = backpressure or alignment)
   Fix: enable unaligned checkpoints
     env.getCheckpointConfig().enableUnalignedCheckpoints();
*/

// Monitoring watermarks in production:
// Register custom metric listener to track watermark lag
env.getConfig().addDefaultKryoSerializer(Order.class, OrderSerializer.class);

// In operator: emit watermark lag metric
public class WatermarkMonitoringOperator extends AbstractStreamOperator<Order>
        implements OneInputStreamOperator<Order, Order> {
    
    private Counter lateEventCounter;
    private Gauge<Long> watermarkLagGauge;
    
    @Override
    public void open() throws Exception {
        lateEventCounter = getRuntimeContext()
            .getMetricGroup().counter("late_events");
        watermarkLagGauge = getRuntimeContext()
            .getMetricGroup().gauge("watermark_lag_ms",
                () -> System.currentTimeMillis() - 
                      (getCurrentWatermark() == Long.MIN_VALUE ? 
                       System.currentTimeMillis() : getCurrentWatermark())
            );
    }
    
    @Override
    public void processElement(StreamRecord<Order> element) throws Exception {
        long eventTime = element.getTimestamp();
        long watermark = getCurrentWatermark();
        
        if (eventTime <= watermark) {
            lateEventCounter.inc();
        }
        output.collect(element);
    }
}

// Key metrics to monitor in production:
// currentInputWatermark:    how old is the most recent watermark
// numLateRecordsDropped:    how many events were dropped as late (in Flink SQL)
// watermarkLagMs:           currentTime - watermark = how far behind are we
// numRecordsIn/Out:         throughput per operator
// Alert thresholds:
//   watermarkLagMs > 2 × watermarkTolerance → investigate source lag
//   numLateRecordsDropped > 5% of total → increase watermark tolerance
```

---

## Advanced Window Patterns

```java
/*
 Pattern: Incremental aggregation with ProcessWindowFunction
 AggregateFunction for efficiency + ProcessWindowFunction for window metadata
*/

DataStream<WindowSummary> result = orders
    .keyBy(Order::getCategory)
    .window(TumblingEventTimeWindows.of(Time.hours(1)))
    .aggregate(
        new OrderAggregator(),             // incremental aggregation (O(1) state)
        new WindowMetadataFunction()       // enriches with window metadata
    );

class WindowMetadataFunction implements ProcessWindowFunction<AggResult, WindowSummary, String, TimeWindow> {
    @Override
    public void process(String category, Context ctx, 
                        Iterable<AggResult> results, 
                        Collector<WindowSummary> out) {
        // results has exactly ONE element (from AggregateFunction)
        AggResult agg = results.iterator().next();
        
        out.collect(new WindowSummary(
            category,
            ctx.window().getStart(),    // window_start timestamp
            ctx.window().getEnd(),      // window_end timestamp
            agg.getCount(),
            agg.getSum(),
            agg.getAvg(),
            ctx.currentWatermark()      // watermark at time of firing
        ));
    }
}

/*
 Pattern: Combining session windows with Flink SQL
 (Session windows are supported in Flink SQL via SESSION_GAP function)
*/
-- Flink SQL: session window (30-minute gap)
SELECT
    user_id,
    SESSION_START(event_time, INTERVAL '30' MINUTE) AS session_start,
    SESSION_END(event_time,   INTERVAL '30' MINUTE) AS session_end,
    COUNT(*)   AS page_views,
    SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchases,
    SUM(CASE WHEN event_type = 'purchase' THEN amount ELSE 0 END) AS revenue
FROM events
GROUP BY
    user_id,
    SESSION(event_time, INTERVAL '30' MINUTE);
```

---

## Interview Tips

> **Tip 1:** "How does Flink's Chandy-Lamport checkpoint algorithm handle out-of-order events?" — Flink's Chandy-Lamport inserts checkpoint barriers into the data stream between events. When an operator receives barriers from all upstream channels, it snapshots state and forwards the barrier. Out-of-order events complicate this: if operator A receives barrier from upstream 1 but not upstream 2 yet, it must buffer events arriving from upstream 1 (to maintain ordering before the barrier point). This "alignment" causes head-of-line blocking — upstream 1 is blocked waiting for upstream 2's barrier. Unaligned checkpoints (Flink 1.11+) solve this by including in-flight buffer data in the snapshot, avoiding blocking. The trade-off: larger checkpoint size and more complex recovery.

> **Tip 2:** "What's the memory impact of using sliding windows vs tumbling windows?" — A sliding window with window_size W and slide_interval S requires each event to be stored in W/S windows simultaneously. Example: 5-minute window with 1-minute slide → each event in 5 windows at once → 5× the state size of a tumbling window. For large sliding windows: prefer `AggregateFunction` (O(1) per window per accumulator) over `ProcessWindowFunction` that collects all records. With RocksDB: 1M events × 5 windows × 500 bytes = 2.5 GB state (manageable). With in-memory: same data causes GC pressure. Always use `AggregateFunction` for sliding windows to keep state bounded at O(keys × windows) not O(keys × windows × events).

> **Tip 3:** "How do you handle a Kafka topic where one partition receives no events for hours (e.g., a rarely-used partition)?" — An idle Kafka partition causes the watermark to stall at its last event's timestamp. All windows waiting for watermark advancement will never fire. Solution: `withIdleness(Duration.ofMinutes(N))` — after N minutes without an event, Flink marks the partition as "idle" and excludes it from watermark calculation. The remaining active partitions advance the watermark independently. When the idle partition receives events again, it rejoins the watermark calculation (the global watermark may decrease temporarily if the idle partition's resuming events are old). Always use idle detection for any production job sourcing from Kafka — sparse partitions are common.
