---
title: "Apache Flink — Intermediate"
topic: real-time-streaming
subtopic: apache-flink
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [flink, watermarks, windows, checkpointing, state, kafka, exactly-once]
---

# Apache Flink — Intermediate

## Watermarks and Event-Time Windows

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;

// Watermark strategy: allows events up to 5 seconds late
WatermarkStrategy<Order> wmStrategy = WatermarkStrategy
    .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
    .withTimestampAssigner((order, recordTimestamp) -> order.getEventTimestamp());

DataStream<Order> ordersWithWatermarks = rawStream
    .assignTimestampsAndWatermarks(wmStrategy);

// Tumbling event-time window (non-overlapping 1-minute windows)
DataStream<WindowResult> results = ordersWithWatermarks
    .keyBy(Order::getCategory)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .apply(new WindowFunction<Order, WindowResult, String, TimeWindow>() {
        @Override
        public void apply(
                String key,
                TimeWindow window,
                Iterable<Order> orders,
                Collector<WindowResult> out) {
            
            double total = 0;
            int count = 0;
            for (Order o : orders) {
                total += o.getAmount();
                count++;
            }
            
            out.collect(new WindowResult(
                key,
                window.getStart(),
                window.getEnd(),
                total,
                count
            ));
        }
    });

// Handle late events that arrive after watermark
OutputTag<Order> lateOutputTag = new OutputTag<Order>("late-events"){};

SingleOutputStreamOperator<WindowResult> mainStream = ordersWithWatermarks
    .keyBy(Order::getCategory)
    .window(TumblingEventTimeWindows.of(Time.minutes(1)))
    .allowedLateness(Time.seconds(30))  // keep window state 30s after watermark passes
    .sideOutputLateData(lateOutputTag)  // route truly late events to side output
    .apply(/* window function */);

// Access late events stream
DataStream<Order> lateEvents = mainStream.getSideOutput(lateOutputTag);
lateEvents.addSink(/* write to DLQ */);
```

---

## Stateful Processing with Keyed State

```java
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

// Example: detect fraudulent transactions (3+ transactions over $1000 in 10 minutes)
public class FraudDetector extends KeyedProcessFunction<String, Transaction, Alert> {
    
    // State: count of large transactions in current 10-min window
    private ValueState<Integer> largeTransactionCount;
    // State: timer handle to clear count after 10 minutes
    private ValueState<Long> timerState;
    
    @Override
    public void open(Configuration config) {
        largeTransactionCount = getRuntimeContext().getState(
            new ValueStateDescriptor<>("large-tx-count", Integer.class, 0)
        );
        timerState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("timer", Long.class)
        );
    }
    
    @Override
    public void processElement(Transaction tx, Context ctx, Collector<Alert> out) 
            throws Exception {
        
        if (tx.getAmount() > 1000.0) {
            int count = largeTransactionCount.value() + 1;
            largeTransactionCount.update(count);
            
            // Register timer if not already set (fires after 10 min of inactivity)
            if (timerState.value() == null) {
                long timer = ctx.timerService().currentProcessingTime() + 600_000L;
                ctx.timerService().registerProcessingTimeTimer(timer);
                timerState.update(timer);
            }
            
            if (count >= 3) {
                out.collect(new Alert(tx.getAccountId(), "3+ large transactions in 10 min"));
                // Reset state
                largeTransactionCount.clear();
                ctx.timerService().deleteProcessingTimeTimer(timerState.value());
                timerState.clear();
            }
        }
    }
    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<Alert> out) 
            throws Exception {
        // Timer fired: reset window state
        largeTransactionCount.clear();
        timerState.clear();
    }
}

// Usage:
DataStream<Alert> alerts = transactions
    .keyBy(Transaction::getAccountId)
    .process(new FraudDetector());
```

---

## Checkpointing and Exactly-Once

```java
// Configure checkpointing for fault tolerance
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Enable checkpointing every 60 seconds
env.enableCheckpointing(60_000);

// Checkpoint configuration
env.getCheckpointConfig()
    .setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)   // vs AT_LEAST_ONCE
    .setMinPauseBetweenCheckpoints(30_000)                   // min 30s between starts
    .setCheckpointTimeout(120_000)                           // fail if takes > 2 min
    .setMaxConcurrentCheckpoints(1)                          // only 1 checkpoint at a time
    .enableExternalizedCheckpoints(                          // retain on job cancellation
        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
    );

// Configure checkpoint storage (S3 or HDFS)
env.getCheckpointConfig().setCheckpointStorage("s3://my-bucket/flink-checkpoints/");

/*
 How checkpointing works (Chandy-Lamport algorithm):
 
 1. JobManager triggers checkpoint (sends barrier to sources)
 2. Sources inject checkpoint barrier into data stream (after current records)
 3. Each operator:
    a. Receives barrier from all upstream channels
    b. Snapshots its state to the checkpoint backend
    c. Forwards barrier downstream
 4. When sink receives barrier: confirms checkpoint to JobManager
 5. JobManager marks checkpoint complete
 
 Recovery:
   Job fails → restart from last completed checkpoint
   Sources reset to saved offsets (Kafka committed offset)
   All operators restore state from checkpoint
   Processing resumes exactly where it left off
   
 Result: no duplicate output (exactly-once) when combined with idempotent sinks
         or transactional sinks (Kafka, Iceberg, etc.)
*/

// Checkpoint backends:
// 1. HashMapStateBackend (default): state in TaskManager JVM heap
//    - Fast, but limited by heap size; lost if TaskManager crashes (recovered from checkpoint)
// 2. EmbeddedRocksDBStateBackend: state in RocksDB on local disk
//    - Supports very large state (TB-scale), slightly slower for point lookups
//    - Use for jobs with large keyed state (joins, aggregations over millions of keys)

env.setStateBackend(new EmbeddedRocksDBStateBackend());  // for large state
```

---

## Flink SQL and Table API

```sql
-- Flink SQL: SQL interface for streaming and batch
-- Create Kafka source table with event-time
CREATE TABLE orders (
    order_id       STRING,
    category       STRING,
    amount         DOUBLE,
    event_time     TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic'     = 'orders',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id'  = 'flink-sql-job',
    'format'    = 'json',
    'scan.startup.mode' = 'latest-offset'
);

-- Create sink (Iceberg table)
CREATE TABLE order_summary (
    window_start TIMESTAMP(3),
    window_end   TIMESTAMP(3),
    category     STRING,
    total_amount DOUBLE,
    order_count  BIGINT
) WITH (
    'connector' = 'iceberg',
    'catalog-name' = 'hive_catalog',
    'database-name' = 'analytics',
    'table-name' = 'order_summary'
);

-- Tumbling window aggregation in SQL
INSERT INTO order_summary
SELECT 
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(event_time,   INTERVAL '1' MINUTE) AS window_end,
    category,
    SUM(amount)  AS total_amount,
    COUNT(*)     AS order_count
FROM orders
GROUP BY 
    TUMBLE(event_time, INTERVAL '1' MINUTE),
    category;

-- Streaming join: enrich orders with customer data
CREATE TABLE customers (
    customer_id STRING PRIMARY KEY NOT ENFORCED,
    name        STRING,
    tier        STRING
) WITH (
    'connector' = 'jdbc',
    'url'       = 'jdbc:mysql://mysql:3306/customers',
    'table-name'= 'customers'
);

-- Temporal join: look up customer at event time (point-in-time correct)
SELECT 
    o.order_id,
    o.amount,
    c.tier,
    o.event_time
FROM orders AS o
LEFT JOIN customers FOR SYSTEM_TIME AS OF o.event_time AS c
    ON o.customer_id = c.customer_id;
```

---

## Interview Tips

> **Tip 1:** "What happens to state when a Flink job fails?" — Flink restores state from the last completed checkpoint. The JobManager restarts the job, TaskManagers restore their operator states from the checkpoint backend (RocksDB or heap). Kafka sources reset their offsets to the committed offsets saved in the checkpoint. Processing resumes from that point. Between the checkpoint and the failure, some events may be reprocessed (at-least-once), but if the sink is idempotent or transactional (Kafka EXACTLY_ONCE, Iceberg 2PC), the output is exactly-once end-to-end. Checkpoint frequency determines how much data is reprocessed on recovery.

> **Tip 2:** "When should you use ValueState vs MapState vs ListState?" — ValueState: single value per key (e.g., last seen transaction amount, running count). MapState: key-value map per key (e.g., session data stored as field → value). ListState: list of values per key (e.g., collect all events in a window before emitting). BroadcastState: special state shared across all parallel instances (e.g., lookup rules broadcast to all partitions). For large state: prefer MapState or ListState with RocksDB backend (which stores state on disk). ValueState with large objects degrades heap-based backend performance.

> **Tip 3:** "What is the difference between `allowedLateness` and watermarks?" — Watermarks define when Flink considers a time window complete and fires it (watermark >= window end time). `allowedLateness` extends how long Flink keeps the window state *after* the watermark has passed — allowing late events to update already-fired window results (the window fires again with updated data). Without `allowedLateness`, late events (after watermark) are dropped or sent to side output. With `allowedLateness(Time.seconds(30))`, Flink keeps the window open 30 more seconds to accept truly late events and fire updated results.
