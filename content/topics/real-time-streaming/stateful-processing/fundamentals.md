---
title: "Stateful Processing — Fundamentals"
topic: real-time-streaming
subtopic: stateful-processing
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [streaming, stateful, state, flink, spark, redis, rocksdb, keyed-state]
---

# Stateful Processing — Fundamentals

## What Is Stateful Processing?

```
Stateless processing: each event processed independently
  Input: record → Transform → Output
  Examples: parse JSON, filter, rename fields
  State: none
  Restart: can restart from any offset (same result)

Stateful processing: event processing depends on history
  Input: record + current state → Update state → (maybe output)
  Examples: running count, fraud detection, session tracking
  State: must be persisted and restored on restart
  
  Without state: "how many orders today?" → impossible
  With state:    running counter per category, updated per event
  
State types in streaming:

  1. Per-operator state:  one state per operator instance
     Example: file offset in a source operator
     
  2. Keyed state:         one state per unique key
     Example: running total per user_id
     This is the most common type
     
  3. Broadcast state:     one state shared across all parallel instances
     Example: fraud rules broadcast to all partitions
```

---

## State in Flink

```java
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

// Most common: keyed state (one state entry per key)
// Must be inside a KeyedStream operator (after keyBy())

public class OrderCounter extends KeyedProcessFunction<String, Order, String> {
    
    // ValueState: single value per key
    private ValueState<Long> orderCount;
    
    // MapState: key-value map per key (e.g., per-product counts)
    private MapState<String, Long> productCounts;
    
    // ListState: list of values per key
    private ListState<Order> recentOrders;
    
    @Override
    public void open(Configuration config) {
        // State descriptors: define state name and type
        orderCount = getRuntimeContext().getState(
            new ValueStateDescriptor<>("order-count", Long.class, 0L)
        );
        productCounts = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("product-counts", String.class, Long.class)
        );
        recentOrders = getRuntimeContext().getListState(
            new ListStateDescriptor<>("recent-orders", Order.class)
        );
    }
    
    @Override
    public void processElement(Order order, Context ctx, Collector<String> out)
            throws Exception {
        
        // ValueState: update order count
        long count = orderCount.value() + 1;
        orderCount.update(count);
        
        // MapState: update count per product
        Long existing = productCounts.get(order.getProductId());
        productCounts.put(order.getProductId(), (existing == null ? 0L : existing) + 1);
        
        // ListState: track last 10 orders
        recentOrders.add(order);
        
        // Emit running total
        out.collect(String.format("User %s: %d total orders", order.getUserId(), count));
    }
}

// Usage:
DataStream<String> orderCounts = orders
    .keyBy(Order::getUserId)
    .process(new OrderCounter());
```

---

## State in Spark Structured Streaming

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Simple stateful aggregation (managed by Spark)
# State: running count + sum per key per window
orders = spark.readStream.format("delta") \
    .load("s3://bucket/delta/silver/orders/") \
    .withWatermark("event_time", "5 minutes")

running_totals = orders \
    .groupBy(
        window("event_time", "1 hour"),
        "user_id"
    ) \
    .agg(
        count("*").alias("order_count"),
        sum("amount").alias("total_spent")
    )
# Spark manages the state (accumulator per key per window)
# State cleared when watermark advances past window

# Custom stateful logic: mapGroupsWithState
from pyspark.sql.streaming import GroupState, GroupStateTimeout

def update_user_stats(user_id, events, state: GroupState):
    """
    Custom state update: track user stats across all their orders.
    State: {total_orders, total_spent, last_order_time}
    """
    # Type annotations for state (Spark needs to know state structure)
    
    if state.hasTimedOut:
        # User inactive for 24 hours → emit final state and clear
        if state.exists:
            final_stats = state.get
            state.remove()
            yield (user_id, final_stats['total_orders'], 
                   final_stats['total_spent'], True)  # is_final=True
        return
    
    # Process new events
    new_events = list(events)
    
    if state.exists:
        current = state.get
    else:
        current = {'total_orders': 0, 'total_spent': 0.0}
    
    updated = {
        'total_orders': current['total_orders'] + len(new_events),
        'total_spent':  current['total_spent'] + sum(e.amount for e in new_events)
    }
    
    state.update(updated)
    state.setTimeoutDuration("24 hours")  # expire after 24h of inactivity
    
    # Emit current stats
    yield (user_id, updated['total_orders'], updated['total_spent'], False)

# Apply custom stateful function
user_stats = orders \
    .withWatermark("event_time", "10 minutes") \
    .groupBy("user_id") \
    .applyInPandasWithState(
        update_user_stats,
        output_schema="user_id string, total_orders long, total_spent double, is_final boolean",
        state_schema="total_orders long, total_spent double",
        output_mode="append",
        timeout_conf=GroupStateTimeout.ProcessingTimeTimeout
    )
```

---

## State Backends

```
State backend: where and how state is stored

Flink backends:
1. HashMapStateBackend (default, in-memory)
   - State stored in JVM heap as Java HashMap
   - Fast (in-memory access)
   - Limited by heap size (OOM if state grows too large)
   - State is in-memory during processing, snapshotted to S3/HDFS on checkpoint
   - Use for: small state (< 1 GB total per TaskManager)

2. EmbeddedRocksDBStateBackend (for large state)
   - State stored in RocksDB on local disk (+ memory cache)
   - Supports TB-scale state per job
   - Slightly slower (disk I/O + serialization)
   - Incremental checkpointing (only upload changed files)
   - Use for: state > 1 GB, millions of unique keys, long retention periods

Spark:
   Default state store: HDFS-backed (state in executor memory + checkpoint to HDFS)
   RocksDB state store (Databricks): same benefits as Flink's RocksDB backend
   
   Enable Spark RocksDB state store:
   spark.conf.set(
     "spark.sql.streaming.stateStore.providerClass",
     "com.databricks.sql.streaming.state.RocksDBStateStoreProvider"
   )

State size estimation:
  Keyed state: num_unique_keys × state_size_per_key
  Example: 1 million users × 100 bytes = 100 MB (fine for heap)
           10 million users × 1 KB = 10 GB (needs RocksDB)
           
State cleanup:
  Without cleanup: state grows forever → OOM
  Cleanup strategies:
    TTL:       expire state after N time of inactivity
    Window:    state tied to window lifecycle (auto-cleaned after window closes)
    Manual:    call state.clear() in processElement when done with a key
```

---

## Interview Tips

> **Tip 1:** "What is the difference between stateful and stateless processing in streaming?" — Stateless: each event is processed independently without reference to previous events (filter, map, parse). Results depend only on current input. Stateful: processing depends on accumulated history across events (running totals, session tracking, fraud detection). State must be persisted to survive failures. Stateful operators are more complex (state management, fault tolerance) but enable richer analytics. Most real-world streaming jobs have a mix: stateless transformation layer followed by stateful aggregation layer.

> **Tip 2:** "Why must stateful operators be inside a `keyBy()` in Flink?" — `keyBy()` ensures all events with the same key go to the same operator instance (partition). If an operator is stateful per key (ValueState, MapState), the state for key "user123" must be on the same machine that processes all events for "user123" — otherwise you'd need distributed state reads/writes (expensive). By routing all events for a key to the same partition, the state is local to the processing thread. This is why Flink (and Spark Structured Streaming) require groupBy/keyBy before stateful operations.

> **Tip 3:** "What happens to state on a Flink job failure?" — Flink periodically checkpoints state to durable storage (S3, HDFS). On failure: Flink restarts the job, restores each operator's state from the last completed checkpoint, and resets source offsets (Kafka) to the committed offsets saved in the same checkpoint. Processing resumes from that point. Any events processed between the last checkpoint and the failure are replayed (at-least-once). With exactly-once semantics (idempotent sinks): replaying those events produces the same output as before — effectively exactly-once end-to-end.
