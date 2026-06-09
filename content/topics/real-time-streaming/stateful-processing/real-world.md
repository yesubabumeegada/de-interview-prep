---
title: "Stateful Processing — Real World"
topic: real-time-streaming
subtopic: stateful-processing
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [streaming, stateful, production, fraud-detection, cep, broadcast-state, rocksdb]
---

# Stateful Processing — Real World

## Pattern 1: Real-Time User Segmentation

```java
/*
 Pattern: stateful per-user segmentation based on streaming behavior
 Segments: HIGH_VALUE, CHURNING, NEW_USER, LOYAL
 
 Rules:
   HIGH_VALUE:  total_spend_30d > $500
   CHURNING:    last_purchase > 14 days ago AND was previously active
   NEW_USER:    account < 30 days old
   LOYAL:       purchase_count_90d > 10
 
 Update: re-evaluate segment on each purchase event
 Output: emit segment change events (Kafka) → downstream marketing systems
*/

public class UserSegmentProcessor 
        extends KeyedProcessFunction<String, PurchaseEvent, SegmentChangeEvent> {
    
    private ValueState<UserSegmentState> segmentState;
    
    @Override
    public void open(Configuration config) {
        StateTtlConfig ttl = StateTtlConfig
            .newBuilder(Time.days(90))           // keep state 90 days after last purchase
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .cleanupInRocksdbCompaction()
            .build();
        
        ValueStateDescriptor<UserSegmentState> desc = 
            new ValueStateDescriptor<>("segment-state", UserSegmentState.class);
        desc.enableTimeToLive(ttl);
        
        segmentState = getRuntimeContext().getState(desc);
    }
    
    @Override
    public void processElement(PurchaseEvent purchase, Context ctx, 
                                Collector<SegmentChangeEvent> out) throws Exception {
        
        UserSegmentState state = segmentState.value();
        if (state == null) {
            state = new UserSegmentState(purchase.getAccountCreatedAt());
        }
        
        // Update state
        state.addPurchase(purchase.getAmount(), purchase.getEventTimeMs());
        
        // Evaluate segments
        String previousSegment = state.getCurrentSegment();
        String newSegment = evaluateSegment(state, purchase.getEventTimeMs());
        
        state.setCurrentSegment(newSegment);
        segmentState.update(state);
        
        // Emit change event if segment changed
        if (!newSegment.equals(previousSegment)) {
            out.collect(new SegmentChangeEvent(
                purchase.getUserId(),
                previousSegment,
                newSegment,
                purchase.getEventTimeMs()
            ));
        }
        
        // Register processing-time timer for CHURNING detection
        // Fire 14 days from now — if no purchase by then, mark as churning
        ctx.timerService().registerProcessingTimeTimer(
            ctx.timerService().currentProcessingTime() + (14L * 24 * 60 * 60 * 1000));
    }
    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, 
                         Collector<SegmentChangeEvent> out) throws Exception {
        
        UserSegmentState state = segmentState.value();
        if (state == null) return;
        
        long daysSinceLastPurchase = 
            (System.currentTimeMillis() - state.getLastPurchaseTime()) / (24 * 60 * 60 * 1000);
        
        if (daysSinceLastPurchase >= 14 && !state.getCurrentSegment().equals("NEW_USER")) {
            String previous = state.getCurrentSegment();
            state.setCurrentSegment("CHURNING");
            segmentState.update(state);
            
            out.collect(new SegmentChangeEvent(
                ctx.getCurrentKey(), previous, "CHURNING", timestamp));
        }
    }
    
    private String evaluateSegment(UserSegmentState state, long currentTimeMs) {
        long accountAgeDays = (currentTimeMs - state.getAccountCreatedMs()) / (24*60*60*1000);
        
        if (accountAgeDays < 30) return "NEW_USER";
        if (state.getTotalSpendLast30Days() > 500.0) return "HIGH_VALUE";
        if (state.getPurchaseCountLast90Days() > 10) return "LOYAL";
        return "STANDARD";
    }
}
```

---

## Pattern 2: Dynamic Fraud Rules with Broadcast State

```java
/*
 Production: fraud rules stored in database, updated dynamically
 Rules change frequently (new patterns discovered daily by fraud team)
 
 Architecture:
   Fraud rules DB → Debezium CDC → Kafka (fraud-rules-changes)
                                       ↓
                   Transactions → Flink (broadcast rules + process tx) → Alerts
*/

public class DynamicFraudRulesJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);
        env.enableCheckpointing(60_000);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        
        // Fraud rules: from Kafka (CDC stream of rule changes)
        DataStream<FraudRuleChange> ruleChanges = env
            .addSource(new FlinkKinesisConsumer<>("fraud-rules-changes", 
                       new FraudRuleChangeDeserializer(), props))
            .name("Rules Source");
        
        // Transactions: from Kafka
        DataStream<Transaction> transactions = env
            .addSource(new FlinkKinesisConsumer<>("transactions", 
                       new TransactionDeserializer(), props))
            .name("Transactions Source");
        
        // Broadcast state descriptor: map of ruleId → FraudRule
        MapStateDescriptor<String, FraudRule> rulesDesc = 
            new MapStateDescriptor<>("active-rules", String.class, FraudRule.class);
        
        BroadcastStream<FraudRuleChange> broadcastRules = 
            ruleChanges.broadcast(rulesDesc);
        
        // Process: each parallel instance applies ALL rules to its partition of transactions
        DataStream<FraudAlert> alerts = transactions
            .keyBy(Transaction::getCardId)
            .connect(broadcastRules)
            .process(new DynamicFraudProcessor(rulesDesc));
        
        // Alert sink
        alerts.sinkTo(
            KafkaSink.<FraudAlert>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                    .setTopic("fraud-alerts")
                    .setValueSerializationSchema(new FraudAlertSerializer())
                    .build())
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .build()
        );
        
        env.execute("Dynamic Fraud Rules");
    }
}

class DynamicFraudProcessor 
        extends KeyedBroadcastProcessFunction<String, Transaction, FraudRuleChange, FraudAlert> {
    
    // Per-card velocity state
    private ValueState<VelocityTracker> velocityState;
    private final MapStateDescriptor<String, FraudRule> rulesDesc;
    
    @Override
    public void processElement(Transaction tx, ReadOnlyContext ctx, Collector<FraudAlert> out)
            throws Exception {
        
        ReadOnlyBroadcastState<String, FraudRule> rules = ctx.getBroadcastState(rulesDesc);
        
        // Apply each enabled rule
        for (Map.Entry<String, FraudRule> entry : rules.immutableEntries()) {
            FraudRule rule = entry.getValue();
            if (!rule.isEnabled()) continue;
            
            FraudAlert alert = applyRule(rule, tx, velocityState);
            if (alert != null) {
                out.collect(alert);
            }
        }
        
        // Update velocity state
        updateVelocity(tx);
    }
    
    @Override
    public void processBroadcastElement(FraudRuleChange change, Context ctx, 
                                         Collector<FraudAlert> out) throws Exception {
        
        BroadcastState<String, FraudRule> rules = ctx.getBroadcastState(rulesDesc);
        
        switch (change.getOperation()) {
            case INSERT:
            case UPDATE:
                rules.put(change.getRuleId(), change.getRule());
                System.out.println("Rule updated: " + change.getRuleId());
                break;
            case DELETE:
                rules.remove(change.getRuleId());
                System.out.println("Rule deleted: " + change.getRuleId());
                break;
        }
        
        // Log rule count for monitoring
        int ruleCount = 0;
        for (Map.Entry<String, FraudRule> e : rules.immutableEntries()) ruleCount++;
        System.out.println("Active rules: " + ruleCount);
    }
}
```

---

## Pattern 3: Real-Time Inventory Management

```python
# Pattern: stateful inventory tracking from order events
# Events: order_placed, order_cancelled, shipment_sent, return_received
# State: current inventory count per product

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.streaming import GroupState, GroupStateTimeout

spark = SparkSession.builder.appName("InventoryManager").getOrCreate()

event_schema = StructType([
    StructField("event_id",    StringType()),
    StructField("product_id",  StringType()),
    StructField("event_type",  StringType()),  # order_placed|cancelled|shipped|returned
    StructField("quantity",    IntegerType()),
    StructField("event_time",  TimestampType())
])

events = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "inventory-events").load() \
    .select(from_json(col("value").cast("string"), event_schema).alias("d")).select("d.*") \
    .withWatermark("event_time", "1 minute")

def update_inventory(product_id, events_iter, state: GroupState):
    """
    Stateful inventory management.
    State: {current_stock: int, reserved: int, sold_ytd: int}
    """
    if state.hasTimedOut:
        # No activity for 24h — keep state but don't emit
        state.setTimeoutDuration("24 hours")
        return iter([])
    
    events = list(events_iter)
    
    if state.exists:
        inventory = state.get
    else:
        inventory = {'current_stock': 1000, 'reserved': 0, 'sold_ytd': 0}
    
    for event in events:
        if event.event_type == 'order_placed':
            inventory['reserved'] += event.quantity
            inventory['current_stock'] -= event.quantity
        elif event.event_type == 'order_cancelled':
            inventory['reserved'] -= event.quantity
            inventory['current_stock'] += event.quantity
        elif event.event_type == 'shipment_sent':
            inventory['reserved'] -= event.quantity
            inventory['sold_ytd'] += event.quantity
        elif event.event_type == 'return_received':
            inventory['current_stock'] += event.quantity
    
    state.update(inventory)
    state.setTimeoutDuration("24 hours")  # reset timeout
    
    # Emit alert if low stock
    results = [{
        'product_id': product_id,
        'current_stock': inventory['current_stock'],
        'reserved': inventory['reserved'],
        'sold_ytd': inventory['sold_ytd'],
        'low_stock_alert': inventory['current_stock'] < 50
    }]
    return iter(results)

inventory_stream = events \
    .groupBy("product_id") \
    .applyInPandasWithState(
        update_inventory,
        output_schema="product_id string, current_stock int, reserved int, sold_ytd int, low_stock_alert boolean",
        state_schema="current_stock int, reserved int, sold_ytd int",
        output_mode="append",
        timeout_conf=GroupStateTimeout.ProcessingTimeTimeout
    )

inventory_stream.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "s3://bucket/ckpt/inventory/") \
    .start("s3://bucket/delta/gold/inventory-state/")
```

---

## Interview Tips

> **Tip 1:** "How do you test stateful streaming operators in isolation?" — Flink provides `KeyedOneInputStreamOperatorTestHarness` and `ProcessFunctionTestHarnesses` for unit testing. You can inject events, advance watermarks, fire timers, and inspect state without running a full Flink cluster. For Spark: use the test harness in `org.apache.spark.sql.execution.streaming` — create a `MemoryStream` source, run the query in a test environment, inspect output tables. For production validation: run the streaming job in "shadow mode" (reading from the same source as production, writing to a shadow table) and compare outputs to the production batch job. State snapshot inspection: `flink savepoint --type native` produces a readable savepoint format; use Flink's `StateSnapshotMigrationTestBase` to validate state integrity.

> **Tip 2:** "What are the implications of using stateful joins in terms of state size and memory?" — Stream-stream stateful joins buffer records from both streams waiting for a matching record from the other stream. State size = sum of both streams' unmatched records. Without time bounds: state grows indefinitely (every order waits forever for its payment, and vice versa). With time bounds (`payment_time BETWEEN order_time AND order_time + 1 hour`): state bounded to 1 hour of data per stream. Estimate state size: (events/sec × 3600 sec × bytes/event) × 2 streams. For 10,000 orders/sec × 3600s × 200 bytes = 7.2 GB per stream = 14.4 GB total. Always use RocksDB for stateful joins at scale.

> **Tip 3:** "How do you handle poison-key problems in stateful processing (one key with extreme state)?" — A poison key has disproportionately large state (e.g., a bot account with millions of events vs. normal users with hundreds). Symptoms: one TaskManager slot is overwhelmed (backpressure on that slot), while others are idle. Solutions: (a) Key-level state size monitoring: alert if a key's state exceeds a threshold; (b) Evict rarely-accessed entries from the key's MapState/ListState (trim to last N items); (c) Detect and route bot traffic to a separate stream (isolated processing); (d) For unbounded-growth keys: cap state explicitly (`ListState`: keep only last 1000 items, rolling off old ones); (e) Composite key: split the poison key into sub-keys (user_id + date_bucket) to distribute load.
