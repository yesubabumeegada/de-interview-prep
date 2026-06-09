---
title: "Apache Flink — Real World"
topic: real-time-streaming
subtopic: apache-flink
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [flink, production, fraud-detection, cdc, kafka, iceberg, real-time]
---

# Apache Flink — Real World

## Pattern 1: Real-Time Fraud Detection Pipeline

```java
/*
 Production fraud detection:
   Kafka (transactions) → Flink → [alerts → Kafka alerts topic]
                                  [all events → Iceberg for audit]
 
 Rules:
   1. Same card, 3+ transactions in 5 minutes → flag
   2. Amount > 5× rolling 30-day average → flag
   3. Country change within 1 hour → flag (impossible travel)
*/

public class FraudDetectionJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(16);
        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true)); // incremental
        
        // Kafka source with event-time
        KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("transactions")
            .setGroupId("fraud-detection-flink")
            .setValueOnlyDeserializer(new TransactionDeserializer())
            .build();
        
        WatermarkStrategy<Transaction> wm = WatermarkStrategy
            .<Transaction>forBoundedOutOfOrderness(Duration.ofSeconds(10))
            .withTimestampAssigner((tx, ts) -> tx.getTimestamp());
        
        DataStream<Transaction> transactions = env
            .fromSource(source, wm, "Kafka Transactions");
        
        // Rule 1: velocity check (3+ in 5 minutes)
        DataStream<FraudAlert> velocityAlerts = transactions
            .keyBy(Transaction::getCardId)
            .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
            .aggregate(new CountAggregator(), new VelocityAlertFunction());
        
        // Rule 2 + 3: stateful per-card fraud detection
        DataStream<FraudAlert> stateAlerts = transactions
            .keyBy(Transaction::getCardId)
            .process(new StatefulFraudDetector());
        
        // Merge all alerts
        DataStream<FraudAlert> allAlerts = velocityAlerts.union(stateAlerts)
            .keyBy(FraudAlert::getCardId)
            .process(new AlertDeduplicator(Duration.ofMinutes(5)));  // dedup within 5 min
        
        // Sink 1: Kafka alerts (transactional = exactly-once)
        KafkaSink<FraudAlert> alertSink = KafkaSink.<FraudAlert>builder()
            .setBootstrapServers("kafka:9092")
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic("fraud-alerts")
                .setValueSerializationSchema(new FraudAlertSerializer())
                .build())
            .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
            .setTransactionalIdPrefix("fraud-flink-")
            .build();
        allAlerts.sinkTo(alertSink);
        
        // Sink 2: Iceberg for audit trail (all transactions)
        TableLoader icebergTable = TableLoader.fromHadoopTable("s3://bucket/iceberg/transactions");
        FlinkSink.forRowData(transactions.map(tx -> tx.toRowData()))
            .tableLoader(icebergTable)
            .upsert(true)               // exactly-once via Iceberg 2PC
            .build();
        
        env.execute("Fraud Detection Job");
    }
}

// Stateful fraud detector (Rule 2: amount spike + Rule 3: impossible travel)
public class StatefulFraudDetector extends KeyedProcessFunction<String, Transaction, FraudAlert> {
    
    private ValueState<Double> avgAmountState;     // rolling 30-day average
    private ValueState<String> lastCountryState;   // last transaction country
    private ValueState<Long> lastTxTimeState;      // last transaction timestamp
    
    @Override
    public void open(Configuration config) {
        avgAmountState   = getRuntimeContext().getState(
            new ValueStateDescriptor<>("avg-amount", Double.class, 0.0));
        lastCountryState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("last-country", String.class));
        lastTxTimeState  = getRuntimeContext().getState(
            new ValueStateDescriptor<>("last-tx-time", Long.class, 0L));
    }
    
    @Override
    public void processElement(Transaction tx, Context ctx, Collector<FraudAlert> out)
            throws Exception {
        
        double avgAmount  = avgAmountState.value();
        String lastCountry = lastCountryState.value();
        long lastTxTime   = lastTxTimeState.value();
        
        // Rule 2: amount > 5× average
        if (avgAmount > 0 && tx.getAmount() > 5 * avgAmount) {
            out.collect(new FraudAlert(tx.getCardId(), "AMOUNT_SPIKE",
                String.format("Amount %.2f is %.1f× average %.2f",
                    tx.getAmount(), tx.getAmount() / avgAmount, avgAmount)));
        }
        
        // Rule 3: impossible travel (different country within 1 hour)
        if (lastCountry != null && !lastCountry.equals(tx.getCountry())) {
            long hourInMs = 3_600_000L;
            if (tx.getTimestamp() - lastTxTime < hourInMs) {
                out.collect(new FraudAlert(tx.getCardId(), "IMPOSSIBLE_TRAVEL",
                    String.format("Country %s → %s in %d minutes",
                        lastCountry, tx.getCountry(),
                        (tx.getTimestamp() - lastTxTime) / 60_000)));
            }
        }
        
        // Update state with exponential moving average (α=0.1)
        double alpha = 0.1;
        avgAmountState.update(avgAmount == 0 ? tx.getAmount()
            : alpha * tx.getAmount() + (1 - alpha) * avgAmount);
        lastCountryState.update(tx.getCountry());
        lastTxTimeState.update(tx.getTimestamp());
    }
}
```

---

## Pattern 2: CDC-to-Analytical Store with Flink

```java
/*
 Pattern: Debezium CDC events (Kafka) → Flink → Iceberg (upsert)
 Goal: keep Iceberg table in sync with MySQL source table in near-real-time
*/

// Flink CDC connector (flink-connector-debezium)
// Reads Debezium JSON from Kafka and materializes changes into Iceberg

// Kafka source: Debezium change events
KafkaSource<String> cdcSource = KafkaSource.<String>builder()
    .setBootstrapServers("kafka:9092")
    .setTopics("mysql.ecommerce.orders")  // Debezium topic: server.db.table
    .setGroupId("flink-cdc-iceberg")
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .setStartingOffsets(OffsetsInitializer.earliest())
    .build();

DataStream<String> cdcStream = env.fromSource(
    cdcSource, WatermarkStrategy.noWatermarks(), "Debezium CDC Source");

// Parse Debezium envelope and apply to Iceberg
DataStream<RowData> rowChanges = cdcStream
    .flatMap(new DebeziumToRowDataMapper());  // converts op=c/u/d to INSERT/UPDATE_AFTER/DELETE RowKind

// Write to Iceberg with upsert (handles inserts, updates, deletes)
FlinkSink.forRowData(rowChanges)
    .tableLoader(TableLoader.fromHadoopTable("s3://bucket/iceberg/orders"))
    .upsert(true)
    .equalityFieldColumns(Arrays.asList("order_id"))   // primary key for upsert
    .build();

// DebeziumToRowDataMapper parses:
// {"before": {...}, "after": {...}, "op": "u"} → UPDATE_BEFORE + UPDATE_AFTER rows
// {"before": {...}, "after": null, "op": "d"} → DELETE row
// {"before": null, "after": {...}, "op": "c"} → INSERT row
```

---

## Pattern 3: Multi-Stream Join

```java
/*
 Pattern: join orders stream with payments stream to detect unmatched orders
 Orders arrive on one Kafka topic, payments on another
 Alert if order has no matching payment within 5 minutes
*/

DataStream<Order> orders = env.fromSource(orderSource, wmOrders, "Orders");
DataStream<Payment> payments = env.fromSource(paySource, wmPayments, "Payments");

// CoProcessFunction: process two streams together
DataStream<String> alerts = orders
    .connect(payments)
    .keyBy(Order::getOrderId, Payment::getOrderId)
    .process(new OrderPaymentMatcher());

public class OrderPaymentMatcher 
        extends CoProcessFunction<Order, Payment, String> {
    
    // State: unmatched orders (waiting for payment)
    private MapState<String, Order> pendingOrders;
    // State: payments that arrived before their order
    private MapState<String, Payment> earlyPayments;
    
    @Override
    public void open(Configuration config) {
        pendingOrders = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("pending-orders", String.class, Order.class));
        earlyPayments = getRuntimeContext().getMapState(
            new MapStateDescriptor<>("early-payments", String.class, Payment.class));
    }
    
    @Override
    public void processElement1(Order order, Context ctx, Collector<String> out) 
            throws Exception {
        // Check if payment already arrived
        Payment p = earlyPayments.get(order.getOrderId());
        if (p != null) {
            out.collect("MATCHED: " + order.getOrderId());
            earlyPayments.remove(order.getOrderId());
        } else {
            pendingOrders.put(order.getOrderId(), order);
            // Set timer: alert if no payment in 5 minutes
            ctx.timerService().registerEventTimeTimer(
                order.getTimestamp() + 300_000L);
        }
    }
    
    @Override
    public void processElement2(Payment payment, Context ctx, Collector<String> out)
            throws Exception {
        Order o = pendingOrders.get(payment.getOrderId());
        if (o != null) {
            out.collect("MATCHED: " + payment.getOrderId());
            pendingOrders.remove(payment.getOrderId());
        } else {
            earlyPayments.put(payment.getOrderId(), payment);
        }
    }
    
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
            throws Exception {
        // Timer fired: check if order still unmatched
        String orderId = /* derive from timer */ null;
        // In practice: store orderId in the timer or use timerService state
        // This fires 5 min after order arrival if no payment received
        out.collect("UNMATCHED_ORDER_ALERT");
        pendingOrders.clear();  // cleanup
    }
}
```

---

## Interview Tips

> **Tip 1:** "How do you handle Kafka consumer lag in a Flink job?" — Monitor `currentInputWatermark` vs wall-clock time. If lag grows: (a) increase Flink parallelism (more parallel Kafka partitions consumed); (b) check if Kafka has enough partitions to support the desired parallelism (Flink parallelism cannot exceed Kafka partition count for Kafka sources); (c) profile bottleneck operators in Flink Web UI for backpressure; (d) for initial catch-up, set `scan.startup.mode=earliest-offset` and let Flink replay — RocksDB state will build from scratch. Monitor with Prometheus: `flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumerGroupMetrics_records-lag`.

> **Tip 2:** "How do you handle schema evolution in Flink with Kafka and Iceberg?" — Kafka: use Schema Registry (Confluent/Glue) with Avro; schema evolution rules (backward-compatible: add optional fields) prevent deserialization failures. Flink: use Avro's `GenericRecord` instead of Java POJOs for flexible schema handling. Iceberg: supports schema evolution natively (add/drop/rename columns, change types within compatible bounds). For Flink-to-Iceberg: when source schema adds a column, update the Iceberg table schema first (`ALTER TABLE`), then deploy new Flink version from savepoint. Flink's `RowData` handles additional fields gracefully.

> **Tip 3:** "What's the difference between Flink SQL and the DataStream API — when to use which?" — Flink SQL: declarative, lower development cost, good for SQL-savvy teams, supports most streaming operations (tumbling/hopping/session windows, temporal joins, MATCH_RECOGNIZE for CEP). DataStream API: procedural, full control over state management, custom windowing logic, custom timers, complex CEP patterns, non-SQL transformations. In practice: use Flink SQL for standard aggregations and joins (80% of use cases), drop down to DataStream API when you need custom state logic (fraud detection rules, complex event correlation, custom windowing that doesn't fit SQL semantics). You can mix both in the same job via `StreamTableEnvironment`.
