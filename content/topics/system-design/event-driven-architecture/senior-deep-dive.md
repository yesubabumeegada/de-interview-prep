---
title: "Event-Driven Architecture — Senior Deep Dive"
topic: system-design
subtopic: event-driven-architecture
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [system-design, event-driven, event-sourcing, exactly-once, kafka-streams]
---

# Event-Driven Architecture — Senior Deep Dive

## Event Sourcing at Production Scale

Event sourcing stores every state change as an immutable event. Current state is always derived by replaying events from the log. This is not just a pattern — at scale it has deep architectural implications.

### The Aggregation Problem

When an entity (e.g., a bank account) has 10 years of events, replaying all events every time you need the balance is impractical.

**Solution: Snapshots**

```python
class AccountAggregate:
    def __init__(self):
        self.balance = 0
        self.version = 0
    
    def apply(self, event: dict):
        if event["type"] == "Deposited":
            self.balance += event["amount"]
        elif event["type"] == "Withdrawn":
            self.balance -= event["amount"]
        self.version += 1
    
    @classmethod
    def rehydrate(cls, account_id: str, event_store, snapshot_store):
        """Load account state efficiently using snapshot + recent events."""
        # Step 1: Load most recent snapshot
        snapshot = snapshot_store.get_latest(account_id)
        
        account = cls()
        if snapshot:
            account.balance = snapshot["balance"]
            account.version = snapshot["version"]
        
        # Step 2: Replay only events AFTER the snapshot
        events = event_store.get_events_after_version(account_id, account.version)
        for event in events:
            account.apply(event)
        
        return account

# Snapshot trigger: create snapshot every 100 events
def save_snapshot_if_needed(account: AccountAggregate, account_id: str):
    if account.version % 100 == 0:
        snapshot_store.save({
            "account_id": account_id,
            "version": account.version,
            "balance": account.balance,
            "snapshot_ts": datetime.utcnow().isoformat()
        })
```

### Event Store Design

```
EventStoreDB (or Kafka as event store):
  
  Stream: "account-1001"
    Event 1: AccountOpened { initial_balance: 0 }
    Event 2: MoneyDeposited { amount: 1000, ts: ... }
    Event 3: MoneyWithdrawn { amount: 200, ts: ... }
    Event 4: [SNAPSHOT] { balance: 800, version: 3 }
    Event 5: MoneyDeposited { amount: 500, ts: ... }
  
  Current state: replay from Event 4 snapshot + Event 5
  → balance = 800 + 500 = 1300

Kafka as event store:
  - Infinite retention (tiered storage to S3)
  - Log compaction for entity streams (keep latest snapshot per key)
  - Partitioned by entity_id for ordering guarantee
```

---

## Exactly-Once Semantics in Kafka

### Why Exactly-Once Is Hard

```
At-least-once scenario:
  1. Consumer reads event from Kafka
  2. Consumer processes event (inserts to DB)
  3. Consumer crashes BEFORE committing offset
  4. Consumer restarts, re-reads same event
  5. Consumer processes again → DUPLICATE

At-most-once scenario:
  1. Consumer reads event
  2. Consumer commits offset (marks as processed)
  3. Consumer crashes BEFORE processing
  4. Event is lost — offset advanced past it
```

### Kafka Transactions (Exactly-Once)

```java
// Producer: transactional — ensures message sent exactly once
Properties producerProps = new Properties();
producerProps.put("transactional.id", "order-processor-001");
producerProps.put("enable.idempotence", "true");
producerProps.put("acks", "all");

KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
producer.initTransactions();

try {
    producer.beginTransaction();
    
    // Read from input topic, process, write to output topic
    // All writes are part of the transaction
    producer.send(new ProducerRecord<>("processed-orders", key, processedEvent));
    
    // Commit offset of input record as part of the same transaction
    producer.sendOffsetsToTransaction(offsetsToCommit, consumerGroupMetadata);
    
    producer.commitTransaction();
} catch (Exception e) {
    producer.abortTransaction();  // Rollback: message not sent
}
```

### Flink Exactly-Once with Checkpointing

```java
// Flink: exactly-once end-to-end via 2-phase commit
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Enable checkpointing every 30 seconds
env.enableCheckpointing(30_000);
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// Kafka source: reads from committed offsets only
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers("kafka:9092")
    .setTopics("orders")
    .setStartingOffsets(OffsetsInitializer.committedOffsets())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();

// Kafka sink: uses Kafka transactions for exactly-once
KafkaSink<String> sink = KafkaSink.<String>builder()
    .setBootstrapServers("kafka:9092")
    .setRecordSerializer(...)
    .setDeliverGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
    .setTransactionalIdPrefix("flink-orders-processor")
    .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
   .process(new OrderProcessor())
   .sinkTo(sink);
```

---

## Event Time vs Processing Time

A critical concept for stream processing correctness.

```
Event time: when the event actually happened (timestamp in the event payload)
Processing time: when the event arrives at the stream processor

Example:
  Customer orders at 11:58 PM on Jan 31 (event time)
  Mobile app was offline → event delivered at 12:02 AM on Feb 1 (processing time)
  
  Processing-time window (midnight): misses this order → wrong Feb sales count
  Event-time window (midnight): correctly counts this in Jan → correct
```

### Watermarks in Flink

```java
// Watermark: tells Flink "all events up to time T have arrived"
// Triggers window computations even when some events are late

WatermarkStrategy<Order> watermarkStrategy = WatermarkStrategy
    .<Order>forBoundedOutOfOrderness(Duration.ofMinutes(5))  // Allow 5 min late arrivals
    .withTimestampAssigner((order, recordTimestamp) -> order.getEventTimestamp());

DataStream<Order> orders = env
    .fromSource(kafkaSource, watermarkStrategy, "orders")
    .keyBy(Order::getCustomerId)
    .window(TumblingEventTimeWindows.of(Time.hours(1)))
    .aggregate(new RevenueAggregator());

// What happens to events that arrive > 5 min late?
// Option 1: Drop them (default)
// Option 2: Route to side output for separate processing
OutputTag<Order> lateOrdersTag = new OutputTag<Order>("late-orders"){};
SingleOutputStreamOperator<Revenue> result = orders
    .window(...)
    .sideOutputLateData(lateOrdersTag)
    .aggregate(new RevenueAggregator());

DataStream<Order> lateOrders = result.getSideOutput(lateOrdersTag);
lateOrders.addSink(lateOrdersSink);  // Store for reconciliation
```

---

## Event Compaction and Retention Strategies

### Log Compaction (Kafka)

For entity state streams, you don't need all historical events — just the latest value per key.

```
Topic with log compaction enabled:
  Time → [k1:v1, k2:v1, k1:v2, k3:v1, k2:v2, k1:v3]
  After compaction: [k1:v3, k2:v2, k3:v1]  ← latest value per key

Use cases:
  - Customer profile stream (latest customer record)
  - Product catalog (latest product price)
  - Configuration (latest config per service)

NOT for: event sourcing full history (use infinite retention there)
```

```properties
# Enable log compaction on a topic
kafka-topics.sh --create \
  --topic customer-profiles \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.1 \
  --config segment.ms=86400000  # Compact daily
```

### Tiered Storage for Long Retention

```
Kafka tiered storage (Confluent / MSK):
  Hot tier (local disk): last 7 days
  Cold tier (S3 / GCS): 1 year
  
Query: Consumer transparently reads from either tier
Cost: S3 tier costs ~10× less than local Kafka disk
```

---

## Event-Driven Pipeline Observability

### End-to-End Event Tracing

```python
# Inject trace ID into every event at the producer
import opentelemetry.trace as trace

tracer = trace.get_tracer("order-service")

def place_order(order_data: dict) -> dict:
    with tracer.start_as_current_span("place_order") as span:
        span.set_attribute("order_id", order_data["order_id"])
        
        # Inject trace context into Kafka message headers
        trace_headers = {}
        propagate.inject(trace_headers)  # OpenTelemetry W3C TraceContext
        
        kafka_producer.send(
            "orders",
            value=json.dumps(order_data).encode(),
            headers=[(k, v.encode()) for k, v in trace_headers.items()]
        )

# Consumer: extract trace context and continue the trace
def consume_order(message: ConsumerRecord):
    # Restore trace context from Kafka message headers
    headers = {k: v.decode() for k, v in message.headers}
    context = propagate.extract(headers)
    
    with tracer.start_as_current_span("process_order", context=context) as span:
        span.set_attribute("kafka_offset", message.offset)
        span.set_attribute("kafka_partition", message.partition)
        process(message.value)

# Result: distributed trace from HTTP request → Kafka → consumer → DB
# Visible in Jaeger / Honeycomb / Datadog APM
```

### Consumer Lag Monitoring

```python
# Alert when consumer lag exceeds threshold
from kafka import KafkaAdminClient, KafkaConsumer
from datadog import initialize, api

def get_consumer_lag(topic: str, group_id: str) -> dict:
    admin = KafkaAdminClient(bootstrap_servers="kafka:9092")
    
    # Get current end offsets (latest messages)
    end_offsets = admin.list_consumer_group_offsets(
        group_id=group_id
    )
    
    # Compare with consumer committed offsets
    # Lag = end_offset - committed_offset per partition
    lag_by_partition = {}
    for tp, offset_meta in end_offsets.items():
        if tp.topic == topic:
            committed = offset_meta.offset
            end = get_end_offset(tp)
            lag_by_partition[tp.partition] = end - committed
    
    total_lag = sum(lag_by_partition.values())
    
    # Send to Datadog
    api.Metric.send(
        metric=f"kafka.consumer.lag.{group_id}",
        points=[(time.time(), total_lag)],
        tags=[f"topic:{topic}", f"group:{group_id}"]
    )
    
    if total_lag > 100_000:
        alert(f"High lag: {total_lag} messages behind on {topic}/{group_id}")
    
    return lag_by_partition
```

---

## Schema Registry at Enterprise Scale

```
Confluent Schema Registry (typical enterprise setup):

Production:
  - 3-node Schema Registry cluster (HA)
  - Backed by a compacted Kafka topic (_schemas)
  - Read-heavy: cache schemas in consumers with 1-hour TTL

CI/CD integration:
  - PR check: validate schema changes don't break BACKWARD compatibility
  - Automated: schema-registry-validate tool in GitHub Actions
  - Block merge if breaking change detected

Cross-environment promotion:
  Dev → Staging → Production schema migration
  New schema version registered in Dev
  Same ID range reserved in Staging, Prod (avoid conflicts)
  Automated promotion script with human approval gate for FULL_TRANSITIVE changes
```
