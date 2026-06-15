---
title: "Event-Driven Architecture — Intermediate"
topic: system-design
subtopic: event-driven-architecture
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [system-design, event-driven, outbox, saga, schema-registry, cqrs]
---

# Event-Driven Architecture — Intermediate

## CQRS in Data Systems

**Command Query Responsibility Segregation:** separate the write model (commands) from the read model (queries).

### Why CQRS for Data Engineering?

In a traditional OLTP database, every query and write hits the same tables. At scale this causes:
- Analytics queries slow down transactional writes
- Schema optimized for writes (normalized) is terrible for reads (queries need many JOINs)
- Cannot scale reads independently from writes

CQRS solves this by maintaining separate models:

```
Commands (writes):           Queries (reads):
  Order placed →               Analyst dashboard →
  OrdersDB (normalized)        OrdersReadModel (denormalized, fast)
  (OLTP, write-optimized)      (OLAP, query-optimized)

Sync mechanism: Events
  OrderPlaced event →
    Updates OrdersDB (write model)
    AND updates OrdersReadModel (read model) via async event consumer
```

### Data Engineering Implementation

```python
# Write model: normalized PostgreSQL (OLTP)
# Read model: denormalized Redshift/BigQuery (OLAP)

# Event: OrderPlaced
# Consumer 1: Update OLTP (synchronous, in transaction)
# Consumer 2: Update OLAP (async, Kafka consumer)

class OLAPOrderConsumer:
    """Maintains denormalized orders table in Redshift for analytics."""
    
    def consume(self, event: dict):
        if event["event_type"] == "OrderPlaced":
            self.redshift.execute("""
                INSERT INTO orders_denormalized
                SELECT
                    o.order_id, o.customer_id, o.total_amount,
                    c.name AS customer_name, c.country AS customer_country,
                    p.category AS product_category
                FROM orders_staging o
                JOIN customers c ON o.customer_id = c.id
                JOIN products p ON o.product_id = p.id
                WHERE o.order_id = %s
            """, [event["data"]["order_id"]])
```

---

## The Outbox Pattern

**The problem:** You need to write to a database AND publish an event atomically. If you write to DB then publish, the app could crash between the two — DB updated but no event emitted.

```
WRONG (dual write — race condition):
  1. INSERT order INTO postgres  ✓
  2. [crash]
  3. publish OrderPlaced to Kafka  ✗ (never happens)
  Result: DB has the order, no one knows about it
```

### Outbox Solution

```sql
-- Same transaction: write to orders AND to outbox
BEGIN;

INSERT INTO orders (order_id, customer_id, amount, status)
VALUES ('ord-123', 'cust-456', 99.99, 'PLACED');

INSERT INTO outbox_events (event_id, event_type, payload, created_at, published)
VALUES (
    gen_random_uuid(),
    'OrderPlaced',
    '{"order_id": "ord-123", "customer_id": "cust-456", "amount": 99.99}',
    NOW(),
    FALSE  -- not yet published
);

COMMIT;  -- Both writes succeed or both fail (atomic)
```

```python
# Outbox publisher: polls unpublished events, sends to Kafka
# Runs as a separate process (or Debezium reads outbox table)

def publish_outbox_events():
    while True:
        events = db.query("""
            SELECT * FROM outbox_events
            WHERE published = FALSE
            ORDER BY created_at
            LIMIT 100
            FOR UPDATE SKIP LOCKED  -- handles multiple publisher instances
        """)
        
        for event in events:
            kafka_producer.send(
                topic=event["event_type"].lower(),
                key=event["event_id"],
                value=event["payload"]
            )
            kafka_producer.flush()  # Ensure delivered
            
            db.execute("""
                UPDATE outbox_events SET published = TRUE
                WHERE event_id = %s
            """, [event["event_id"]])
        
        time.sleep(0.1)  # 100ms poll interval
```

**Better: Debezium Outbox Transform**
```json
{
  "transforms": "outbox",
  "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
  "transforms.outbox.table.fields.additional.placement": "type:header:eventType"
}
```
Debezium watches the outbox table via CDC and publishes to Kafka automatically — no polling process needed.

---

## Schema Evolution for Events

Events are the API contract between services. Breaking changes break consumers.

### Schema Registry

```
Producer (Order Service)         Consumer (Analytics Service)
      │                                   │
      │ Publishes Avro-encoded event       │
      ▼                                   ▼
[Confluent Schema Registry] ← validates schema compatibility
  Stores: schema ID → schema definition
  Enforces: BACKWARD / FORWARD / FULL compatibility

Producer sends: [magic byte][schema_id][avro bytes]
Consumer decodes: looks up schema_id → decodes bytes with that schema
```

### Compatibility Modes Explained

```
BACKWARD (default): New schema can READ data written with old schema
  Safe changes: add optional field (with default), remove field
  Breaking: remove required field, change field type

FORWARD: Old schema can READ data written with new schema
  Safe changes: add required field, remove optional field
  Breaking: renaming a field

FULL: Both BACKWARD and FORWARD
  Most restrictive, safest for long-lived topics
```

### Avro Schema Evolution Example

```json
// Schema v1.0 — initial
{
  "type": "record",
  "name": "OrderPlaced",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "double"}
  ]
}

// Schema v1.1 — BACKWARD COMPATIBLE: add optional field with default
{
  "type": "record",
  "name": "OrderPlaced",
  "fields": [
    {"name": "order_id", "type": "string"},
    {"name": "amount", "type": "double"},
    {"name": "currency", "type": "string", "default": "USD"}  // ← safe addition
  ]
}

// Schema v2.0 — BREAKING: remove field (old consumers expect it)
// DON'T DO THIS without migrating all consumers first
```

---

## Event Ordering Guarantees

### The Partition Key Problem

```python
# WRONG: random partition key → events for same order go to different partitions
# → no ordering guarantee for same order
producer.send("orders", value=event)  # no key → round-robin

# RIGHT: use entity ID as key → all events for same order go to same partition
producer.send(
    topic="orders",
    key=b"ord-123",    # Kafka hashes key → deterministic partition
    value=event_bytes
)
# Now: OrderPlaced, OrderShipped, OrderDelivered for ord-123 → all to partition 2
# Consumers of partition 2 see events in order
```

### Global Ordering (When You Need It)

```
If you need strict global ordering across all events:
  Option 1: Single partition (1 partition = total order, but no parallelism)
  Option 2: Sequence numbers from a centralized counter
  Option 3: Event time + watermarks (stream processors like Flink)

In practice: per-entity ordering (by partition key) is sufficient for 99% of use cases.
Global ordering doesn't scale.
```

---

## Saga Pattern for Distributed Transactions

When a business operation spans multiple services, you can't use a single database transaction.

### Example: Place Order Saga

```
Customer places order → must:
1. Reserve inventory (Inventory Service)
2. Charge payment (Payment Service)
3. Schedule delivery (Delivery Service)

If any step fails → compensate all previous steps
```

### Choreography-based Saga (event-driven)

```
Order Service:       publishes OrderCreated
Inventory Service:   consumes OrderCreated → reserves stock → publishes InventoryReserved
                     OR publishes InventoryReservationFailed
Payment Service:     consumes InventoryReserved → charges card → publishes PaymentCharged
                     OR publishes PaymentFailed
Delivery Service:    consumes PaymentCharged → schedules delivery → publishes DeliveryScheduled

Compensation:
  PaymentFailed → Order Service consumes → cancels order
                  Inventory Service consumes → releases reserved stock
```

```python
# Choreography: each service only knows about events, not other services
class InventoryService:
    def on_order_created(self, event: dict):
        order_id = event["data"]["order_id"]
        items = event["data"]["items"]
        
        try:
            self.reserve_inventory(order_id, items)
            self.kafka.publish("InventoryReserved", {
                "order_id": order_id,
                "reservation_id": str(uuid4())
            })
        except InsufficientStockError:
            self.kafka.publish("InventoryReservationFailed", {
                "order_id": order_id,
                "reason": "INSUFFICIENT_STOCK"
            })
```

### Orchestration-based Saga (central coordinator)

```python
# One saga orchestrator manages the whole flow
class OrderSagaOrchestrator:
    def execute(self, order_id: str):
        try:
            # Step 1: Reserve inventory (synchronous call or async with wait)
            reservation_id = inventory_client.reserve(order_id)
            
            # Step 2: Charge payment
            payment_id = payment_client.charge(order_id)
            
            # Step 3: Schedule delivery
            delivery_id = delivery_client.schedule(order_id, payment_id)
            
            return {"status": "SUCCESS", "delivery_id": delivery_id}
        
        except PaymentFailedError:
            # Compensate: release inventory reservation
            inventory_client.release(reservation_id)
            return {"status": "FAILED", "reason": "payment_failed"}
```

**Choreography vs Orchestration trade-off:**

| | Choreography | Orchestration |
|---|---|---|
| Coupling | Low (services only know events) | Higher (orchestrator knows all services) |
| Visibility | Hard to see the full flow | Easy (orchestrator logs each step) |
| Failure handling | Complex (each service must handle compensation) | Centralized |
| Scalability | Better (no central bottleneck) | Orchestrator can be bottleneck |
| Best for | Simple flows, <4 steps | Complex flows, need auditability |

---

## Event-Driven ETL: CDC → Kafka → Lakehouse

```
MySQL (source)
   │ binary log
   ▼
Debezium (reads binlog) → Kafka topic per table
   │
   ▼
Flink (stream processor)
   │ enrichment: JOIN with reference data from Kafka compacted topic
   │ transformation: parse, flatten, clean
   ▼
Delta Lake (upsert via MERGE INTO)
   │
   ▼
dbt (incremental models, runs every 15 min)
   │
   ▼
Redshift / BigQuery (analytical queries)
```

**Flink enrichment pattern:**
```java
// Enrich order events with customer data (from compacted "customers" Kafka topic)
DataStream<Order> orders = kafkaSource("orders");
DataStream<Customer> customers = kafkaSource("customers");  // compacted = latest value per key

orders
  .connect(customers.keyBy(Customer::getId))
  .process(new EnrichmentFunction())  // looks up customer by ID
  .addSink(deltaLakeSink);
```
