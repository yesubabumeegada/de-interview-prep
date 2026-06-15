---
title: "Event-Driven Architecture — Fundamentals"
topic: system-design
subtopic: event-driven-architecture
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [system-design, event-driven, kafka, streaming, data-engineering]
---

# Event-Driven Architecture — Fundamentals

## Event-Driven vs Request-Driven

**Request-driven (traditional):**
- Service A calls Service B synchronously: "Give me the order status"
- Service B must be available when A makes the call
- Tight coupling: A knows about B, A fails if B is down

**Event-driven:**
- Service A publishes an event: "OrderPlaced { order_id: 123, amount: 49.99 }"
- Any interested service consumes it asynchronously
- Loose coupling: A doesn't know who consumes the event

```
Request-driven:
  Order Service ─── HTTP GET /inventory/123 ──► Inventory Service
                 ◄─────────── 200 OK ──────────

Event-driven:
  Order Service ──► [Kafka: orders topic] ──► Inventory Service (consumer)
                                          ──► Fraud Service (consumer)
                                          ──► Email Service (consumer)
```

**When event-driven is better for data pipelines:**
- Multiple consumers need the same data (fan-out)
- Producer and consumer have different throughput (decoupling via buffer)
- Audit trail needed (events are immutable log)
- Retry/replay needed (Kafka retains events)

---

## The Three Core Patterns

### 1. Event Notification

The simplest pattern: "Something happened, consumers react."

```
Payment processed → PaymentEvent published
  Consumer 1: Update order status
  Consumer 2: Send receipt email
  Consumer 3: Update inventory
```

### 2. Event-Carried State Transfer

Event contains all the data consumers need (no follow-up API call required):

```json
{
  "event_type": "OrderPlaced",
  "event_id": "evt-uuid-123",
  "timestamp": "2024-01-15T10:30:00Z",
  "data": {
    "order_id": "ord-456",
    "customer_id": "cust-789",
    "items": [{"sku": "ABC", "qty": 2, "price": 24.99}],
    "total_amount": 49.98,
    "shipping_address": "123 Main St, New York, NY"
  }
}
```

Consumer doesn't need to call the Order API to get order details — it's all in the event.

### 3. Event Sourcing

The most powerful pattern: **the event log IS the database**. Current state is derived by replaying events.

```
Events (append-only):
  1. AccountOpened { account_id: 1, initial_balance: 0 }
  2. MoneyDeposited { account_id: 1, amount: 100 }
  3. MoneyWithdrawn { account_id: 1, amount: 30 }
  4. MoneyDeposited { account_id: 1, amount: 50 }

Current state (derived):
  balance = 0 + 100 - 30 + 50 = $120
```

**Benefits:**
- Complete audit trail (every state change recorded)
- Replay to rebuild any derived view
- Debug by replaying history
- Time travel (what was the balance on Jan 10?)

---

## Kafka as the Event Bus for Data Engineering

Apache Kafka is the most common event bus for DE pipelines.

### Core Kafka Concepts

```
Topic: "orders"
  Partition 0: [msg0, msg1, msg4, msg7]
  Partition 1: [msg2, msg5, msg8]
  Partition 2: [msg3, msg6, msg9]
```

| Concept | Definition |
|---|---|
| **Topic** | Named stream of events (like a database table for events) |
| **Partition** | Ordered, immutable log within a topic — unit of parallelism |
| **Offset** | Position of a message within a partition |
| **Consumer Group** | Multiple consumers sharing work — each partition consumed by one member |
| **Retention** | How long Kafka keeps messages (default 7 days, configurable) |
| **Replication Factor** | How many copies of each partition (RF=3 for production) |

### Key Guarantees

| Guarantee | Kafka Behavior |
|---|---|
| **Ordering** | Within a partition only (not across partitions) |
| **Durability** | Messages persisted to disk, replicated to N brokers |
| **At-least-once** | Default: consumer may re-process a message after failure |
| **Exactly-once** | Possible with Kafka transactions + idempotent producers |
| **Retention** | Messages available for replay during retention period |

---

## Event Schema Design

Good event schemas are critical — once published, consumers rely on them.

### Schema Best Practices

```json
{
  "event_type": "OrderPlaced",        // What happened (past tense)
  "event_id": "uuid-v4",             // Unique ID — enables idempotent consumers
  "event_version": "1.2",            // Schema version
  "timestamp": "2024-01-15T10:30Z", // When it happened (ISO 8601)
  "source_service": "order-service", // Who produced it
  "correlation_id": "req-uuid",     // Trace requests across services
  "data": {                          // The event payload
    "order_id": "ord-123",
    "customer_id": "cust-456",
    "total_amount": 99.99
  }
}
```

**Do NOT:**
- Put mutable data directly in events (put IDs + immutable facts)
- Use vague event names (`DataChanged`, `Updated`)
- Omit the event ID (consumers can't deduplicate without it)

---

## At-Least-Once vs Exactly-Once Delivery

The fundamental trade-off in event-driven systems:

| Mode | How | Risk | Use Case |
|---|---|---|---|
| **At-most-once** | No retry, no ack | May lose events | Non-critical metrics |
| **At-least-once** | Consumer acks after processing, retries on failure | May process twice | Most pipelines (make consumer idempotent) |
| **Exactly-once** | Kafka transactions + transactional writes | Complex, some overhead | Financial transactions |

### Making At-Least-Once Safe (Idempotency)

```python
# Idempotent consumer: safe to process the same message twice
def process_order_event(event: dict, db):
    order_id = event["data"]["order_id"]
    event_id = event["event_id"]
    
    # Check if already processed using event_id as idempotency key
    if db.exists(f"processed_events:{event_id}"):
        logger.info(f"Skipping duplicate event {event_id}")
        return
    
    # Process the event
    db.upsert("orders", {"order_id": order_id, **event["data"]})
    
    # Mark as processed (TTL = 7 days, matches Kafka retention)
    db.set(f"processed_events:{event_id}", "1", ex=604800)
```

---

## Dead Letter Queues (DLQ)

When a consumer fails to process a message, it goes to a DLQ for investigation.

```
Kafka Topic: orders
     │
     ▼
Consumer (order processor)
     │
     │ Processing fails (invalid data, downstream outage)
     ▼
DLQ Topic: orders.DLQ
     │
     ├── Alert: Slack/PagerDuty notification
     └── Manual review: engineer inspects, fixes, replays

# DLQ replay after fix:
from kafka import KafkaConsumer, KafkaProducer

consumer = KafkaConsumer("orders.DLQ", ...)
producer = KafkaProducer(bootstrap_servers="kafka:9092")

for message in consumer:
    # Fix the message or just replay
    producer.send("orders", value=message.value)
```

---

## Key Terms

| Term | Definition |
|---|---|
| **Event** | An immutable record of something that happened |
| **Event sourcing** | Using the event log as the authoritative source of truth |
| **CQRS** | Command Query Responsibility Segregation — separate write and read models |
| **Outbox pattern** | Write to DB + event log atomically to prevent inconsistency |
| **Saga** | Sequence of local transactions with compensating actions for failures |
| **DLQ** | Dead Letter Queue — destination for messages that fail processing |
| **Idempotency** | Processing the same event multiple times produces the same result |
