---
title: "Event-Driven Architecture — Real World"
topic: system-design
subtopic: event-driven-architecture
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [system-design, event-driven, kafka, real-world, case-study]
---

# Event-Driven Architecture — Real World

## How LinkedIn Uses Kafka (The Origin Story)

LinkedIn built Kafka in 2011 specifically for their event-driven data platform. Their original problem:

**Before Kafka:**
- 80+ data pipelines connecting different systems
- Each pipeline had its own protocol, reliability guarantees, and monitoring
- Adding a new consumer required changing the producer

**After Kafka:**
```
LinkedIn's production Kafka (2024 scale):
  - 7 trillion messages/day
  - 100+ Kafka clusters
  - 1,000+ topics
  - 10,000+ consumer groups
```

**The key insight from LinkedIn:** A unified event bus decouples data producers from consumers. Once an event is in Kafka, any team can build a new consumer without touching the producer.

### LinkedIn's Databus → Kafka Pattern

```
Oracle/MySQL (source) → Databus (CDC) → Kafka → Multiple consumers:
  1. Hadoop (batch analytics)
  2. Voldemort (key-value store, online features)
  3. Espresso (document store)
  4. Samza jobs (stream processing)

This is now replicated everywhere:
  MySQL → Debezium → Kafka → (Delta Lake / Redis / Elasticsearch)
```

---

## Uber's Event-Driven Architecture (Michelangelo + Kafka)

Uber built their ML platform on top of Kafka events. Key patterns:

### Pattern 1: Feature Computation from Events

```
Trip completed event → Kafka
     │
     ├── Driver app (updates driver status) — real-time consumer
     ├── Michelangelo Feature Pipeline — computes ML features from trip events
     │     → online feature store (Cassandra): driver acceptance rate, ETA accuracy
     │     → offline feature store (Hive): training data
     └── Surge pricing model — reacts to demand events
```

### Pattern 2: Ride Matching as Event-Driven Saga

```
RideRequested (by rider)
     │
     ▼
[Driver Matching Service]
     │ → DriverNotified events sent to nearby drivers
     │ ← DriverAccepted (or DriverDeclined within timeout)
     │
     ▼ (first accept wins)
[Booking Service] publishes RideConfirmed
     │
     ├── Rider app: show driver en route
     ├── Driver app: show pickup navigation
     ├── Pricing service: lock in the fare
     └── Analytics: record match latency
```

**Uber's lesson:** The saga orchestrator (Booking Service) must handle timeout/retry: if no driver accepts within 30s, the saga re-broadcasts to a wider radius.

---

## The Outbox Pattern at Shopify's Scale

Shopify processes 10,000+ orders/minute during peak (Black Friday). Their challenge:

**The dual-write problem:** When a merchant's order is placed, Shopify must:
1. Write to the Orders database (MySQL/Aurora)
2. Publish an event to partners (Kafka/webhooks)
3. Update inventory
4. Trigger fulfillment

If any of these fail mid-sequence, data inconsistency occurs.

**Shopify's Outbox implementation:**
```ruby
# ActiveRecord + Outbox (simplified)
class OrdersController < ApplicationController
  def create
    ActiveRecord::Base.transaction do
      order = Order.create!(order_params)
      
      # Write event to outbox in SAME transaction
      OutboxEvent.create!(
        aggregate_type: "Order",
        aggregate_id: order.id,
        event_type: "order_placed",
        payload: order.to_event_payload,
        status: "pending"
      )
      # If either fails, both roll back — atomically safe
    end
    # Outbox publisher (separate Sidekiq job) will pick up and publish to Kafka
  end
end
```

**Scale:** At 10,000 orders/minute, the outbox table receives 10,000 rows/minute. Shopify uses a dedicated outbox publisher process per shard.

---

## Stripe's Event System Design

Stripe publishes events to customers via webhooks. Behind their webhook system is an event-driven architecture:

```
Payment attempt → [Stripe Internal Event Bus] → Payment Processor
                                              → Risk/Fraud Engine
                                              → Webhook Dispatcher → Customer's endpoint
                                              → Event Log (for replay via API)
```

**Stripe's event guarantees (from their engineering blog):**
- **At-least-once delivery** to webhooks (may duplicate, your endpoint must be idempotent)
- **Event ID** included in every webhook: `evt_1ABC...` — use this as idempotency key
- **Replay capability**: customers can query events API for past 30 days

**Retry strategy:**
```
Retry schedule on webhook failure:
  5 min → 30 min → 1 hour → 2 hours → 4 hours → 8 hours → 16 hours
  After 72 hours of failed delivery → event marked as failed, alert customer
```

---

## Common Event-Driven Pitfalls

### 1. Event Schema Without Registry = Schema Chaos

**Real scenario:** Team A starts publishing `{ user_id: "string" }`. 6 months later they change to `{ userId: "integer" }` without notice. All consumers break.

**Solution:** Schema Registry enforced at CI/CD level. No schema change without registry validation.

### 2. Missing DLQ = Silent Data Loss

```python
# WITHOUT DLQ: malformed event causes consumer to crash in loop
while True:
    for msg in consumer.poll():
        try:
            process(msg)
        except Exception:
            # What happens? Consumer retries forever, blocks all subsequent messages
            pass  # ← THIS IS WRONG

# WITH DLQ: malformed events are quarantined, pipeline continues
while True:
    for msg in consumer.poll():
        try:
            process(msg)
        except MalformedEventError as e:
            # Route to DLQ, log, alert, continue
            dlq_producer.send("orders.DLQ", value=msg.value,
                              headers=[("error", str(e).encode())])
            metrics.increment("events.dlq.orders")
        except TransientError:
            # Retriable: stop processing, let coordinator rebalance
            raise  # Causes partition reassignment → retry from committed offset
```

### 3. Saga Without Idempotency = Duplicate Side Effects

**Scenario:** Payment saga crashes after charging the card but before marking the saga as complete. On retry, the card gets charged again.

**Solution:**
```python
# Use idempotency key in payment API call
def charge_card(order_id: str, amount: float):
    # Idempotency key: same order_id → same charge result (no duplicate)
    response = stripe.PaymentIntent.create(
        amount=int(amount * 100),
        currency="usd",
        idempotency_key=f"order-{order_id}",  # ← safe to retry
        metadata={"order_id": order_id}
    )
    return response
```

### 4. Event Ordering Assumption Without Partition Key

```python
# WRONG assumption: events arrive in order across partitions
# User deletes account → account deletion event on partition 3
# User registration event (earlier!) arrives on partition 1 LATER
# Consumer sees: AccountDeleted before AccountCreated → inconsistency

# Solution: partition by user_id → all user events on same partition → ordered
producer.send("user-events", key=user_id.encode(), value=event_bytes)
```

---

## Interview-Ready Summary

**When an interviewer says "event-driven" they're testing:**

1. **Do you know why?** Not just "Kafka is fast" — but loose coupling, fan-out, replay, audit trail
2. **Do you know the failure modes?** DLQ, idempotency, ordering, duplicate delivery
3. **Can you pick the right pattern?** Outbox for dual-write, Saga for distributed tx, compaction for entity state
4. **Schema versioning?** Schema Registry, BACKWARD compatibility, Avro/Protobuf vs JSON
5. **Exactly-once vs at-least-once?** Trade-offs, when each is appropriate, how to implement

**The one-sentence summary:** Event-driven architecture decouples producers from consumers via an immutable, replayable log (Kafka), enabling fan-out, auditing, and stream processing — but requires careful schema management, idempotent consumers, and DLQ handling.
