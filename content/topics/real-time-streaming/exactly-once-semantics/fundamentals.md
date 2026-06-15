---
title: "Exactly-Once Semantics - Fundamentals"
topic: real-time-streaming
subtopic: exactly-once-semantics
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [streaming, exactly-once, kafka, flink, idempotency, delivery-guarantees]
---

# Exactly-Once Semantics — Fundamentals

## 🎯 Analogy

Think of delivery guarantees like sending a package:
- **At-most-once**: fire-and-forget mail — might arrive, might get lost, never duplicate
- **At-least-once**: certified mail with retries — definitely arrives but might be delivered twice if the first attempt was slow
- **Exactly-once**: a bank wire transfer — guaranteed to complete exactly one time, no duplicates, no loss

---

## The Three Delivery Guarantees

```
Delivery Guarantee Comparison:

                  Message Loss?   Duplicates?   Complexity   Use Case
At-most-once      Possible        Never         Low          Metrics, logs (loss OK)
At-least-once     Never           Possible      Medium       Most event pipelines
Exactly-once      Never           Never         High         Payments, inventory, billing
```

### At-Most-Once
- Producer sends and forgets — no retry on failure
- Consumer acknowledges before processing
- Simplest, lowest latency, lowest overhead
- **Risk**: data loss on broker or network failure

### At-Least-Once
- Producer retries until acknowledgment received
- Consumer processes then acknowledges
- **Risk**: duplicate processing if ack is lost after processing but before sending

```
At-least-once duplicate scenario:

  Producer → Broker: message M1
  Broker processes M1
  Broker → Producer: ACK (lost in network)
  Producer retries → sends M1 again
  Broker receives M1 again → duplicate!
```

### Exactly-Once
- Each message is processed exactly one time, no loss, no duplicates
- Requires coordination between producer, broker, and consumer
- Higher latency and complexity — use only when duplicates are unacceptable

---

## Why Exactly-Once Is Hard

```
Distributed systems challenges:

  Network failures:     Messages can be lost or delayed
  Node crashes:         State may be lost mid-processing
  Partitioned networks: Producer and broker may disagree on what was committed
  The Two Generals Problem:
    - Two armies must coordinate attack simultaneously
    - Messages may be lost
    - No protocol guarantees both armies attack at exactly the same moment
    → Analogous to guaranteeing a message is processed exactly once
       across distributed systems
```

Key insight: **Exactly-once across systems requires all participants to support it.** A chain is only as strong as its weakest link — if your sink doesn't support idempotent writes, you can't have true end-to-end exactly-once.

---

## Idempotency: The Building Block

An operation is **idempotent** if applying it multiple times produces the same result as applying it once.

```
Idempotent vs non-idempotent:

  Non-idempotent:
    INSERT INTO orders VALUES (order_id=1, amount=100)
    -- Run twice → two rows (duplicate!)

  Idempotent (upsert):
    INSERT INTO orders VALUES (order_id=1, amount=100)
    ON CONFLICT (order_id) DO UPDATE SET amount = EXCLUDED.amount
    -- Run twice → same result (safe)

  Idempotent counter-example:
    counter += 1   → NOT idempotent (each call changes state)
    counter = 5    → idempotent (same result every time)
```

**Idempotent producers in Kafka** (`enable.idempotence=true`):
- Kafka assigns each producer a **Producer ID (PID)**
- Each message gets a **sequence number** per partition
- Broker deduplicates retries using PID + sequence number
- Guarantees no duplicates within a single producer session

```python
# Kafka producer with idempotence enabled
from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'enable.idempotence': True,       # enables PID + sequence numbers
    'acks': 'all',                    # required for idempotence
    'retries': 2147483647,            # max retries (required)
    'max.in.flight.requests.per.connection': 5  # max 5 (required)
})

producer.produce('my-topic', key='k1', value='v1')
producer.flush()
```

---

## Kafka Transactions Basics

Kafka transactions extend idempotent producers to span **multiple partitions and topics atomically**.

```
Kafka transaction flow:

  Producer                 Broker (Transaction Coordinator)
  ─────────────────────────────────────────────────────────
  initTransactions()  →    Assigns transactional.id → PID mapping
  beginTransaction()  →    Marks transaction as open
  produce(topic-A)    →    Writes to topic-A (pending)
  produce(topic-B)    →    Writes to topic-B (pending)
  commitTransaction() →    Two-phase commit:
                             1. Write PREPARE to transaction log
                             2. Write data markers to partitions
                             3. Write COMMITTED to transaction log
  ─────────────────────────────────────────────────────────
  (If crash before commit → broker aborts, consumers skip pending msgs)
```

```python
from confluent_kafka import Producer

producer = Producer({
    'bootstrap.servers': 'localhost:9092',
    'transactional.id': 'my-transactional-producer-1',  # unique per producer
    'enable.idempotence': True,
})

producer.init_transactions()

try:
    producer.begin_transaction()
    producer.produce('output-topic', key='k1', value='processed-value')
    producer.produce('output-topic', key='k2', value='another-value')
    producer.commit_transaction()
except Exception as e:
    producer.abort_transaction()
    raise
```

---

## Consumer-Side: Isolation Level

Transactional producers write data marked as "pending" until committed. Consumers must be configured to only read committed data:

```python
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',
    'isolation.level': 'read_committed',  # skip pending/aborted messages
    # Default is 'read_uncommitted' — would read pending messages!
})
```

---

## Cost of Exactly-Once

```
Performance tradeoffs:

  Kafka transactions add ~10-15% latency/throughput overhead:
  - Two-phase commit round trips to transaction coordinator
  - Consumers must wait for commit marker before reading
  - Transaction log writes on every commit

  Recommendation:
  - Use exactly-once for: financial transactions, inventory, billing
  - Use at-least-once + idempotent sinks for: analytics, dashboards
  - Use at-most-once for: non-critical metrics, high-volume logs
```

---

## Key Terms to Know

| Term | Definition |
|------|-----------|
| Idempotent producer | Producer that uses PID + sequence numbers to prevent duplicates |
| Transactional ID | Stable identifier that survives producer restarts |
| Two-phase commit | Protocol to atomically commit across multiple partitions |
| read_committed | Consumer isolation level that skips uncommitted messages |
| Exactly-once illusion | End-to-end exactly-once requires every component to cooperate |
