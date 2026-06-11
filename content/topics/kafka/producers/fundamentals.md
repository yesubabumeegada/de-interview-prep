---
title: "Kafka Producers - Fundamentals"
topic: kafka
subtopic: producers
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [kafka, producers, messaging, partitioning, batching, reliability]
---

# Kafka Producers — Fundamentals


## 🎯 Analogy

Think of a Kafka producer like a news wire service: it sends articles (messages) to specific sections (topics/partitions) and gets a receipt (offset) confirming each article was filed.

---
## What Is a Kafka Producer?

A Kafka producer is a client application that **sends (publishes) records to Kafka topics**. It's the entry point for getting data into Kafka.

> **Why producers matter for DE:** Every streaming pipeline starts with a producer. Understanding producer configuration determines whether your pipeline is fast, reliable, and produces no duplicates or data loss.

---

## Producer Architecture

```mermaid
flowchart LR
    A["Application"] --> B["Producer Client"]
    B --> C["Serializer\n(Key + Value)"]
    C --> D["Partitioner\n(choose partition)"]
    D --> E["Record Accumulator\n(batch buffer)"]
    E --> F["Sender Thread\n(network I/O)"]
    F --> G["Kafka Broker"]
```

**What this shows:**
- Your application creates records and hands them to the producer client
- Records are serialized (converted to bytes)
- The partitioner decides which partition to send to (based on key hash)
- Records accumulate in a buffer (batching for efficiency)
- A background sender thread transmits batches to brokers

---

## Basic Producer Example

```python
from kafka import KafkaProducer
import json

# Create a producer
producer = KafkaProducer(
    bootstrap_servers=['broker1:9092', 'broker2:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8') if k else None,
)

# Send a message
producer.send(
    topic='user-events',
    key='user-123',
    value={
        'event_type': 'purchase',
        'amount': 99.99,
        'timestamp': '2024-01-15T10:30:00Z'
    }
)

# Flush to ensure all buffered messages are sent
producer.flush()

# Or use callbacks for async confirmation
def on_success(metadata):
    print(f"Sent to {metadata.topic}:{metadata.partition} offset {metadata.offset}")

def on_error(exception):
    print(f"Failed: {exception}")

future = producer.send('user-events', key='user-456', value={'event': 'click'})
future.add_callback(on_success)
future.add_errback(on_error)
```

---

## Key Configuration Parameters

| Parameter | Default | Production Recommended | Purpose |
|-----------|---------|----------------------|---------|
| `acks` | 1 | `'all'` | Durability: how many replicas must confirm |
| `retries` | 2147483647 | Same (default is fine) | Auto-retry on transient failures |
| `batch.size` | 16384 (16 KB) | 65536 (64 KB) | Buffer records before sending (throughput) |
| `linger.ms` | 0 | 5-20 | Wait to fill batch before sending |
| `compression.type` | none | `'snappy'` or `'lz4'` | Reduce network bandwidth |
| `enable.idempotence` | false | `true` | Prevent duplicates on retry |
| `max.in.flight.requests` | 5 | 5 (with idempotence) | Concurrent requests per connection |
| `buffer.memory` | 33554432 (32 MB) | 67108864 (64 MB) | Total memory for buffering |

---

## Partitioning — Where Messages Go

The partition determines which broker/shard handles the message:

```python
# With key: hash(key) % num_partitions → deterministic partition
producer.send('orders', key='order-123', value=data)
# Same key ALWAYS goes to same partition → preserves per-key ordering

# Without key: round-robin (or sticky partition in newer Kafka)
producer.send('metrics', value=data)
# Spread evenly — no ordering guarantee across partitions

# Custom partitioner
from kafka.partitioner import Murmur2Partitioner

producer = KafkaProducer(
    partitioner=Murmur2Partitioner(),  # Consistent hashing
    # Or implement your own:
    # partitioner=lambda key, all_partitions, available: custom_logic(key)
)
```

**Partition key best practices:**
- Use entity ID (user_id, order_id) for per-entity ordering
- Don't use a key with extreme skew (one value = 80% of traffic)
- Use NULL key for maximum parallelism when ordering doesn't matter

---

## Batching and Throughput

```python
# High-throughput configuration
producer = KafkaProducer(
    batch_size=65536,          # 64 KB batches (more records per network call)
    linger_ms=10,              # Wait 10ms to fill the batch before sending
    compression_type='snappy', # Compress batches (3-5x smaller on wire)
    buffer_memory=67108864,    # 64 MB buffer (hold more unsent records)
)
```

**How batching works:**
- Records accumulate in per-partition buffers
- When buffer reaches `batch.size` OR `linger.ms` passes → send the batch
- Higher batch size = fewer network calls = higher throughput
- Higher linger.ms = more time to fill batch = higher throughput but more latency

**Throughput vs latency tradeoff:**

| Configuration | Throughput | Latency |
|--------------|-----------|---------|
| linger.ms=0, batch=16KB | Lower (many small sends) | Lowest (immediate) |
| linger.ms=5, batch=64KB | Higher | 5ms added |
| linger.ms=50, batch=256KB | Highest | 50ms added |

---

## Delivery Guarantees

| acks | Guarantee | When Record Is Considered "Sent" |
|------|-----------|----------------------------------|
| `0` | Fire-and-forget | Immediately (may be lost) |
| `1` | Leader acknowledged | Leader wrote to its log |
| `all` | All in-sync replicas | All ISR replicas confirmed |

```python
# Production-safe: all replicas must confirm
producer = KafkaProducer(
    acks='all',
    enable_idempotence=True,  # Prevents duplicates on retry
    retries=5,
    retry_backoff_ms=100,
)
```

---

## Idempotent Producer (Exactly-Once Producing)

```python
producer = KafkaProducer(
    enable_idempotence=True,   # Enables dedup on broker side
    acks='all',                # Required for idempotence
    max_in_flight_requests_per_connection=5,  # Safe with idempotence
)

# How it works internally:
# Producer gets a ProducerID (PID) from the broker
# Each message gets a sequence number (per partition)
# Broker deduplicates: if same PID + sequence arrives twice → ignored
# Result: network retries can't create duplicates
```

---

## Error Handling

```python
from kafka.errors import KafkaTimeoutError, NoBrokersAvailable

producer = KafkaProducer(bootstrap_servers=['broker:9092'], acks='all')

try:
    future = producer.send('orders', value=b'data')
    # Block for up to 10 seconds waiting for confirmation
    metadata = future.get(timeout=10)
    print(f"Success: partition={metadata.partition}, offset={metadata.offset}")
except KafkaTimeoutError:
    print("Broker not responding — check cluster health")
except NoBrokersAvailable:
    print("No brokers reachable — check network/DNS")
except Exception as e:
    print(f"Unexpected error: {e}")
    # Route to dead-letter queue or retry later
```

---


## ▶️ Try It Yourself

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks="all",          # Wait for all replicas to confirm
    retries=3,
)

for order_id in range(5):
    msg = {"order_id": order_id, "amount": order_id * 50}
    future = producer.send("orders", key=str(order_id).encode(), value=msg)
    record_meta = future.get(timeout=10)
    print(f"Sent to partition {record_meta.partition}, offset {record_meta.offset}")

producer.flush()
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "How do you ensure no message loss?" — "`acks='all'` ensures all in-sync replicas confirm before the producer considers it sent. Combined with `retries` for transient failures and `enable.idempotence=True` to prevent duplicates on retry. On the broker side: `min.insync.replicas=2` ensures at least 2 replicas have the data."

> **Tip 2:** "How do you maximize producer throughput?" — "Three levers: (1) Batch size — larger batches mean fewer network calls. (2) Linger.ms — add small delay to fill batches. (3) Compression — snappy/lz4 reduces bytes on wire by 3-5x. Combined: can achieve 100K+ messages/sec from a single producer."

> **Tip 3:** "What determines message ordering?" — "Messages with the SAME key always go to the same partition (hash-based). Within a partition, ordering is guaranteed. Across partitions, no ordering guarantee. So: use a meaningful key (user_id, order_id) when per-entity ordering matters."
