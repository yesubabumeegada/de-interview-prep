---
title: "Kafka Producers - Scenario Questions"
topic: kafka
subtopic: producers
content_type: scenario_question
tags: [kafka, producers, interview, scenarios]
---

# Scenario Questions — Kafka Producers

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Diagnose Duplicate Messages

**Scenario:** Your consumer is seeing duplicate messages. The producer uses `acks=1` and `retries=3`. When the producer retries (network timeout on first attempt), the broker may have actually received the first attempt — resulting in the same message stored twice. How do you fix this without impacting throughput?

<details>
<summary>✅ Solution</summary>

**Root cause:** Without idempotence, a retry can produce a duplicate. The producer sent the message, the broker received and stored it, but the acknowledgment was lost due to a network timeout. The producer retries → duplicate.

**Fix: Enable idempotent producer**

```python
producer = KafkaProducer(
    bootstrap_servers=['broker:9092'],
    acks='all',                    # Required for idempotence
    enable_idempotence=True,       # Prevents duplicates on retry
    max_in_flight_requests_per_connection=5,  # Safe up to 5 with idempotence
    retries=5,
)
```

**How it eliminates duplicates:**
- Producer gets a unique ProducerID (PID) from the broker on startup
- Each message is assigned a sequence number per partition
- Broker tracks: (PID, partition, sequence_number)
- If same (PID, partition, seq) arrives again: broker ignores it (dedup)
- Result: retry is safe — broker won't store the duplicate

**Throughput impact:** Negligible. Idempotence adds a small amount of per-message metadata but doesn't require additional network round-trips.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a High-Throughput Producer for 1M Events/Second

**Scenario:** Your application generates 1 million events per second (average 500 bytes each = 500 MB/s). Design the producer configuration and architecture to handle this load reliably with no data loss.

<details>
<summary>✅ Solution</summary>

**Single producer max throughput:** ~100-200K msgs/sec (varies by message size and network)

**Need:** 1M/sec → requires 5-10 producer instances

**Architecture:**

```python
# Configuration per producer instance (run 8 instances)
producer_config = {
    'bootstrap_servers': ['broker1:9092', 'broker2:9092', 'broker3:9092'],
    'acks': 'all',
    'enable_idempotence': True,
    'batch_size': 262144,            # 256 KB batches (large for throughput)
    'linger_ms': 20,                 # 20ms wait to fill batch
    'compression_type': 'lz4',       # Fastest compression
    'buffer_memory': 134217728,      # 128 MB buffer per producer
    'max_in_flight_requests_per_connection': 5,
    'send_buffer_bytes': 1048576,    # 1 MB socket buffer
}

# Topic configuration
# 500 MB/sec write → each partition handles ~5 MB/sec max
# Partitions needed: 500 / 5 = 100 partitions minimum
# With headroom: 128 partitions

# Broker cluster
# Each broker handles ~200 MB/sec write
# Brokers needed: 500 MB/sec × 3 (replication) / 200 = 8 brokers minimum
```

**Multi-producer architecture:**

```python
import multiprocessing as mp
from kafka import KafkaProducer

def producer_worker(queue, config):
    """Each worker runs its own producer instance."""
    producer = KafkaProducer(**config)
    while True:
        event = queue.get()
        if event is None:
            break
        producer.send(
            topic='high-volume-events',
            key=event['partition_key'].encode(),
            value=json.dumps(event).encode()
        )
    producer.flush()
    producer.close()

# Launch 8 producer workers
queues = [mp.Queue(maxsize=100000) for _ in range(8)]
workers = [mp.Process(target=producer_worker, args=(q, producer_config)) for q in queues]
for w in workers:
    w.start()

# Distribute events across workers (round-robin)
for i, event in enumerate(event_stream):
    queues[i % 8].put(event)
```

**Estimated throughput:**
- 8 producers × 125K msgs/sec each = 1M msgs/sec ✓
- Each producer: 256 KB batch × 50 sends/sec = 12.8 MB/sec per producer
- Total: 8 × 62.5 MB/sec = 500 MB/sec (before compression)
- After lz4 compression: ~200 MB/sec on wire

**Broker sizing:**
- 8 brokers (i3.2xlarge or similar)
- Replication factor 3: 500 MB/s × 3 = 1.5 GB/s cluster write throughput
- 128 partitions across 8 brokers = 16 partitions per broker
- Retention: 7 days × 500 MB/s × 86400 = 300 TB → need adequate disk

</details>

</article>
