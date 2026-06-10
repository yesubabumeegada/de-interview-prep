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

---

## ⚡ Quick-fire Q&A

**Q: What is a Kafka producer and what are its main responsibilities?**
A: A Kafka producer is a client that publishes messages to Kafka topics. It serializes data, determines the target partition (using a partitioner), buffers records in batches, handles retries on failure, and manages acknowledgment from brokers.

**Q: How does Kafka determine which partition a message goes to?**
A: By default, messages with a key use a hash of the key (murmur2) modulo partition count to select a partition consistently. Messages without a key use a sticky partitioner (batch to one partition, then rotate). Custom partitioners can implement any routing logic.

**Q: What is the role of the producer's internal buffer and how does it affect throughput?**
A: Producers buffer records in memory (controlled by `buffer.memory`, default 32MB) into batches per partition before sending. Larger batches (`batch.size`) and longer wait times (`linger.ms`) increase throughput by amortizing network overhead across more records per request.

**Q: How does a producer handle broker failures?**
A: On send failure, the producer retries up to `retries` times with an internal backoff. With `enable.idempotence=true`, retries are safe — the broker deduplicates messages using producer ID and sequence numbers. Without idempotence, retries may produce duplicates.

**Q: What is the difference between synchronous and asynchronous producer sends?**
A: Asynchronous send (default) dispatches the record to the buffer and returns immediately, invoking a callback on completion. Synchronous send calls `future.get()` blocking until the broker acknowledges. Async maximizes throughput; sync is used when immediate error handling per record is required.

**Q: What happens when the producer buffer is full?**
A: If the buffer fills faster than records are sent (e.g., broker is slow), the `send()` call blocks for up to `max.block.ms` waiting for space. After this timeout, a `TimeoutException` is thrown. This is a backpressure mechanism signaling the application to slow down production.

**Q: How do you guarantee message ordering in Kafka producers?**
A: Messages to the same partition are delivered in order. To ensure ordering for a logical entity (e.g., all events for user ID 123), use a consistent partition key. With retries, set `max.in.flight.requests.per.connection=1` (or use idempotent producer which allows up to 5 in-flight safely) to prevent reordering on retry.

**Q: What is a producer callback and when should you use it?**
A: A callback is a function invoked after the broker acknowledges (or fails) a send. Use it for error logging, metrics collection, alerting on delivery failures, and dead-letter routing. Always register callbacks for production producers — fire-and-forget without callbacks makes failures invisible.

---

## 💼 Interview Tips

- Know the key → partition → broker routing chain deeply — interviewers ask how to ensure all records for a customer go to the same partition, and the answer is consistent key-based partitioning.
- Explain the batching model (buffer → batch per partition → send) clearly — it's the foundation of Kafka producer performance and directly connects to `batch.size` and `linger.ms` tuning.
- Discuss idempotent producer as the recommended default, not an advanced feature — it's been stable since Kafka 0.11 and prevents duplicate-on-retry without performance penalty for most workloads.
- Be ready to explain `max.in.flight.requests.per.connection` and ordering — the "set to 1 for ordering" advice is outdated with idempotent producers, and knowing the nuance separates senior candidates.
- Mention callback handling proactively — production producers without delivery callbacks have silent failure modes, and knowing to implement them shows operational maturity.
- For senior roles, discuss producer metrics to monitor: record-send-rate, record-error-rate, batch-size-avg, request-latency-avg — knowing which producer JMX metrics to track signals you've operated producers at scale.
