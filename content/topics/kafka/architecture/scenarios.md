---
title: "Kafka Architecture - Scenario Questions"
topic: kafka
subtopic: architecture
content_type: scenario_question
tags: [kafka, architecture, interview, scenarios]
---

# Scenario Questions — Kafka Architecture

<article data-difficulty="junior">

## 🟢 Junior: Why Can't Consumers Keep Up?

**Scenario:** Your team has a Kafka topic `user-events` with 6 partitions. You deployed 8 consumers in the same consumer group. Two of them are idle and not processing any messages. Why?

<details>
<summary>💡 Hint</summary>

Think about the relationship between number of partitions and number of consumers in a group.

</details>

<details>
<summary>✅ Solution</summary>

**Answer:** In a consumer group, each partition can only be assigned to ONE consumer. With 6 partitions and 8 consumers, only 6 consumers get assigned a partition. The remaining 2 consumers sit idle.

**Fix:** Either:
1. Reduce consumers to 6 (match partition count)
2. Increase partitions to 8 or more (allows all consumers to work)

**Key rule:** Maximum useful consumers = number of partitions. Adding more consumers beyond this provides standby redundancy but not additional throughput.

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Messages Arriving Out of Order

**Scenario:** You're producing messages to a topic `order-updates` with order_id as the key. You notice that updates for the same order sometimes arrive out of order at the consumer. What could cause this?

<details>
<summary>✅ Solution</summary>

**Possible causes:**

1. **No key specified on some messages:** Without a key, producers use round-robin, sending same-order messages to different partitions. Order is only guaranteed within a partition.

2. **Multiple producer instances with retries:** Producer A sends update-1, fails, retries. Producer B sends update-2 which succeeds first. Update-2 arrives before update-1.

3. **max.in.flight.requests > 1 without idempotence:** Producer sends batch-1, batch-2 in flight. Batch-1 fails and retries, landing after batch-2.

**Fix:**
```python
producer = KafkaProducer(
    key_serializer=lambda k: k.encode('utf-8'),  # Always use order_id as key
    enable_idempotence=True,                      # Prevents reorder on retry
    max_in_flight_requests_per_connection=5,      # Safe with idempotence=True
)
# Always provide the key:
producer.send('order-updates', key=order_id, value=event_data)
```

**Rule:** To guarantee ordering for a specific entity, ALWAYS use that entity's ID as the message key and enable idempotent producers.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design a Topic Strategy for an E-Commerce Platform

**Scenario:** You're designing the Kafka topic strategy for an e-commerce platform. Events include: user clicks, page views, add-to-cart, purchases, inventory updates, and shipping status changes. How would you organize topics, choose partition counts, and set retention?

<details>
<summary>✅ Solution</summary>

**Topic design:**

| Topic | Key | Partitions | Retention | Reason |
|-------|-----|-----------|-----------|--------|
| `clickstream.page-views` | user_id | 24 | 3 days | High volume, short-lived analytics |
| `clickstream.add-to-cart` | user_id | 12 | 7 days | Funnel analysis |
| `orders.placed` | order_id | 12 | 30 days | Critical, moderate volume |
| `orders.payments` | order_id | 12 | 90 days | Financial, keep longer |
| `inventory.updates` | product_id | 6 | Compact | Only need latest per product |
| `shipping.status` | order_id | 6 | 14 days | Track delivery lifecycle |

**Design principles applied:**

1. **Separate by domain:** clickstream, orders, inventory, shipping are different bounded contexts with different SLAs
2. **Key by entity ID:** Ensures all events for same user/order/product land in same partition (ordering + locality)
3. **Partition count by throughput:**
   - Clickstream: millions/day → 24 partitions for high parallelism
   - Orders: thousands/day → 12 partitions (moderate)
   - Inventory: hundreds/day → 6 partitions (low volume)
4. **Retention by business need:**
   - Page views: 3 days (quick analytics, no long-term need)
   - Payments: 90 days (compliance, dispute resolution)
   - Inventory: compacted (always need current state)
5. **Naming convention:** `domain.entity` makes topics discoverable

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Handling Consumer Rebalance Storms

**Scenario:** Your 20-consumer group processing a critical topic experiences frequent rebalances (5-10 per hour). During each rebalance, processing stops for 30-60 seconds. Diagnose and fix.

<details>
<summary>✅ Solution</summary>

**Diagnosis checklist:**

1. **GC pauses exceeding session timeout?** Check JVM GC logs. Long GC pauses cause missed heartbeats → broker thinks consumer is dead → rebalance.

2. **Processing time exceeding max.poll.interval.ms?** If processing a batch takes longer than `max.poll.interval.ms` (default 5 min), consumer is removed from group.

3. **Frequent deployments/restarts?** Each restart triggers a rebalance for the entire group.

4. **Network issues?** Intermittent connectivity causes heartbeat failures.

**Fix — Configuration changes:**

```properties
# Increase timeouts to tolerate brief hiccups
session.timeout.ms=45000              # 45s (from default 10s)
heartbeat.interval.ms=15000           # Every 15s
max.poll.interval.ms=600000           # 10 min for slow processing

# Use cooperative rebalancing (doesn't stop ALL consumers)
partition.assignment.strategy=org.apache.kafka.clients.consumer.CooperativeStickyAssignor

# Use static membership (brief disconnects don't trigger rebalance)
group.instance.id=consumer-${HOSTNAME}  # Unique per instance
```

**Fix — Application changes:**

```python
# Process in smaller batches to stay within poll interval
consumer = KafkaConsumer(
    max_poll_records=100,  # Reduce from 500 to 100
)

# If processing is inherently slow, use a separate thread
import threading

def poll_loop(consumer):
    while True:
        records = consumer.poll(timeout_ms=1000, max_records=100)
        for record in records:
            work_queue.put(record)  # Hand off to worker threads
        consumer.commit()
```

**Result:** Cooperative sticky assignment + static membership + tuned timeouts reduced rebalances from 5-10/hour to near-zero.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design for Exactly-Once Processing Across Services

**Scenario:** You have a payment processing pipeline: `payment-requests` → Processing Service → `payment-results` + Database write. If the service crashes after writing to the database but before producing to `payment-results`, you get inconsistency. Design an exactly-once solution.

<details>
<summary>💡 Hint</summary>

You need atomic "consume + process + produce + commit" semantics. Consider the transactional outbox pattern or Kafka transactions.

</details>

<details>
<summary>✅ Solution</summary>

**Option 1: Kafka Transactions (If DB Write Can Be Deferred)**

```python
consumer = KafkaConsumer(
    'payment-requests',
    group_id='payment-processor',
    enable_auto_commit=False,
    isolation_level='read_committed',
)

producer = KafkaProducer(
    transactional_id='payment-processor-1',
    enable_idempotence=True,
)
producer.init_transactions()

for message in consumer:
    producer.begin_transaction()
    try:
        # Process payment
        result = process_payment(message.value)
        
        # Produce result (inside transaction)
        producer.send('payment-results', key=message.key, value=result)
        
        # Commit consumer offset (inside same transaction)
        producer.send_offsets_to_transaction(
            {TopicPartition(message.topic, message.partition): 
             OffsetAndMetadata(message.offset + 1, '')},
            consumer.group_id
        )
        
        producer.commit_transaction()
    except Exception:
        producer.abort_transaction()
```

**Option 2: Transactional Outbox (If DB Write Is Required)**

```python
# 1. Write to DB + outbox in SAME database transaction
with db.begin_transaction() as txn:
    # Business write
    txn.execute("INSERT INTO payments (id, amount, status) VALUES (?, ?, 'completed')",
                [payment_id, amount])
    
    # Outbox write (same transaction — atomic!)
    txn.execute("INSERT INTO outbox (id, topic, key, value, created_at) VALUES (?, ?, ?, ?, NOW())",
                [uuid(), 'payment-results', payment_id, json.dumps(result)])
    
    txn.commit()  # Both succeed or both fail

# 2. Separate outbox publisher (polls outbox table, produces to Kafka)
# Can safely retry — if Kafka produce succeeds, delete from outbox
# If it crashes, outbox entry remains and gets retried
```

**Comparison:**

| Approach | When to Use | Limitation |
|----------|-------------|-----------|
| Kafka Transactions | Processing is Kafka-to-Kafka (no external DB) | Can't include external DB in transaction |
| Transactional Outbox | Must write to DB + produce to Kafka atomically | Requires outbox table + polling/CDC publisher |
| Idempotent Consumer | Simpler; tolerate duplicates | Requires natural dedup key in consumer logic |

**Recommendation:** For the payment scenario, use **Transactional Outbox** since the DB write is essential. The outbox pattern guarantees consistency between DB state and Kafka events.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Kafka Cluster Capacity Planning

**Scenario:** You're designing a Kafka cluster for a new platform expecting:
- 500K events/second peak (avg message size 1KB)
- 7-day retention
- 3x replication
- 99.99% availability SLA
- Budget: $30K/month on AWS

Size the cluster (broker count, instance type, storage, partition count).

<details>
<summary>✅ Solution</summary>

**Step 1: Calculate storage needs**

```
Ingest rate: 500K msgs/sec × 1KB = 500 MB/sec
Daily volume: 500 MB/sec × 86,400 sec = 43.2 TB/day
7-day retention: 43.2 × 7 = 302 TB (raw)
With 3x replication: 302 × 3 = 907 TB total storage needed
With compression (lz4 ~60% ratio): 907 × 0.6 = 544 TB
Add 30% headroom: ~710 TB total
```

**Step 2: Calculate broker count by throughput**

```
Single broker throughput (m5.2xlarge): ~200 MB/sec write
Total write throughput needed: 500 MB/sec × 3 (replication) = 1.5 GB/sec
Minimum brokers for throughput: 1500 / 200 = 8 brokers
```

**Step 3: Calculate broker count by storage**

```
Storage per broker (using d3.2xlarge with 12TB NVMe): 12 TB usable
Brokers needed for storage: 710 TB / 12 TB = ~60 brokers
```

**Storage is the bottleneck.** But d3 instances are expensive. Alternative:

```
Use m5.2xlarge + EBS gp3 volumes:
  - Each broker: 12 TB EBS (gp3: $0.08/GB/month = $960/month per broker)
  - Brokers needed: 710 / 12 = ~60
  - Too many! Use larger EBS: 20 TB per broker → 36 brokers
```

**Step 4: Final architecture**

| Component | Spec | Quantity | Monthly Cost |
|-----------|------|----------|-------------|
| Brokers (m5.2xlarge) | 8 vCPU, 32GB RAM | 9 (8 + 1 spare) | $2,700 |
| EBS gp3 (per broker) | 20 TB, 3000 IOPS | 9 | $14,400 |
| Controller nodes (m5.large) | KRaft mode | 3 | $300 |
| Networking (inter-broker) | Enhanced networking | Included | $0 |
| Monitoring (Prometheus) | m5.large | 1 | $100 |
| **Total** | | | **~$17,500** |

Wait — 9 brokers × 20TB = 180 TB, but we need 710 TB. Let me recalculate with tiered storage:

**With Tiered Storage (Kafka 3.6+):**

```
Hot storage (on broker): Last 1 day = 43.2 TB × 3 replication = 130 TB
  → 9 brokers × 15 TB each = 135 TB ✓

Cold storage (S3): Days 2-7 = 259 TB × 3 = 777 TB compressed to ~470 TB
  → S3 cost: 470 TB × $0.023/GB = $10,800/month
```

**Revised with tiered storage:**

| Component | Monthly Cost |
|-----------|-------------|
| 9 brokers (m5.2xlarge) | $2,700 |
| EBS gp3 (9 × 15TB) | $10,800 |
| S3 tiered storage | $10,800 |
| Controllers + monitoring | $400 |
| **Total** | **$24,700** ✓ Under $30K budget |

**Partition count:**
```
Target: 500K msgs/sec
Per-partition throughput: ~10K msgs/sec per partition
Minimum: 500K / 10K = 50 partitions
With headroom: 72 partitions (evenly divisible by 9 brokers)
```

</details>

</article>
