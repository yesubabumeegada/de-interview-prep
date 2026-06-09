---
title: "Stream Processing Patterns — Intermediate"
topic: real-time-streaming
subtopic: stream-processing-patterns
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [streaming, patterns, dead-letter-queue, replay, exactly-once, backpressure, routing]
---

# Stream Processing Patterns — Intermediate

## Dead Letter Queue Pattern

```python
"""
DLQ Pattern: route unprocessable records to a separate queue for inspection.
Prevents one bad record from blocking the entire stream.

Types of failures:
  Transient: timeout, throttling → retry with backoff
  Permanent: malformed JSON, schema mismatch, business rule violation → DLQ
"""

from confluent_kafka import Consumer, Producer
import json
import logging

logger = logging.getLogger(__name__)

class DLQConsumer:
    """Consumer with DLQ routing for permanent failures."""
    
    def __init__(self, source_topic: str, dlq_topic: str, max_retries: int = 3):
        self.consumer = Consumer({
            'bootstrap.servers': 'kafka:9092',
            'group.id': 'my-consumer-group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False   # manual commit for at-least-once
        })
        self.dlq_producer = Producer({'bootstrap.servers': 'kafka:9092'})
        self.dlq_topic = dlq_topic
        self.max_retries = max_retries
        self.consumer.subscribe([source_topic])
    
    def process_with_dlq(self):
        while True:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue
            
            success = False
            for attempt in range(self.max_retries):
                try:
                    self._process_record(msg)
                    success = True
                    break
                except TransientError as e:
                    # Retry transient failures
                    logger.warning(f"Transient failure attempt {attempt+1}: {e}")
                    time.sleep(2 ** attempt)  # exponential backoff
                except PermanentError as e:
                    # Don't retry permanent failures
                    logger.error(f"Permanent failure: {e}")
                    break
            
            if not success:
                # Route to DLQ with error metadata
                self._send_to_dlq(msg, context="max_retries_exceeded")
            
            # Commit only after processing (at-least-once)
            self.consumer.commit(msg)
    
    def _send_to_dlq(self, original_msg, context: str):
        """Wrap original message with error metadata and send to DLQ."""
        dlq_record = {
            'original_topic':     original_msg.topic(),
            'original_partition': original_msg.partition(),
            'original_offset':    original_msg.offset(),
            'original_key':       original_msg.key().decode('utf-8') if original_msg.key() else None,
            'original_value':     original_msg.value().decode('utf-8') if original_msg.value() else None,
            'error_context':      context,
            'failed_at':          datetime.utcnow().isoformat(),
            'consumer_group':     'my-consumer-group'
        }
        self.dlq_producer.produce(
            topic=self.dlq_topic,
            key=original_msg.key(),
            value=json.dumps(dlq_record).encode('utf-8')
        )
        self.dlq_producer.flush()
        logger.info(f"Sent to DLQ: offset {original_msg.offset()}")
    
    def _process_record(self, msg):
        """Business logic — raises TransientError or PermanentError."""
        try:
            data = json.loads(msg.value())
            # validate schema
            assert 'order_id' in data and 'amount' in data
            # write to database
            write_to_db(data)
        except json.JSONDecodeError as e:
            raise PermanentError(f"Invalid JSON: {e}")
        except AssertionError as e:
            raise PermanentError(f"Schema validation failed: {e}")
        except DatabaseTimeoutError as e:
            raise TransientError(f"DB timeout: {e}")

# DLQ monitoring: alert if DLQ has unread messages after 1 hour
# DLQ replay: fix the bug, then replay DLQ messages back to the original topic
```

---

## Backpressure Pattern

```python
"""
Backpressure: slow consumers signal producers to slow down.
Without backpressure: buffer overflow → OOM → crash.

Approaches:
1. Bounded queues: consumer reads from queue; if queue full, producer blocks
2. Rate limiting: consumer advertises max rate; producer caps at that rate
3. Credits/permits: consumer sends "credits" to producer; producer uses them
4. Kafka: natural backpressure via poll loop (consumer only requests when ready)
"""

# Kafka: natural backpressure
# Consumer controls pull rate:
import time

consumer = Consumer({'bootstrap.servers': 'kafka:9092', 'group.id': 'my-group'})
consumer.subscribe(['orders'])

MAX_RECORDS_PER_SECOND = 1000
BATCH_PAUSE = 1.0 / MAX_RECORDS_PER_SECOND

while True:
    batch = consumer.consume(num_messages=100, timeout=0.1)  # batch pull
    for msg in batch:
        start = time.time()
        process(msg)
        elapsed = time.time() - start
        sleep_time = BATCH_PAUSE - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)  # rate limit to MAX_RECORDS_PER_SECOND

# Spark Structured Streaming: maxOffsetsPerTrigger
spark.readStream.format("kafka") \
    .option("maxOffsetsPerTrigger", 50_000)  # limit per batch = backpressure

# Flink: automatic backpressure via network buffers
# If downstream is slow: buffers fill up → upstream stalls → source slows down
# No configuration needed — built into Flink's credit-based flow control
# Monitor via Flink Web UI: outPoolUsage > 0.8 = backpressured

# Circuit breaker pattern: if sink is down, don't buffer forever
class CircuitBreakerSink:
    CLOSED, OPEN, HALF_OPEN = 'closed', 'open', 'half_open'
    
    def __init__(self, failure_threshold=5, recovery_timeout=60):
        self.state = self.CLOSED
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.last_failure_time = None
    
    def write(self, record):
        if self.state == self.OPEN:
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = self.HALF_OPEN
            else:
                raise Exception("Circuit breaker OPEN — not writing")
        
        try:
            self._actual_write(record)
            if self.state == self.HALF_OPEN:
                self.state = self.CLOSED
                self.failure_count = 0
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = time.time()
            if self.failure_count >= self.failure_threshold:
                self.state = self.OPEN
                logger.error(f"Circuit breaker OPEN: {e}")
            raise
```

---

## Content-Based Routing Pattern

```python
"""
Route events to different destinations based on content.
Example: route orders to different queues based on tier (VIP, standard, etc.)
"""

from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'kafka:9092'})

def route_order(order: dict):
    """Route order to appropriate processing queue based on content."""
    
    amount = order.get('amount', 0)
    customer_tier = order.get('customer_tier', 'standard')
    region = order.get('region', 'us')
    is_fraud_flagged = order.get('fraud_score', 0) > 0.7
    
    # Route to DLQ if fraud flagged
    if is_fraud_flagged:
        topic = 'orders-fraud-review'
    # Route high-value orders to priority queue
    elif amount > 10_000 or customer_tier == 'enterprise':
        topic = 'orders-high-value'
    # Route by region
    elif region == 'eu':
        topic = 'orders-eu'
    else:
        topic = 'orders-standard'
    
    producer.produce(
        topic=topic,
        key=order['order_id'].encode('utf-8'),
        value=json.dumps(order).encode('utf-8')
    )

# Flink: multi-output routing with OutputTags
from org.apache.flink.util import OutputTag

OutputTag<Order> highValueTag = new OutputTag<Order>("high-value"){};
OutputTag<Order> fraudTag     = new OutputTag<Order>("fraud"){};

SingleOutputStreamOperator<Order> mainStream = orders
    .process(new ProcessFunction<Order, Order>() {
        @Override
        public void processElement(Order o, Context ctx, Collector<Order> out) {
            if (o.getFraudScore() > 0.7) {
                ctx.output(fraudTag, o);       // route to fraud side output
            } else if (o.getAmount() > 10_000) {
                ctx.output(highValueTag, o);   // route to high-value side output
            } else {
                out.collect(o);                // route to main output
            }
        }
    });

mainStream.getSideOutput(highValueTag).addSink(prioritySink);
mainStream.getSideOutput(fraudTag).addSink(fraudSink);
mainStream.addSink(standardSink);
```

---

## Exactly-Once Pattern

```
Achieving exactly-once delivery requires coordination across:
  Source:     idempotent reads (replay without duplicates)
  Processing: deterministic (same input → same output)
  Sink:       idempotent writes or transactional writes

Pattern 1: Idempotent write (simplest)
  Use natural primary key from business data
  Write with ON CONFLICT DO NOTHING or MERGE ON primary key
  Re-running with same data → no duplicates
  
  Example:
    INSERT INTO orders (order_id, amount, status)
    VALUES (%s, %s, %s)
    ON CONFLICT (order_id) DO UPDATE
    SET amount = EXCLUDED.amount, status = EXCLUDED.status;
  
  Safe to replay: running twice produces same result

Pattern 2: Transactional write (Kafka + Flink 2PC)
  Kafka transactions: producer writes within a transaction
  Transaction ID: stable across restarts (tied to job instance + partition)
  Pre-commit: write to transaction (not visible to consumers)
  Commit: when checkpoint completes → transaction committed → visible
  On failure: transaction rolled back → records replayed from checkpoint
  
  Consumer isolation: read_committed level (skips uncommitted transactions)

Pattern 3: Sequence number deduplication
  Store last-seen sequence number per partition key in Redis/DynamoDB
  On receive: check if sequence_number > last_seen → process, update
  On receive: if sequence_number <= last_seen → skip (duplicate)
  
  Trade-off: adds latency (one Redis round-trip per record)
             requires durable storage for sequence numbers
             complex cleanup when keys expire

Choose Pattern 1 when: sink supports UPSERT, primary key available (90% of cases)
Choose Pattern 2 when: Kafka sink, Flink processing, need atomic multi-record writes
Choose Pattern 3 when: sink is append-only, no natural primary key, must dedup per-partition
```

---

## Interview Tips

> **Tip 1:** "How do you design a DLQ strategy for a streaming pipeline?" — A good DLQ strategy has three parts: (1) Detection: classify errors as transient (retry: DB timeout, network blip) vs permanent (don't retry: schema mismatch, business rule violation). (2) Routing: for permanent failures, wrap the original message with error metadata (original topic/partition/offset, error message, timestamp, consumer group) and send to the DLQ topic. (3) Remediation: monitor DLQ for unexpected volumes (alert if DLQ > N records/hour), investigate and fix the root cause, then replay DLQ messages back through the main topic (or a replay topic) after fixing the bug. Never silently drop records — always route to DLQ with context.

> **Tip 2:** "What's the difference between at-least-once and exactly-once, and when does each matter?" — At-least-once: every record is processed at minimum once. On failure and retry, a record may be processed twice. Safe when processing is idempotent (count += 1 twice = wrong; INSERT ... ON CONFLICT DO NOTHING twice = safe). Exactly-once: every record processed exactly once, even on failure. Requires end-to-end coordination (source + processing + sink). Matters for: financial transactions, inventory updates, billing aggregations. Not needed for: monitoring dashboards (approximate is fine), log archival (duplicates easily filtered). In practice: exactly-once adds complexity and latency — use at-least-once with idempotent sinks for 90% of use cases.

> **Tip 3:** "How do you handle a poison pill message (one message that always causes the consumer to crash)?" — A poison pill causes a consumer crash loop: consume → fail → restart → consume same message → fail again. Solution: (1) Implement retry limit — if message fails N times, send to DLQ (not back to main topic); (2) Configure Kafka consumer with `max.poll.interval.ms` — if processing takes too long, the consumer group rebalances (don't process poison pills in an infinite loop); (3) Wrap processing in try-catch — never let a single message crash the process; (4) For Lambda: use `bisect-on-error` (splits the failing batch in half, recursively narrows to the one failing record, sends to DLQ). The DLQ receives the message for human inspection.
