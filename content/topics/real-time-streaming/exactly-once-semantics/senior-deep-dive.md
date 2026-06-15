---
title: "Exactly-Once Semantics - Senior Deep Dive"
topic: real-time-streaming
subtopic: exactly-once-semantics
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [streaming, exactly-once, kafka, flink, transactions, two-phase-commit, distributed-systems]
---

# Exactly-Once Semantics — Senior Deep Dive

## Kafka Transactions: Protocol Internals

### Transaction Coordinator

Each Kafka broker can act as a **Transaction Coordinator** for a subset of producers. The coordinator is determined by hashing `transactional.id` to a partition of the internal `__transaction_state` topic.

```
Kafka transaction protocol (detailed):

  Producer                 Transaction Coordinator      Data Partitions
  ──────────────────────────────────────────────────────────────────────
  InitProducerIdRequest  → Assign PID, epoch, sequence  (persisted to __transaction_state)
  AddPartitionsToTxnRq   → Register partition in txn     Mark partition as "in_transaction"
  ProduceRequest         →                               Write data (UNCOMMITTED)
  AddOffsetsToTxnRq      → (for consume-transform-prod)  Register consumer group offsets
  TxnOffsetCommitRq      →                               Stage offset commit
  EndTxnRequest(COMMIT)  → Write PREPARE_COMMIT to log
                           → Send WriteTxnMarkersRq to each partition
                                                         Write COMMIT marker
                         → Write COMMITTED to log
```

### Zombie Fencing

When a producer restarts with the same `transactional.id`, Kafka increments the **epoch** number. Old producer instances receive `ProducerFencedException` on any write attempt, preventing split-brain writes from multiple producer instances.

```python
# Zombie fencing in action:
# Producer A (epoch=0) crashes
# Producer B restarts with same transactional.id (epoch=1)
# Producer A recovers and tries to write → ProducerFencedException
# This prevents A from corrupting B's transactions

producer = Producer({
    'transactional.id': 'order-processor-partition-3',  # maps to a fixed epoch sequence
    'enable.idempotence': True,
})
producer.init_transactions()  # fetches current epoch; fences any lower-epoch producers
```

### Exactly-Once Consume-Transform-Produce (Kafka → Kafka)

```python
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaError

consumer = Consumer({
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'order-enricher',
    'isolation.level': 'read_committed',
    'enable.auto.commit': False,       # manual commit inside transaction
})

producer = Producer({
    'bootstrap.servers': 'kafka:9092',
    'transactional.id': 'order-enricher-txn-1',
    'enable.idempotence': True,
})

producer.init_transactions()
consumer.subscribe(['raw-orders'])

while True:
    msgs = consumer.consume(num_messages=100, timeout=1.0)
    if not msgs:
        continue

    producer.begin_transaction()
    try:
        for msg in msgs:
            enriched = enrich_order(msg.value())
            producer.produce('enriched-orders', value=enriched)

        # Atomically commit offsets with the transaction
        # This is the key: offsets committed only when data committed
        offsets = {
            TopicPartition(msg.topic(), msg.partition(), msg.offset() + 1)
            for msg in msgs
        }
        producer.send_offsets_to_transaction(
            list(offsets),
            consumer.consumer_group_metadata()
        )
        producer.commit_transaction()

    except Exception as e:
        producer.abort_transaction()
        # Consumer will re-read from last committed offset
```

---

## Flink TwoPhaseCommitSinkFunction Deep Dive

### Protocol Steps

```
Flink 2PC with external sink:

  State:  PRE_COMMIT                    COMMITTED
          ──────────────────────────────────────────────────────────
  Step 1: Checkpoint barrier arrives at sink operator
  Step 2: snapshotState() → pre-commit current transaction, open new one
          Save transaction handle to checkpoint state
  Step 3: Checkpoint completes (all operators acknowledged)
  Step 4: notifyCheckpointComplete() → commit pre-committed transaction

  Failure scenarios:
  ┌──────────────────────────┬──────────────────────────────────────────┐
  │ Failure point            │ Recovery                                 │
  ├──────────────────────────┼──────────────────────────────────────────┤
  │ Before snapshotState     │ Replay from previous checkpoint          │
  │ During commit            │ Re-attempt commit (transaction handle    │
  │                          │ stored in checkpoint)                    │
  │ After commit             │ Checkpoint complete — no action needed   │
  └──────────────────────────┴──────────────────────────────────────────┘
```

### Custom TwoPhaseCommitSinkFunction (Java)

```java
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

public class PostgresExactlyOnceSink
    extends TwoPhaseCommitSinkFunction<Order, Connection, Void> {

    public PostgresExactlyOnceSink() {
        super(new ConnectionSerializer(), VoidSerializer.INSTANCE);
    }

    @Override
    protected Connection beginTransaction() throws Exception {
        Connection conn = DriverManager.getConnection(JDBC_URL, USER, PASS);
        conn.setAutoCommit(false);  // start transaction
        return conn;
    }

    @Override
    protected void invoke(Connection conn, Order order, Context context) throws Exception {
        PreparedStatement ps = conn.prepareStatement(
            "INSERT INTO orders (id, amount, ts) VALUES (?, ?, ?) " +
            "ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount"
        );
        ps.setString(1, order.getId());
        ps.setLong(2, order.getAmount());
        ps.setTimestamp(3, new Timestamp(order.getTs()));
        ps.executeUpdate();
    }

    @Override
    protected void preCommit(Connection conn) throws Exception {
        // Flush any pending writes — called at checkpoint barrier
    }

    @Override
    protected void commit(Connection conn) {
        try {
            conn.commit();
            conn.close();
        } catch (Exception e) {
            // commit() must be idempotent — may be called multiple times on recovery
        }
    }

    @Override
    protected void abort(Connection conn) {
        try {
            conn.rollback();
            conn.close();
        } catch (Exception e) { /* ignore */ }
    }
}
```

---

## Cost Analysis: Exactly-Once Overhead

```
Benchmark data (Kafka 3.x, 1KB messages, 10 partitions):

  Mode                    Throughput    P99 Latency   Notes
  ──────────────────────────────────────────────────────────────
  At-most-once            100%          baseline      No retries, no txn
  At-least-once           ~97%          +5ms          Retries, acks=all
  Exactly-once (txn)      ~85-90%       +10-20ms      2PC overhead
  Exactly-once + 2PC sink ~75-85%       +30-50ms      Additional commit RTT

  Kafka transaction coordinator bottleneck:
  - All transactions for a transactional.id go through ONE coordinator
  - High-frequency short transactions → coordinator becomes bottleneck
  - Solution: batch more records per transaction (tune linger.ms, batch.size)
```

### Tuning Kafka Transactions for Throughput

```python
producer = Producer({
    'bootstrap.servers': 'kafka:9092',
    'transactional.id': 'my-producer',
    'enable.idempotence': True,
    # Batch tuning — reduce transaction frequency
    'linger.ms': 50,           # wait up to 50ms to batch records
    'batch.size': 1048576,     # 1MB batch size
    'compression.type': 'lz4', # reduce network overhead
    # Transaction timeout — abort if no commit within this window
    'transaction.timeout.ms': 60000,  # 60s (must be less than broker's max)
})
```

---

## Deduplication at Scale: Advanced Patterns

### Watermark-Based Stream Deduplication (Flink SQL)

```sql
-- Flink SQL: deduplicate within a 1-hour window using ROW_NUMBER
SELECT order_id, amount, event_time
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY event_time ASC
    ) AS rn
  FROM orders_stream
)
WHERE rn = 1;
-- Flink maintains state only within the watermark window
-- Records outside the window are automatically evicted
```

### Spark dropDuplicatesWithinWatermark

```python
from pyspark.sql.functions import col, to_timestamp

df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "orders")
    .load()
)

parsed = df.select(
    col("value").cast("string").alias("json")
).selectExpr("from_json(json, 'order_id STRING, amount LONG, event_time TIMESTAMP') as d") \
 .select("d.*")

deduped = (
    parsed
    .withWatermark("event_time", "1 hour")        # allow 1h late arrivals
    .dropDuplicatesWithinWatermark(["order_id"])   # dedup within watermark window
)

query = (
    deduped.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/orders")
    .outputMode("append")
    .start("/data/delta/orders")
)
```

---

## End-to-End Exactly-Once Architecture

```
Production architecture for payment processing:

  Kafka (source)
  ├── enable.idempotence=true
  ├── acks=all
  ├── isolation.level=read_committed (consumers)
  └── transactional.id per processor instance

  Flink (processing)
  ├── checkpointing every 60s to S3
  ├── CheckpointingMode.EXACTLY_ONCE
  ├── KafkaSource with committed offsets
  └── KafkaSink with EXACTLY_ONCE delivery guarantee

  Delta Lake (sink)
  ├── ACID transactions
  ├── MERGE with order_id as dedup key
  └── Optimistic concurrency control

  Monitoring
  ├── Kafka consumer lag (Prometheus/Grafana)
  ├── Flink checkpoint duration and size
  ├── Duplicate rate metric (count of MERGE matched vs inserted)
  └── Alert: checkpoint failure → pipeline at risk of at-least-once
```

---

## Senior Interview Questions

**Q: Explain how Kafka's epoch mechanism prevents zombie producers.**
A: When a producer calls `initTransactions()` with a given `transactional.id`, the Transaction Coordinator increments the epoch associated with that ID. Any inflight requests from the previous producer instance (with the old epoch) are rejected with `ProducerFencedException`. This prevents two producer instances from writing under the same transactional context simultaneously.

**Q: What happens if a Flink job crashes between pre-commit and commit?**
A: The transaction handle is stored in the checkpoint state. On restart, Flink calls `notifyCheckpointComplete()` for the recovered checkpoint, which re-attempts the commit. Since `commit()` must be idempotent, this is safe. If the crash happens before `snapshotState()` captures the handle, Flink replays from the previous checkpoint and the transaction is aborted.

**Q: Why can't you guarantee exactly-once to an arbitrary REST API?**
A: REST APIs typically lack idempotency keys or transactional semantics. If Flink commits a checkpoint but the HTTP request times out, Flink will retry the request on recovery — potentially duplicating the side effect. The solution is to design the API with idempotency keys (include a unique request ID that the server deduplicates).
