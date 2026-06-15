---
title: "Exactly-Once Semantics - Scenario Questions"
topic: real-time-streaming
subtopic: exactly-once-semantics
content_type: scenario_question
tags: [streaming, exactly-once, scenarios, interview, kafka, flink, deduplication]
---

# Exactly-Once Semantics — Scenario Questions

<article data-difficulty="junior">

## Scenario: Duplicate Orders in the Database

Your team runs a Kafka → Python consumer → PostgreSQL pipeline for order ingestion. Users are complaining that they occasionally see duplicate orders in the database. The pipeline uses `enable.auto.commit=True` in the Python consumer and does a plain `INSERT` for each order.

**Questions:**
1. What delivery guarantee does this configuration provide, and why?
2. What are two ways duplicates can occur in this setup?
3. How would you fix the sink to tolerate at-least-once delivery?
4. Would enabling `enable.idempotence=True` on the producer solve this problem? Why or why not?

<details>
<summary>✅ Solution</summary>

**1. Delivery guarantee:** This is **at-least-once** (in practice closer to at-most-once in some failure modes). With `enable.auto.commit=True`, Kafka commits offsets periodically regardless of whether the `INSERT` succeeded.

**2. How duplicates occur:**
- **Scenario A (consumer crash before auto-commit):** Consumer processes the order and does the INSERT, but crashes before the next auto-commit interval. On restart, Kafka replays from the last committed offset → double INSERT.
- **Scenario B (network retry):** The INSERT succeeds on PostgreSQL but the connection drops before the Python code gets the response. The code retries → duplicate row.

**3. Fix the sink — use an upsert:**
```python
import psycopg2

conn = psycopg2.connect(...)
cursor = conn.cursor()

cursor.execute("""
    INSERT INTO orders (order_id, customer_id, amount, created_at)
    VALUES (%s, %s, %s, %s)
    ON CONFLICT (order_id) DO NOTHING
""", (order_id, customer_id, amount, created_at))

conn.commit()
# Then manually commit Kafka offset AFTER successful DB commit
consumer.commit()
```
With `ON CONFLICT DO NOTHING`, re-inserting the same `order_id` is safe.

**4. Idempotent producer alone does NOT solve this:** `enable.idempotence=True` prevents the **producer** from sending duplicate messages to Kafka (transport-level dedup). But the duplicates here happen at the **consumer+sink level** — the producer has no role in that. You need idempotency at the sink (the `ON CONFLICT` clause).

</details>
</article>

---

<article data-difficulty="mid">

## Scenario: Flink Job Restart Causes Duplicate Charges

Your company runs a Flink streaming job that reads from Kafka (`payments` topic) and writes processed payment records to a PostgreSQL table using a JDBC sink. The job uses exactly-once checkpointing. After a TaskManager crash, the job restarts from the last checkpoint, but the DBA reports seeing duplicate payment records in PostgreSQL.

**Questions:**
1. Why does exactly-once checkpointing in Flink not automatically guarantee exactly-once to PostgreSQL JDBC?
2. What is the `TwoPhaseCommitSinkFunction` and how would it solve this?
3. As an alternative, how could you redesign the sink using `foreachBatch`-style idempotent writes?
4. What state TTL consideration applies to the deduplication state in Flink?

<details>
<summary>✅ Solution</summary>

**1. Why checkpointing alone is not enough:**
Flink's exactly-once checkpointing guarantees that Flink's **internal state** is consistent after recovery. But the JDBC sink writes to PostgreSQL on every record. If the job processes records, writes to PostgreSQL, but crashes before the checkpoint completes, on restart Flink replays those records (from the checkpointed Kafka offset) and writes them again — PostgreSQL gets duplicates because it has no knowledge of Flink's checkpoint state.

**2. TwoPhaseCommitSinkFunction:**
This Flink class implements a two-phase commit protocol with the external system:
- **Phase 1 (pre-commit):** At checkpoint barrier, the sink flushes writes into a pending transaction (not yet committed to the DB). The transaction handle is saved in checkpoint state.
- **Phase 2 (commit):** When checkpoint completes, `notifyCheckpointComplete()` commits the DB transaction.
- **On failure:** If the job restarts, it finds the pending transaction handle in the checkpoint and re-attempts the commit (or aborts if the checkpoint failed).

This requires the external system to support transactions (PostgreSQL does).

```java
// Pre-commit: open a JDBC transaction, hold it
@Override
protected void preCommit(Connection conn) throws Exception {
    // Writes are already done via invoke(); transaction is open
}

@Override
protected void commit(Connection conn) {
    conn.commit();  // called when checkpoint succeeds
}

@Override
protected void abort(Connection conn) {
    conn.rollback();  // called when checkpoint fails
}
```

**3. Idempotent upsert alternative (simpler, preferred for PostgreSQL):**
```java
// In the JDBC sink, use UPSERT instead of INSERT:
String sql = "INSERT INTO payments (payment_id, amount, status) VALUES (?, ?, ?) " +
             "ON CONFLICT (payment_id) DO UPDATE SET status = EXCLUDED.status";
// Now re-running the same records on recovery is idempotent
// No need for 2PC — the sink itself is idempotent
```
Combined with Flink checkpointing (replays from correct Kafka offset), this gives end-to-end exactly-once behavior without the complexity of 2PC.

**4. State TTL for deduplication:**
If you use a Flink `KeyedProcessFunction` to deduplicate by `payment_id`, the state (seen payment IDs) grows unboundedly without TTL. Configure:
```java
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.hours(24))  // payments are retried within 24h max
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .build();
descriptor.enableTimeToLive(ttlConfig);
```
TTL should be at least as long as the maximum window in which duplicates can arrive. Too short → legitimate duplicates slip through. Too long → state grows large and increases checkpoint size.

</details>
</article>

---

<article data-difficulty="senior">

## Scenario: Designing End-to-End Exactly-Once for a Financial Ledger

You are the lead data engineer at a payments company. You must design an end-to-end exactly-once pipeline for the following requirement:

- **Source:** Mobile payment events ingested via Kafka (1M events/day, peaks at 200K/hour)
- **Processing:** Flink enriches events (lookup account info from Redis, validate amounts)
- **Sink:** Delta Lake ledger table on S3 (must be the source of truth for billing)
- **Constraint:** Any duplicate in the ledger causes a regulatory audit finding
- **Constraint:** P99 latency must be under 500ms end-to-end
- **Constraint:** Pipeline must recover within 5 minutes of any single-node failure

**Questions:**
1. Design the full exactly-once architecture. What configuration is required at each layer?
2. How do you handle the case where the Redis lookup (enrichment) is not transactional?
3. What is the checkpoint strategy, and how does it interact with Delta Lake commits?
4. How do you validate that the pipeline is actually maintaining exactly-once semantics in production? What metrics and reconciliation jobs would you build?
5. What are the tradeoffs of this design vs a simpler at-least-once + dedup approach?

<details>
<summary>✅ Solution</summary>

**1. Full architecture design:**

```
Layer 1 — Kafka (source):
  - enable.idempotence=true (producer side)
  - acks=all, min.insync.replicas=2
  - Consumer: isolation.level=read_committed
  - Consumer: enable.auto.commit=false (manual commit tied to checkpoint)

Layer 2 — Flink (processing):
  - Checkpointing: every 30s, EXACTLY_ONCE mode, min pause 10s
  - Checkpoint storage: S3 (incremental checkpoints with RocksDB)
  - KafkaSource: committed offsets initializer, offset stored in checkpoint
  - Parallelism: 8 (matches Kafka partition count for 1:1 assignment)
  - State backend: RocksDB (for large deduplication state)
  - Restart strategy: fixed-delay, 3 attempts, 30s delay

Layer 3 — Delta Lake (sink):
  - Flink-Delta connector (or foreachBatch via Spark mini-jobs)
  - MERGE on payment_id (idempotent upsert)
  - Delta transaction log ensures atomicity
  - Checkpoint completion triggers Delta commit (2PC-like)
```

**2. Handling non-transactional Redis lookup:**
Redis enrichment (account lookup) is a read operation — it doesn't write state, so it's not part of the transaction boundary. The concern is stale reads: if account data changes between the original processing and a replay, the enriched value might differ.

Solution: **enrich at sink time, not processing time** — store raw events in Delta and enrich in a downstream batch job. Or: accept that enrichment is best-effort and store the enriched value with a `enriched_at` timestamp, allowing downstream correction.

For the deduplication key (`payment_id`), Redis can serve as a fast L1 cache:
```python
def process_payment(payment):
    # Fast path: Redis check (probabilistic, bloom filter)
    if redis_client.bf().exists("seen_payments", payment.payment_id):
        return None  # likely duplicate, skip enrichment

    # Expensive enrichment only for new payments
    account = redis_client.hgetall(f"account:{payment.account_id}")
    enriched = {**payment, "account_name": account["name"]}

    # Flink state is the source of truth for dedup (Redis is best-effort)
    return enriched
```

**3. Checkpoint strategy and Delta interaction:**

```
Timeline:
  t=0:   Flink begins processing batch B1
  t=30s: Checkpoint C1 triggered
         - Flink operators snapshot state to S3
         - KafkaSource records offset O1 in checkpoint
         - Delta sink pre-commits: writes parquet files, opens Delta transaction T1
  t=35s: Checkpoint C1 complete (all operators acknowledged)
         - notifyCheckpointComplete() fires
         - Delta transaction T1 committed (new snapshot added to _delta_log)
         - Kafka offsets O1 committed to __consumer_offsets
  t=60s: Checkpoint C2 triggered...

Recovery scenario (crash at t=33s, between checkpoint complete and Delta commit):
  - Flink restarts, loads state from C1
  - Delta transaction T1 is in pre-committed state (not in _delta_log yet)
  - Flink replays events from Kafka offset O1
  - Creates new parquet files and commits new Delta transaction T2
  - T1's parquet files become orphaned (cleaned up by VACUUM)
  → No duplicates in Delta, no data loss
```

**4. Production validation — metrics and reconciliation:**

```python
# Real-time metrics:
# - Flink: checkpoint_duration_ms, checkpoint_failed_count, records_in/out per operator
# - Kafka: consumer_lag (alert if > 10K records)
# - Delta: duplicate_merge_count (WHEN MATCHED vs WHEN NOT MATCHED ratio)

# Daily reconciliation job (Spark batch):
def reconcile_payments(processing_date: str):
    """Compare source count vs sink count and flag anomalies."""
    kafka_count = spark.sql(f"""
        SELECT COUNT(DISTINCT payment_id)
        FROM kafka_audit_log
        WHERE DATE(event_time) = '{processing_date}'
    """).collect()[0][0]

    delta_count = spark.sql(f"""
        SELECT COUNT(*)
        FROM delta.`/data/delta/payments`
        WHERE DATE(event_time) = '{processing_date}'
    """).collect()[0][0]

    # Check for duplicates within Delta
    delta_dup_count = spark.sql(f"""
        SELECT COUNT(*) - COUNT(DISTINCT payment_id)
        FROM delta.`/data/delta/payments`
        WHERE DATE(event_time) = '{processing_date}'
    """).collect()[0][0]

    assert delta_dup_count == 0, f"DUPLICATES FOUND: {delta_dup_count}"
    assert abs(kafka_count - delta_count) < 10, f"COUNT MISMATCH: kafka={kafka_count}, delta={delta_count}"
```

**5. Tradeoffs vs at-least-once + dedup approach:**

```
Exactly-once (this design):
  + Stronger guarantee — duplicates prevented at all layers
  + Simpler audit story ("we guarantee exactly-once")
  - 10-15% throughput overhead from 2PC and checkpointing
  - Higher complexity (checkpoint tuning, Delta connector config)
  - Slower recovery (30s checkpoint interval + state restore)
  - More expensive (S3 checkpoint storage, larger state)

At-least-once + idempotent sink + reconciliation:
  + Simpler pipeline, higher throughput
  + Faster recovery (no large state to restore)
  - Duplicates reach the sink; must be deduped by MERGE
  - Reconciliation job is critical path for correctness
  - "Eventual exactly-once" — short window where duplicates visible
  - Requires more careful sink design

Recommendation: For this regulatory use case (billing ledger), use exactly-once.
For analytics/reporting pipelines at this same company, use at-least-once + MERGE.
```

</details>
</article>
