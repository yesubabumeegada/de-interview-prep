---
title: "Lambda & Kappa Architecture — Scenarios"
topic: system-design
subtopic: lambda-kappa-architecture
content_type: scenario_question
tags: [lambda, kappa, architecture, scenarios]
---

# Lambda & Kappa Architecture — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: What Are Lambda and Kappa Architectures?

**Scenario:** Your interviewer asks you to explain Lambda and Kappa architectures. When would you choose one over the other?

<details>
<summary>💡 Hint</summary>

Lambda has two paths: batch (accurate, slow) and stream (fast, approximate). Kappa has one path: stream-only. Lambda handles reprocessing via batch layer; Kappa reprocesses by replaying Kafka. Consider operational complexity vs correctness requirements.

</details>

<details>
<summary>✅ Solution</summary>

**Lambda Architecture:**

```
Sources → Kafka → ┌─ Batch Layer (Spark) → Batch Views ─┐
                  │                                       ├─ Serving Layer → Queries
                  └─ Speed Layer (Flink) → RT Views ─────┘
```

- **Batch layer:** Processes all historical data, produces accurate results (hours latency)
- **Speed layer:** Processes recent data in real-time, fills the gap (seconds latency)
- **Serving layer:** Merges batch + speed views for queries

**Kappa Architecture:**

```
Sources → Kafka → Flink (single streaming job) → Views → Queries
                        ↑
               Replay from Kafka offset 0 for reprocessing
```

- Single processing path: streaming only
- Reprocessing: replay Kafka topic from beginning with updated logic
- Simpler to operate: one codebase, one runtime

**Comparison:**

| Factor | Lambda | Kappa |
|--------|--------|-------|
| Complexity | High (2 systems) | Low (1 system) |
| Reprocessing | Batch re-run | Kafka replay |
| Historical data | Unlimited (S3/lake) | Limited by Kafka retention |
| Latency | Dual (seconds + hours) | Single (seconds) |
| Accuracy | Batch layer is authoritative | Single authoritative stream |

**When to Choose:**
- **Lambda:** Need both real-time AND batch-accurate results for the same metric; historical data > Kafka retention window
- **Kappa:** Prefer operational simplicity; all processing logic fits in streaming; Kafka retention covers needed history (or source is replayable)

**Modern trend:** Kappa with lakehouse. Flink writes to Iceberg (serving as the "batch layer" equivalent), enabling both stream processing and historical reprocessing from the lake.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Implementing Kappa Architecture with Flink and Iceberg

**Scenario:** Your team is migrating from a Lambda architecture (Spark batch + Flink streaming) to a Kappa architecture. The current batch job processes 30 days of clickstream data nightly. Design the Kappa replacement and explain how you handle reprocessing when ML scoring logic changes.

<details>
<summary>💡 Hint</summary>

Kappa reprocessing = replay Kafka from offset 0 (or a specific timestamp) with a new job version. Key challenges: Kafka retention (30 days needed), state management during reprocessing, and serving layer updates without downtime.

</details>

<details>
<summary>✅ Solution</summary>

**Migration Plan:**

**Step 1: Extend Kafka Retention**
```bash
# Ensure 30+ days retention for reprocessing
kafka-configs.sh --bootstrap-server kafka:9092   --entity-type topics   --entity-name clickstream   --alter   --add-config retention.ms=2592000000  # 30 days in ms
```

**Step 2: Flink Job — Streaming to Iceberg**

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(60_000); // 1-min checkpoints

DataStream<ClickEvent> clicks = env
    .addSource(new FlinkKafkaConsumer<>("clickstream", schema, kafkaProps)
        .setStartFromEarliest());  // reprocessing: start from beginning

// Apply ML scoring
DataStream<ScoredEvent> scored = clicks
    .map(event -> {
        double score = scoringModel.predict(event.getFeatures());
        return new ScoredEvent(event, score);
    });

// Write to Iceberg with upsert (safe for reprocessing)
scored.addSink(
    IcebergSink.forRow(rowType)
        .tableLoader(tableLoader)
        .equalityFieldColumns(List.of("click_id"))  // upsert key
        .build()
);
```

**Step 3: Blue-Green Reprocessing**

When scoring logic changes, run reprocessing job in parallel without interrupting production:

```python
# Blue (current): writes to prod.gold.scored_clicks
# Green (new): writes to prod.gold.scored_clicks_v2

# 1. Start green job from Kafka beginning
green_job = start_flink_job(
    topic="clickstream",
    start_offset="earliest",
    output_table="prod.gold.scored_clicks_v2",
    scoring_model_version="v2"
)

# 2. Monitor catch-up progress
while not is_caught_up(green_job, lag_threshold_seconds=60):
    time.sleep(30)

# 3. Atomic cutover: update serving layer pointer
update_serving_config("scored_clicks", "prod.gold.scored_clicks_v2")

# 4. Stop blue job after traffic verified
stop_flink_job(blue_job_id)
```

**Step 4: Serving with Time-Travel**

```sql
-- During reprocessing, old results still available via Iceberg snapshots
SELECT * FROM prod.gold.scored_clicks
FOR SYSTEM_TIME AS OF '2024-01-15 00:00:00'
WHERE user_id = 12345;
-- Returns pre-reprocessing scores for debugging
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Exactly-Once Semantics in a Distributed Streaming Pipeline

**Scenario:** Your Flink pipeline reads from Kafka, joins with a Redis lookup, and writes to both Iceberg (for analytics) and PostgreSQL (for operational dashboards). Business requirement: each financial transaction must be counted exactly once in both sinks, even after job failures and restarts. Design the end-to-end exactly-once architecture.

<details>
<summary>💡 Hint</summary>

Exactly-once in Flink requires: exactly-once Kafka source (offset committed only on checkpoint), idempotent or transactional sinks. Iceberg supports exactly-once via two-phase commit. PostgreSQL needs idempotent writes (INSERT ... ON CONFLICT DO NOTHING). Redis lookups are read-only — no concern there.

</details>

<details>
<summary>✅ Solution</summary>

**Exactly-Once Guarantee Chain:**

```
Kafka (EOS source) → Flink (checkpoint barrier) → Iceberg (2PC sink)
                                                 → PostgreSQL (idempotent sink)
```

**1. Kafka Source — Exactly-Once**

```java
Properties kafkaProps = new Properties();
kafkaProps.put("isolation.level", "read_committed"); // only read committed msgs
kafkaProps.put("enable.auto.commit", "false"); // Flink manages offsets

FlinkKafkaConsumer<Transaction> source = new FlinkKafkaConsumer<>(
    "financial-transactions",
    new TransactionDeserializer(),
    kafkaProps
);
// Offsets committed ONLY when checkpoint completes
source.setCommitOffsetsOnCheckpoints(true);
```

**2. Flink Checkpoint Configuration**

```java
env.enableCheckpointing(30_000); // 30s checkpoints
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5_000);
env.getCheckpointConfig().setCheckpointTimeout(120_000); // 2 min timeout
env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

// Use RocksDB for large state (Redis join keys)
env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
env.getCheckpointConfig().setCheckpointStorage("s3://checkpoints/transactions/");
```

**3. Iceberg Sink — Two-Phase Commit**

```java
// Iceberg implements TwoPhaseCommitSinkFunction natively
DataStreamSink<RowData> icebergSink = FlinkSink.forRowData(stream)
    .tableLoader(TableLoader.fromCatalog(catalogLoader, tableId))
    .overwrite(false)
    // Exactly-once: files committed only when checkpoint succeeds
    .build();
```

How Iceberg 2PC works with Flink:
1. `preCommit`: write data files to S3 (not yet visible)
2. On checkpoint complete: `commit` — atomic metadata update makes files visible
3. On failure before checkpoint: files are orphaned (cleaned up by `remove_orphan_files`)

**4. PostgreSQL Sink — Idempotent Writes**

PostgreSQL doesn't support distributed 2PC with Flink's checkpoint protocol, so use idempotent writes:

```java
public class PostgresSink extends RichSinkFunction<Transaction> {
    private Connection conn;

    @Override
    public void invoke(Transaction txn, Context ctx) throws Exception {
        // ON CONFLICT DO NOTHING = idempotent
        PreparedStatement stmt = conn.prepareStatement("""
            INSERT INTO operational.transactions
                (transaction_id, amount, currency, processed_at, score)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (transaction_id) DO UPDATE SET
                score = EXCLUDED.score,
                processed_at = EXCLUDED.processed_at
        """);
        stmt.setString(1, txn.getId());
        stmt.setBigDecimal(2, txn.getAmount());
        stmt.setString(3, txn.getCurrency());
        stmt.setTimestamp(4, txn.getProcessedAt());
        stmt.setDouble(5, txn.getScore());
        stmt.executeUpdate();
    }
}
```

**5. Failure Scenario Analysis**

| Failure Point | What Happens | Recovery |
|--------------|--------------|----------|
| Flink task fails mid-checkpoint | Checkpoint aborted, restart from last checkpoint | Kafka offsets rewound, data re-read |
| Iceberg commit fails | Files on S3 but not committed | Next checkpoint attempt re-commits (2PC recovery) |
| PostgreSQL write fails | Exception → task restarts from checkpoint | Idempotent INSERT ON CONFLICT handles duplicates |
| Kafka broker down | Consumer blocks, checkpoint stalls | Alert; Flink waits for Kafka recovery |

**6. Monitoring Exactly-Once Health**

```python
# Prometheus metrics to watch
metrics = {
    "flink_jobmanager_job_lastCheckpointDuration": "< 10s ideally",
    "flink_jobmanager_job_numberOfFailedCheckpoints": "should be 0",
    "kafka_consumer_records_lag_max": "healthy < 10000",
    # Custom: duplicate detection
    "transactions_duplicate_count": "should always be 0"
}

# Audit query (run daily)
spark.sql("""
    SELECT transaction_id, count(*) as cnt
    FROM prod.gold.transactions
    GROUP BY transaction_id
    HAVING cnt > 1
""").show()  # Should return 0 rows
```

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What is the difference between at-least-once and exactly-once processing?" — At-least-once: records are processed at minimum once (duplicates possible on retry). Exactly-once: records are processed precisely once even after failures. Exactly-once requires coordinated checkpointing between source, processing, and sink.
> **Tip 2:** "Is exactly-once always necessary?" — No. For aggregation metrics (counts, sums), at-least-once with idempotent updates (increment counters) is sufficient. Exactly-once is critical for financial transactions, billing records, and any count-based SLAs.
> **Tip 3:** "What is Flink's two-phase commit protocol?" — On checkpoint, Flink sinks write data but don't commit (pre-commit). Only after all operators confirm the checkpoint succeeds does Flink call commit on each sink. If any step fails, uncommitted data is rolled back on restart.
