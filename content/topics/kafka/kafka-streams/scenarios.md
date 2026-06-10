---
title: "Kafka Streams - Scenario Questions"
topic: kafka
subtopic: kafka-streams
content_type: scenario_question
tags: [kafka, kafka-streams, windowing, stateful, joins, EOS, interactive-queries]
---

# Scenario Questions — Kafka Streams

<article data-difficulty="junior">

## 🟢 Junior: Counting Events Per User

**Scenario:** You receive a stream of click events on the `clicks` topic. Each message has `user_id` as the key. You need to maintain a running count of clicks per user and write it to the `click-counts` topic.

<details>
<summary>💡 Hint</summary>

Think about which abstraction models "latest count per user" — KStream or KTable? Use `groupByKey()` and `count()`. The output is a KTable (one count per user), which you convert back to a stream to write to the output topic.
</details>

<details>
<summary>✅ Solution</summary>

```java
StreamsBuilder builder = new StreamsBuilder();

KStream<String, String> clicks = builder.stream("clicks");

KTable<String, Long> clickCounts = clicks
    .groupByKey()
    .count(Materialized.as("click-count-store"));

clickCounts.toStream().to("click-counts",
    Produced.with(Serdes.String(), Serdes.Long()));
```

**Key points:**
- `groupByKey()` groups by existing key (user_id) — no repartition needed
- `count()` returns a KTable — one entry per key (latest count)
- `.toStream()` converts KTable to KStream for writing to output topic
- The state is stored in RocksDB under `click-count-store` and backed up to a changelog topic
</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Revenue Per Category Per Hour

**Scenario:** You have an `orders` topic with events containing `category` (String) and `amount` (Double). Compute total revenue per category per hour and emit a single final result per window (not intermediate updates). Handle late events arriving up to 15 minutes after the window closes.

<details>
<summary>💡 Hint</summary>

Use a tumbling window of 1 hour with a 15-minute grace period. Use `suppress(untilWindowCloses(...))` to emit only the final result after the window closes. You'll need to `groupBy` on category (not the existing key), then aggregate.
</details>

<details>
<summary>✅ Solution</summary>

```java
StreamsBuilder builder = new StreamsBuilder();

KStream<String, Order> orders = builder.stream(
    "orders", Consumed.with(Serdes.String(), orderSerde));

// Re-key by category (triggers repartition)
KTable<Windowed<String>, Double> hourlyRevenue = orders
    .selectKey((k, order) -> order.getCategory())
    .groupByKey()
    .windowedBy(
        TimeWindows.ofSizeAndGrace(Duration.ofHours(1), Duration.ofMinutes(15))
    )
    .aggregate(
        () -> 0.0,
        (category, order, total) -> total + order.getAmount(),
        Materialized.<String, Double, WindowStore<Bytes, byte[]>>as("hourly-revenue-store")
            .withValueSerde(Serdes.Double())
    )
    .suppress(
        Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded())
    );

hourlyRevenue.toStream()
    .map((windowed, revenue) -> KeyValue.pair(
        windowed.key() + "@" + windowed.window().startTime().toEpochMilli(),
        revenue
    ))
    .to("hourly-revenue", Produced.with(Serdes.String(), Serdes.Double()));
```

**Key points:**
- `selectKey()` on category triggers a repartition (internal topic created automatically)
- Grace period accepts events up to 15 min after window close
- `suppress()` holds results until the window fully closes — no intermediate updates emitted
- `suppress()` requires state memory; `BufferConfig.maxRecords(n)` or `maxBytes(n)` limits can be used as safety valves
</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Late Event Handling and Exactly-Once Billing

**Scenario:** Your company bills customers based on the total GB of data transferred per hour. Data transfer events arrive on `transfers` topic, keyed by `account_id`, with a timestamp embedded in the payload (not the Kafka timestamp). Events can arrive up to 2 hours late due to mobile device buffering. You need to:
1. Use event time (not processing time)
2. Emit exactly one billing record per account per hour
3. Guarantee exactly-once processing end-to-end

**Question:** Design the full Kafka Streams topology including configuration choices. What are the failure modes and how does your design handle them?

<details>
<summary>💡 Hint</summary>

Custom `TimestampExtractor` extracts event time from payload. Use `TimeWindows.ofSizeAndGrace` with 2-hour grace. `suppress(untilWindowCloses)` for final-only emission. `EXACTLY_ONCE_V2` processing guarantee. Consider what happens if the app crashes between suppress buffer and commit.
</details>

<details>
<summary>✅ Solution</summary>

**Architecture:**

```mermaid
graph TD
    A["transfers topic<br>event time in payload"] --> B["TimestampExtractor<br>extracts event ts"]
    B --> C["groupByKey<br>by account_id"]
    C --> D["1-hour tumbling window<br>2-hour grace period"]
    D --> E["aggregate GB transferred"]
    E --> F["suppress untilWindowCloses<br>buffer in memory"]
    F --> G["billing-events topic<br>exactly-once"]
```

**Implementation:**

```java
// Custom timestamp extractor
public class TransferTimestampExtractor implements TimestampExtractor {
    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        TransferEvent event = (TransferEvent) record.value();
        return event != null ? event.getEventTimeMs() : partitionTime;
    }
}

// Config
Properties config = new Properties();
config.put(StreamsConfig.APPLICATION_ID_CONFIG, "billing-processor");
config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker:9092");
config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG,
           TransferTimestampExtractor.class);
config.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);  // fast failover

// Topology
StreamsBuilder builder = new StreamsBuilder();

KTable<Windowed<String>, Double> hourlyBilling = builder
    .stream("transfers", Consumed.with(Serdes.String(), transferSerde))
    .groupByKey()
    .windowedBy(
        TimeWindows.ofSizeAndGrace(Duration.ofHours(1), Duration.ofHours(2))
    )
    .aggregate(
        () -> 0.0,
        (accountId, transfer, totalGb) -> totalGb + transfer.getGbTransferred(),
        Materialized.<String, Double, WindowStore<Bytes, byte[]>>as("billing-store")
            .withValueSerde(Serdes.Double())
    )
    .suppress(
        Suppressed.untilWindowCloses(
            Suppressed.BufferConfig.maxBytes(512 * 1024 * 1024L)  // 512 MB
                .emitEarlyWhenFull()   // safety: emit early if buffer fills
        )
    );

hourlyBilling.toStream()
    .map((windowed, totalGb) -> KeyValue.pair(
        windowed.key(),
        new BillingRecord(windowed.key(), windowed.window(), totalGb)
    ))
    .to("billing-events", Produced.with(Serdes.String(), billingSerde));
```

**Failure modes and mitigations:**

| Failure | Impact | Mitigation |
|---------|--------|-----------|
| App crash during suppress buffer | Buffer lost; window re-computed from changelog | EOS: transaction aborted; state restored from changelog |
| Suppress buffer full | Early emission (partial result) | Set large `maxBytes`, monitor `suppress-buffer-size-avg` |
| Event arrives after grace period | Late event dropped | Alert on late arrival rate; extend grace if needed |
| Standby instance unavailable | Slower failover (replay changelog) | Monitor standby lag metric; ensure replicas are healthy |

**Key design decisions:**
- `EXACTLY_ONCE_V2` wraps output + offset commit in single Kafka transaction
- Standby replicas ensure state recovery in seconds, not minutes
- `emitEarlyWhenFull()` prevents OOM but produces a partial result — downstream billing must handle potential follow-up correction records
- Monitor `records-lag-max` specifically against LSO (not HWM) for `read_committed` consumers downstream
</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is Kafka Streams and how does it differ from Kafka consumers?**
A: Kafka Streams is a client-side stream processing library that runs inside your Java application without a separate cluster. Unlike plain Kafka consumers, it provides stateful operations (aggregations, joins, windowing), built-in fault tolerance via changelog topics, and interactive query capabilities.

**Q: What is a KStream vs. a KTable?**
A: A KStream represents an unbounded stream of events — each record is an independent event (append semantics). A KTable represents a changelog stream — each record is an update to a key's latest value (upsert semantics), maintaining a materialized view of the latest state per key.

**Q: What is a GlobalKTable?**
A: A GlobalKTable is fully replicated to every Streams instance (unlike KTable which is partitioned). It's used for joining a large stream against a small lookup table without requiring co-partitioning, since every instance has the full table locally.

**Q: How does Kafka Streams handle stateful operations and fault tolerance?**
A: Stateful operations use RocksDB stores backed by Kafka changelog topics. On failure or restart, Streams restores the state by replaying the changelog topic, restoring the exact state before the failure. Standby replicas (optional) pre-populate state on other instances to reduce recovery time.

**Q: What is a windowed aggregation in Kafka Streams?**
A: Windowed aggregations group stream records by key within time windows — tumbling (fixed non-overlapping), hopping (overlapping), or session (activity-based). Kafka Streams manages per-window state stores and handles late records based on the configured grace period.

**Q: How does co-partitioning affect Kafka Streams joins?**
A: KStream-KTable and KStream-KStream joins require co-partitioned inputs — the same key must land in the same partition for both topics. This is enforced by using the same number of partitions and the same partitioner. Violating this causes incorrect join results.

**Q: What is the difference between Kafka Streams and Apache Flink?**
A: Kafka Streams is a Java library embedded in your application, suitable for simpler stateful stream processing with Kafka as the only source and sink. Flink is a standalone distributed streaming engine supporting complex stateful processing, multiple sources/sinks, and advanced windowing — preferred for large-scale, multi-source pipelines.

**Q: How do you scale a Kafka Streams application?**
A: Run multiple instances of the Streams application with the same `application.id`. Kafka Streams automatically distributes partitions across instances, providing horizontal scaling. The maximum parallelism equals the maximum number of partitions across all input topics.

---

## 💼 Interview Tips

- Know the KStream vs. KTable distinction well — it's the most common Kafka Streams interview question. Use append-vs-upsert semantics as your framing, with practical examples (event log vs. user profile table).
- Explain co-partitioning as a join prerequisite — not knowing this is a red flag for anyone who claims Kafka Streams experience, as co-partitioning issues are a frequent production bug.
- Discuss changelog topic-based state recovery — it's Kafka Streams' key fault tolerance mechanism and understanding it shows you grasp the architectural design, not just the API.
- Compare Kafka Streams to Flink honestly: Streams is simpler to operate (no cluster, Java library only, Kafka-native); Flink is more powerful (complex stateful ops, multiple sources). Knowing when each is appropriate shows judgment.
- For senior roles, discuss standby replicas and interactive queries — they're advanced features that show production-level depth beyond basic aggregations.
- Be ready to discuss exactly-once in Kafka Streams: set `processing.guarantee=exactly_once_v2`, which handles transactions automatically — knowing the config and its performance implications separates seniors from juniors.
