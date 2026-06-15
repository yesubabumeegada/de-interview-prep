---
title: "Kafka-Flink Integration - Scenario Questions"
topic: real-time-streaming
subtopic: kafka-flink-integration
content_type: scenario_question
tags: [kafka, flink, streaming, scenarios, interview, integration, checkpointing]
---

# Kafka-Flink Integration — Scenario Questions

<article data-difficulty="junior">

## Scenario: Consumer Group Lag Growing

Your team has a Flink job reading from a Kafka topic `user-events` (8 partitions) and writing aggregated counts to a downstream Kafka topic. You check the Kafka consumer group metrics and see the lag has grown from 0 to 500,000 records over the past 30 minutes. The Flink job is still running (no errors).

**Questions:**
1. What does "consumer lag" mean in this context?
2. List three possible causes for this lag buildup.
3. How would you identify which cause is responsible using available tools?
4. What would you change if the root cause is that the Flink job's parallelism is too low?

<details>
<summary>✅ Solution</summary>

**1. Consumer lag definition:**
Consumer lag is the difference between the latest offset in a Kafka partition and the last committed offset by the consumer group. A lag of 500,000 means the consumer (Flink) is 500,000 records behind the latest produced records — it's not keeping up with the incoming rate.

**2. Three possible causes:**
- **Insufficient parallelism:** Flink is processing events slower than they arrive. E.g., Kafka has 8 partitions but Flink source parallelism is 2 — each Flink task reads 4 partitions but processes too slowly.
- **Backpressure from a slow operator:** A downstream operator (e.g., an external lookup or expensive aggregation) is slow, backing up pressure to the source, which slows Kafka polling.
- **Slow or unavailable sink:** If the downstream Kafka topic or broker is experiencing issues, the Flink sink blocks, causing backpressure throughout the pipeline.

**3. Identification approach:**
- **Flink Web UI:** Check the "Backpressure" tab for each operator. A backpressured operator (shown in red/orange) is the bottleneck. Navigate upstream from the bottleneck to find the root cause.
- **Kafka metrics:** Check `kafka_consumer_lag` per partition — if lag is evenly distributed, it's a throughput problem. If concentrated on specific partitions, it could be data skew.
- **TaskManager metrics:** Check CPU and memory usage. High GC pauses or CPU saturation indicate resource contention.
- **Flink checkpoint duration:** If checkpoints are taking very long, it's a sign of overall pipeline stress.

**4. Fix: increase parallelism**
```python
# Increase source parallelism to match Kafka partitions
source = KafkaSource.builder()...build()

stream = env.from_source(source, watermark_strategy, "User Events Source")
stream = stream.set_parallelism(8)  # match 8 Kafka partitions (was 2)

# Also scale downstream operators
aggregated = (
    stream
    .key_by(lambda e: e["user_id"])
    .reduce(...)
    .set_parallelism(8)
)
```
Note: you cannot set source parallelism higher than the number of Kafka partitions. If lag persists, you must first increase Kafka partition count (requires topic recreation or `kafka-topics.sh --alter`).

</details>
</article>

---

<article data-difficulty="mid">

## Scenario: Watermarks Not Advancing, Windows Never Fire

You have a Flink job with a 5-minute tumbling window on a Kafka source with 6 partitions. You've been running for 20 minutes and no window has ever fired. You check the Flink Web UI and see the watermark is stuck at -9223372036854775808 (Long.MIN_VALUE) on one source subtask.

**Questions:**
1. What does a watermark of Long.MIN_VALUE indicate?
2. What are the two most likely causes for this specific symptom?
3. How would you fix each cause?
4. How would you configure watermarks to handle partition idleness in production?

<details>
<summary>✅ Solution</summary>

**1. Long.MIN_VALUE watermark:**
This is Flink's representation of "no watermark has been emitted yet." A source subtask emits `Long.MIN_VALUE` before it receives any events (or when events have no valid timestamps). Flink's global watermark is the minimum across all subtasks — one subtask stuck at `Long.MIN_VALUE` blocks the entire pipeline's watermark from ever advancing.

**2. Two most likely causes:**

**Cause A — Idle partition (no messages):**
One of the 6 Kafka partitions has received no messages, so its assigned Flink subtask has never seen an event and therefore never emitted a watermark.

**Cause B — Timestamp extraction returning null/negative:**
The timestamp extractor is returning `Long.MIN_VALUE` (e.g., because event timestamps are null or in epoch-seconds rather than epoch-milliseconds, causing nonsensical values far in the past).

**3. Fixes:**

Fix for Cause A (idle partition):
```java
WatermarkStrategy<Event> strategy = WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
    .withTimestampAssigner((event, ts) -> event.getEventTimeMs())
    .withIdleness(Duration.ofSeconds(30));  // mark idle after 30s of no events
// Idle partitions are excluded from global watermark calculation
```

Fix for Cause B (wrong timestamp extraction):
```java
WatermarkStrategy<Event> strategy = WatermarkStrategy
    .<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
    .withTimestampAssigner((event, ts) -> {
        long eventTime = event.getEventTimeMs();
        if (eventTime <= 0 || eventTime > System.currentTimeMillis() + 3_600_000L) {
            // Reject obviously wrong timestamps — fall back to processing time
            return System.currentTimeMillis();
        }
        return eventTime;
    });
```

**4. Production watermark configuration:**
```python
from pyflink.common import WatermarkStrategy
from pyflink.common.time import Duration

strategy = (
    WatermarkStrategy
    .for_bounded_out_of_orderness(Duration.of_seconds(10))  # tolerate 10s late arrivals
    .with_timestamp_assigner(...)
    .with_idleness(Duration.of_seconds(30))  # handle idle partitions
)

# Monitor watermark progress in production:
# Flink metric: <operator>.currentInputWatermark (per subtask)
# Alert if watermark age > 5 minutes (indicating stuck watermark)
```

</details>
</article>

---

<article data-difficulty="senior">

## Scenario: Designing a Multi-Region Kafka-Flink Pipeline

Your company is expanding globally. You need to design a Kafka-Flink streaming pipeline that:
- Processes user activity events from both `us-east-1` and `eu-west-1` Kafka clusters
- Runs Flink processing in `us-east-1` only (single region compute)
- Writes results to both a Delta Lake table (analytics) and a Kafka topic (real-time alerts)
- Must survive a failure of the `eu-west-1` Kafka cluster without stopping the pipeline
- Must maintain exactly-once semantics for the Delta Lake write

**Questions:**
1. How would you use Kafka MirrorMaker 2 (MM2) to aggregate events from both regions into the `us-east-1` Flink cluster?
2. How does the Flink job's watermark strategy need to account for cross-region event-time skew?
3. Design the checkpoint and recovery strategy for this multi-region setup.
4. What happens to the pipeline when `eu-west-1` Kafka is unavailable? How do you handle partial region failure?
5. What are the data residency / GDPR concerns with this architecture, and how would you address them?

<details>
<summary>✅ Solution</summary>

**1. MirrorMaker 2 for cross-region aggregation:**

```yaml
# mm2.properties (runs in us-east-1)
clusters = us-east-1, eu-west-1
us-east-1.bootstrap.servers = kafka-us:9092
eu-west-1.bootstrap.servers = kafka-eu:9092

# Replicate eu events to us
eu-west-1->us-east-1.enabled = true
eu-west-1->us-east-1.topics = user-activity
eu-west-1->us-east-1.replication.factor = 3
eu-west-1->us-east-1.sync.group.offsets.enabled = true

# Topic naming in us-east-1:
# user-activity (local US events)
# eu-west-1.user-activity (replicated EU events)
```

Flink consumes from both topics:
```java
KafkaSource<UserEvent> source = KafkaSource.<UserEvent>builder()
    .setBootstrapServers("kafka-us:9092")  // only connect to us-east-1
    .setTopics("user-activity", "eu-west-1.user-activity")  // both US and replicated EU
    .setGroupId("activity-processor")
    ...
    .build();
```

**2. Cross-region event-time skew:**

EU events are replicated to US with some MirrorMaker lag (typically 1-5 seconds, can spike to 30+ seconds during network issues). This creates out-of-orderness: US events at time T may arrive before EU events at time T-1 second.

```java
// Increase out-of-orderness tolerance to account for replication lag
WatermarkStrategy<UserEvent> strategy = WatermarkStrategy
    .<UserEvent>forBoundedOutOfOrderness(Duration.ofSeconds(60))  // tolerate 60s (MirrorMaker lag buffer)
    .withTimestampAssigner((event, ts) -> event.getEventTimeMs())
    .withIdleness(Duration.ofSeconds(120));  // if EU cluster fails, don't block on idle partitions

// Trade-off: 60s watermark bound = 60s minimum window latency
// For real-time alerts, use a separate processing-time triggered branch:
DataStream<Alert> processingTimeAlerts = stream
    .keyBy(UserEvent::getUserId)
    .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
    .apply(new AlertFunction());  // faster, less accurate
```

**3. Checkpoint and recovery strategy:**

```
Checkpoint design:
  - Interval: 60 seconds (balance recovery time vs S3 cost)
  - Storage: S3 in us-east-1 (single region, accessible to Flink cluster)
  - Incremental RocksDB: yes (state includes per-user activity counters)
  - Savepoint before any maintenance: yes

Recovery scenario A (Flink TaskManager failure):
  - Flink restarts from last checkpoint automatically
  - Kafka source reseeks to checkpointed offsets (both US and replicated EU topics)
  - MM2 continues replicating independently
  - Recovery time: ~90 seconds (state restore + Kafka replay)

Recovery scenario B (MirrorMaker failure):
  - EU events stop arriving at us-east-1
  - withIdleness(120s) prevents EU partitions from blocking watermarks
  - US processing continues normally
  - EU events accumulate in eu-west-1 cluster
  - On MM2 recovery: replicated events flow in with original event timestamps
  - Flink's out-of-orderness window (60s) handles late EU arrivals up to 60s late
  - Events > 60s late: use side output to capture and reprocess via batch job
```

**4. Handling eu-west-1 unavailability:**

```
Failure modes and Flink behavior:

  MM2 down (network partition):
    → EU partitions go idle after 120s (withIdleness)
    → US events continue processing normally
    → EU events queue in eu-west-1 cluster (retention period applies)
    → On recovery: MM2 replicates queued events using original offsets
    → Flink processes them with original event timestamps (out-of-orderness window handles short gaps)

  eu-west-1 cluster fully down (> retention period):
    → Data loss for EU events beyond retention
    → Alert: MM2 consumer lag alarm
    → Fallback: EU service writes directly to us-east-1 Kafka during outage (failover mode)

  Flink handling of partial region data:
    → Accept degraded mode: process only US events, EU events catch up later
    → For SLA-critical metrics: maintain separate US-only and EU-only aggregations
```

**5. GDPR / Data residency:**

```
GDPR concern: EU user data (PII) being replicated to us-east-1 violates GDPR
Article 46 if there's no adequate transfer mechanism.

Approach 1 — Pseudonymization before replication:
  EU Flink job (runs in eu-west-1):
    - Strips PII fields (name, email, IP)
    - Replaces with pseudonymous user_hash (HMAC-SHA256 with EU-held key)
    - Publishes anonymized events to replication topic
  US Flink job: processes pseudonymized events (no GDPR transfer issue)

Approach 2 — Process in EU, aggregate results to US:
  EU Flink job: aggregates per user, emits only aggregate metrics (no PII)
  MM2: replicates aggregated metrics topic only (no PII)
  US Flink job: combines US and EU aggregate results

Approach 3 — Standard Contractual Clauses (SCCs):
  Legal mechanism allowing EU→US transfer
  Requires DPA, technical measures (encryption in transit + at rest)
  Audit logging of all data access

Recommended: Approach 1 (pseudonymization) + SCCs for non-PII attributes.
Architecture decision: document which fields are PII, implement field-level
masking in the EU Kafka producer before events ever enter Kafka.
```

</details>
</article>
