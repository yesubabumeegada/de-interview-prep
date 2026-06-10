---
title: "Pub/Sub — Interview Scenarios"
topic: gcp
subtopic: pub-sub
content_type: scenario_question
tags: [gcp, pub-sub, interview]
---

# Pub/Sub — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Two Teams Want the Same Events — One Team Gets Half

**Scenario:** Your company publishes order events to a Pub/Sub topic. The analytics team already consumes them. The fraud team now points *their* new service at the analytics team's subscription "since the events are the same." Within an hour both teams report receiving roughly half the messages each and accuse each other of stealing events. Explain what's happening and how to set this up correctly.

<details>
<summary>💡 Hint</summary>

Think about the difference between attaching multiple *subscribers* to one subscription versus creating multiple *subscriptions* on one topic. Which one load-balances and which one fans out? Also consider what the fraud team will have missed even after the fix.

</details>

<details>
<summary>✅ Solution</summary>

**What's happening:** Multiple subscribers on a **single subscription split the messages** — that's Pub/Sub's load-balancing behavior, designed for scaling one logical consumer horizontally. The fraud service and analytics service are competing as if they were replicas of the same worker pool.

**The rule:**

| Setup | Behavior |
|-------|----------|
| 1 subscription, N subscribers | Messages divided among them (work sharing) |
| N subscriptions, 1 topic | Each subscription gets ALL messages (fan-out) |

**Fix:** give the fraud team their own subscription:

```bash
gcloud pubsub subscriptions create fraud-sub \
  --topic=orders \
  --ack-deadline=30 \
  --expiration-period=never
```

**Two follow-up points that earn extra credit:**

1. **The gap is permanent for fraud**: a subscription only receives messages published *after* it's created. Events consumed by fraud's misconfigured hour (acked on analytics-sub) and everything before `fraud-sub` existed won't arrive. If analytics-sub had `retain-acked-messages`, analytics could seek/replay — but that replays into *analytics'* subscription, not fraud's. Backfill must come from wherever events are persisted (e.g., BigQuery).
2. Each new subscription is a **full copy of delivery costs** — fan-out is the right pattern, but teams should know subscriptions aren't free.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Build a Poison-Proof, Replay-Capable Consumer

**Scenario:** You're standing up a Pub/Sub consumer for `inventory-updates` that writes to a warehouse database. Requirements from the team lead: (1) one malformed message must never block the rest; (2) when the database is down for an hour, Pub/Sub must not hammer the service with instant retries; (3) after a bad deploy that wrote wrong values for 30 minutes, you must be able to reprocess that window. Produce the subscription configuration and the consumer-side practices, justifying each choice.

<details>
<summary>💡 Hint</summary>

Map each requirement to a specific Pub/Sub feature: something that moves repeatedly-failing messages aside, something that spaces out redeliveries, and something that lets you rewind a subscription's ack state. Don't forget the IAM detail that silently breaks the first feature, and what the consumer needs so reprocessing doesn't double-apply updates.

</details>

<details>
<summary>✅ Solution</summary>

**Subscription configuration:**

```bash
# Dead-letter topic + its own monitoring subscription
gcloud pubsub topics create inventory-updates-dlq
gcloud pubsub subscriptions create inventory-dlq-sub \
  --topic=inventory-updates-dlq --expiration-period=never

gcloud pubsub subscriptions create inventory-sub \
  --topic=inventory-updates \
  --ack-deadline=60 \
  --dead-letter-topic=inventory-updates-dlq \
  --max-delivery-attempts=7 \
  --min-retry-delay=10s \
  --max-retry-delay=600s \
  --retain-acked-messages \
  --message-retention-duration=7d \
  --expiration-period=never
```

The IAM step that everyone forgets — without it, DLQ silently never triggers:

```bash
PROJECT_NUMBER=$(gcloud projects describe my-proj --format='value(projectNumber)')
SA="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

gcloud pubsub topics add-iam-policy-binding inventory-updates-dlq \
  --member="serviceAccount:${SA}" --role=roles/pubsub.publisher
gcloud pubsub subscriptions add-iam-policy-binding inventory-sub \
  --member="serviceAccount:${SA}" --role=roles/pubsub.subscriber
```

**Mapping to requirements:**

| Requirement | Mechanism | Notes |
|-------------|-----------|-------|
| (1) Poison messages | DLQ after 7 attempts | Alert on DLQ depth; inspect, fix, republish |
| (2) DB outage | Exponential retry policy 10s→600s | Without it, redelivery is near-immediate — a nack storm |
| (3) Reprocess window | `retain-acked-messages` + seek | `gcloud pubsub subscriptions seek inventory-sub --time=<deploy_time>` |

**Consumer-side practices:**

```python
def callback(message):
    try:
        update = parse(message.data)          # throws → nack path
    except ValueError:
        message.nack()                        # counts toward DLQ attempts
        return
    # Idempotent, replay-safe write:
    db.execute(
        """
        INSERT INTO inventory (sku, qty, version)
        VALUES (%(sku)s, %(qty)s, %(version)s)
        ON CONFLICT (sku)
        DO UPDATE SET qty = EXCLUDED.qty, version = EXCLUDED.version
        WHERE inventory.version < EXCLUDED.version
        """, update)
    message.ack()
```

- **Idempotent + version-guarded upsert** makes requirement (3)'s replay safe (re-applying old messages can't regress newer state) and makes at-least-once duplicates harmless.
- Take a **snapshot before each deploy** (`gcloud pubsub snapshots create pre-deploy --subscription=inventory-sub`) — seeking to a snapshot is more precise than a timestamp.
- Metric/alert set: oldest unacked message age (SLA), DLQ message count, ack latency p99 vs deadline.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Kafka Migration Assessment — What Moves, What Doesn't

**Scenario:** Your company is consolidating onto GCP. The platform team proposes migrating all 40 Kafka topics to Pub/Sub to eliminate cluster operations. The Kafka estate includes: (a) high-volume clickstream consumed by 3 analytics pipelines; (b) a `wallet-transactions` topic where strict per-account ordering is mandatory and consumers use Kafka transactions for exactly-once into Postgres; (c) several compacted topics serving as changelog/state for services; (d) a topic with 30-day retention that ML re-reads end-to-end weekly. As the senior engineer, assess which workloads map cleanly to Pub/Sub, which need redesign, and which shouldn't move. The interviewer wants reasoning from the primitives, not a feature checklist.

<details>
<summary>💡 Hint</summary>

For each workload, identify the Kafka primitive it depends on — partition ordering, transactions, log compaction, offset-based re-reads — and ask whether Pub/Sub has an equivalent primitive, a workaround with different trade-offs, or nothing. Remember Pub/Sub Lite is deprecated, and GCP now offers a managed Kafka — "don't migrate" is an allowed answer.

</details>

<details>
<summary>✅ Solution</summary>

**Workload-by-workload assessment:**

**(a) Clickstream fan-out → clean fit, Pub/Sub wins.**
At-least-once, unordered, multiple independent consumers: this is Pub/Sub's home turf. Three subscriptions replace three consumer groups, with no partition/rebalance management and elastic scaling. Redesign cost: consumers swap offset commits for acks; add idempotency in sinks (they should have it already). Bonus: BigQuery subscription may delete one pipeline entirely.

**(b) wallet-transactions → needs real redesign; consider keeping on Kafka.**
Two hard dependencies:
- *Per-account ordering*: Pub/Sub ordering keys deliver this **if** per-key throughput < 1 MB/s and consumers tolerate suffix-replays on nack (ordering amplifies redelivery). Probably fine for per-account volumes.
- *Kafka transactions (EOS into Postgres)*: **no Pub/Sub equivalent.** Exactly-once delivery ≠ transactional produce-consume. The redesign is the standard one: idempotent, version-guarded upserts keyed on transaction ID + transactional outbox on producers. That's a real engineering project on a money path — if the team can't fund it, this topic is a candidate for **Google Managed Service for Apache Kafka** rather than forcing Pub/Sub.

**(c) Compacted changelog topics → does not map. Redesign the pattern, not the transport.**
Pub/Sub has no log compaction and no "read the latest value per key from the beginning" semantics — subscriptions are ack-based, not a queryable log. The Pub/Sub-native pattern is: events to Pub/Sub + materialized current state in a database (Firestore/Bigtable/CloudSQL), with new consumers bootstrapping from the DB snapshot then tailing the topic. If services deeply embed Kafka Streams/KTable logic, keep them on managed Kafka.

**(d) 30-day full re-reads → workable but unnatural; cost it honestly.**
Pub/Sub *can* do it: 31-day max retention + `retain-acked-messages` + seek-to-timestamp on a dedicated ML subscription. But weekly full re-reads mean re-*delivering* the entire 30-day volume each week (delivery is billed per TiB), and seek rewinds a subscription wholesale rather than giving consumer-controlled offset reads. Cleaner GCP-native design: Cloud Storage subscription archiving the stream to GCS (Avro), ML reads files at file-read costs, Pub/Sub stays a transport, not a database.

**Summary table for the interviewer:**

| Workload | Verdict | Key reason |
|----------|---------|------------|
| (a) Clickstream | Migrate as-is | Fan-out queue semantics match |
| (b) Wallet | Migrate only with outbox+idempotent-sink redesign; else managed Kafka | No transactions; ordering OK with caveats |
| (c) Compacted | Re-architect (event + DB state) or stay Kafka | No compaction primitive |
| (d) Re-read | Migrate with GCS archive pattern | Retention cap + re-delivery cost |

**Close with the principle:** "Kafka is a replayable log where the partition couples ordering, parallelism, and state; Pub/Sub is an acked queue that decouples them. Workloads using the *queue* subset migrate cleanly; workloads using the *log* properties — compaction, transactions, offset-controlled re-reads — need redesign or should stay on Kafka. A hybrid landing (Pub/Sub for a, d-with-archive; managed Kafka for b, c initially) de-risks the program."

</details>

</article>

## Interview Tips

> **Tip 1:** "How does Pub/Sub guarantee delivery?" — answer with the lifecycle: publish-ack means durably stored; redelivery until subscriber ack; ack deadline + retry policy + DLQ as the control knobs; then immediately say "therefore consumers must be idempotent." Connecting at-least-once to idempotency unprompted is the signal interviewers want.

> **Tip 2:** "Pub/Sub vs Kafka" is the most likely senior question — argue from primitives (acked queue vs replayable log; who owns consumer state; what the partition couples together), concede where Kafka wins (compaction, transactions, total ordering, long replay), and mention Pub/Sub Lite's deprecation to show currency.

> **Tip 3:** When given any consumer-design scenario, name the three production defaults you'd change before writing code: `expiration-period=never`, exponential retry policy, and DLQ with the service-account IAM bindings. Reciting sharp defaults proves production experience.

## ⚡ Quick-fire Q&A

**Q:** One subscription, three subscribers — who gets what?
A: They split the messages (load balancing). Fan-out requires separate subscriptions.

**Q:** What's the max message size, and the pattern for bigger payloads?
A: 10 MB; claim-check — store the blob in GCS, publish a pointer.

**Q:** What does enabling exactly-once delivery NOT protect against?
A: Publisher-side duplicates and crashes after processing but before ack — sinks still need idempotency.

**Q:** Per-ordering-key throughput limit?
A: 1 MB/s publish per key — choose high-cardinality keys.

**Q:** How do you replay messages from last Tuesday?
A: Retention (+`retain-acked-messages` for acked ones) and `subscriptions seek --time=...`, or seek to a pre-taken snapshot.

**Q:** Push subscription: what counts as an ack?
A: An HTTP success response (e.g., 200/204) within the ack deadline; errors or timeouts trigger redelivery per retry policy.

**Q:** Why might a DLQ never receive anything despite configuration?
A: The Pub/Sub service account lacks publisher rights on the DLQ topic and/or subscriber rights on the source subscription.

**Q:** Which metric tells you you're breaching freshness SLAs?
A: `oldest_unacked_message_age` — backlog age, not just depth.
