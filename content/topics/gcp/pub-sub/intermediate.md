---
title: "Pub/Sub — Intermediate"
topic: gcp
subtopic: pub-sub
content_type: study_material
layer: intermediate
difficulty_level: mid-level
tags: [gcp, pub-sub, interview]
---

# Pub/Sub — Intermediate

Mid-level Pub/Sub interviews dig into the mechanics: flow control and lease management, exactly-once delivery (what it does and doesn't promise), ordering-key throughput implications, filtering, retention/replay, and the configuration knobs that prevent production incidents.

## Subscription Configuration That Matters

```bash
gcloud pubsub subscriptions create orders-sub \
  --topic=orders \
  --ack-deadline=60 \
  --message-retention-duration=7d \
  --retain-acked-messages \
  --expiration-period=never \
  --min-retry-delay=10s \
  --max-retry-delay=600s \
  --dead-letter-topic=orders-dlq \
  --max-delivery-attempts=5 \
  --enable-exactly-once-delivery
```

| Setting | Default | Why you change it |
|---------|---------|-------------------|
| `ack-deadline` | 10 s | Raise for slow handlers (libraries auto-extend, but the initial deadline matters for crashes and push) |
| `message-retention-duration` | 7 d | How long unacked messages survive; up to 31 d |
| `retain-acked-messages` | off | Required for replaying *acked* messages via seek |
| `expiration-period` | 31 d idle | Set `never` for prod — idle subscriptions silently auto-delete! |
| Retry policy (min/max delay) | immediate redelivery | Exponential backoff stops nack storms hammering your service |
| DLQ + max attempts | none | Poison-message handling |
| Exactly-once | off | Stronger ack guarantees (pull only) |

The `expiration-period` default is a classic production gotcha: a subscription with no activity for 31 days is deleted, and messages published afterwards are gone for that consumer.

## Flow Control & Lease Management (Pull)

The streaming-pull client holds a configurable number of outstanding messages, auto-extends ack deadlines while processing ("lease management"), and applies backpressure:

```python
from google.cloud import pubsub_v1

flow_control = pubsub_v1.types.FlowControl(
    max_messages=500,                 # outstanding messages cap
    max_bytes=100 * 1024 * 1024,      # outstanding bytes cap
)

subscriber = pubsub_v1.SubscriberClient()
streaming_pull = subscriber.subscribe(
    sub_path,
    callback=callback,
    flow_control=flow_control,
)
```

Mechanics to articulate:

- The client library extends leases up to `max_lease_duration` (default ~1 h). A *hung* handler therefore blocks redelivery for a long time — set sane timeouts in your handler.
- Too-high `max_messages` + slow processing ⇒ messages expire mid-flight elsewhere or hog memory; too low ⇒ underutilized throughput.
- Each streaming pull connection has finite throughput (~10 MB/s per open stream); scale with multiple subscriber processes/threads — they load-balance automatically on one subscription.

## Exactly-Once Delivery — Precisely What It Means

When enabled (pull subscriptions, single region):

- No redelivery of a message **while its ack deadline is honored** (no "races" where a duplicate arrives during processing).
- **Acks become reliable**: `ack()` returns a future you can check; a successful ack guarantees no redelivery.
- The subscriber must use the supporting client library APIs (`ack_with_response`).

```python
def callback(message):
    process(message)                      # do the work first
    ack_future = message.ack_with_response()
    try:
        ack_future.result(timeout=30)     # confirm the ack succeeded
    except Exception:
        # ack failed — message WILL be redelivered; processing must be idempotent
        log.warning("Ack failed for %s", message.message_id)
```

Boundaries to state in an interview:

1. It's exactly-once **delivery**, not exactly-once **processing**: if your app crashes after processing but before a successful ack, redelivery happens. End-to-end exactly-once still needs idempotent effects or a transactional sink.
2. Publisher-side duplicates (app retries publishing the same business event) are **not** deduplicated — Pub/Sub has no publisher idempotence keys; dedupe by business ID downstream (or via Dataflow `id_label`).
3. Costs: lower max throughput per subscription and added ack latency — only enable when duplicate suppression is genuinely valuable.

## Ordering Keys — the Throughput Fine Print

- Ordering is **per key, per region**: same-key messages are delivered in publish order.
- **Per-key publish throughput cap: 1 MB/s.** A hot ordering key serializes; design keys with high cardinality (`order_id`, not `country`).
- On a nack/redelivery of message N, Pub/Sub redelivers N **and everything after it for that key** — preserving order amplifies retries; handlers must tolerate replays of suffixes.
- Publisher must enable ordering and handle "key paused" errors: after a publish failure for a key, subsequent publishes for that key fail until you `resume_publish(ordering_key)` — this prevents silent gaps.

```python
publisher = pubsub_v1.PublisherClient(
    publisher_options=pubsub_v1.types.PublisherOptions(
        enable_message_ordering=True),
)

try:
    publisher.publish(topic_path, data, ordering_key="acct-42").result()
except Exception:
    publisher.resume_publish(topic_path, "acct-42")   # explicit, deliberate
```

## Filters: Cheaper Fan-out

Subscription filters drop non-matching messages **server-side** (on attributes only, not payload):

```bash
gcloud pubsub subscriptions create eu-orders-sub \
  --topic=orders \
  --message-filter='attributes.region = "eu" AND attributes.type != "test"'
```

- You are **not charged** delivery for filtered-out messages (you do pay for the topic-side message storage/throughput).
- Filters are immutable after creation; plan attribute schemas early.
- Pattern: one firehose topic, per-team filtered subscriptions — beats maintaining N topics with N publishers.

## Replay: Snapshots & Seek

With retained messages (and optionally retained *acked* messages):

```bash
# Take a snapshot of a subscription's ack state
gcloud pubsub snapshots create pre-deploy-snap --subscription=orders-sub

# Deploy goes wrong? Rewind:
gcloud pubsub subscriptions seek orders-sub --snapshot=pre-deploy-snap

# Or seek to a timestamp
gcloud pubsub subscriptions seek orders-sub \
  --time=2026-06-09T00:00:00Z
```

Seek-to-time marks everything after the time unacked (replay) and everything before as acked (skip). This is the Pub/Sub equivalent of "reset Kafka consumer offsets" — and a standard pre-deployment safety ritual for streaming consumers.

## Push Subscriptions in Production

- Endpoint must be HTTPS with valid cert; authenticate with OIDC tokens (`--push-auth-service-account`); the receiver validates the JWT.
- Pub/Sub adapts push rate with **slow-start**-like behavior: ramps up on 2xx, backs off on errors.
- Response code semantics: 102/200/201/202/204 = ack; anything else = nack (with retry policy backoff).
- No client flow control: your endpoint *is* the flow control. For spiky high-volume topics, pull (or push into Cloud Run with autoscaling and sane concurrency limits).
- Push + ordering: supported, but throughput-limited; pull is preferred for ordered high volume.

## Common Pitfalls Checklist

- Subscription created **after** publishing started ⇒ earlier messages never delivered to it.
- Default `expiration-period` (31 d) deleting idle prod subscriptions.
- No retry policy ⇒ immediate redelivery storms when a downstream dependency is down (pair: exponential backoff + DLQ).
- DLQ configured but Pub/Sub service account missing publisher rights on the DLQ topic and subscriber rights on the source — messages silently keep retrying:

```bash
PROJECT_NUMBER=$(gcloud projects describe my-project --format='value(projectNumber)')
PUBSUB_SA="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

gcloud pubsub topics add-iam-policy-binding orders-dlq \
  --member="serviceAccount:${PUBSUB_SA}" --role=roles/pubsub.publisher

gcloud pubsub subscriptions add-iam-policy-binding orders-sub \
  --member="serviceAccount:${PUBSUB_SA}" --role=roles/pubsub.subscriber
```

- Hot ordering keys capping throughput at 1 MB/s per key.
- Treating exactly-once delivery as a substitute for idempotency — it isn't, across crashes or publisher retries.
- 10 MB max message size; large payloads belong in GCS with a pointer message (claim-check pattern).
- Forgetting that attributes (not payload) are the only filterable surface.

## Cost Model Notes

- Pricing is throughput-based: ~$40/TiB for publish and for delivery (first 10 GiB/month free); retained-acked storage and snapshots billed per GiB-month; egress across regions extra.
- Filters reduce delivery cost; BigQuery/GCS subscriptions avoid paying for a middle-tier consumer entirely when no transform is needed.

## Mini Practice

Design the subscription config for: a payments consumer that must survive 2-day outages, replay after bad deploys, never lose poison messages, and avoid hammering a flaky downstream.

```bash
gcloud pubsub subscriptions create payments-sub \
  --topic=payments \
  --ack-deadline=60 \
  --message-retention-duration=7d \
  --retain-acked-messages \
  --expiration-period=never \
  --min-retry-delay=10s \
  --max-retry-delay=600s \
  --dead-letter-topic=payments-dlq \
  --max-delivery-attempts=10
```

Be ready to justify each flag — that's the interview format.
