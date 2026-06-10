---
title: "Pub/Sub — Real-World Case Studies"
topic: gcp
subtopic: pub-sub
content_type: study_material
difficulty_level: mid-level
tags: [gcp, pub-sub, interview]
---

# Pub/Sub — Real-World Case Studies

Three production stories — an outage caused by a default nobody read about, a duplicate-payments incident, and a cost-driven redesign — each with the investigation steps and numbers interviewers expect.

## Case Study 1: The Subscription That Deleted Itself

### Context

A retail company had a low-traffic but business-critical flow: `refund-events` topic → `finance-recon-sub` → a reconciliation service that ran weekly batch pulls. Volume: a few hundred messages a week, sometimes zero for stretches.

### Symptom

A quarterly audit found 5 weeks of refunds missing from the reconciliation ledger. No errors, no alerts, nothing in the consumer logs — the consumer simply pulled zero messages and reported success.

### Investigation

```bash
gcloud pubsub subscriptions describe finance-recon-sub
# ERROR: NOT_FOUND: Resource not found (resource=finance-recon-sub)
```

The subscription didn't exist. Cloud Audit Logs showed a `DeleteSubscription` event by the **Pub/Sub service itself**: the subscription had hit the default **31-day expiration policy** during a quiet period (the consumer had been down for maintenance for part of it, and traffic was near zero — no pulls, no acks, no "activity").

A teammate had then "fixed" the consumer's NOT_FOUND errors by recreating the subscription — which silently discarded everything published in the gap, because **a subscription only receives messages published after its creation**.

### Fix

1. All production subscriptions updated with `--expiration-period=never` (enforced via Terraform policy check):

```bash
gcloud pubsub subscriptions update finance-recon-sub \
  --expiration-period=never
```

2. Backfill: refunds were recoverable from the source service's database (the outbox table) — replayed 5 weeks into the recon system. Without that source of truth, the data would have been unrecoverable.
3. Monitoring added: alert if `subscription/pull_request_count` is zero for 48h on critical subs, and an org policy scanner that flags any subscription with a finite expiration.

### Outcome

| Item | Value |
|------|-------|
| Data at risk | 5 weeks of refund events (~2,100 messages) |
| Recovery | 100% (thanks to outbox table) |
| Recurrences | 0 after Terraform guardrail |

Interview soundbite: "Pub/Sub deletes idle subscriptions after 31 days by default. Quiet-but-critical flows are exactly the ones it bites. `expiration-period=never` plus an IaC policy check is the fix."

## Case Study 2: Duplicate Payment Notifications After a Slow Deploy

### Context

Fintech: `payment-completed` topic → push subscription → notification service on Cloud Run → sends customer emails/SMS. ~30 msg/s normally.

### Symptom

After a deploy, ~7% of customers received two or three "payment received" notifications. Support tickets spiked within the hour.

### Debugging Story

1. Cloud Run logs: many requests taking 12–18 s (a new synchronous call to a slow template-rendering service in the deploy).
2. The push subscription's ack deadline was **10 s (default)**. Pub/Sub treats a response slower than the deadline as failure ⇒ redelivery ⇒ another email. Each redelivery also hit the slow path: retry storm.
3. Confirmed in metrics: `subscription/push_request_latencies` p95 > 10 s exactly when `delivery_attempt` counts climbed.

The deeper finding: the handler **sent the email before being sure it would respond 200 in time**, and had no idempotency — the classic at-least-once trap, dormant until latency crossed the deadline.

### Fix

Immediate:

```bash
gcloud pubsub subscriptions update notify-sub --ack-deadline=120

gcloud pubsub subscriptions update notify-sub \
  --min-retry-delay=10s --max-retry-delay=600s
```

Plus rollback of the slow template call.

Permanent — idempotency keyed on business ID:

```python
@app.post("/pubsub")
def handle(envelope: dict):
    msg = envelope["message"]
    payment_id = msg["attributes"]["payment_id"]

    # Atomic first-writer-wins guard (Firestore create / Redis SETNX)
    if not idempotency_guard.acquire(f"notify:{payment_id}", ttl_days=7):
        return "", 204                      # duplicate: ack, do nothing

    send_notification(payment_id)
    return "", 204
```

And a process rule: any push handler must respond within half the ack deadline (timeout budget enforced in code), doing slow work asynchronously after persisting intent.

### Outcome

| Metric | Before | After |
|--------|--------|-------|
| Duplicate notification rate | ~7% during incident | < 0.01% (guard hits logged) |
| p95 handler latency | 14 s | 800 ms |
| Redeliveries/day | ~180k during incident | ~40 |

Interview soundbite: "At-least-once isn't a corner case — any latency regression past the ack deadline turns it on at full volume. Idempotency keys by business ID are non-negotiable for side-effecting consumers."

## Case Study 3: Cutting a $41k/Month Pub/Sub Bill by Restructuring Fan-out

### Context

IoT company: every device event (~80k msg/s, ~1.5 KB) published to one `device-events` firehose topic. Over time, **seven** subscriptions had accumulated: analytics Dataflow, alerting, two team-specific consumers, an ML feature pipeline, a debugging tap, and an "archive" consumer writing to GCS.

### Investigation

Pricing math: ~10 TiB/day published. At ~$40/TiB:

```text
Publish:                ~10 TiB/day  →  ~$400/day
Delivery: 7 subs × 10 TiB × $40     →  ~$2,800/day... but
  (2 subs used filters ≈ 30% volume) →  actual ~ $1,000/day total
Monthly total                        →  ~$41k
```

Per-subscription byte metrics confirmed: the archive consumer and the debugging tap each took the **full firehose**; the two team consumers wanted < 5% of messages each but had **no filters** (filtering client-side after paying for delivery).

### Fix

1. **Filters on the team subscriptions** (server-side, attribute-based) — they only wanted specific event types:

```bash
gcloud pubsub subscriptions create team-thermo-sub \
  --topic=device-events \
  --message-filter='attributes.event_type = "thermo_reading"'
```

Delivered volume for those two subs: 100% → ~9% combined. (Filters are create-time only, so this was create-new, migrate, delete-old.)

2. **Archive consumer replaced with a Cloud Storage subscription** — no consumer fleet at all:

```bash
gcloud pubsub subscriptions create archive-gcs-sub \
  --topic=device-events \
  --cloud-storage-bucket=device-events-archive \
  --cloud-storage-file-prefix=raw/ \
  --cloud-storage-max-duration=5m \
  --cloud-storage-output-format=avro
```

Saved the GKE archive deployment (~$1,900/month) on top of simplifying ops.

3. **Debugging tap converted to on-demand**: a filtered subscription created when needed (IaC module) instead of a standing full-volume copy. Standing cost → zero.
4. ML feature pipeline merged into the analytics Dataflow job (one delivery instead of two; the job already read everything).

### Outcome

| Item | Before | After |
|------|--------|-------|
| Full-volume deliveries | 5 | 2 (Dataflow, GCS sub) |
| Pub/Sub monthly cost | ~$41k | ~$17k |
| Consumer infra removed | — | archive fleet (~$1.9k/mo) |

Interview soundbite: "Every subscription on a firehose topic is a full copy of the bill. Filters, direct GCS/BigQuery subscriptions, and consolidating consumers are the three levers — we halved the bill without touching producers."

## Patterns Across All Three Cases

1. **Defaults are sharp**: 31-day expiration, 10-s ack deadline, immediate retry — read every default on a subscription you create for production.
2. **Idempotency by business key** is the universal answer to at-least-once, regardless of exactly-once features.
3. **Audit subscriptions like you audit IAM** — they're each a cost multiplier and a data consumer; stale ones cost money and mask data flows.
4. **The platform can replace consumers**: BigQuery/GCS subscriptions delete entire services worth of code for transform-free paths.
