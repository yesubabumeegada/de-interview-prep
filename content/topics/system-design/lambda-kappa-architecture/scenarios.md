---
title: "Lambda & Kappa Architecture — Scenarios"
topic: system-design
subtopic: lambda-kappa-architecture
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [system-design, lambda-architecture, kappa-architecture, interview, scenarios]
---

# Lambda & Kappa Architecture — Interview Scenarios

## Scenario 1: Choose an Architecture for a Real-Time Analytics Platform

**Question:** A logistics company needs: (1) real-time shipment tracking dashboard (< 30 second latency), (2) daily operational reports (exact numbers, run overnight), (3) ML model to predict delivery delays (trained weekly on 2 years of history). Recommend an architecture.

**Answer:**

```
Recommended: Hybrid (Kappa-like with separate ML batch layer)

Architecture:

Ingestion:
  GPS trackers + warehouse systems → Kafka (shipments.events)
  Retention: 2 years (enables ML retraining from Kafka)
  Partitioned by: shipment_id (32 partitions)

Real-Time Tracking (< 30 sec latency):
  Spark Structured Streaming (30-second trigger)
  → Delta Lake (shipments_current — current status per shipment)
  → Redis (hot path: last 1000 updates per shipment for dashboard)
  Dashboard queries Redis for sub-second response

Daily Reports (exact numbers):
  Spark batch job at 1 AM, reads Delta Lake full history
  → Snowflake summary tables (daily_shipment_stats)
  NOT a separate batch layer — same Delta Lake data, just different processing time
  Why exact? Batch can handle late arrivals from the full previous day

ML Training (weekly):
  Reads Kafka from offset=90_days_ago (2 weeks of fresh data)
  Or: reads Delta Lake time-travel (same data, easier SQL)
  Trains on full 2-year history in Spark MLlib
  Deploys new model → streaming job picks up new model file

Why not pure Lambda:
  Don't need two separate codebases — Delta Lake handles both streaming + batch reads
  
Why not pure Kappa:
  ML training requires historical bulk reads → batch job is the right tool
  (Not a "speed layer" — just batch ML, which is expected to be batch)

This is closest to Kappa + batch ML:
  One event log (Kafka) → one storage (Delta) → streaming + batch read same table
  Separate batch job only for ML (inherently batch workload)
```

---

## Scenario 2: Debug Speed Layer and Batch Layer Disagree

**Question:** Your Lambda Architecture has a problem: the speed layer shows $5.2M revenue today, but when the batch job runs tonight, it shows $4.9M for the same day. Why might they differ, and how do you fix this?

**Answer:**

```
Root Causes for Speed vs Batch Discrepancy:

1. Late-arriving events (most common):
   Speed layer processed events that arrived on time
   Batch layer reprocessed with all late arrivals included
   → Batch is MORE accurate (counts transactions that arrived late)
   Fix: this is expected behavior; document that today's speed layer number
   updates when batch runs (this is Lambda's intended design)

2. Different data sources:
   Speed layer reads from Kafka (some events lost before Kafka)
   Batch layer reads from database (authoritative source)
   → Batch is authoritative
   Fix: align sources; speed layer should read from same authoritative source
   (CDC events from database → Kafka → speed layer)

3. Timezone handling:
   Speed layer: uses UTC
   Batch layer: uses America/New_York
   "Today" means different cutoff times
   Fix: standardize to UTC everywhere; convert to local time at display layer only

4. Duplicate events in speed layer:
   Speed layer retried failed events; some counted twice
   Batch layer: MERGE on transaction_id deduplicates
   Fix: add deduplication to speed layer (dropDuplicates on transaction_id
   within a 1-hour window)

5. Different business logic (drift):
   Speed layer: calculates revenue = gross_amount
   Batch layer: calculates revenue = gross_amount - refunds (updated last month)
   Batch was updated; speed layer wasn't
   Fix: This is the dual codebase problem → migrate to Kappa/Lakehouse

Debugging approach:
  1. Compare counts by hour: WHERE day='today' GROUP BY EXTRACT(HOUR FROM ts)
     → find the hour where they diverge
  2. Check if specific transaction_ids exist in one but not the other
  3. Check for duplicates: SELECT transaction_id, COUNT(*) GROUP BY 1 HAVING COUNT(*) > 1
```

---

## Scenario 3: Design Kappa Architecture for a Payment System

**Question:** Design a Kappa Architecture for a payment processing company. Requirements: process 50,000 transactions/second, provide real-time account balances, support historical audit queries, handle model updates without downtime.

**Answer:**

```
Kappa Architecture for Payments:

Kafka setup:
  Topic: payments.events (100 partitions, RF=3, acks=all, retention=3 years)
  Topic: payments.dlq (failed events, retention=1 year)
  Throughput: 50K tx/sec × 2KB avg = 100MB/sec → well within 100 partition capacity

Processing (Spark Structured Streaming):
  Job: payments_processor_v5
  Input: payments.events (all partitions)
  Watermark: 2 minutes (tolerate network delays)
  Trigger: 5-second micro-batch (balance freshness SLO)

Output targets (all from same streaming job):
  1. Delta Lake (payments_history): append-only, partitioned by payment_date
     → for audit queries: SELECT * FROM payments_history
       WHERE account_id=X AND TIMESTAMP AS OF '2024-01-15' (time-travel)
  
  2. Delta Lake (account_balances): MERGE on account_id (current balance)
     → real-time balance queries: SELECT balance FROM account_balances WHERE account_id=X
  
  3. Delta Lake (fraud_alerts): high-risk transactions flagged by rule engine

Zero-downtime model/logic update:
  1. Deploy v6 job reading from "3 years ago" (full replay for accuracy)
  2. v6 writes to separate Delta tables (payments_history_v6, account_balances_v6)
  3. Validation: v6 catches up to real-time → compare sample accounts
     SELECT * FROM account_balances_v5 WHERE account_id = 'test_account'
     EXCEPT
     SELECT * FROM account_balances_v6 WHERE account_id = 'test_account'
     -- Should return 0 rows if logic is equivalent
  4. Switch: UPDATE serving_config SET active_version = 'v6'
  5. Decommission v5 after 24 hours of stable v6 operation

Audit compliance:
  Delta time-travel satisfies "what was the balance at time T" requirements
  Kafka log satisfies "what events were processed" requirements
  Both immutable: append-only / version-controlled
```
