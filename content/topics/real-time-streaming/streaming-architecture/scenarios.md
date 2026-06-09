---
title: "Streaming Architecture Patterns — Scenarios"
topic: real-time-streaming
subtopic: streaming-architecture
content_type: scenario_question
tags: [streaming, architecture, design, interview, scenarios, kafka, flink, production]
---

# Streaming Architecture Patterns — Interview Scenarios

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design a Real-Time Ride-Sharing Event Platform

**Scenario:** You're a senior data engineer at a ride-sharing company. The company has 10,000 drivers and 100,000 riders. You need to design a streaming platform to power: (a) driver position tracking (update every 5 seconds), (b) real-time surge pricing (recalculate every 30 seconds per geographic area), (c) trip lifecycle events for billing (trip_started, trip_completed, payment_processed), (d) fraud detection on payments. Design the complete streaming architecture.

<details>
<summary>💡 Hint</summary>
Design four separate pipelines with different characteristics: GPS tracking (Flink + Redis GEOADD, 5s TTL), surge pricing (Flink SQL TUMBLE 30s → Redis), billing (Kafka → Delta medallion), fraud (Flink CEP).
</details>

<details>
<summary>✅ Solution</summary>

```
Four separate pipelines, each with distinct requirements:

(a) Driver Position Tracking:
    Source:   Mobile app → REST API → Kafka
    Volume:   10,000 drivers × 1 event/5sec = 2,000 events/sec
    Topic:    prod.ridesharing.driver-positions.v1
              Partitioned by: driver_id (50 partitions)
    Consumer: Flink job (KeyedProcessFunction, keyed by driver_id)
              State: ValueState<DriverPosition> (last known position per driver)
              Sink: Redis (key=driver:{id}:position, TTL=30s)
              → Rider app reads Redis for nearest drivers (< 10ms)
    
    Redis value: {lat, lon, heading, speed, timestamp}
    Geospatial: Redis GEOADD (native geo commands)
    → Query: GEORADIUS center_lat center_lon 5 km → nearest drivers

(b) Surge Pricing:
    Source:   Consumes prod.ridesharing.driver-positions.v1 (same topic)
              + prod.ridesharing.trip-requests.v1 (from rider app)
    Topic:    prod.ridesharing.surge-pricing.v1 (output)
    Logic:    Every 30 seconds per H3 hexagonal cell (~1km²):
              demand = count(trip_requests, last 5 min, H3 cell)
              supply = count(active_drivers, H3 cell)
              surge_multiplier = max(1.0, demand / supply × 1.5)
    
    Flink SQL:
      SELECT
        h3_cell,
        window_start,
        GREATEST(1.0, COUNT_RIDER_REQUESTS / NULLIF(COUNT_ACTIVE_DRIVERS, 0) * 1.5)
          AS surge_multiplier
      FROM TABLE(TUMBLE(TABLE combined_stream, DESCRIPTOR(event_time), INTERVAL '30' SECONDS))
      GROUP BY h3_cell, window_start
      
    Sink: Redis (key=surge:{h3_cell}, TTL=45s — auto-expires if job dies)
    
    Pricing API reads Redis on each trip request (< 5ms, no DB hit)

(c) Trip Lifecycle Events:
    Source:   prod.ridesharing.trip-events.v1
              Events: trip_started, trip_completed, payment_processed
              Partitioned by: trip_id (100 partitions)
    
    Consumer A: Flink (billing enrichment)
      - Join trip_started + trip_completed → compute distance, duration
      - Join payment_processed → validate amount
      - Sink: Delta Lake (Gold) for billing analytics
    
    Consumer B: Kinesis Firehose (via Lambda bridge)
      - Buffers 1 minute → S3 Parquet (raw trip archive)
      - Glue crawler → Athena for ad-hoc analysis
    
    Consumer C: Kafka → Snowflake Kafka Connector
      - Near-real-time DW for Finance dashboards
      - Latency: < 5 minutes

(d) Fraud Detection:
    Source:   prod.ridesharing.payment-events.v1
    Flink CEP: detect patterns within 10 minutes per driver:
               3+ payments → same destination → different riders = suspicious
               Payment > $500 (threshold for investigation)
    
    Pattern:
      Pattern<PaymentEvent, ?> fraud = Pattern.<PaymentEvent>begin("first")
          .where(e -> e.getAmount() > 50)
          .followedByAny("second")
          .where(e -> e.getAmount() > 50)
          .within(Time.minutes(10));
    
    Sink: Kafka alerts topic → Lambda → fraud team Slack notification
    Also: Delta Lake (fraud_events table) for ML training

Infrastructure summary:
  Kafka: 6 brokers, 200 partitions total, 7-day retention
  Flink: 3 jobs (position+surge, billing, fraud), 32 tasks each
  Redis: cluster mode, 3 primaries + 3 replicas, 16 GB RAM
  Delta Lake: Silver (trip events) + Gold (billing, fraud, analytics)
  Estimated throughput: 5,000 events/sec peak, 1,000 events/sec average
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Streaming Pipeline Latency Degradation

**Scenario:** Your streaming pipeline normally processes events with < 2-second end-to-end latency. This morning, latency jumped to 45 seconds and is still climbing. The Grafana dashboard shows consumer lag growing on 3 of 16 Kafka partitions, but not all. Flink checkpoint duration increased from 30 seconds to 4 minutes. How do you diagnose and fix this?

<details>
<summary>💡 Hint</summary>
Consumer lag growing on only 3/16 partitions suggests hot partitions, not a global throughput issue. Check per-subtask state size and input rate. A hot-key scenario (bot accounts or specific user_id range) causes one partition's subtask to fall behind.
</details>

<details>
<summary>✅ Solution</summary>

```
Symptoms summary:
  - Latency: 2s → 45s (22× increase)
  - Consumer lag growing on 3/16 partitions (not uniform)
  - Checkpoint duration: 30s → 4 minutes (8× increase)
  - Pattern: 3 specific partitions affected → likely hot partition or slow operator

Step 1: Identify which partitions are affected
  kafka-consumer-groups.sh --describe --group flink-streaming-job
  
  → Partition 5, 9, 13 have growing lag (same 3 from Grafana)
  → All 3 have similar partition key range
  
  Hypothesis: hot keys in partitions 5, 9, 13 causing backpressure

Step 2: Check Flink backpressure metrics
  Flink Web UI → Job → click operator "KeyedProcessFunction"
  → Check: "Back Pressured" column for each subtask
  
  → Subtask 5, 9, 13 show HIGH backpressure (red)
  → Upstream operators are BLOCKED waiting for these subtasks to drain buffer

Step 3: Check state size per subtask
  Flink metrics: flink_taskmanager_job_task_operator_numRecordsIn (per subtask)
  → Subtask 5, 9, 13: 5× more records/second than other subtasks
  
  Also check: RocksDB state size per subtask
  → Subtasks 5, 9, 13: each has 12 GB state (other subtasks: ~2 GB)
  
  Root cause found: USER_ID range that lands in partitions 5, 9, 13 has extremely
  active users (e.g., a bot or bulk test accounts generating 50× normal traffic).
  State per key is growing because state TTL was configured to 30 days,
  and these keys have millions of historical state entries.

Step 4: Immediate mitigation
  Option A: Add randomized sub-key to hot key (re-partition before keyBy)
    Before keyBy: add col = (user_id, random.randint(0, 3))
    → Distributes hot key across 4 subtasks instead of 1
    → Requires change to processing logic (merge results from 4 sub-keys)
    
  Option B: Reduce state TTL for all keys (immediate, no code change needed)
    Current: StateTtlConfig.newBuilder(Time.days(30))
    Change:  StateTtlConfig.newBuilder(Time.days(7))
    → Take savepoint → update config → restore from savepoint
    → Compaction runs → state size drops → backpressure clears
    
  Option C: Identify and block bot keys (if traffic is illegitimate)
    Flink: filter out known bot user_ids before keyBy
    → Immediate reduction in hot key traffic

Chosen approach:
  1. Immediate: reduce state TTL to 7 days (Option B, deploy in 30 minutes)
  2. Short-term: identify bot user_ids and filter (Option C, deploy today)
  3. Long-term: implement hot key detection + re-partitioning (Option A, 1 week)

Checkpoint duration fix:
  Long checkpoints caused by large state (12 GB × 3 = 36 GB to serialize)
  After state TTL reduction → state drops to 2 GB → checkpoint returns to 30s

Prevention:
  - Alert: per-subtask state size > 5 GB (detect hot partitions early)
  - Alert: per-subtask input rate > 10× average (detect hot keys)
  - Periodic ANALYZE on state: find top-10 largest keys every 24 hours
  - State TTL: never > 7 days unless explicitly justified

Resolution timeline:
  T+0: alert triggered (latency > 10s)
  T+15: diagnosed root cause (bot user_ids in 3 partitions)
  T+30: deployed state TTL reduction (savepoint → config change → restore)
  T+45: latency dropped to 3s (state compaction starting)
  T+60: latency back to < 2s (normal)
  T+2hr: bot user_ids identified and filtered
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing for Regulatory Compliance in Streaming

**Scenario:** You need to add PII (Personally Identifiable Information) compliance to an existing Kafka + Flink streaming pipeline. Requirements: (a) no PII in Kafka topics accessible to analytics engineers; (b) PII must be erasable within 30 days of a GDPR deletion request; (c) audit trail of who accessed PII data and when. How do you redesign the pipeline?

<details>
<summary>💡 Hint</summary>
Use a PII separation Flink job as the first stage: split each event into anonymized record (no PII) and encrypted PII record (per-user KMS key). Two output Kafka topics with different ACLs. GDPR erasure = delete the KMS key (crypto shredding).
</details>

<details>
<summary>✅ Solution</summary>

```
Current state:
  Kafka topics contain full user records (email, name, phone, IP address)
  Analytics engineers have broad read access to Kafka topics
  No audit trail for data access

Target architecture:

┌─────────────────────────────────────────────────────────────────┐
│                    PII-COMPLIANT STREAMING                      │
│                                                                 │
│  Source DB → Debezium → ┌─────────────────────────────────┐    │
│                          │ PII Separation Flink Job        │    │
│                          │ (runs in secure enclave)        │    │
│                          │                                 │    │
│                          │ IN:  full record (has PII)      │    │
│                          │                                 │    │
│                          │ OUT A: anonymized record        │    │
│                          │   (no PII → analytics topic)    │    │
│                          │                                 │    │
│                          │ OUT B: PII-only record          │    │
│                          │   (encrypted → secure topic)   │    │
│                          └─────────────────────────────────┘    │
│                                                                 │
│  Analytics Topic (no PII):  analytics engineers can read       │
│  Secure PII Topic:          ops/DBA only, audit logged         │
└─────────────────────────────────────────────────────────────────┘

Implementation:

1. PII field identification:
   Data catalog (Purview/Collibra): tag fields as PII
   Fields: email, full_name, phone, ip_address, address
   Non-PII: user_id (pseudonymized), order_amount, product_id, timestamps
   
2. PII separation Flink job:
   def separate_pii(record):
       anonymized = {
           "user_id":     record["user_id"],       # pseudonym (not real identity)
           "order_id":    record["order_id"],
           "amount":      record["amount"],
           "status":      record["status"],
           "event_time":  record["event_time"],
           # NO: email, name, phone, ip
       }
       
       # Encrypt PII with per-user key from KMS
       user_key = kms.get_or_create_key(f"user-pii-{record['user_id']}")
       pii_encrypted = {
           "user_id": record["user_id"],
           "email_encrypted":  kms.encrypt(record["email"], user_key),
           "name_encrypted":   kms.encrypt(record["full_name"], user_key),
           "phone_encrypted":  kms.encrypt(record["phone"], user_key),
       }
       
       return anonymized, pii_encrypted
   
   anonymized_stream → publish to: prod.ecommerce.orders-anon.v1 (no ACL restriction)
   pii_stream       → publish to: prod.ecommerce.orders-pii.v1  (ACL: ops, DBA only)

3. Kafka ACLs:
   # Analytics engineers can only read anonymized topic
   kafka-acls.sh --add --allow-principal User:analytics-group \
     --operation Read --topic prod.ecommerce.orders-anon.v1
   
   # Analytics group explicitly DENIED access to PII topic
   kafka-acls.sh --add --deny-principal User:analytics-group \
     --operation Read --topic prod.ecommerce.orders-pii.v1
   
   # Only authorized ops roles can read PII topic
   kafka-acls.sh --add --allow-principal User:ops-group \
     --operation Read --topic prod.ecommerce.orders-pii.v1

4. Audit logging for PII access:
   Kafka audit log: Confluent Platform "audit log" plugin
   → Every read from PII topic logged to: prod.infra.kafka-audit
   Log entry: {timestamp, principal, topic, partition, offset, action}
   
   Flink consumes audit log → writes to S3 (Parquet, 7-year retention for compliance)
   Queryable via Athena: "who accessed PII topic user_id=78421 in the last 30 days"

5. GDPR erasure via crypto shredding:
   User deletion request → delete KMS key for user_id
   → All PII records in Kafka (encrypted) become unreadable
   → All PII records in S3 archives (encrypted) become unreadable
   → anonymized records in analytics topic remain (no PII, safe to keep)
   
   Erasure SLA: < 1 hour (delete KMS key immediately on request)
   Audit: record deletion in compliance DB with timestamp

6. Data retention:
   Anonymized topic: 7 days (analytics engineers can replay)
   PII topic: 3 days (minimize PII exposure window)
   S3 PII archive (encrypted): 90 days → then delete (lifecycle policy)
   S3 anonymized archive: 7 years (business analytics)

Architecture benefits:
  - Analytics engineers get full event history, no PII exposure
  - PII accessible only to authorized ops with full audit trail
  - GDPR erasure: < 1 hour via crypto shredding (no file rewrites)
  - No streaming pipeline change needed for erasure (just KMS key deletion)
```

</details>

</article>
---

## Interview Tips

> **Tip 1:** "How do you explain the trade-off between Lambda and Kappa architectures to a non-technical stakeholder?" — Lambda Architecture: two separate pipelines run in parallel. One processes data in real-time (stream layer, fast but approximate). The other reprocesses all historical data in batch (batch layer, slow but accurate). Results are merged at query time. Analogy: a news agency that broadcasts live updates AND publishes a verified daily summary. You get speed AND accuracy. Trade-off: double the code and infrastructure to maintain. Kappa Architecture: one streaming pipeline processes everything (both real-time and historical data). Reprocessing is done by replaying the event log (Kafka) from the beginning. Simpler (one codebase), but requires long event retention and the stream processor must handle both historical and real-time modes efficiently. Recommendation: start with Kappa (simpler). Switch to Lambda only if you have proven accuracy requirements that streaming alone can't meet (e.g., ML training requires batch reprocessing with new features).

> **Tip 2:** "How do you handle a situation where your streaming pipeline needs to be upgraded but you can't have any downtime?" — Blue-green deployment with savepoints: (a) take a savepoint of the current job; (b) deploy the new job version and restore from the savepoint (new job picks up exactly where old job left off — same Kafka offsets, same state); (c) new job runs in parallel briefly (seconds, consuming from same offset); (d) stop the old job. Kafka consumer groups allow two consumers at the same offset only during the overlap window — both jobs process the same events briefly. For idempotent sinks: duplicate processing is harmless (ON CONFLICT DO NOTHING). For non-idempotent: ensure new job starts consuming only after old job stops (use operator UID to match state, different consumer group ID for new job). Zero downtime: consumers never stop reading from Kafka (only a brief duplicate window during the switchover).

> **Tip 3:** "What are the top three metrics you monitor in a production streaming system?" — (1) Consumer lag (per topic, per partition): measures how far behind consumers are from the latest messages. P0 alert if lag > 30 minutes of data (approaching retention boundary, risk of data loss). This is the single most important streaming metric. (2) End-to-end latency (event_time to sink write time): measures whether the pipeline is meeting its SLA. Embed a `produced_at` timestamp in Kafka messages; measure `current_time - produced_at` when writing to the sink. Alert if p99 > 2× SLA. (3) Checkpoint duration and success rate: a failing or slow checkpoint means the job can't recover to a recent state. If checkpoints start failing, the recovery point is getting older. Alert if checkpoint duration > 5 minutes or if 3 consecutive checkpoints fail. Secondary metrics: DLQ message rate, state size growth, late event rate.

