---
title: "Stream Processing Patterns — Scenarios"
topic: real-time-streaming
subtopic: stream-processing-patterns
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [streaming, patterns, interview, scenarios, architecture, exactly-once, dlq]
---

# Stream Processing Patterns — Interview Scenarios

## Scenario 1: Design a Real-Time Metrics Pipeline

**Question:** Design a metrics collection and alerting system for a SaaS platform. 1,000 microservices emit metrics (CPU, memory, error rate, latency). Requirements: detect anomalies within 30 seconds, store 90 days of data, support ad-hoc queries.

**Answer:**

```
Architecture:

  Microservices → Kafka (metrics, 32 partitions, partitioned by service_id)
                      │
          ┌───────────┼──────────────┐
          │           │              │
    Flink job     Firehose       Kafka Streams
    (real-time    (90-day        (per-service
     anomaly)     archive→S3)    aggregation)
          │
     DynamoDB (alerts)
     + SNS (notifications)

Kafka topic design:
  Topic: service-metrics
  Partitions: 32 (1000 services / 31 ≈ 32 services per partition on average)
  Partition key: service_id (guarantees ordering per service)
  Retention: 7 days (Flink reads live stream; Firehose archives to S3 for 90 days)
  Message format: {"service_id": "...", "metric": "cpu_pct", "value": 78.5,
                   "timestamp": "2024-01-15T14:30:00Z", "host": "..."}

Flink anomaly detection job:
  Source: Kafka (32 parallel consumers)
  Watermark: 30 seconds (metrics arrive within 30 sec of event time)
  
  Per-service rolling statistics (10-minute tumbling window):
    .keyBy(metric -> metric.getServiceId() + ":" + metric.getMetric())
    .window(TumblingEventTimeWindows.of(Time.minutes(10)))
    .aggregate(new RollingStatsAggregator())  // mean + stddev
  
  Anomaly detection (stateful, per service-metric pair):
    .keyBy(stats -> stats.getKey())
    .process(new AnomalyDetector())  // z-score > 3σ = anomaly
  
  Output:
    Alerts → Kafka (metric-alerts) → Lambda → SNS → PagerDuty / Slack
    All stats → DynamoDB (service-metrics-stats, TTL=90 days) for dashboard queries

Archive for ad-hoc queries:
  Firehose: Kafka → S3 (Parquet, partitioned by service_id/date/hour)
  Glue Catalog: table definition on S3 data
  Athena: ad-hoc SQL on 90 days of metrics
  Query example: "Show average CPU for service-auth in January" → Athena scan of S3

Alerting logic:
  Z-score alerting: (current_value - rolling_mean) / rolling_stddev > 3 → alert
  Absolute threshold: error_rate > 5% → always alert
  Alert deduplication: DynamoDB "last_alert_time" per service-metric
    → don't re-alert within 5 minutes (avoid alert storm)

SLA: anomaly detected within 30 seconds of event
     Achieved: event → Kafka (1s) → Flink window (10s avg) → alert (< 5s) ≈ 15s total
```

---

## Scenario 2: Streaming Pipeline Producing Incorrect Aggregations

**Question:** Your streaming pipeline counts purchases per category per minute. After investigating, you notice the counts are 20-30% lower than what the database shows for the same time range. The pipeline uses Spark Structured Streaming with event-time windowing. What are the likely causes?

**Answer:**

```
Differential: batch count > streaming count by 20-30%
Likely cause: late events are being dropped

Step 1: Check watermark configuration
  Current config: .withWatermark("event_time", "30 seconds")
  Problem: if events arrive > 30 seconds late (mobile apps batch events),
           they are DROPPED by the watermark
  
  Verify: add side output for late events (Flink) or log them in foreachBatch
  Evidence: late event rate = 25% → exactly matches the discrepancy

Step 2: Check event time vs processing time
  Plot event_time distribution relative to kafka_timestamp
  Findings:
    - Desktop events: avg lag 2 seconds (within watermark)
    - Mobile events: avg lag 45-90 seconds (OUTSIDE watermark)
    - Mobile = 25% of purchase volume → 25% events dropped
  
  Root cause: mobile apps batch events locally and send when online
              Events can be 1-2 minutes old when they reach Kafka

Fix 1: Increase watermark tolerance
  .withWatermark("event_time", "5 minutes")  # was 30 seconds
  Trade-off: +5 minute latency for Gold layer (window fires 5 min later)
  Result: captures 98% of events (only truly late > 5 min are dropped)

Fix 2: Use Lambda-hybrid for accuracy (if latency increase unacceptable)
  Streaming (30s watermark): fast approximate counts for dashboard
  Hourly batch job: recount using Silver Delta table (has all events including late)
  Reconcile: update Gold table with batch counts (replace streaming counts)
  Result: dashboard shows fast counts, reports use accurate batch counts

Fix 3: Monitor late events (regardless of fix chosen)
  Add metric: count of events dropped due to watermark
  Alert: if late event rate > 5% of total → investigate source latency increase

Lesson: always validate streaming counts against batch source of truth
        (run parallel batch job on same time window for first 2 weeks)
        Late data is THE most common cause of streaming count discrepancy.
```

---

## Scenario 3: Design for the Kappa Architecture Migration

**Question:** Your company has a Lambda architecture: batch Spark jobs (daily) + streaming Kafka Streams application (hourly). The engineering team complains about duplicate logic and maintenance burden. Design a Kappa architecture migration.

**Answer:**

```
Current state:
  Batch: Spark job reads S3 daily → accurate counts for billing
  Stream: Kafka Streams reads last 1 hour → fast dashboard counts
  Problem: same aggregation logic in two systems, both need maintenance

Assessment of Kappa feasibility:
  1. Kafka retention: currently 7 days → extend to 90 days (for replay)
  2. Sink idempotency: can Delta Lake upsert handle replayed data? YES (merge on primary key)
  3. Replay speed: how fast can we catch up? 
     - 1 year of events, 10 MB/sec average
     - 1 year × 365 × 24 × 3600 × 10 MB/sec = 315 TB
     - With 32 Flink parallelism at 50 MB/sec processing: 315 TB / 50 MB = 6300 seconds ≈ 2 hours
     - Feasible: 2-hour replay to catch up ✓
  4. Event completeness: are all events in Kafka? NO — some historical events only in S3
     → Need to replay S3 historical data first (one-time migration)

Migration plan:

Phase 1: Historical bootstrap (1 week)
  Write S3 historical events to Kafka topic (using Spark):
    spark.read.parquet("s3://bucket/events/year=*/month=*/")
        .write.format("kafka").option("topic", "events-replay").save()
  Events in Kafka: from day-1 + recent (90 days already retained)
  Extend Kafka retention on "events" topic to 90 days
  Total events in Kafka: 90 days + historical bootstrap

Phase 2: Implement unified Flink job (2 weeks)
  Single Flink job with all business logic:
    Source: events (both bootstrap topic and live topic, union)
    Watermark: 10 minutes (handles mobile lag)
    Aggregations: per-minute counts, per-hour summaries, per-day totals
    Sink: Delta Lake (upsert by key + window_start → idempotent for replay)
  
  Test: replay last 30 days, compare output to batch job
  Tolerance: < 0.1% variance (late events handled better with 10-min watermark)

Phase 3: Parallel running (2 weeks)
  New Flink job writes to new Delta tables
  Old batch + stream still running
  Compare daily: Flink vs batch job counts
  Establish baseline agreement

Phase 4: Cutover
  Redirect dashboards to new Flink Delta tables
  Decommission Kafka Streams app
  Decommission daily batch Spark job
  Keep replay capability: retain 90-day Kafka for future reprocessing

Benefits achieved:
  Single codebase: 2 systems → 1 Flink job
  Latency: daily → sub-minute (window fires every minute)
  Accuracy: watermark-based late handling (better than batch day-boundary)
  Replay: extend Kafka retention, rerun Flink job for any bug fix

Cost comparison:
  Before: Spark cluster (daily, expensive) + Kafka Streams (always-on)
  After: Flink on Kubernetes (always-on, right-sized) + 90-day Kafka storage
  Net: slight cost increase in storage, significant reduction in engineering overhead
```

---

## Interview Tips

> **Tip 1:** "What are the key differences in designing a streaming system for operational use cases vs analytical use cases?" — Operational streaming (fraud detection, payment processing): low latency (< 1 second), per-event decisions, small state (per-key lookup), write to transactional sinks (PostgreSQL, Redis, DynamoDB). Analytical streaming (real-time dashboards, aggregations): window-based aggregations, larger state (windowed counts, running sums), write to analytical sinks (Delta Lake, Iceberg, Redshift), slightly higher latency acceptable (1-5 minutes). Design differences: operational uses stateful per-key processing with async lookups; analytical uses windowed aggregations with watermarks. Don't try to build one streaming job that does both — they have different latency, state, and throughput trade-offs.

> **Tip 2:** "How do you ensure high availability for a streaming pipeline?" — Multiple layers: (a) Source HA: Kafka replication factor = 3, min.insync.replicas = 2 (tolerate 1 broker failure); (b) Processing HA: Flink with checkpointing (restart from checkpoint on failure, automatic recovery); Spark Structured Streaming with checkpoint location in S3/ADLS; (c) Sink HA: Delta Lake with ADLS/S3 (replicated by cloud provider); Kafka sink with replication factor = 3; (d) Monitoring: alert on lag, checkpoint failures, backpressure before they cascade; (e) Multi-AZ deployment: Kafka brokers across 3 AZs; Flink/Databricks cluster spans multiple AZs. RTO (Recovery Time Objective): with checkpointing every 60 seconds, recovery takes 60 seconds of reprocessing + job startup time (typically 2-5 minutes total).

> **Tip 3:** "How would you debug a streaming job where the output topic has 0 records for the last 10 minutes?" — 5-step diagnosis: (1) Check job status: is the streaming job running? (Flink UI, Databricks UI); (2) Check source: is there data in the input Kafka topic? (kafka-consumer-groups.sh --describe → is lag growing?); (3) Check processing: are there exceptions in logs? (Flink TaskManager logs, Spark driver logs); (4) Check watermark: for window-based queries, the watermark must advance past the window end time before output is emitted — if event time is old, no output until watermark catches up; (5) Check sink: is the sink accepting writes? (Kafka producer errors, Delta write errors, S3 permission errors). The most common causes in order: (a) job crashed silently, (b) source topic empty/disconnected, (c) watermark stuck on old timestamp.
