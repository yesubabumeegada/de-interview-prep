---
title: "Fault Tolerance & Reliability — Scenarios"
topic: system-design
subtopic: fault-tolerance
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [system-design, fault-tolerance, interview, scenarios, incident]
---

# Fault Tolerance & Reliability — Interview Scenarios

## Scenario 1: Design a Fault-Tolerant Streaming Pipeline

**Question:** Design a streaming pipeline that ingests 100,000 financial transactions per second and stores them for reporting. The requirements: zero data loss, handle node failures, recover within 5 minutes of failure.

**Answer:**

```
Architecture overview:

Layer 1 — Ingestion:
  Transaction service → Kafka (RF=3, acks=all, min.insync.replicas=2)
  Zero data loss: producer blocks until 2 of 3 brokers ACK
  Partition count: 100 partitions → 1000 events/sec/partition (well within limits)
  Retention: 7 days (allows consumer catch-up and replay)

Layer 2 — Processing:
  Spark Structured Streaming (Kubernetes, 10 executors)
  Checkpoint: s3://bucket/checkpoints/ (atomic offset + state save)
  Trigger: processingTime="10 seconds" (10-sec micro-batches)
  Exactly-once: Delta Lake sink + Spark checkpoint = atomic offset+data commit

Layer 3 — Storage:
  Delta Lake on S3 (Multi-AZ S3 = 11 nines durability)
  Partitioned by: transaction_date
  OPTIMIZE daily: compact small micro-batch files → 1GB files

Failure scenarios:

Spark executor fails mid-batch:
  → Driver detects failure, reschedules tasks on other executors
  → If checkpoint written: resume from last checkpoint (< 10 sec redo)
  → If no checkpoint: micro-batch is replayed from Kafka offset (exactly-once)
  → Recovery time: 1-2 minutes ✓

Kafka broker fails:
  → ISR (in-sync replicas): 2 remaining brokers take over partitions
  → Producer: retries with exponential backoff → new leader elected in < 30 sec
  → Consumer lag: grows during election (~30 sec) then catches up
  → Recovery: 1-2 minutes ✓

Full AZ failure:
  → Kafka: RF=3 across 3 AZs → 2 AZs still have 2 replicas → continues
  → Spark: Kubernetes reschedules pods in other AZs → 3-5 min recovery ✓
  → Delta on S3: S3 Multi-AZ → no impact ✓

RTO: 5 minutes (within requirement for full AZ failure)
RPO: 0 (Kafka acks=all + Delta exactly-once = zero data loss) ✓
```

---

## Scenario 2: Debugging a Flaky Pipeline

**Question:** Your pipeline fails about 10% of the time with "Connection timeout after 30 seconds". It almost always succeeds on retry. What is happening and how do you fix it?

**Answer:**

```
Root Cause Analysis:
  10% transient failure + always succeeds on retry = transient network issue
  
  Most likely causes:
  1. Source database overloaded during peak hours → slow to respond
  2. Network congestion between pipeline and DB
  3. DB connection pool exhausted (other jobs competing)
  4. VPC security group rule timing out idle connections

Diagnosis:
  grep "Connection timeout" pipeline.log | awk '{print $1, $2}' | sort | uniq -c
  # Check: do failures cluster at specific times? (peak hours → load issue)
  
  Check DB performance at failure times:
  SELECT query_start, state, wait_event_type, wait_event
  FROM pg_stat_activity
  WHERE state != 'idle'
  ORDER BY query_start;

Fixes:
  1. Increase connection timeout: 30s → 60s (buys time for slow DB)
  2. Add retry with exponential backoff (immediate fix):
     def query_with_retry(sql, max_retries=3):
         for attempt in range(max_retries):
             try:
                 return db.execute(sql)
             except TimeoutError:
                 if attempt < max_retries - 1:
                     time.sleep(2 ** attempt)
                 else: raise
  
  3. Connection pooling: use SQLAlchemy pool_size=5, pool_timeout=60
  4. Schedule pipeline during off-peak hours (avoid DB peak load window)
  5. Add read replica for pipeline queries (decouple from OLTP load)

Prevention:
  Set retry=3, retry_delay=5 in Airflow default_args
  Add alert only after 2 consecutive failures (1 failure = likely transient)
  Monitor source DB load; alert if CPU > 80% during pipeline window
```

---

## Scenario 3: Data Loss Investigation

**Question:** Someone reports that 50,000 orders are missing from your DW for last Tuesday. How do you investigate and prevent this in the future?

**Answer:**

```
Investigation:

Step 1: Quantify the gap
  SELECT COUNT(*) FROM orders_fact WHERE order_date = '2024-01-16';
  -- 1,250,000 rows (expected: ~1,300,000 based on daily average)
  -- Missing: ~50,000 rows

Step 2: Identify the time range of missing data
  SELECT
    EXTRACT(HOUR FROM order_timestamp) AS hour_of_day,
    COUNT(*) AS row_count
  FROM orders_fact WHERE order_date = '2024-01-16'
  GROUP BY hour_of_day ORDER BY hour_of_day;
  -- Hour 14 (2pm): 2,000 rows (expected: ~55,000) ← clear gap

Step 3: Check pipeline run logs
  SELECT * FROM pipeline_run_log
  WHERE pipeline_name = 'orders_pipeline' AND execution_date = '2024-01-16';
  -- status=failed, error="S3 permission denied at 2024-01-16 14:32:00"
  -- Retry succeeded at 14:45 but only processed orders from 3pm onward

Step 4: Root cause
  S3 credentials expired mid-run; retry restarted with new batch starting at 15:00
  Orders between 14:00-15:00 were never loaded

Step 5: Recovery
  -- Re-extract that specific hour from source:
  INSERT INTO orders_fact
  SELECT * FROM orders_source_extract
  WHERE order_timestamp BETWEEN '2024-01-16 14:00' AND '2024-01-16 15:00'
    AND order_id NOT IN (SELECT order_id FROM orders_fact WHERE order_date = '2024-01-16');

Prevention:
  1. Add row count assertion per hour: if any hour < 70% of hourly average → alert
  2. Credential rotation: use IAM roles (no expiring credentials) instead of access keys
  3. Post-run reconciliation: compare source count vs DW count for execution_date
     ASSERT source_count = dw_count ± 0.01% (within 99.99% tolerance)
  4. Never restart a job without a proper watermark: retry always resumes from last checkpoint
```
