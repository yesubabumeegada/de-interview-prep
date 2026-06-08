---
title: "Azure Stream Analytics — Intermediate"
topic: azure
subtopic: azure-stream-analytics
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [azure, stream-analytics, complex-queries, reference-data, scaling, ci-cd]
---

# Azure Stream Analytics — Intermediate

## Complex Query Patterns

```sql
-- Pattern 1: Multi-step fraud detection (5+ transactions from same card in 1 min)

WITH TransactionCounts AS (
    SELECT
        card_id,
        COUNT(*)              AS txn_count,
        SUM(amount)           AS total_amount,
        System.Timestamp()    AS window_end
    FROM transactions TIMESTAMP BY event_time
    GROUP BY
        card_id,
        TumblingWindow(minute, 1)
)
SELECT
    card_id,
    txn_count,
    total_amount,
    window_end,
    'HIGH_FREQUENCY' AS alert_type
INTO fraud_alerts
FROM TransactionCounts
WHERE txn_count >= 5 OR total_amount >= 10000;

-- Forward non-fraud events to normal output
SELECT *
INTO normal_output
FROM transactions TIMESTAMP BY event_time
WHERE card_id NOT IN (
    SELECT card_id FROM TransactionCounts WHERE txn_count >= 5
);
-- Note: multiple outputs from same job — routes events differently

-- Pattern 2: Join two streams (enrich in real-time)
-- Match orders with payments within 5 minutes of each other

SELECT
    o.order_id,
    o.customer_id,
    o.amount        AS order_amount,
    p.amount        AS payment_amount,
    o.event_time    AS order_time,
    p.event_time    AS payment_time,
    CASE WHEN ABS(o.amount - p.amount) < 0.01 THEN 'MATCHED' ELSE 'MISMATCH' END AS match_status
INTO matched_transactions
FROM orders o TIMESTAMP BY event_time
JOIN payments p TIMESTAMP BY event_time
ON o.order_id = p.order_id
AND DATEDIFF(minute, o, p) BETWEEN 0 AND 5   -- payment within 5 min of order
```

---

## Reference Data and Dynamic Updates

```sql
-- Reference data: join stream with slowly-changing lookup table
-- Stored in ADLS Gen2 as CSV/JSON, refreshed periodically

-- Setup in ASA:
-- Input type: Reference data
-- Path pattern: products/current.csv (no date pattern = static)
-- Refresh interval: every 60 minutes (ASA re-reads file every hour)

SELECT
    s.product_id,
    s.quantity,
    s.unit_price,
    r.product_name,   -- from reference
    r.category,
    r.cost_price,
    (s.unit_price - r.cost_price) * s.quantity AS gross_margin
FROM sales_stream s TIMESTAMP BY event_time
JOIN products_reference r ON s.product_id = r.product_id

-- Pattern: date-based reference data (refreshed daily)
-- Path pattern: products/{date}/products.csv
-- ASA resolves {date} to today's UTC date automatically
-- Use for: daily price lists, daily exchange rates, promotions
```

---

## Output Configuration and Exactly-Once

```sql
-- Multiple outputs from one job:
-- Route different event types to different destinations

-- High-priority alerts → Event Hubs (for immediate action)
SELECT card_id, txn_count, total_amount
INTO fraud_alerts_eventhub    -- Event Hubs output
FROM transactions...
WHERE txn_count >= 5;

-- All events → ADLS (for long-term storage and analysis)
SELECT *
INTO all_transactions_adls    -- ADLS Gen2 output, Parquet format
FROM transactions;

-- Aggregated metrics → SQL DB (for Power BI)
SELECT region, COUNT(*) AS txn_count, SUM(amount) AS revenue
INTO metrics_sql              -- Azure SQL DB output
FROM transactions
GROUP BY region, TumblingWindow(minute, 1);

-- ADLS output settings:
-- Path pattern: {date}/{time}/transactions.parquet
-- Minimum rows: 10,000 (batch until 10K rows before writing file)
-- Time window: 10 minutes (write at least every 10 min even if < 10K rows)
-- This controls file size: avoid 1-row Parquet files

-- Exactly-once delivery:
-- ASA provides at-least-once by default
-- For exactly-once to SQL: use output batch size + idempotent UPSERT
-- Configure SQL output with primary key → ASA uses UPSERT (not INSERT)
-- Duplicate events: deduplicate on primary key via MERGE at sink
```

---

## Scaling and Performance

```
Scaling guidelines:

Step 1: Calculate SU requirement
  Input throughput: 50 MB/sec
  Query complexity:
    Simple filter/select: 1 SU per 1 MB/sec = 50 SUs
    Aggregation (GroupBy): 2-3 SUs per MB/sec = 100-150 SUs
    Join (two streams): 3-6 SUs per MB/sec = 150-300 SUs
  
Step 2: Match Event Hub partitions
  Event Hubs: 10 partitions
  ASA: set SUs to multiple of partition count (10, 20, 30...)
  Each ASA partition instance reads from exactly 1 EH partition (1:1)

Step 3: PARTITION BY for embarrassingly parallel queries
  -- Partitioned query (runs independently per partition)
  SELECT
    card_id,
    COUNT(*) AS txn_count
  FROM transactions PARTITION BY PartitionId  -- each partition independent
  GROUP BY card_id, TumblingWindow(minute, 5), PartitionId
  
  Without PARTITION BY: all partitions funnel through one aggregation node
  With PARTITION BY: N partitions = N independent aggregations (linear scale)
  Limitation: PARTITION BY only works when output doesn't need cross-partition aggregation

Step 4: Monitor in Azure Monitor
  Metrics to watch:
    SU% Utilization: keep < 80% for burst headroom
    Watermark Delay: how far behind real-time (should be < 30 sec for most use cases)
    Output Events: verify data is flowing
    Input Events: verify Event Hubs is publishing
  
  Alert: SU% > 80% for 5 minutes → auto-scale SUs (or trigger notification to increase manually)
```

---

## CI/CD for Stream Analytics

```bash
# ASA CI/CD with Azure DevOps

# ASA project structure (local development with VS Code ASA extension):
# /project
#   ├── asaproj.json          (project config)
#   ├── Inputs/
#   │   ├── orders_input.json
#   │   └── products_reference.json
#   ├── Outputs/
#   │   ├── adls_output.json
#   │   └── sql_output.json
#   ├── Functions/            (UDFs in JavaScript)
#   ├── query.asaql           (main SQL query)
#   └── JobConfig.json        (SU count, compatible level, etc.)

# Azure DevOps pipeline:
# stages:
#   - Build: compile + validate ASA project
#   - Test: run local tests with sample data
#   - Deploy: ARM template deployment to target environment

# Deployment via ARM template (generated from ASA project):
az stream-analytics job create \
  --resource-group rg-streaming \
  --name asa-fraud-detection \
  --location eastus \
  --compatibility-level "1.2" \
  --events-out-of-order-policy "Adjust" \
  --events-out-of-order-max-delay-in-seconds 10 \
  --events-late-arrival-max-delay-in-seconds 5

# Parameterize connection strings per environment:
# Use ASA job's system-managed identity to access Event Hubs + ADLS
# No connection strings in ARM template — MSI authenticates automatically

# Testing locally (VS Code ASA extension):
# 1. Add sample input data (JSON files matching input schema)
# 2. Run query locally → check output in VS Code
# 3. Unit test: verify expected output rows given test input
```

---

## Interview Tips

> **Tip 1:** "How do you handle late-arriving events in Stream Analytics?" — TIMESTAMP BY specifies which field to use as event time. ASA supports `late arrival tolerance`: events arriving late within a configured window are still included in their correct time window. Configure: `Events late arrival max delay = 5 minutes` (job-level setting). Events arriving more than 5 minutes late are dropped or adjusted (configurable policy: Adjust = use arrival time, Drop = discard). For queries that need to tolerate more lateness, increase the late arrival tolerance and accept that window results are delayed by that duration.

> **Tip 2:** "What's the compatibility level in ASA and why does it matter?" — Compatibility level (1.0, 1.1, 1.2) controls which SQL language features and behaviors are available. 1.2 is current and recommended (supports JSON functions, geo-spatial, newer windowing). 1.0 is deprecated but some older jobs still run on it. Upgrading compatibility level can change query behavior (e.g., NULL handling, timestamp behavior). Always test query output on the new compatibility level before upgrading a production job. New jobs should always use 1.2.

> **Tip 3:** "How does ASA handle exactly-once output to SQL Database?" — ASA itself provides at-least-once delivery (events may be reprocessed on failure). To achieve effectively-once at the SQL sink: configure the SQL output with a primary key on the output table. ASA will generate MERGE/UPSERT statements instead of plain INSERTs. If the same event is reprocessed, the UPSERT updates the existing row to the same value (idempotent). For ADLS output: enable the "minimum rows before write" option so partial micro-batches don't create incomplete files that get re-written on retry.
