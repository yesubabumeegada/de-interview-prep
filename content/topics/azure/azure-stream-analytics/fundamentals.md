---
title: "Azure Stream Analytics — Fundamentals"
topic: azure
subtopic: azure-stream-analytics
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [azure, stream-analytics, real-time, sql, iot, event-processing]
---

# Azure Stream Analytics — Fundamentals


## 🎯 Analogy

Think of Azure Stream Analytics like a continuous SQL query running over a live data stream: you write a SELECT statement with window functions (tumbling, hopping, sliding), point it at an Event Hub, and results flow to your output sink in real time.

---
## What Is Azure Stream Analytics?

Azure Stream Analytics (ASA) is a **fully managed real-time stream processing service** that uses SQL-like queries to analyze and process streaming data. No infrastructure to manage — define a query, connect inputs and outputs, and Azure runs it at scale.

```
ASA use cases:
  IoT sensor monitoring:   aggregate temperature readings every 30 sec, alert on anomalies
  Real-time dashboards:    aggregate clickstream events per minute for live Power BI
  Fraud detection:         detect 5+ transactions from same card in 1 minute
  Log analysis:            parse application logs, filter errors, write to storage
  ETL from streams:        reformat/filter Event Hubs data, write to ADLS/SQL/Cosmos DB

ASA components:
  Input sources:     Event Hubs, IoT Hub, Blob Storage / ADLS (batch reference)
  Output sinks:      Event Hubs, Service Bus, Blob/ADLS, SQL DB, Cosmos DB, 
                     Power BI, Azure Functions, Azure Synapse
  Query:             SQL-like syntax (SAQL = Stream Analytics Query Language)
  Streaming Units:   compute capacity (1 SU = 1 vCPU + ~1.5 GB RAM)

Compared to Flink/Spark Streaming:
  ASA:   serverless, SQL only, Azure-native, fast to set up, less flexible
  Flink: custom code (Java/Python), stateful, complex patterns, any cloud
  Spark: batch + micro-batch, PySpark/Scala, general purpose
  Choose ASA: SQL-first teams, Azure-only, simple aggregation/alert patterns
```

---

## Core Concepts: Windowing

```sql
-- Time windows in ASA: aggregate events over time ranges

-- Tumbling Window: fixed-size, non-overlapping
-- Example: count events every 5 minutes
SELECT
    System.Timestamp() AS window_end,
    region,
    COUNT(*)           AS event_count,
    AVG(amount)        AS avg_amount
FROM orders TIMESTAMP BY event_time
GROUP BY
    region,
    TumblingWindow(minute, 5)
-- Every 5 min: emit one row per region with count for that window

-- Hopping Window: fixed-size, overlapping (hop < window = overlap)
-- Example: 10-minute window, advancing every 5 minutes (data appears in 2 windows)
SELECT
    System.Timestamp() AS window_end,
    user_id,
    COUNT(*) AS txn_count
FROM transactions TIMESTAMP BY event_time
GROUP BY
    user_id,
    HoppingWindow(minute, 10, 5)  -- 10-min window, 5-min hop
-- Use for: rolling aggregations (last 10 min, updated every 5 min)

-- Sliding Window: continuous window that emits only when value changes
-- Less common — used for threshold-based alerting

-- Session Window: groups events with activity gaps
-- Example: user sessions (group events with < 5-min gap as one session)
SELECT
    System.Timestamp() AS session_end,
    user_id,
    COUNT(*) AS page_views
FROM pageviews TIMESTAMP BY event_time
GROUP BY
    user_id,
    SessionWindow(minute, 5)   -- end session after 5 min inactivity
-- Useful for: session analytics, user journey analysis
```

---

## Inputs and Outputs

```
Inputs:
  Event Hubs (primary): real-time event streams from applications
    - Connect via: Event Hubs namespace + consumer group
    - Serialization: JSON, CSV, Avro, Parquet
    - Consumer group: use dedicated consumer group per ASA job (not $Default)
  
  IoT Hub: device telemetry (built on Event Hubs, adds device registry)
  
  Blob Storage / ADLS (Reference Input):
    - Static or slowly changing lookup data (product catalog, zip code → region)
    - Loaded into memory at job start or refreshed on schedule (every 1 min-8 hours)
    - Join with stream: stream.product_id = reference.product_id

Outputs (select one or more per job):
  ADLS Gen2 / Blob Storage: write Parquet/JSON files (batch accumulation)
  Azure SQL DB / Synapse:   write rows (INSERT/UPSERT, max ~10K rows/sec)
  Power BI:                 live streaming dataset (real-time dashboard)
  Cosmos DB:                JSON documents (good for high write throughput)
  Event Hubs:               forward processed events to next stage
  Azure Functions:          call custom code for complex actions (send email, call API)
  Service Bus:              fire-and-forget alerts to downstream subscribers

Parallelism:
  Partitioned input (Event Hubs: N partitions) → N parallel query instances
  Higher partition count = higher throughput
  Streaming Units must match partition count for max throughput
  1 partition = 1 SU (roughly)
```

---

## ASA Query Language Basics

```sql
-- Reference join (enrich stream with static data)
SELECT
    o.order_id,
    o.customer_id,
    o.amount,
    r.region_name,     -- from reference data
    r.timezone
FROM orders o TIMESTAMP BY event_time
JOIN region_reference r ON o.zip_code = r.zip_code

-- Filter and reformat
SELECT
    order_id,
    CAST(amount AS FLOAT) AS amount_float,
    LOWER(region)        AS region,
    event_time
FROM orders TIMESTAMP BY event_time
WHERE amount > 0 AND order_id IS NOT NULL

-- Late arrival tolerance (TIMESTAMP BY with late arrival)
SELECT ...
FROM events TIMESTAMP BY event_time
WHERE DATEDIFF(minute, event_time, System.Timestamp()) < 5  -- tolerate 5-min late

-- Anomaly detection (built-in ML function)
SELECT
    ANOMALYDETECTION_SPIKEANDDIP(amount, 95, 120, 'spikesanddips') OVER (
        LIMIT DURATION(second, 120)
    ) AS AnomalyDetection
FROM transactions
```

---


## ▶️ Try It Yourself

```sql
-- Azure Stream Analytics job query
-- Input: orders-eventhub (Event Hub)
-- Output: orders-summary (Power BI or Blob Storage)

SELECT
    System.Timestamp() AS window_end,
    region,
    COUNT(*) AS order_count,
    SUM(amount) AS total_revenue
INTO [orders-summary]
FROM [orders-eventhub]
TIMESTAMP BY EventEnqueuedUtcTime
GROUP BY
    region,
    TumblingWindow(minute, 5)  -- 5-minute non-overlapping windows

-- Sliding window: detect fraud (3+ orders in 1 minute from same IP)
-- SELECT ip_address, COUNT(*) AS cnt
-- FROM clicks TIMESTAMP BY event_time
-- GROUP BY ip_address, SlidingWindow(minute, 1)
-- HAVING COUNT(*) >= 3
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "When would you use Azure Stream Analytics vs Apache Flink?" — Use ASA when: the team knows SQL, the workload is simple aggregations/alerts/routing, you want zero infrastructure management, and you're 100% on Azure. Use Flink when: you need complex stateful logic (pattern matching across events, CEP), custom Python/Java functions, multi-cloud portability, or very high throughput (millions of events/sec where ASA costs more). ASA is perfect for IoT alerting, real-time dashboards, and simple ETL from Event Hubs to storage. Flink is better for sophisticated fraud detection, exactly-once complex stateful processing.

> **Tip 2:** "What is a Streaming Unit (SU) and how do you size it?" — 1 SU = approximately 1 vCPU + 1.5 GB RAM within ASA. Throughput: 1 SU handles ~1 MB/sec input. For 100 MB/sec: ~100 SUs. ASA pricing: $0.08/SU-hour. General guideline: start with number of input Event Hub partitions (1 SU per partition), monitor CPU/SU utilization in Azure Monitor, scale up if CPU > 80% sustained. For complex queries with multiple joins or aggregations: 2-3× the partition count.

> **Tip 3:** "What's the difference between Tumbling and Hopping windows?" — Tumbling: fixed, non-overlapping windows (each event belongs to exactly one window). E.g., 0-5min, 5-10min, 10-15min. Use for: periodic summaries, billing aggregations. Hopping: overlapping windows where each event can appear in multiple windows (window > hop). E.g., 10-min window every 5 min means events from 5:00-5:05 appear in both the 0-10 and 5-15 min windows. Use for: rolling averages that give smoother transitions than tumbling.
