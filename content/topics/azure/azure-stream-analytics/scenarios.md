---
title: "Azure Stream Analytics — Scenarios"
topic: azure
subtopic: azure-stream-analytics
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [azure, stream-analytics, scenarios, interview, real-time, architecture]
---

# Azure Stream Analytics — Interview Scenarios

## Scenario 1: Real-Time Retail Dashboard

**Question:** A retail company wants to show live store performance on a Power BI dashboard: current hour sales by store, running 7-day trend, and real-time low inventory alerts. Design the ASA pipeline.

**Answer:**

```
Architecture:
  POS systems → Event Hubs (sales_events) → ASA → Power BI + ADLS + Service Bus

Event Hubs:
  Namespace: retail-streaming
  Event Hub: sales_events (10 partitions)
  Consumer group: asa-consumer (dedicated, not $Default)

Reference data (ADLS):
  Path: reference/inventory/{date}/current.csv
  Content: product_id, store_id, current_stock, reorder_threshold
  Refresh: every 60 minutes in ASA configuration

ASA Job: retail_dashboard_asa (20 SUs)

Query 1: Current hour sales per store → Power BI streaming dataset
SELECT
    store_id,
    COUNT(*)       AS transactions,
    SUM(amount)    AS revenue,
    AVG(amount)    AS avg_basket,
    System.Timestamp() AS updated_at
INTO powerbi_current_hour
FROM sales_events TIMESTAMP BY event_time
GROUP BY
    store_id,
    TumblingWindow(hour, 1)

Query 2: Low inventory alerts → Service Bus → store manager notification
SELECT
    s.store_id,
    s.product_id,
    i.current_stock,
    i.reorder_threshold,
    'LOW_INVENTORY' AS alert_type,
    System.Timestamp() AS detected_at
INTO service_bus_alerts
FROM sales_events s TIMESTAMP BY event_time
JOIN inventory_reference i
    ON s.store_id = i.store_id AND s.product_id = i.product_id
WHERE i.current_stock <= i.reorder_threshold

Query 3: All events to ADLS (for Databricks to build 7-day trend separately)
SELECT *
INTO adls_all_sales
FROM sales_events TIMESTAMP BY event_time
-- Path pattern: sales/{date}/{time}

Power BI configuration:
  Streaming dataset connected to ASA output
  Dashboard tile: Card (current revenue), Bar chart (by store), Line (trend)
  Refresh: Power BI receives push from ASA → dashboard refreshes every 1 minute
  
7-day trend:
  Built in Databricks (daily batch job) from ADLS historical data
  Power BI imports: 7-day aggregated table from Synapse/Delta
  Not real-time (updated daily) — trend doesn't need real-time precision

Cost estimate:
  ASA: 20 SUs × $0.08/hr × 24 × 30 = $1,152/month
  Event Hubs: 10 partitions Standard tier = ~$200/month
  Power BI Premium: included in existing license
  Total streaming: ~$1,350/month for real-time retail dashboard
```

---

## Scenario 2: ASA Job Stops Producing Output

**Question:** Production ASA job was running fine, but Power BI stopped updating 3 hours ago. Alerts haven't fired in 3 hours either. How do you diagnose?

**Answer:**

```
Systematic investigation:

Step 1: Check job status
  Azure Portal → Stream Analytics → job_name → Overview
  Status: "Running" (good) vs "Stopped" / "Failed" / "Degraded"
  
  If "Failed": check Error tab → usually shows the error
  Common errors:
    "Output write failed": downstream SQL/ADLS not accepting writes
    "Input deserialization error": malformed events in Event Hubs
    "Insufficient permissions": managed identity lost access

Step 2: Check job metrics (last 3 hours)
  Azure Monitor → stream analytics job → Metrics
  
  a) InputEventBytes = 0 → no events coming from Event Hubs
     → Check Event Hubs: is publisher still sending?
     → az eventhubs eventhub show-consumer-group → check lag
     → Event Hubs consumer group lag = 0? Publisher stopped
     → Lag = millions? ASA stopped reading (check job state)
  
  b) InputEventBytes > 0 but OutputEventBytes = 0 → query producing no output
     → Query logic issue: WHERE clause filtering everything
     → Window not closing: if time-based window and no events → no output
     → Late arrival: all events arriving as "late" and being dropped

  c) ResourceUtilization = 0 → job paused or stopped
     → Check: was job stopped for maintenance and not restarted?

Step 3: Check Event Hubs metrics
  Portal → Event Hubs → Metrics → "Incoming Messages"
  If 0 messages in last 3 hours: upstream publisher stopped
  → Contact the publishing application team

Step 4: Check output sinks
  If ASA shows output events being emitted but Power BI isn't updating:
  → Check Power BI streaming dataset: portal.azure.com → Power BI
  → May be a Power BI service issue, not ASA
  
  If SQL output: check Azure SQL DB for errors, DTU usage

Step 5: Remediation by root cause
  Job failed: fix error → restart job (will replay from checkpoint)
  Publisher stopped: escalate to publishing team
  Query filtering all events: test with portal query tester using sample events
  Late arrival: increase late arrival tolerance (days if needed for catch-up)

Post-incident:
  Add Azure Monitor alert: WatermarkDelaySeconds > 60 → immediate notification
  Add alert: InputEventBytes == 0 for 30 minutes → escalate
```

---

## Scenario 3: Design Real-Time Anomaly Detection for Industrial IoT

**Question:** A factory has 5,000 machines, each with 10 sensors reporting every second. You need to detect anomalies (temperature/pressure spikes, equipment failure signatures) in real time. Design the system.

**Answer:**

```
Scale:
  5,000 machines × 10 sensors × 1 event/sec = 50,000 events/sec
  Each event: ~500 bytes (JSON) = 25 MB/sec

Infrastructure:
  IoT Hub: 50,000 msg/sec requires S3 tier × N units
  ASA: 25 MB/sec input → ~50 SUs minimum for simple queries
       Add 50 SUs for anomaly detection (ML functions) → 100 SUs total

ASA Query Design:

-- 1. Per-machine window aggregations (PARTITION BY machine_id for parallelism)
SELECT
    machine_id,
    sensor_name,
    AVG(value)         AS avg_value,
    MAX(value)         AS max_value,
    STDEV(value)       AS std_dev,
    System.Timestamp() AS window_end
INTO machine_stats_adls
FROM sensor_readings PARTITION BY machine_id TIMESTAMP BY event_time
GROUP BY
    machine_id,
    sensor_name,
    TumblingWindow(second, 30),
    PartitionId

-- 2. Threshold-based immediate alerts (< 1 sec latency)
SELECT
    machine_id,
    sensor_name,
    value,
    event_time,
    'THRESHOLD_EXCEEDED' AS alert_type
INTO critical_alerts_serviceBus
FROM sensor_readings TIMESTAMP BY event_time
WHERE 
    (sensor_name = 'temperature' AND value > 180) OR
    (sensor_name = 'pressure' AND value > 250)

-- 3. Anomaly detection using ASA ML (spike detection)
SELECT
    machine_id,
    sensor_name,
    ANOMALYDETECTION_SPIKEANDDIP(value, 95, 120, 'spikesanddips')
    OVER (PARTITION BY machine_id, sensor_name LIMIT DURATION(second, 120)) AS ad_result
INTO anomaly_events_eventhub
FROM sensor_readings PARTITION BY machine_id TIMESTAMP BY event_time

-- 4. All data to ADLS (for Databricks ML model training)
SELECT * INTO adls_all_readings FROM sensor_readings;

Downstream:
  critical_alerts → Service Bus → Azure Functions → PLC shutdown command
  anomaly_events → Event Hubs → Databricks (build advanced ML signature models)
  machine_stats → ADLS → Power BI (factory floor dashboard)
  all_readings → ADLS Delta → Databricks ML feature store

Latency targets:
  Threshold alerts: < 1 second (ASA WHERE clause, no windowing)
  Anomaly detection: 30-120 seconds (window-based ML)
  Dashboard update: 30 seconds (tumbling window)
```

---

## Interview Tips

> **Tip 1:** "How would you achieve sub-second latency with ASA?" — Sub-second latency is achievable for simple filter/projection queries (no windowing). A WHERE clause query emits each event immediately after processing (no waiting for window to close). For aggregations, minimum latency = window duration. Use the smallest window size needed: for alerts that don't require aggregation, use direct WHERE filtering (event-at-a-time). For IoT threshold alerts, a simple `WHERE temperature > 180` query produces output in <1 second of the event arriving.

> **Tip 2:** "When would you replace ASA with Databricks Structured Streaming?" — When you need: (a) exactly-once semantics (ASA is at-least-once), (b) complex stateful processing (CEP, multi-event patterns across long time windows), (c) Python UDFs for sophisticated ML scoring, (d) joins with Delta Lake tables (historical data enrichment), (e) throughput > 1 GB/sec, (f) sub-100ms latency. ASA strengths: zero infra, SQL-only, fast setup, cheap at moderate scale. Databricks strengths: flexibility, exactly-once, rich ecosystem, handles petabyte scale.

> **Tip 3:** "How do you handle a schema change in the Event Hubs input (new field added)?" — ASA queries are schema-agnostic for non-referenced fields: if you use SELECT specific_columns FROM input, new fields are ignored. If you use SELECT *, the new fields appear in output (may break downstream if output has strict schema like SQL DB). Best practice: SELECT named columns (not *) in production queries — new fields are transparently ignored. For SELECT * to ADLS (schema-on-read): new fields appear in new files and Databricks/Spark can read them with `mergeSchema=true`. Update query in ASA: stop job, update query, restart (processes from last checkpoint — no data loss).
