---
title: "Azure Stream Analytics — Senior Deep Dive"
topic: azure
subtopic: azure-stream-analytics
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [azure, stream-analytics, advanced, cep, ml, custom-deserializer, architecture]
---

# Azure Stream Analytics — Senior Deep Dive

## ASA Internal Architecture

```
ASA execution model:

Control Plane:
  Job manager: receives query, compiles to execution DAG
  Partition assignment: maps Event Hubs partitions to ASA instances

Data Plane:
  Streaming Unit (SU) → maps to physical execution instance
  Each SU: processes events from assigned partition(s)
  State store: distributed in-memory store (for window aggregations)
  
Event processing flow:
  1. Event Hubs → ASA ingestion layer (buffered, ordered per partition)
  2. Deserialization → in-memory Event objects
  3. Query execution (SQL compiled to streaming execution DAG)
  4. State updates (window accumulators in state store)
  5. Output: when window closes → emit result → write to output sink

Watermark and late arrival:
  ASA uses system time + event time (TIMESTAMP BY column)
  Watermark = max(event_time) - late_arrival_tolerance
  When watermark passes window end time → window results emitted
  
  Example:
    Window: 12:00-12:05
    Late arrival tolerance: 2 minutes
    Window closes: when system observes 12:07 (12:05 + 2-min tolerance)
    Any events with event_time in 12:00-12:05 arriving before 12:07 → included
    Events arriving after 12:07 → dropped (or adjusted to arrival time)

Fault tolerance:
  ASA checkpoints state to Azure Storage every few seconds
  On failure: restart from last checkpoint, replay Event Hubs from checkpoint offset
  Recovery time: 2-5 minutes (less for jobs with simple state)
  Data loss: at-least-once (events between last checkpoint and failure may be reprocessed)
```

---

## Machine Learning Integration

```sql
-- ASA built-in ML: Anomaly Detection (ADTSS — Azure Anomaly Detection)

-- Spike and Dip detection
SELECT
    ANOMALYDETECTION_SPIKEANDDIP(
        avg_temperature,    -- value to analyze
        95,                 -- confidence level (95%)
        120,                -- history window size (120 events)
        'spikesanddips'     -- detect both spikes and dips
    ) OVER (LIMIT DURATION(second, 120)) AS AnomalyScore
FROM sensors

-- Parse anomaly score:
SELECT
    sensor_id,
    temperature,
    CAST(GetRecordPropertyValue(anomaly_result, 'IsAnomaly') AS BIGINT) AS is_anomaly,
    CAST(GetRecordPropertyValue(anomaly_result, 'Score') AS FLOAT)      AS anomaly_score
FROM (
    SELECT
        sensor_id,
        temperature,
        ANOMALYDETECTION_SPIKEANDDIP(temperature, 95, 120, 'spikesanddips')
            OVER (PARTITION BY sensor_id LIMIT DURATION(minute, 5)) AS anomaly_result
    FROM sensors TIMESTAMP BY event_time
) AS anomaly_results
WHERE CAST(GetRecordPropertyValue(anomaly_result, 'IsAnomaly') AS BIGINT) = 1

-- Custom ML via Azure ML endpoint (UDF):
-- Define ML endpoint as ASA JavaScript UDF
-- Call HTTP endpoint for each event (latency-sensitive, use batch)

-- Change Point Detection (trend change):
SELECT
    ANOMALYDETECTION_CHANGEPOINT(value, 80, 1200)
    OVER (LIMIT DURATION(second, 1200)) AS ChangePointScore
FROM time_series
```

---

## Custom Deserializers and JavaScript UDFs

```javascript
// JavaScript UDF: custom business logic in ASA queries
// Use when SAQL (SQL) is insufficient

// UDF: compute distance between two geo-coordinates (Haversine formula)
function haversineDistance(lat1, lon1, lat2, lon2) {
    const R = 6371; // Earth radius in km
    const dLat = (lat2 - lat1) * Math.PI / 180;
    const dLon = (lon2 - lon1) * Math.PI / 180;
    const a = Math.sin(dLat/2) * Math.sin(dLat/2) +
              Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
              Math.sin(dLon/2) * Math.sin(dLon/2);
    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1-a));
    return R * c; // Distance in km
}

// Use in ASA query:
// SELECT
//     order_id,
//     pickup_lat, pickup_lng,
//     delivery_lat, delivery_lng,
//     UDF.haversineDistance(pickup_lat, pickup_lng, delivery_lat, delivery_lng) AS distance_km
// FROM orders

// UDF: parse custom protobuf-like binary format (simplified example)
function parseCustomFormat(hexString) {
    // Custom parsing logic for proprietary message format
    const buffer = Buffer.from(hexString, 'hex');
    return {
        version: buffer.readUInt8(0),
        deviceId: buffer.readUInt32BE(1),
        value: buffer.readFloatBE(5)
    };
}

// Limitations of JavaScript UDFs:
// - Synchronous only (no async/await, no external HTTP calls)
// - For external calls: use Azure Functions output and call from there
// - Performance: each event goes through JS engine (some overhead)
// - Max array elements: 1,000 per UDF call
```

---

## ASA vs Alternatives: Architecture Decisions

```
Decision matrix:

                ASA          Spark Streaming    Apache Flink
Language:       SQL only      Python/Scala/SQL   Java/Python/SQL
Mgmt:           Serverless    Managed (ADB/EMR)  Semi-managed (AKS/EMR)
State:          Limited       Full stateful       Full stateful + queryable
Throughput:     ~1GB/sec      10+ GB/sec         100+ GB/sec
Latency:        Seconds       10s - minutes      Milliseconds
ML integration: Built-in ADT  MLflow/sklearn      External
Cost:           $0.08/SU/hr   ~$1-3/hr (cluster) ~$1-3/hr (cluster)
Exactly-once:   At-least-once Exactly-once (ckpt) Exactly-once (ckpt)
Best for:       IoT, alerts   Mixed batch+stream  Complex CEP, low latency

Enterprise streaming architecture (layered):
  Device/App → Event Hubs (transport) → ASA (alerting + routing) → ADLS (persistence)
                                      → Databricks Streaming (complex transform) → Delta Lake
                                      → Power BI (real-time dashboard from ASA)

Why ASA for alerts + Databricks for transforms:
  ASA: <5 sec latency for alerts (fraud, anomalies), zero infra overhead
  Databricks: handles complex stateful transforms, joins with historical data,
              feature engineering for ML scoring
  Event Hubs fan-out: multiple consumer groups read same data (ASA + Databricks + storage)
```

---

## Interview Tips

> **Tip 1:** "How does ASA handle stateful operations like window aggregations at scale?" — ASA stores window state in a distributed in-memory state store partitioned by the PARTITION BY key (or Event Hubs partition). Each Streaming Unit manages state for its assigned partition. On checkpoint (every few seconds): state is persisted to Azure Storage. This allows recovery without replaying all history. Limitation: if your state is very large (millions of unique users in a session window), ASA's in-memory state may be insufficient — Flink with RocksDB state backend handles this better. For most IoT and monitoring workloads, ASA's state model is sufficient.

> **Tip 2:** "Can you do exactly-once processing with ASA?" — ASA provides at-least-once semantics (events may be redelivered on failure). To achieve effectively-once results: use idempotent sinks. For SQL DB: configure primary key on output table → ASA generates UPSERT (reprocessed event updates to same value). For ADLS: path pattern with timestamp + minimum rows per file → duplicate micro-batches overwrite same path. For Event Hubs output: deduplication at the consumer using message IDs. True exactly-once (like Flink's TwoPhaseCommitSinkFunction) is not natively supported in ASA.

> **Tip 3:** "What's the maximum throughput of Azure Stream Analytics and how do you exceed it?" — ASA scales to approximately 1 GB/sec input with sufficient SUs (100-200 SUs). For higher throughput: (a) partition the query with PARTITION BY to run independently per Event Hubs partition (embarrassingly parallel), (b) split into multiple ASA jobs (each reading from different partitions), (c) switch to Databricks Structured Streaming or Apache Flink (no meaningful throughput ceiling). In practice, most use cases don't hit ASA's limit — typical IoT/clickstream workloads are 10-100 MB/sec. If you need >1 GB/sec with sub-second latency, Flink on AKS is the right answer.
