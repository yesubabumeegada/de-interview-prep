---
title: "Interview Practice Problems — Intermediate"
topic: system-design
subtopic: interview-practice-problems
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [system-design, interview, practice, data-engineering]
---

# Interview Practice Problems — Intermediate

## Problem 3: Design an Event Tracking System (100K Events/Sec)

**Prompt:** Design a system to track user events (clicks, page views, purchases) at 100,000 events/sec for a large e-commerce platform.

### Step 1: Requirements Clarification

- **Ingestion rate:** 100K events/sec peak, 40K avg
- **Event size:** ~1 KB per event
- **Latency:** Real-time dashboards (< 5 sec lag), batch analytics (daily)
- **Retention:** raw events 30 days hot, 2 years cold
- **Consumers:** real-time dashboard, ML feature pipelines, data warehouse
- **Durability:** no event loss (at-least-once delivery acceptable)

### Step 2: Capacity Estimation

```
Peak: 100K events/sec × 1 KB = 100 MB/sec = 360 GB/hour
Daily raw: 100K × 86,400 sec × 1 KB = ~8.64 TB/day
With 5× compression (Parquet): ~1.7 TB/day
Kafka (3× replication, 30-day retention): 100 MB/sec × 3 = 300 MB/sec write throughput
30-day Kafka storage: 300 MB/sec × 86,400 × 30 ≈ 777 TB (needs tiered storage)
```

### Step 3: High-Level Design

```
Mobile/Web SDK
      │
      ▼
[Load Balancer + API Gateway]  ← HTTP/gRPC collectors
      │
      ▼
[Kafka Cluster]  ← 30 partitions, RF=3, tiered storage (S3)
   │        │
   ▼        ▼
[Flink   [Spark
 Stream]  Batch]
   │        │
   ▼        ▼
[ClickHouse] [Delta Lake / S3]
(real-time)  (long-term)
      │
      ▼
[BI: Superset / Grafana]
```

### Step 4: Detailed Design

#### Collection Layer

Use a **dual-path SDK** to prevent event loss:
```javascript
// SDK pseudocode: buffer locally, batch-send, retry on failure
class EventTracker {
  constructor() {
    this.buffer = [];
    this.flushInterval = 5000; // ms
  }
  track(event) {
    this.buffer.push({ ...event, client_ts: Date.now(), uuid: uuidv4() });
    if (this.buffer.length >= 100) this.flush();
  }
  async flush() {
    const batch = this.buffer.splice(0, 100);
    await fetch('/collect', { method: 'POST', body: JSON.stringify(batch) })
      .catch(() => this.retryQueue.push(...batch)); // retry on failure
  }
}
```

#### Kafka Topic Design

```
Topic: user-events
  Partitions: 30  (100K/sec ÷ ~3K/sec per partition)
  RF: 3
  Retention: 7 days on-disk, tiered to S3 after 1 day
  Key: user_id (ensures per-user ordering within a partition)
  Compression: LZ4 (low latency) or Snappy (balanced)
```

#### Real-Time Layer (Flink)

```java
// Flink job: compute page views per minute per product
DataStream<Event> events = env.fromSource(kafkaSource, ...);

events
  .filter(e -> e.getType().equals("page_view"))
  .keyBy(Event::getProductId)
  .window(TumblingEventTimeWindows.of(Time.minutes(1)))
  .aggregate(new CountAggregator())
  .addSink(clickHouseSink);
```

#### Batch Layer (Spark → Delta Lake)

```python
# Hourly Spark job: compact Kafka output to Delta
spark.readStream \
  .format("kafka") \
  .option("subscribe", "user-events") \
  .load() \
  .writeStream \
  .format("delta") \
  .partitionBy("event_date", "event_type") \
  .option("checkpointLocation", "s3://checkpoints/events") \
  .trigger(processingTime="1 hour") \
  .start("s3://datalake/events/")
```

#### Data Quality Checks

```python
# Great Expectations on hourly Delta writes
from great_expectations.dataset import SparkDFDataset

ge_df = SparkDFDataset(df)
ge_df.expect_column_values_to_not_be_null("user_id")
ge_df.expect_column_values_to_be_between("event_ts",
    min_value=yesterday_ts, max_value=tomorrow_ts)
ge_df.expect_column_values_to_be_in_set("event_type",
    ["page_view", "click", "purchase", "add_to_cart"])
```

### Step 5: Trade-offs

| Decision | Alternative | Reason |
|---|---|---|
| Kafka partitioned by user_id | round-robin | Per-user ordering for session stitching |
| ClickHouse for real-time | Druid, Pinot | ClickHouse has simpler ops, great compression |
| Delta Lake for batch | Iceberg | Delta has better Spark integration at this org |
| LZ4 compression on Kafka | Gzip | LZ4 is 3× faster decompression at similar ratio |

---

## Problem 4: Design a CDC Pipeline (50 MySQL Tables → Lakehouse)

**Prompt:** You have 50 MySQL tables in a production OLTP system. Design a pipeline to replicate all changes (inserts, updates, deletes) into a data lakehouse in near real-time.

### Requirements
- Latency: < 5 minutes from MySQL commit to lakehouse
- Correctness: no missed rows, no duplicates in final tables
- Source impact: zero impact on MySQL write performance
- Schema evolution: handle column additions without breaking pipeline

### Architecture

```
MySQL (binlog enabled)
      │
      ▼
[Debezium connector] ← reads binary log via replica (not primary!)
      │
      ▼
[Kafka] ← one topic per table: mysql.db.orders, mysql.db.customers
      │
      ▼
[Spark Structured Streaming]
      │
   ┌──┴──────────────────┐
   ▼                     ▼
[Upsert to Delta Lake]  [Schema Registry check]
(MERGE INTO)            (Confluent Schema Registry)
      │
      ▼
[Iceberg / Delta tables] ← query with Trino or Athena
```

### Key Implementation Details

**Debezium Configuration:**
```json
{
  "connector.class": "io.debezium.connector.mysql.MySqlConnector",
  "database.hostname": "mysql-replica.internal",
  "database.user": "debezium",
  "database.include.list": "ecommerce",
  "table.include.list": "ecommerce.orders,ecommerce.customers",
  "snapshot.mode": "initial",
  "decimal.handling.mode": "double",
  "transforms": "unwrap",
  "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
  "transforms.unwrap.delete.handling.mode": "rewrite",
  "transforms.unwrap.add.fields": "op,ts_ms"
}
```

**Delta Lake Upsert (MERGE):**
```python
from delta.tables import DeltaTable

def upsert_to_delta(micro_batch_df, epoch_id):
    delta_table = DeltaTable.forPath(spark, "s3://lake/orders/")
    
    delta_table.alias("target").merge(
        micro_batch_df.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdate(
        condition="source.__op = 'u'",
        set={"*": "source.*"}
    ).whenNotMatchedInsert(
        condition="source.__op != 'd'",
        values={"*": "source.*"}
    ).whenMatchedDelete(
        condition="source.__op = 'd'"
    ).execute()

stream = spark.readStream.format("kafka") \
    .option("subscribe", "mysql.ecommerce.orders") \
    .load()

stream.writeStream \
    .foreachBatch(upsert_to_delta) \
    .option("checkpointLocation", "s3://checkpoints/orders") \
    .start()
```

**Schema Evolution Handling:**
- Enable `"schema.compatibility": "BACKWARD"` in Schema Registry
- Debezium emits schema in each message envelope — Spark can evolve automatically with `mergeSchema = true`

### Trade-offs

| Concern | Approach |
|---|---|
| MySQL performance impact | Read from replica, not primary |
| Exactly-once | Delta MERGE is idempotent — Kafka at-least-once is safe |
| Schema changes | Schema Registry + Delta mergeSchema |
| Initial load (50 tables) | Debezium snapshot mode = `initial`, then switch to streaming |

---

## Problem 5: Design a Data Quality Monitoring System (500 Pipelines)

**Prompt:** You manage 500 data pipelines. Design a system that automatically detects data quality issues: null rates, volume anomalies, freshness violations, and distribution shifts.

### Architecture

```
[Pipeline outputs] → [DQ Agent (per pipeline)] → [DQ Metrics Store]
                                                        │
                                              [Anomaly Detection Engine]
                                                        │
                                              [Alert Router] → PagerDuty / Slack
```

### Check Types

| Check Type | Detection Method | Example |
|---|---|---|
| **Freshness** | Max(timestamp) < now - threshold | Orders table not updated in 2h |
| **Volume** | Row count vs rolling 7-day avg (± 3σ) | 80% fewer rows than usual |
| **Null rate** | NULL% per column vs baseline | user_id NULL rate jumped from 0% to 15% |
| **Distribution** | KL divergence vs reference window | Country distribution shifted significantly |
| **Schema** | Column count, type changes | Column `amount` changed from FLOAT to STRING |

### Implementation

```python
import great_expectations as ge
from scipy import stats

class DQMonitor:
    def check_volume_anomaly(self, table: str, today_count: int) -> bool:
        """Z-score based volume anomaly detection."""
        historical = self.metrics_store.get_last_n_days(table, n=30)
        mean = np.mean(historical)
        std = np.std(historical)
        z_score = (today_count - mean) / std if std > 0 else 0
        return abs(z_score) > 3.0  # 3-sigma threshold

    def check_null_rate(self, df, column: str, baseline_null_rate: float) -> bool:
        current_null_rate = df.filter(df[column].isNull()).count() / df.count()
        return abs(current_null_rate - baseline_null_rate) > 0.05  # 5% tolerance

    def check_freshness(self, table: str, max_lag_hours: int) -> bool:
        latest_ts = self.catalog.get_max_timestamp(table)
        lag_hours = (datetime.utcnow() - latest_ts).total_seconds() / 3600
        return lag_hours > max_lag_hours
```

### Scalability for 500 Pipelines

- Run DQ checks as a **Spark job** that scans all tables in parallel
- Store metrics in **TimescaleDB** (time-series optimized Postgres)
- Use **Monte Carlo** or **Elementary** as managed alternatives
- PagerDuty routing by team ownership in a **pipeline registry** (YAML or catalog)

---

## Key Interview Takeaways

1. **Always clarify the latency requirement first** — it determines batch vs stream
2. **Use Debezium + Kafka for CDC** — it's the industry standard pattern
3. **Delta MERGE for upserts** — handles CDC operations cleanly
4. **Statistical anomaly detection > static thresholds** for volume checks
5. **Replicate from MySQL replica** — never touch the primary for replication
