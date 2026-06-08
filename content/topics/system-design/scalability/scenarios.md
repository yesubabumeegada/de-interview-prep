---
title: "Scalability & Partitioning — Scenarios"
topic: system-design
subtopic: scalability
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [system-design, scalability, interview, scenarios]
---

# Scalability & Partitioning — Interview Scenarios

## Scenario 1: Design a System for 1 Billion Events Per Day

**Question:** Design a data pipeline that ingests 1 billion events per day from mobile apps (click events, page views). The data must be queryable for analytics within 5 minutes. Estimated: 12,000 events/second average, 50,000/second peak.

**Answer:**

```
Ingestion layer:
  Mobile SDK → Kinesis Data Streams (or Kafka)
  12,000 events/sec avg × 1KB avg size = 12MB/sec
  Kinesis: 200 shards × 1MB/sec/shard = 200MB/sec capacity (16× headroom for peak)
  Or Kafka: 20 brokers × 300MB/sec = 6GB/sec capacity

Buffer layer:
  Kinesis / Kafka: 24-hour retention buffer
  Partitioned by event_type (click, view, purchase) → separate consumers per type

Processing layer:
  Spark Structured Streaming: 5-minute micro-batches
  Read from Kafka → parse JSON → deduplicate (event_id window 10 min) → write Delta
  Target: process 1B events × 1KB = 1TB/day = ~42GB/hour = 700MB/min
  Cluster: 20 executor nodes × 8 cores = 160 cores; processes 5-min batch in < 2 minutes

Storage layer:
  Bronze: raw Parquet on S3 (event_date=YYYY-MM-DD / event_type=click)
  Silver: Delta Lake, cleaned, typed, deduplicated
    Partitioned by (event_date, event_type): ~500 files × 200MB = 100GB/day
  Gold: ClickHouse or Druid for sub-second dashboard queries

Query layer:
  ClickHouse: MergeTree table partitioned by toDate(event_ts)
  Supports 1B row aggregation in < 1 second with columnar storage
  Dashboard: Grafana/Superset reads ClickHouse directly

Sizing:
  Storage: 1TB raw × 365 = 365TB/year. After Parquet compression (10:1): 36TB
  Cost estimate: 36TB × $0.023/GB/month = ~$800/month S3 (hot)
               + 90TB retained in ClickHouse cluster (3 nodes, 30TB each)
```

---

## Scenario 2: Partition Strategy for a Multi-Tenant Data Warehouse

**Question:** You're building a data warehouse for a SaaS company with 500 enterprise customers. Each customer has their own schema in the source database. Queries are always within a single customer. Row count per customer: 5 biggest have 100M rows each; average is 1M rows; 450 customers have < 500K rows.

**Answer:**

```
Option A: Single table, partition by customer_id
  orders (order_id, customer_id, ...) PARTITION BY customer_id
  Pro: simple, one object to manage
  Con: 500 partitions × 5 subtables = 2500+ partition objects (manageable)
       Top 5 customers need pruning to avoid cross-customer scans
  
Option B: Customer-level tables (one table per customer)
  customer_001.orders, customer_002.orders, ...
  Pro: perfect isolation, per-customer vacuuming/optimization
  Con: 500 tables × schema changes = 500 ALTER TABLE statements
       Hard to query across customers for platform analytics

Option C: Tiered approach (recommended for this pattern)
  Large customers (>10M rows): dedicated table (customer_bigco_orders)
  Mid-tier (1-10M rows): shared table partitioned by customer_id
  Small customers (<1M rows): shared table, no partition (just customer_id filter + index)

Why tiered:
  - Big customers need dedicated resources (their queries shouldn't compete)
  - Small customers don't need partition overhead (stats gathering, metadata)
  - Mid-tier: partition pruning helps enough to justify overhead

Row-level security:
  All queries must include customer_id filter (application enforced)
  Add: row-level security policy in Snowflake/BigQuery as defense in depth
  Snowflake: CREATE ROW ACCESS POLICY customer_isolation AS (customer_id VARCHAR)
             RETURNS BOOLEAN → customer_id = CURRENT_ROLE();
```

---

## Scenario 3: Fix a Slow Spark Join

**Question:** A Spark job joins `orders` (500GB, 2B rows) with `customers` (500MB, 50M rows). The job takes 4 hours and you see OOM errors. Fix it.

**Answer:**

```python
# Step 1: Diagnose
# Check Spark UI → Stages → look for shuffle read/write sizes and task duration variance
# If one task takes 3 hours and others take 30 seconds: DATA SKEW

# Check skew:
orders.groupBy("customer_id").count().orderBy("count", ascending=False).show(10)
# customer_id=9999: 500M rows (25% of all data) ← SEVERE SKEW

# Step 2: Is broadcast join possible?
# customers table is 500MB — exceeds default broadcast threshold (10MB)
# But: can we afford to broadcast 500MB?
# With 50 executors × 16GB: yes, 500MB broadcast is fine

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "600000000")  # 600MB
result = orders.join(broadcast(customers), "customer_id")
# No shuffle needed: each executor has full customers table in memory
# 500GB orders, 500MB broadcast = ~500GB shuffle eliminated → 30 min instead of 4 hours

# Step 3: If broadcast not possible (customers table too large):
# Use AQE + skew join handling
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")  
# AQE splits partitions 5× larger than median into sub-partitions

# Step 4: Pre-partition both tables on join key
orders_partitioned = orders.repartition(400, col("customer_id"))
customers_partitioned = customers.repartition(400, col("customer_id"))
# Same customer_id on both sides lands in same partition → reduced shuffle
result = orders_partitioned.join(customers_partitioned, "customer_id")
```
