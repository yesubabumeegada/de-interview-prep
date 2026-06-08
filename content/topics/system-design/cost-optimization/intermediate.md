---
title: "Cost Optimization — Intermediate"
topic: system-design
subtopic: cost-optimization
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [system-design, cost-optimization, spot-instances, finops, tagging, showback]
---

# Cost Optimization — Intermediate

## FinOps Principles for Data Engineering

```
FinOps (Cloud Financial Management) principles:
  1. Teams should own their costs (not just cloud ops)
  2. Cost visibility enables behavior change
  3. Showback/chargeback: attribute costs to teams/projects
  4. Optimize continuously (not a one-time project)

Cost allocation:
  Every cloud resource must have cost tags
  Minimum tags: team, project, environment, owner
  
  AWS tag example on EMR cluster:
    team:         data-engineering
    project:      orders-pipeline
    environment:  production
    cost-center:  12345
  
  Tools: AWS Cost Explorer, GCP Billing, Azure Cost Analysis
  Weekly cost review: top 10 most expensive resources per team
```

---

## Spark Cost Optimization

```python
# Spark cost = cluster size × time running

# Strategy 1: Right-size the cluster
# Profile: Spark UI → Executors → Memory Used vs Max Memory
# If memory used < 30% of allocated: cluster is over-provisioned
# Rule: memory utilization should be 60-80% target

# Bad: fixed large cluster
spark = SparkSession.builder \
    .config("spark.executor.instances", "50") \
    .config("spark.executor.memory", "16g") \
    .config("spark.executor.cores", "4") \
    .getOrCreate()
# 50 × 16GB × 4 cores = 800GB RAM, 200 cores — even if job only needs 20

# Better: auto-scaling (Databricks / EMR managed scaling)
# spark.dynamicAllocation.enabled=true (Spark native)
# Min: 2 executors (always warm), Max: 50 (for peak)
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "50")
spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "60s")  # scale up after 60s of backlog

# Strategy 2: Spot instances for workers
# AWS EMR: specify spot bid price for task nodes
# Databricks: spot instance pools for worker nodes (driver stays on-demand)
# Savings: 60-80% cheaper than on-demand

# Strategy 3: Eliminate data shuffles (most expensive operation)
# Each shuffle: writes intermediate data to disk, reads across network
# Shuffles happen during: groupBy, join (non-broadcast), repartition, orderBy

# Anti-pattern: unnecessary wide transform
df.groupBy("user_id").agg(spark_sum("amount"))  # shuffle: group by user_id
# Then immediately:
df.groupBy("user_id", "region").agg(spark_sum("amount"))  # another shuffle!

# Better: combine transforms into one shuffle
df.groupBy("user_id", "region").agg(spark_sum("amount"))  # single pass

# Strategy 4: Cache only what's re-used
df.cache()  # DON'T cache unless you use the DataFrame 3+ times
# Cache uses executor memory → if not re-used, wastes memory → forces GC → slower
df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # if caching large DFs

# Serialize before cache: SER (serialized) uses less memory than MEMORY_ONLY
# At cost of CPU to deserialize on read
```

---

## Snowflake Cost Optimization

```sql
-- Snowflake costs: compute (credits = virtual warehouse time) + storage

-- Cost visibility: query credit usage by warehouse
SELECT warehouse_name,
       SUM(credits_used) AS total_credits,
       ROUND(SUM(credits_used) * 3, 2) AS estimated_cost_usd  -- ~$3/credit standard tier
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time > DATEADD(day, -30, CURRENT_TIMESTAMP)
GROUP BY warehouse_name
ORDER BY total_credits DESC;

-- Find expensive queries
SELECT query_text,
       ROUND(execution_time / 60000, 2) AS execution_minutes,
       ROUND(credits_used_cloud_services, 4) AS cloud_credits,
       partitions_scanned, partitions_total
FROM snowflake.account_usage.query_history
WHERE start_time > DATEADD(day, -7, CURRENT_TIMESTAMP)
ORDER BY execution_time DESC
LIMIT 20;

-- Multi-cluster warehouse: avoid (very expensive!)
-- Multi-cluster scales OUT (adds warehouses for concurrency)
-- Only enable for high-concurrency BI use cases (>50 concurrent users)
-- For most cases: one warehouse per workload type (ETL, BI, ad-hoc)

-- Warehouse sizing guide:
-- XS: simple queries, <1GB scan
-- S: normal BI queries, <10GB scan
-- M: complex queries, joins, 10-100GB scan
-- L: very complex, 100GB-1TB scan
-- XL+: only when profiling shows L is CPU-bound, not memory-bound

-- Reduce Snowflake Time Travel for cheap tables
ALTER TABLE staging.orders SET DATA_RETENTION_TIME_IN_DAYS = 0;
-- Staging tables: no need for time travel → $0 time travel storage

-- Transient tables: no Fail-Safe (7 days = 7 days of extra storage)
CREATE TRANSIENT TABLE staging.orders_temp (...);
-- Transient = no Fail-Safe; use for staging, temp tables; can't recover
```

---

## Data Transfer Cost Reduction

```
Cloud data transfer costs (often overlooked):
  AWS: outbound internet: $0.09/GB
  AWS: cross-AZ transfer: $0.01/GB (each direction)
  AWS: cross-region: $0.02/GB
  Within same AZ: free

Common expensive patterns:
  1. Spark cluster in us-east-1 reads data from S3 in us-west-2
     Fix: co-locate compute and storage in same region
  
  2. BI tool in one region queries Snowflake in another
     Fix: Snowflake region should match your primary users' region
  
  3. Streaming pipeline sends data across regions for processing
     Fix: process in the same region as the Kafka cluster
  
  4. Large exports to external systems
     Fix: use PrivateLink / VPC endpoints (no internet egress charges)

Practical rules:
  - Always deploy compute in same region as data storage
  - Use VPC endpoints for S3 access from EC2/EMR (no NAT Gateway costs)
  - Compress before cross-region transfer (zstd, gzip)
  - Aggregate before sending to external systems (send summaries, not raw events)
```

---

## Interview Tips

> **Tip 1:** "How do you attribute cloud costs to different teams in a shared data platform?" — Tag every resource with team, project, and environment tags. Use AWS Cost Allocation Tags or GCP Labels. Build a weekly cost report that shows cost per team, broken down by service category (compute, storage, data transfer). Implement showback (show teams their costs) before chargeback (bill teams for their costs). Most important: make cost visible — teams optimize what they can see.

> **Tip 2:** "A Spark job is expensive. How do you reduce its cost?" — Profile first: check Spark UI for (1) shuffle read/write size — reduce with broadcast joins, (2) executor memory utilization — right-size the cluster, (3) idle executors — enable dynamic allocation, (4) job duration vs actual processing time — check for excessive serialization. Move workers to spot instances (60-80% savings). Schedule during off-peak hours to use spot capacity. Cache DataFrames only if reused 3+ times.

> **Tip 3:** "What is Snowflake credit and how do you minimize credit usage?" — 1 Snowflake credit = 1 hour of 1 virtual warehouse node. Cost: ~$2-4/credit. Minimize: (1) auto-suspend at 60 seconds for idle warehouses, (2) right-size warehouses — use M for most queries, only scale to L/XL when profiling shows bottleneck, (3) query result cache (free: repeated query within 24h returns cached result), (4) separate warehouses for ETL vs BI vs ad-hoc — prevent large ETL jobs from wasting credits allocated to BI.
