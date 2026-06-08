---
title: "Scalability & Partitioning — Fundamentals"
topic: system-design
subtopic: scalability
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [system-design, scalability, partitioning, sharding, horizontal-scaling]
---

# Scalability & Partitioning — Fundamentals

## Vertical vs Horizontal Scaling

```
Vertical Scaling (Scale Up):
  Add more CPU/RAM/disk to a single machine
  Simple: no code changes needed
  Limited: physical hardware ceiling
  Single point of failure
  Example: Upgrade RDS from db.r5.large → db.r5.4xlarge

Horizontal Scaling (Scale Out):
  Add more machines; distribute work across nodes
  Theoretically unlimited
  Requires distributed coordination
  Resilient: losing one node doesn't lose everything
  Example: Add nodes to Spark cluster; scale Kafka brokers from 3 → 9
```

**When to scale vertically:** small-medium workloads, transactional databases (OLTP), when simplicity matters.  
**When to scale horizontally:** large-scale analytics, streaming, > TB of data.

---

## Partitioning Strategies

Partitioning divides a large dataset into smaller, manageable pieces:

### Range Partitioning
```sql
-- By date (most common for time-series data)
CREATE TABLE orders (
  order_id BIGINT,
  order_date DATE,
  amount DECIMAL(10,2)
)
PARTITION BY RANGE (order_date);

-- Each month is a separate physical partition
-- Query: WHERE order_date = '2024-01-15' → reads only January partition (partition pruning)
-- Benefit: queries on recent data skip historical partitions entirely
```

### Hash Partitioning
```sql
-- Evenly distribute rows by hashing a key
-- Good for: high-cardinality keys (user_id, order_id), even distribution needed

-- Spark example:
df.repartition(200, col("user_id"))  # 200 partitions, evenly distributed by user_id
# All rows with same user_id land in same partition (local aggregation possible)
```

### List Partitioning
```sql
-- Partition by discrete values
-- Good for: region, country, status

PARTITION BY LIST (region)
PARTITION us VALUES ('US', 'CA', 'MX')
PARTITION eu VALUES ('DE', 'FR', 'UK', 'IT')
PARTITION apac VALUES ('JP', 'AU', 'SG', 'IN')
```

---

## Data Skew — The #1 Scalability Problem

Skew = uneven data distribution: some partitions are 100× larger than others.

```python
# Diagnosing skew in Spark
df.groupBy("country").count().orderBy("count", ascending=False).show(20)
# If US has 80% of rows and other countries have <1%: SKEWED

# Symptoms:
# - Most tasks finish in 10 seconds, 1-2 tasks take 10 minutes
# - Spark UI: one executor at 100% CPU, others idle
# - OOM errors on specific executors

# Fix 1: Salting (add random prefix to distribute)
import random
df_salted = df.withColumn("user_id_salted",
    concat(col("user_id"), lit("_"), (rand() * 10).cast("int")))
df_salted.repartition(200, col("user_id_salted"))
# After aggregation: group by original user_id and re-aggregate

# Fix 2: Adaptive Query Execution (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# AQE automatically splits skewed partitions at runtime
```

---

## Partitioning Best Practices

| Principle | Explanation | Example |
|---|---|---|
| **Partition by query pattern** | Partition on the column most filtered in queries | `order_date` if most queries filter by date |
| **Avoid high-cardinality partitioning** | Too many small files = metadata overhead | Don't partition by `order_id` (millions of partitions) |
| **Target partition size** | 128MB–1GB per partition in Spark | Too small = overhead; too large = OOM |
| **Avoid over-partitioning** | Hive/S3: 10K+ partitions → slow metadata ops | Coarser partitioning (month not day) for small data |
| **Composite partitioning** | Partition by date + region for very large tables | `(order_date, region)` reduces scan to hours |

---

## Interview Tips

> **Tip 1:** "What is the difference between partitioning and sharding?" — Partitioning = splitting a table into smaller pieces within one database/system (logical separation). Sharding = distributing data across multiple independent databases/servers (physical separation). Partitioning is transparent (optimizer handles routing); sharding requires application-level routing logic. Partitioning for query optimization; sharding for write scalability beyond single-server limits.

> **Tip 2:** "How do you choose partition keys?" — Match the partition key to the most common query filter. For time-series data: partition by date/month. For multi-tenant SaaS: partition by tenant_id. Partition key must have low-to-medium cardinality (not too many partitions) and should provide roughly equal distribution (avoid hot partitions). The goal: a query should touch as few partitions as possible.

> **Tip 3:** "What is data skew and how do you detect it?" — Skew = some partitions/tasks get disproportionately large input. Detect in Spark: check the Spark UI Stages tab — if max task duration >> median task duration, you have skew. Also check `df.groupBy(partition_key).count()` to see distribution. Fix: salting (add random suffix to keys), broadcast joins (broadcast small table to avoid shuffle), or use Spark AQE's built-in skew join handling.
