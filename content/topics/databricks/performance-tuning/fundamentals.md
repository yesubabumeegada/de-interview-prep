---
title: "Performance Tuning - Fundamentals"
topic: databricks
subtopic: performance-tuning
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [databricks, spark, performance, optimization, partitioning, caching, aqe]
---

# Performance Tuning — Fundamentals

## 🎯 Analogy

Spark performance tuning is like optimizing a restaurant kitchen. Caching is having ingredients prepped (mise en place). Partitioning is assigning each chef to one station so they don't interfere. Broadcast joins are like posting the menu on the wall so each chef doesn't have to run to the office to check it. Spill to disk is when you run out of counter space and have to use the floor — slow, and means you need a bigger kitchen.

---

## Key Performance Concepts

| Problem | Symptom | Solution |
|---------|---------|---------|
| **Data skew** | Some tasks take 10x longer than others | Salt skewed keys, repartition |
| **Shuffle overhead** | Stage with many tasks is slow | Reduce shuffle with joins, AQE |
| **Spill to disk** | Tasks write to disk during shuffle | Increase executor memory or reduce partition size |
| **Small files** | Many tiny files, slow reads | Compact with OPTIMIZE |
| **Full table scan** | Reading 100GB to get 1MB | Add partition pruning, Z-ordering |

---

## Adaptive Query Execution (AQE)

AQE is Spark's runtime optimizer — enabled by default in Databricks. It re-optimizes plans based on actual data statistics:

```python
# AQE is on by default in Databricks
# But you can configure it
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")  # merge small partitions
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")            # handle skewed joins

# AQE does three things automatically:
# 1. Coalesces small shuffle partitions (1000 tiny → 50 normal)
# 2. Switches sort-merge joins to broadcast joins when table is small enough
# 3. Splits skewed partitions in joins
```

---

## Broadcast Joins

When one table is small, broadcast it to all executors — eliminates shuffle:

```python
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

# Without broadcast: huge shuffle (both tables sent to same nodes)
result = large_orders.join(small_country_codes, on="country_code")

# With broadcast: small table sent to each executor, no shuffle
result = large_orders.join(broadcast(small_country_codes), on="country_code")

# Or set threshold — tables under this size auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50mb")  # default: 10mb
```

**When to broadcast:** Table fits in memory of each executor (typically < 200MB).

---

## Partitioning Basics

```python
# Check current number of partitions
print(df.rdd.getNumPartitions())

# Repartition — full shuffle, creates N evenly-sized partitions
df_repartitioned = df.repartition(200)

# Coalesce — reduces partitions without full shuffle (only useful for reducing)
df_smaller = df.coalesce(50)

# Repartition by column — puts all rows with same key in same partition
# Best for joins and group-by on that column
df_by_date = df.repartition("event_date")

# Rule of thumb: aim for 128-256MB per partition
# Total data size / target partition size = number of partitions
# E.g., 100GB data / 200MB per partition = 500 partitions
```

---

## Caching

Cache frequently-reused DataFrames in memory:

```python
# Cache in memory (default)
df_filtered = df.filter("status = 'active'").cache()

# Force materialization (cache is lazy without an action)
df_filtered.count()

# Or use persist() for more control
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_AND_DISK)  # spill to disk if memory full
df.persist(StorageLevel.DISK_ONLY)         # don't use memory at all

# Always unpersist when done — free up memory for other jobs
df_filtered.unpersist()

# Cache a Delta table (Databricks disk cache — different from Spark cache)
spark.conf.set("spark.databricks.io.cache.enabled", "true")
# Databricks IO cache persists across sessions on the same cluster nodes
```

---

## Reading Query Plans

```python
# See the physical plan before running
df.explain()          # readable
df.explain("cost")    # with cost estimates
df.explain("codegen") # with generated code

# Look for:
# - FileScan with pushed filters (good: partition pruning)
# - BroadcastHashJoin (good: no shuffle)
# - SortMergeJoin (watch: large shuffle)
# - Exchange (shuffle) with high partition count (bad: 50,000 tiny partitions)
```

---

## Delta Lake Optimization

```sql
-- OPTIMIZE: compact small files (run after batch loads)
OPTIMIZE prod.sales.orders;

-- OPTIMIZE with Z-ordering: co-locate rows with similar values for a column
-- Speeds up queries that filter on that column
OPTIMIZE prod.sales.orders ZORDER BY (customer_id, order_date);

-- VACUUM: remove old files (required for GDPR erasure, saves storage)
VACUUM prod.sales.orders RETAIN 168 HOURS;  -- keep 7 days of history

-- ANALYZE: update statistics so the optimizer has better information
ANALYZE TABLE prod.sales.orders COMPUTE STATISTICS FOR ALL COLUMNS;
```

---

## Interview Tips

> **Tip 1:** "What does AQE do and why is it important?" — "AQE (Adaptive Query Execution) re-optimizes Spark query plans at runtime using actual partition statistics rather than estimates. Three main benefits: (1) Coalesces tiny shuffle partitions into fewer larger ones — reduces task overhead. (2) Switches sort-merge joins to broadcast joins when a table turns out to be small enough. (3) Handles skewed joins by splitting the large partition."

> **Tip 2:** "When would you use repartition vs coalesce?" — "Use `repartition(N)` to increase OR evenly redistribute partitions — it does a full shuffle, producing N evenly-sized partitions. Use `coalesce(N)` only to reduce partitions — it avoids a full shuffle by merging local partitions. Coalesce is cheaper but can produce uneven partitions; use it when you just want fewer (not more balanced) partitions."

> **Tip 3:** "What is Z-ordering and when does it help?" — "Z-ordering sorts data within Delta files so that rows with similar values in the Z-ordered columns are physically co-located. This enables Delta's data skipping to skip more files when you query with a filter on those columns. Most effective on high-cardinality columns you frequently filter on — e.g., customer_id, order_date. Useless on low-cardinality columns (e.g., status with 3 values)."
