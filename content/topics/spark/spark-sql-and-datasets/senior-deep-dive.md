---
title: "Spark SQL & Datasets — Senior Deep Dive"
topic: spark
subtopic: spark-sql-and-datasets
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [spark, sql, query-hints, adaptive-query-execution, z-ordering, bloom-filter, runtime-stats]
---

# Spark SQL & Datasets — Senior Deep Dive

## Query Hints

Force Spark to use specific join strategies:

```sql
-- Broadcast a specific table (override auto-broadcast threshold)
SELECT /*+ BROADCAST(small_table) */ *
FROM large_table l
JOIN small_table s ON l.key = s.key

-- Sort-merge join (when both sides are large and co-partitioned)
SELECT /*+ MERGE(orders, customers) */ *
FROM orders o
JOIN customers c ON o.customer_id = c.id

-- Shuffle-hash join (hash one side, stream the other)
SELECT /*+ SHUFFLE_HASH(orders) */ *
FROM orders o
JOIN customers c ON o.customer_id = c.id

-- Repartition before write (control output file count)
SELECT /*+ REPARTITION(50) */ * FROM orders

-- Coalesce (reduce partitions without full shuffle)
SELECT /*+ COALESCE(10) */ * FROM orders
```

```python
# DataFrame API hints:
from pyspark.sql.functions import broadcast
orders.join(broadcast(small_dim), "key")

# Hint API (Spark 2.2+):
orders.hint("broadcast").join(small_dim, "key")
orders.hint("merge", "key").join(customers, "key")
orders.hint("shuffle_hash").join(customers, "key")
```

---

## Adaptive Query Execution (AQE) Deep Dive

AQE (Spark 3.0+, default on in 3.2+) re-optimizes plans using actual runtime statistics:

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")

# 1. Partition coalescing (post-shuffle)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "64mb")
spark.conf.set("spark.sql.adaptive.coalescePartitions.initialPartitionNum", "500")
# Effect: after shuffle, tiny partitions merged → fewer, larger tasks

# 2. Skew join handling
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")  # 5× median
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb")
# Effect: skewed partitions split and joined with duplicated non-skewed data

# 3. Join strategy switching
# AQE can downgrade SortMergeJoin → BroadcastHashJoin if one side turns out small
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", "true")
```

**What AQE cannot fix:**
- Cartesian products (always full cross-join)
- Very deeply nested subqueries (plan rewrite limited)
- UDFs — opaque to optimizer
- Skew where key is not joinable (must still read all data)

---

## Statistics and Cost-Based Optimization

```sql
-- Compute table stats for CBO
ANALYZE TABLE orders COMPUTE STATISTICS;
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS customer_id, amount, status;

-- View stats
DESCRIBE TABLE EXTENDED orders;
-- Look for: Statistics, sizeInBytes, rowCount
```

```python
# Enable CBO
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")   # reorder multi-way joins
spark.conf.set("spark.sql.cbo.planStats.enabled", "true")     # propagate stats through plan

# When CBO helps most:
# - Multi-way joins (3+ tables) — CBO reorders to minimize intermediate results
# - After ANALYZE TABLE — without stats, Spark assumes worst-case sizes
# - Broadcast decision — CBO uses actual size estimate to decide threshold
```

---

## Bloom Filter Join (Spark 3.0+)

For joins where one side is large and many rows don't match, a bloom filter pre-filters non-matching rows:

```python
spark.conf.set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
spark.conf.set("spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold", "10mb")

# Example: joining 100M orders against 1M customer IDs
# Bloom filter built on customer_id set from customers table
# Orders side filters out non-matching customer_ids BEFORE network transfer
# Typical speedup: 2-5× on large selective joins
```

---

## Z-Ordering and Data Skipping (Delta Lake)

When combined with Delta Lake, Spark SQL can skip entire file groups:

```python
# Z-order by common filter columns
spark.sql("OPTIMIZE orders ZORDER BY (customer_id, order_date)")

# After Z-ordering, queries like this skip 90%+ of files:
spark.sql("""
    SELECT * FROM orders
    WHERE customer_id = 'C12345'
      AND order_date BETWEEN '2024-01-01' AND '2024-03-31'
""")

# Min/max statistics per file:
# Delta writes file-level min/max for each column
# Spark checks: does this file's max(customer_id) >= 'C12345' AND min <= 'C12345'?
# Files where condition is impossible are skipped entirely
```

---

## Query Plan Reading: Expert Level

```python
df.explain(mode="formatted")
# Sections:
# == Parsed Logical Plan ==      raw AST from your code
# == Analyzed Logical Plan ==    column names/types resolved
# == Optimized Logical Plan ==   after Catalyst rule-based optimization
# == Physical Plan ==            actual execution plan

# Key nodes to understand:
# BroadcastHashJoin   — small table broadcast, no shuffle
# SortMergeJoin       — both sides sorted, merged; requires shuffle
# BroadcastNestedLoopJoin — non-equi join; extremely slow for large inputs
# ShuffleExchange     — repartition / shuffle boundary
# Sort                — sorting (may be pre-sort for SortMergeJoin)
# HashAggregate       — two-phase: partial hash agg + final merge
# ObjectHashAggregate — used when partial aggregation is not possible
# *(N)                — enclosed in whole-stage codegen stage N
# +- AdaptiveSparkPlan — AQE wrapper; shows isFinalPlan=true when done

# Find unexpected operations:
# BroadcastNestedLoopJoin  → missing equi-join condition? Check your ON clause
# CartesianProduct         → cross join — intentional?
# Sort at top of plan      → avoidable? Does downstream need ordering?
```

---

## Data Source V2 API

Spark 3.x's Data Source V2 enables pushdown-aware connectors:

```python
# Modern connectors (Delta Lake, Iceberg, Hudi) use DSv2
# They implement:
# - SupportsPushDownFilters  → filter pushed to storage layer
# - SupportsPushDownProjection → only required columns fetched
# - SupportsReportPartitioning → tell Spark partitioning to avoid shuffle

df = spark.read.format("iceberg").load("catalog.db.orders")
df.filter("region = 'US'").select("order_id", "amount").explain()
# Physical plan shows filters/projections inside IcebergScan
# Not re-applied by Spark — handled by Iceberg reader
```

---

## Interview Tips

> **Tip 1:** "What does AQE do for skewed joins?" — At runtime, AQE measures partition sizes after the shuffle. If any partition exceeds the skew threshold (5× median and >256 MB by default), it splits that partition into smaller pieces and duplicates the corresponding partition from the other side to match. The skewed partition is joined in parallel sub-tasks. This is automatic from Spark 3.0 — you don't need salt keys unless skew is extreme or you're on an older Spark version.

> **Tip 2:** "When would a join use BroadcastNestedLoopJoin and why is that bad?" — BroadcastNestedLoopJoin is used for non-equi joins (joins without `=` condition, e.g., `a.date BETWEEN b.start AND b.end`). It broadcasts one side and does a nested loop scan — O(N×M) comparisons. For a 10M × 1M join that's 10 trillion comparisons. If you see this in a plan unexpectedly, check for implicit cross joins (missing ON clause) or reformulate the join as an equi-join with an inequality post-filter.

> **Tip 3:** "How does Spark's Bloom filter join optimization work?" — When enabled, Spark builds a probabilistic Bloom filter on the join keys from the smaller side, then applies it as a filter on the larger side before the shuffle. Rows that definitely don't match are dropped early — before any data movement. It's not a new join type; it's a pre-filter that reduces shuffle volume. Most effective when the join is highly selective (e.g., joining 100M rows against 10K matching keys).
