---
title: "Spark Performance & Tuning — Senior Deep Dive"
topic: spark
subtopic: performance-and-tuning
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [spark, performance, cost-model, io-coalescing, dynamic-pruning, runtime-filter, z-order, liquid-clustering]
---

# Spark Performance & Tuning — Senior Deep Dive

## Dynamic Partition Pruning (DPP)

DPP (Spark 3.0+) extends static partition pruning to work with joins — filtering one side based on the actual values on the other side:

```python
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

# Example: fact-dim join where dim is filtered
orders = spark.read.parquet("s3://bucket/orders/")    # partitioned by region
regions = spark.read.parquet("s3://bucket/dim_region/")

# Without DPP: full scan of orders
result = orders.join(regions.filter("continent = 'EU'"), "region_id")

# With DPP: Spark first evaluates the dim_region filter → gets {'DE', 'FR', 'IT'}
# Then prunes orders scan to only read region_id IN ('DE', 'FR', 'IT')
```

**How DPP works internally:**
```
1. Evaluate filtered dimension (small side of star join)
2. Build a broadcast for values: {'DE', 'FR', 'IT'}  ← re-use of BroadcastHashJoin
3. Inject this as a filter on the fact table scan
4. Storage-level: if fact is partitioned by region_id, actual directories are skipped
```

DPP activates automatically when:
- Join has a dimension filter on the probe side
- Fact table is partitioned by the join key
- The broadcast threshold is not exceeded on the dimension side

---

## Runtime Bloom Filters

Bloom filters extend DPP to non-partitioned data — files are skipped via probabilistic membership:

```python
spark.conf.set("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
spark.conf.set(
    "spark.sql.optimizer.runtime.bloomFilter.applicationSideScanSizeThreshold",
    "10mb")  # only inject bloom filter if scan > 10MB

# Effect:
# Before join: build bloom filter from customer_id values in filtered customers
# At scan time for orders: test each order's customer_id against bloom filter
# False negative impossible; ~1% false positive rate → skip ~99% of non-matching rows
# Network savings: massive (skip data before it leaves storage layer)
```

---

## I/O Coalescing and File Merging

Too many small files is one of the most common production Spark performance killers:

```python
# Problem: 10K small 1MB Parquet files → 10K tasks, each taking 50ms setup + 1ms work
# 10K × 51ms = 510 seconds just for task overhead!

# Fix 1: Coalesce before writing
df.coalesce(40).write.parquet("output/")   # 40 files × 250MB each

# Fix 2: spark.sql.files.maxPartitionBytes
# How large Spark tries to make each input partition by merging small files
spark.conf.set("spark.sql.files.maxPartitionBytes", str(256 * 1024 * 1024))  # 256 MB
spark.conf.set("spark.sql.files.openCostInBytes", str(4 * 1024 * 1024))       # 4MB per file overhead

# After maxPartitionBytes, small files are coalesced into larger input tasks
# Before: 10K files → 10K tasks (too many)
# After: 10K files coalesced → 400 tasks (256 MB each)

# Fix 3: Delta Lake OPTIMIZE — compact small files into large ones
spark.sql("OPTIMIZE delta_table ZORDER BY (customer_id)")

# Fix 4: Iceberg rewrite
spark.sql("""
    CALL catalog.system.rewrite_data_files(
        table => 'db.orders',
        strategy => 'sort',
        sort_order => 'customer_id ASC NULLS LAST'
    )
""")
```

---

## Nested Data and Semi-Structured Performance

```python
# Problem: deeply nested JSON in Parquet is read as binary → full deserialization
# Each nested access deserializes the full struct

# WRONG: repeatedly access nested struct
df.withColumn("city", F.col("address.city")) \
    .withColumn("state", F.col("address.state")) \
    .withColumn("zip", F.col("address.zip"))
# address struct deserialized 3 times!

# RIGHT: extract all nested fields at once
addr = F.col("address")
df.select(
    F.col("customer_id"),
    addr.getField("city").alias("city"),
    addr.getField("state").alias("state"),
    addr.getField("zip").alias("zip"),
)

# Or flatten the schema upstream:
# Write flat Parquet instead of nested JSON → much faster reads
flat_df = df.select(
    "customer_id",
    F.col("address.city").alias("addr_city"),
    F.col("address.state").alias("addr_state"),
)
```

---

## Cost Model Internals: How Spark Decides Join Strategy

```
Decision tree:
  1. Is either side <= autoBroadcastJoinThreshold?
     YES → BroadcastHashJoin
  2. Is spark.sql.join.preferSortMergeJoin enabled?
     YES → SortMergeJoin
  3. Can the smaller side fit in executor memory?
     YES → ShuffleHashJoin
  4. Fallback → SortMergeJoin

With AQE:
  After shuffle, if actual smaller side <= broadcastThreshold → switch to BHJ
  (overrides static plan — plan can change at runtime!)
```

```python
# Force Spark to show you the cost model decisions:
spark.conf.set("spark.sql.cbo.enabled", "true")
df.explain(mode="cost")
# Each plan node shows estimated: rowCount, sizeInBytes, isBroadcastable
```

---

## Liquid Clustering (Delta Lake 3.1+ / Databricks)

Z-ordering computes static clusters; Liquid Clustering uses online clustering with incremental updates:

```sql
-- Enable liquid clustering
CREATE TABLE orders (
    order_id STRING,
    customer_id STRING,
    amount DOUBLE,
    order_date DATE
) USING DELTA
CLUSTER BY (customer_id, order_date);

-- Incremental clustering (only processes new/changed data)
OPTIMIZE orders;   -- uses liquid clustering automatically

-- No need to ZORDER — clustering columns defined at table creation
-- Automatically re-clusters as data is written
```

Benefits over ZORDER:
- Incremental (no full table rewrite for each OPTIMIZE)
- Handles clustering column changes without full rewrite
- Works well with frequent small writes (streaming)

---

## Interview Tips

> **Tip 1:** "Explain Dynamic Partition Pruning and when it activates." — DPP evaluates the dimension side of a star-schema join first, collects the result as a broadcast, and injects it as a storage-level filter on the fact table scan. It activates when: the join has a filter on the dimension side, the fact table is partitioned by the join key, and the dimension side qualifies for broadcast. Result: Spark skips irrelevant partitions in the fact table before reading any data — similar to static partition pruning but dynamically populated at runtime.

> **Tip 2:** "What is the impact of too many small files and how do you address it?" — Small files create two problems: (1) Task overhead dominates — Spark launches one task per input partition, and with 1MB files each task spends 50× more on setup than useful work. (2) Driver metadata overhead — listing millions of files can take minutes and OOM the Driver. Solutions: coalesce on write (most immediate), `maxPartitionBytes` for automatic coalescing on read, Delta OPTIMIZE for compaction, or Iceberg rewrite_data_files.

> **Tip 3:** "Walk me through how Spark decides which join algorithm to use." — Static analysis first: if either side is under `autoBroadcastJoinThreshold` (10MB), use BroadcastHashJoin. Otherwise, if `preferSortMergeJoin=true` (default), use SortMergeJoin (two shuffles + sort). ShuffleHashJoin is used when the smaller side can fit in executor memory without sort. With AQE enabled, the static choice can be revised at runtime: if the actual shuffle output is small enough after measuring, SMJ is downgraded to BHJ — no shuffle needed.
