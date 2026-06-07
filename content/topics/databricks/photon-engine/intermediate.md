---
title: "Photon Engine - Intermediate"
topic: databricks
subtopic: photon-engine
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, photon, optimization, fallback, query-plans, tuning]
---

# Photon Engine — Intermediate

## Understanding Photon Execution Plans

```sql
-- Photon operators vs Spark operators in query plans:
EXPLAIN FORMATTED
SELECT region, product_category, SUM(amount) AS revenue, COUNT(*) AS orders
FROM production.silver.orders o
JOIN production.silver.customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01'
GROUP BY region, product_category;

-- PHOTON plan shows:
-- PhotonScan (Delta) ← native C++ file reader
-- PhotonFilter ← vectorized predicate evaluation
-- PhotonBroadcastHashJoin ← native hash join
-- PhotonGroupingAgg ← vectorized aggregation
-- PhotonShuffleExchange ← optimized serialization

-- If you see regular Spark operators (HashAggregate, BroadcastHashJoin without "Photon"):
-- → Photon fell back for that operator (check why)
```

### Why Photon Falls Back to Spark

```python
# Common reasons for Photon fallback:

# 1. Python/Scala UDFs (Photon can't execute arbitrary code)
@udf("string")
def custom_parse(x): return x.split("|")[0]
# Fix: rewrite with native functions: split(col("x"), "\\|")[0]

# 2. Unsupported data types (rare, e.g., complex nested arrays)
# Fix: flatten nested types before aggregation

# 3. Unsupported operations (very rare in recent versions)
# Photon coverage grows with each Databricks Runtime version
# Most SQL operations are supported in 14.x+

# 4. Small data / trivial queries
# Photon has startup overhead — for <1000 rows, Spark may be faster
# Not a concern for production ETL (data is always large)

# CHECK for fallback in Spark UI:
# Stages tab → look for operators without "Photon" prefix
# Or: query metrics show "photon_fallback_count" > 0
```

---

## Photon-Optimized Patterns

### String Operations (Biggest Photon Win)

```python
# String operations benefit MOST from Photon (6-8x speedup):

# These run at native C++ speed with Photon:
df = (spark.table("production.silver.logs")
    .withColumn("host", split(col("url"), "/")[2])
    .withColumn("path", regexp_extract(col("url"), r"/([^?]+)", 1))
    .withColumn("domain", lower(trim(col("email_domain"))))
    .filter(col("url").contains("api/v2"))
    .filter(col("user_agent").rlike("(?i)chrome|firefox|safari"))
)

# JVM (traditional Spark): String objects → GC pressure → slow
# Photon (C++): raw byte arrays → zero GC → 6-8x faster for string-heavy ETL
```

### Aggregation Patterns

```python
# Multi-level aggregations benefit significantly:

# Single-level GROUP BY (3-4x speedup):
df.groupBy("region", "category").agg(
    count("*").alias("orders"),
    sum("amount").alias("revenue"),
    avg("amount").alias("aov"),
    countDistinct("customer_id").alias("unique_customers"),
)

# Window functions (2-3x speedup):
from pyspark.sql.window import Window
w = Window.partitionBy("customer_id").orderBy("order_date")
df.withColumn("running_total", sum("amount").over(w))

# CUBE/ROLLUP (3-4x speedup on multi-dimensional aggregations):
df.cube("region", "category", "quarter").agg(sum("revenue"))
```

### Delta DML Operations

```sql
-- MERGE is significantly faster with Photon:
MERGE INTO production.silver.customers t
USING staging.updates s ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
-- Photon: vectorized probe + vectorized write = 2-4x faster

-- OPTIMIZE with Z-ORDER:
OPTIMIZE production.silver.orders ZORDER BY (customer_id, order_date);
-- Photon: faster file reading + sorting + writing = 2x faster compaction

-- DELETE with complex predicates:
DELETE FROM production.silver.events
WHERE event_date < '2023-01-01' AND event_type IN ('debug', 'trace');
-- Photon: vectorized scan + filter = faster identification of rows to delete
```

---

## Photon Memory Management

```python
# Photon uses off-heap memory (NOT JVM heap):
# - No garbage collection pauses (C++ manages its own memory)
# - More efficient memory usage (columnar, compressed in-memory)
# - Spill to disk when memory is full (using local NVMe)

# Memory allocation for Photon:
spark.conf.set("spark.databricks.photon.memorySize", "auto")
# Photon automatically claims a portion of executor memory
# Default: uses ~50% of executor off-heap memory

# If Photon runs out of memory: it spills to local disk gracefully
# (unlike JVM OOM which crashes the executor)

# Recommendation: use i3 instances (NVMe SSD for fast spill)
# Photon + i3 = native execution + fast local storage = optimal performance
```

---

## Comparing Photon Across Workload Types

```python
# Run the same workload on Standard vs Photon and compare:

def benchmark_photon(query: str, table: str):
    """Compare execution time and cost between Standard and Photon."""
    import time
    
    # Standard runtime (configured separately)
    start = time.time()
    spark.sql(query)
    standard_time = time.time() - start
    
    # Photon runtime (current cluster)
    start = time.time()
    spark.sql(query)
    photon_time = time.time() - start
    
    speedup = standard_time / photon_time
    
    # Cost comparison (accounting for DBU rate difference)
    standard_cost = standard_time / 3600 * 8 * 0.15  # 8 workers × standard rate
    photon_cost = photon_time / 3600 * 8 * 0.20      # 8 workers × photon rate
    
    return {
        "standard_seconds": standard_time,
        "photon_seconds": photon_time,
        "speedup": f"{speedup:.1f}x",
        "standard_cost": f"${standard_cost:.3f}",
        "photon_cost": f"${photon_cost:.3f}",
        "net_savings": f"{(1 - photon_cost/standard_cost) * 100:.0f}%",
    }

# Typical results by workload type:
# Scan + filter: 3.5x faster, 40% cheaper
# Joins (large × small): 2.8x faster, 35% cheaper  
# GroupBy aggregation: 3.2x faster, 38% cheaper
# String parsing: 6.0x faster, 55% cheaper
# Window functions: 2.5x faster, 30% cheaper
# MERGE (upsert): 3.0x faster, 37% cheaper
```

---

## Photon with Adaptive Query Execution (AQE)

```python
# Photon works alongside AQE (both enabled by default in Photon runtime):
# AQE optimizes the PLAN (join strategy, partition count)
# Photon optimizes the EXECUTION (vectorized processing)
# Together: optimal plan + fastest execution = best performance

# AQE features that complement Photon:
# 1. Coalesce shuffle partitions: fewer tasks = less overhead per task
# 2. Convert sort-merge to broadcast: smaller shuffle = Photon processes less data
# 3. Skew join optimization: balanced partitions = Photon processes evenly

# Both are on by default in Photon runtime — no configuration needed:
spark.conf.set("spark.sql.adaptive.enabled", "true")  # Default: true
spark.conf.set("spark.databricks.photon.enabled", "true")  # Default: true with Photon runtime
```

---

## Photon for Delta Lake Operations

```sql
-- Photon accelerates ALL Delta operations:

-- 1. OPTIMIZE (file compaction): 2x faster
OPTIMIZE production.silver.orders;
-- Photon reads old files + writes new compacted files using native C++ I/O

-- 2. Z-ORDER: 2-3x faster
OPTIMIZE production.silver.orders ZORDER BY (customer_id);
-- Sorting phase is vectorized (Photon sort is 2-3x faster)

-- 3. VACUUM: 1.5-2x faster
VACUUM production.silver.orders RETAIN 7 DAYS;
-- File listing + deletion is I/O bound, slight Photon improvement

-- 4. Data skipping (file pruning): slightly faster stats evaluation
-- Photon evaluates min/max stats faster during query planning

-- RECOMMENDATION: Always use Photon for Delta table maintenance
-- Your OPTIMIZE jobs finish in half the time = less cluster cost
```

---

## Interview Tips

> **Tip 1:** "How do you verify Photon is actually being used?" — Check the query plan (EXPLAIN): look for operators with "Photon" prefix (PhotonScan, PhotonGroupingAgg, PhotonBroadcastHashJoin). In Spark UI: check for Photon-specific metrics in stage details. If operators show regular Spark names → Photon fell back (usually due to UDFs or unsupported operations).

> **Tip 2:** "When would Photon fall back to Spark?" — Python/Scala UDFs (Photon can't execute arbitrary user code), unsupported data types (very rare), or very small data sets (Photon startup overhead exceeds benefit). Fix: replace UDFs with native Spark SQL functions. In 14.x+, Photon covers nearly all SQL operations.

> **Tip 3:** "Photon memory model?" — Photon uses off-heap memory (C++ managed, not JVM heap). No garbage collection pauses. If memory exceeds allocation, Photon spills to local disk gracefully (i3 instances with NVMe recommended). This means Photon avoids the GC-pause problem that plagues large JVM-based Spark jobs.
