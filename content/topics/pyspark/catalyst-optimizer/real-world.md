---
title: "PySpark Catalyst Optimizer - Real-World Production Examples"
topic: pyspark
subtopic: catalyst-optimizer
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [pyspark, catalyst, optimizer, production, predicate-pushdown]
---

# PySpark Catalyst Optimizer — Real-World Production Examples

## Case Study 1: Predicate Pushdown Saving 90% of Reads

**Problem:** Query reads 10 TB from S3 but only needs 1 day's data.

```python
# Without pushdown awareness (Catalyst can't optimize):
df = spark.read.parquet("s3://lake/events/")  # 10 TB, all partitions
result = df.filter("event_date = '2024-01-15'").select("user_id", "event_type")
# Does Catalyst push the filter to the Parquet reader? YES — for partitioned data!

# Verify pushdown in physical plan:
result.explain()
# == Physical Plan ==
# FileScan parquet [user_id, event_type, event_date]
#   PushedFilters: [EqualTo(event_date, 2024-01-15)]  ← PUSHED DOWN!
#   PartitionFilters: [event_date = 2024-01-15]        ← PARTITION PRUNING!
#   ReadSchema: struct<user_id:string,event_type:string>  ← COLUMN PRUNING!

# Result: reads only ~30 GB (one day) instead of 10 TB. 99.7% data skipped!
```

**When pushdown DOESN'T work:**

```python
# UDF blocks pushdown (Catalyst can't reason about Python code)
from pyspark.sql.functions import udf
is_target_date = udf(lambda d: d == "2024-01-15")
df.filter(is_target_date(col("event_date")))
# Plan shows: NO PushedFilters! Full 10 TB scan.

# FIX: Use native Spark expression instead of UDF
df.filter(col("event_date") == "2024-01-15")  # Pushdown works!
```

---

## Case Study 2: Column Pruning on Wide Tables

**Problem:** Table has 100 columns. Query uses 5. Without Catalyst, all 100 columns are read.

```python
# Wide table: 100 columns, 500 GB total
wide_df = spark.read.parquet("s3://lake/customer_360/")

# Query needs only 5 columns
result = wide_df.select("customer_id", "name", "segment", "last_order_date", "ltv") \
    .filter("segment = 'Gold'")

result.explain()
# ReadSchema: struct<customer_id,name,segment,last_order_date,ltv>
# Only these 5 columns are read from Parquet! (5/100 = 5% of data)
# 500 GB × 5% = 25 GB actually read from S3
```

**Why this matters:** Parquet is columnar — each column is stored in separate column chunks. Spark only reads the column chunks it needs. With 100 columns, reading only 5 = 95% less I/O.

---

## Case Study 3: Join Reordering Fix

**Problem:** Three-table join runs 10x slower after someone reordered the SQL.

```python
# SLOW order (Catalyst may not reorder in all cases):
# Join 1B-row fact with 1B-row fact first, then tiny dim
result = fact_a.join(fact_b, "key").join(broadcast(dim_small), "category")
# Intermediate: 1B × 1B join produces massive result before filtering with dim

# FAST order (Catalyst should pick this, but let's verify):
# Filter with small dim first (reduces data), then join facts
result = fact_a.join(broadcast(dim_small), "category") \
    .join(fact_b, "key")
# Small dim join (broadcast, instant) filters fact_a down by 80%
# Then joining the filtered fact_a (200M rows) with fact_b is much cheaper

# Check what Catalyst chose:
result.explain()
# Look at join order in the physical plan — should show broadcast first
```

**If Catalyst gets join order wrong:**
```python
# Force order with explicit join hints
result = fact_a.hint("merge").join(fact_b, "key")  # Force sort-merge
# Or rewrite as CTEs/subqueries to guide the optimizer
```

---

## Case Study 4: When Catalyst Fails (UDFs)

```python
# Catalyst optimization is BLOCKED by UDFs:

# 1. Predicate pushdown blocked
df.filter(my_udf(col("amount")) > 100)  # Can't push to source

# 2. Column pruning may be blocked
df.withColumn("result", my_udf(col("col1"), col("col2"), ...col("col50")))
# UDF references 50 columns → all 50 must be read (even if downstream uses only "result")

# 3. Constant folding blocked
df.withColumn("x", my_udf(lit(42)))  # Catalyst can't evaluate at compile time

# FIXES: Replace UDFs with native expressions where possible
# Common UDF → native replacements:
# - String parsing UDF → regexp_extract()
# - Date formatting UDF → date_format()
# - Conditional logic UDF → when().otherwise()
# - Math UDF → built-in math functions
# - JSON parsing UDF → from_json(), get_json_object()
```

---

## Verifying Catalyst Optimizations

```python
# Method 1: explain() with different modes
df.explain("simple")     # Physical plan only
df.explain("extended")   # Logical + Physical plans
df.explain("cost")       # With cost estimates
df.explain("formatted")  # Most readable

# Method 2: Check specific optimizations
# Look for in the plan:
# "PushedFilters:" → predicate pushed to source
# "ReadSchema:" → only needed columns listed (column pruning)
# "BroadcastHashJoin" → small table broadcast
# "PartitionFilters:" → partition pruning active

# Method 3: Compare optimized vs unoptimized
spark.conf.set("spark.sql.optimizer.excludedRules", "")  # Normal (all rules)
df.explain()  # Optimized plan

spark.conf.set("spark.sql.optimizer.excludedRules", 
    "org.apache.spark.sql.catalyst.optimizer.PushDownPredicates")
df.explain()  # Without pushdown — compare to see the difference
```

---

## Catalyst Optimization Checklist for Production

| Optimization | Verify By | Impact |
|-------------|-----------|--------|
| Predicate pushdown | `PushedFilters` in plan | 10-100x less I/O |
| Column pruning | `ReadSchema` shows only needed cols | 2-20x less I/O |
| Partition pruning | `PartitionFilters` in plan | 10-100x less data |
| Broadcast detection | `BroadcastHashJoin` in plan | Eliminates shuffle |
| Constant folding | Literals pre-computed in plan | Negligible |
| Filter before join | Filter appears below join in plan | 2-10x less shuffle |

---

## Interview Tips

> **Tip 1:** "What optimizations does Catalyst apply?" — "Four major ones: (1) Predicate pushdown (filters pushed to data source, skip irrelevant rows at I/O level). (2) Column pruning (only read needed columns from columnar storage). (3) Constant folding (evaluate constants at compile time). (4) Join optimization (choose broadcast vs sort-merge, reorder joins). Together these can reduce a 10 TB scan to 30 GB."

> **Tip 2:** "Why do UDFs hurt performance?" — "UDFs are opaque to Catalyst — it can't see inside them to push predicates, prune columns, or fold constants. Additionally, Python UDFs add serialization overhead (JVM → Python → JVM per row). Always prefer native Spark functions. If you must use a UDF, apply filters BEFORE the UDF (manually do what Catalyst would do automatically)."

> **Tip 3:** "How do you verify Catalyst is optimizing your query?" — "Call `df.explain('formatted')` and check for: PushedFilters (predicates pushed to source), ReadSchema (columns pruned), PartitionFilters (partitions pruned), and BroadcastHashJoin (small table detected). If any expected optimization is missing: check for UDFs blocking it, or verify your data source supports pushdown."
