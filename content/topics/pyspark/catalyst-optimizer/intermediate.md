---
title: "PySpark Catalyst Optimizer - Intermediate"
topic: pyspark
subtopic: catalyst-optimizer
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [pyspark, catalyst, predicate-pushdown, column-pruning, constant-folding, filter-reordering, join-reordering]
---

# PySpark Catalyst Optimizer — Intermediate

## Predicate Pushdown in Detail

Predicate pushdown moves filters as close to the data source as possible, reducing data read and processed:

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("CatalystIntermediate").getOrCreate()

# Level 1: Push to storage engine (Parquet/ORC row group filtering)
df = spark.read.parquet("s3://data/events/")
filtered = df.filter(F.col("amount") > 100)
filtered.explain(mode="formatted")
# FileScan: PushedFilters: [GreaterThan(amount, 100)]
# Parquet skips entire row groups where max(amount) <= 100

# Level 2: Push through joins
result = (orders
    .join(customers, "customer_id")
    .filter(F.col("order_date") > "2024-01-01")  # Only applies to orders
    .filter(F.col("region") == "US"))             # Only applies to customers

result.explain()
# Catalyst pushes:
# - order_date filter → applied to orders BEFORE join
# - region filter → applied to customers BEFORE join

# Level 3: Push through projections
result = (df
    .select("user_id", "amount", "event_date")
    .filter(F.col("status") == "active"))  # References column not in select!
# Catalyst reads status for filtering, then drops it from output
```

### What Blocks Predicate Pushdown

```python
# UDFs block pushdown — optimizer can't analyze Python function
@F.udf("boolean")
def custom_filter(amount):
    return amount > 100

df.filter(custom_filter(F.col("amount"))).explain()
# Filter: UDF(amount) ← NOT pushed to FileScan

# Non-deterministic functions block pushdown
df.filter(F.rand() > 0.5).explain()
# Cannot push — results must be consistent

# Complex expressions on source columns may not push
df.filter(F.col("amount") + F.col("tax") > 100).explain()
# Depends on data source support

# Functions on partition columns block PARTITION pruning
df.filter(F.year(F.col("event_date")) == 2024).explain()
# No partition pruning! Use: F.col("event_date") >= "2024-01-01"
```

---

## Column Pruning

Catalyst only reads columns that are actually needed downstream:

```python
# Table has 50 columns, but we only use 3
result = (spark.read.parquet("s3://data/wide_table/")  # 50 columns
    .select("user_id", "event_type", "timestamp")       # Only need 3
    .filter(F.col("event_type") == "click"))

result.explain(mode="formatted")
# ReadSchema: struct<user_id:string,event_type:string,timestamp:long>
# Only 3 columns read from Parquet (columnar format makes this efficient)
```

### Column Pruning Through Operations

```python
# Pruning works through joins too
result = (large_table              # 50 columns
    .join(small_table, "key")       # 20 columns
    .select("key", "name", "value") # Only need 3 from the combined 70
)

# Catalyst reads:
# - large_table: only key + columns needed for final select
# - small_table: only key + columns needed for final select
# Not all 70 columns!

# Pruning through aggregations
result = (df
    .groupBy("department")
    .agg(F.avg("salary").alias("avg_salary")))
# Only reads: department, salary (ignores 48 other columns)
```

---

## Constant Folding

Catalyst evaluates constant expressions at compile time:

```python
# Before optimization:
df.withColumn("threshold", F.lit(100) * F.lit(1.1))
# After optimization: withColumn("threshold", lit(110.0))

# Date arithmetic
df.filter(F.col("event_date") > F.date_sub(F.current_date(), 30))
# Evaluates current_date() - 30 ONCE at plan time, not per row

# String concatenation of constants
df.withColumn("prefix", F.concat(F.lit("event_"), F.lit("v2_")))
# Becomes: lit("event_v2_")
```

---

## Filter Reordering

Catalyst reorders filters to evaluate the most selective (cheapest) ones first:

```python
# Your code:
df.filter(
    (expensive_udf(F.col("text")) == "positive") &  # Expensive
    (F.col("amount") > 100) &                        # Cheap, highly selective
    (F.col("status") == "active")                    # Cheap
)

# Catalyst reorders to:
# 1. F.col("status") == "active" — cheap, eliminates 80% of rows
# 2. F.col("amount") > 100 — cheap, eliminates 60% of remaining
# 3. expensive_udf() — expensive, but runs on much less data now!

# With CBO enabled and column statistics:
spark.conf.set("spark.sql.cbo.enabled", "true")
# Catalyst uses histogram stats to estimate selectivity
# and puts the most selective cheap filter first
```

---

## Join Reordering

For multi-way joins, the order matters significantly:

```python
# Tables: A(1M rows), B(100M rows), C(1000 rows), D(50M rows)

# Your code (left-to-right join):
result = A.join(B, "key_ab").join(C, "key_ac").join(D, "key_ad")

# Without CBO: Joins in the order you wrote them
# A ⋈ B → 100M intermediate rows
# (A ⋈ B) ⋈ C → still large
# Everything ⋈ D → massive

# With CBO (and table stats): Catalyst reorders
# A ⋈ C (smallest first) → small intermediate
# (A ⋈ C) ⋈ D → medium intermediate
# Everything ⋈ B (largest last)

# Enable join reordering
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")

# Compute statistics for reordering to work
spark.sql("ANALYZE TABLE A COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE B COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE C COMPUTE STATISTICS")
spark.sql("ANALYZE TABLE D COMPUTE STATISTICS")
```

---

## Optimization Verification Workflow

```python
def verify_optimizations(df, check_name="query"):
    """Verify that expected optimizations are applied."""
    plan = df._jdf.queryExecution().executedPlan().toString()
    
    checks = {
        "predicate_pushdown": "PushedFilters:" in plan or "PartitionFilters:" in plan,
        "broadcast_join": "BroadcastHashJoin" in plan or "BroadcastExchange" in plan,
        "whole_stage_codegen": "WholeStageCodegen" in plan or "*(" in plan,
        "no_cartesian": "CartesianProduct" not in plan and "BroadcastNestedLoopJoin" not in plan,
    }
    
    print(f"\n=== Optimization Check: {check_name} ===")
    for check, passed in checks.items():
        status = "PASS" if passed else "WARN"
        print(f"  [{status}] {check}")
    
    return checks

# Usage
result = orders.join(F.broadcast(products), "product_id").filter(F.col("amount") > 100)
verify_optimizations(result, "order_enrichment")
```

---

## Catalyst Rules You Should Know

| Rule | What It Does | Impact |
|------|-------------|--------|
| PushDownPredicate | Moves filters before joins/aggregations | Reduces data early |
| ColumnPruning | Removes unused columns from scans | Less I/O and memory |
| ConstantFolding | Pre-computes constant expressions | Eliminates runtime computation |
| BooleanSimplification | Removes redundant conditions | Cleaner plan |
| CombineFilters | Merges adjacent filter operations | Single filter scan |
| PushPredicateThroughJoin | Pushes join-independent filters | Filters before shuffle |
| ReorderJoin | Reorders multi-way joins | Smaller intermediates |
| EliminateSorts | Removes unnecessary sorts | Avoids expensive operations |
| CollapseProject | Merges adjacent select/project | Fewer plan nodes |

---

## Interview Tips

> **Tip 1:** "Explain predicate pushdown." — "Predicate pushdown moves filter conditions as close to the data source as possible. For Parquet files, filters are pushed to the file reader which skips entire row groups using min/max statistics. For partitioned tables, partition columns are pruned at the directory level. Through joins, filters are pushed to the correct side. This can reduce data read by 90%+ depending on selectivity."

> **Tip 2:** "What's column pruning and why does it matter?" — "Column pruning ensures Spark only reads columns that are actually used in the query. With columnar formats like Parquet, unneeded columns are never deserialized from disk. For a 50-column table where you only use 3, this is a 94% reduction in I/O. Catalyst traces column usage through joins, aggregations, and projections to determine the minimum required set."

> **Tip 3:** "How does join reordering work?" — "With CBO enabled and table statistics computed, Catalyst estimates the output size of each possible join order and picks the one with smallest intermediate results. The key insight: joining small tables first produces smaller intermediates that make subsequent joins cheaper. Without statistics, Catalyst uses file size heuristics which are less accurate. Always ANALYZE TABLE for critical joins."
