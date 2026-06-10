---
title: "PySpark Catalyst Optimizer - Senior Deep Dive"
topic: pyspark
subtopic: catalyst-optimizer
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [pyspark, catalyst, execution-plan, optimizer-limitations, udf-barriers, catalyst-extensions, codegen]
---

# PySpark Catalyst Optimizer — Senior Deep Dive

## Reading Execution Plans in Detail

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("CatalystDeepDive").getOrCreate()

# Complex query for plan analysis
result = (spark.read.parquet("s3://data/orders/")
    .join(F.broadcast(spark.read.parquet("s3://data/products/")), "product_id")
    .filter(F.col("order_date") >= "2024-01-01")
    .groupBy("category", F.month("order_date").alias("month"))
    .agg(
        F.sum("amount").alias("revenue"),
        F.countDistinct("customer_id").alias("customers"),
    )
    .filter(F.col("revenue") > 10000)
    .orderBy(F.desc("revenue")))

result.explain(mode="extended")
```

### Plan Breakdown

```
== Optimized Logical Plan ==
Sort [revenue DESC]
+- Filter (revenue > 10000)
   +- Aggregate [category, month(order_date)], [sum(amount) AS revenue, count(distinct customer_id) AS customers]
      +- Project [category, order_date, amount, customer_id]
         +- Filter (order_date >= 2024-01-01)
            +- Join Inner, (product_id = product_id)
               :- Relation[orders] parquet
               +- Relation[products] parquet

== Physical Plan ==
*(4) Sort [revenue#50 DESC], true, 0
+- Exchange rangepartitioning(revenue#50 DESC, 200)
   +- *(3) Filter (revenue#50 > 10000)
      +- *(3) HashAggregate(keys=[category#20, month(order_date#15)], functions=[sum(amount#12), count(distinct customer_id#10)])
         +- Exchange hashpartitioning(category#20, month(order_date#15), 200)
            +- *(2) HashAggregate(keys=[category#20, month(order_date#15)], functions=[partial_sum(amount#12), partial_count(distinct customer_id#10)])
               +- *(2) Project [category#20, order_date#15, amount#12, customer_id#10]
                  +- *(2) BroadcastHashJoin [product_id#11], [product_id#30], Inner, BuildRight
                     :- *(2) Filter (isnotnull(product_id#11) AND (order_date#15 >= 2024-01-01))
                     :  +- *(2) FileScan parquet orders[customer_id#10,product_id#11,amount#12,order_date#15]
                     :        PushedFilters: [IsNotNull(product_id), GreaterThanOrEqual(order_date,2024-01-01)]
                     :        ReadSchema: struct<customer_id:string,product_id:string,amount:double,order_date:date>
                     +- BroadcastExchange HashedRelationBroadcastMode(List(product_id#30))
                        +- *(1) FileScan parquet products[product_id#30,category#20]
                              ReadSchema: struct<product_id:string,category:string>
```

### Key Observations

| Observation | What It Means |
|-------------|---------------|
| `*(2)` wrapping multiple operations | Whole-stage codegen — these operations fused into one Java method |
| `partial_sum`, `partial_count` | Two-phase aggregation: local pre-aggregate → shuffle → final aggregate |
| `PushedFilters` includes order_date | Predicate pushdown successful |
| Products ReadSchema has only 2 cols | Column pruning — only product_id and category read |
| `BroadcastExchange` before join | Products table broadcast to all executors |
| Two `Exchange` nodes | Two shuffles: one for aggregation, one for final sort |

---

## Optimizer Limitations

### Limitation 1: UDFs Are Opaque

```python
@F.udf("double")
def custom_calc(amount, tax_rate):
    return amount * (1 + tax_rate)

# Catalyst cannot:
# - Push filters through UDFs
# - Fold constants in UDFs
# - Include UDFs in whole-stage codegen
# - Estimate output size of UDFs

df.filter(custom_calc("amount", "tax_rate") > 100).explain()
# Filter node stays above scan — NOT pushed down
```

### Limitation 2: Correlated Subqueries

```python
# Correlated subquery — optimizer has limited ability to decorrelate
spark.sql("""
    SELECT * FROM orders o
    WHERE amount > (
        SELECT AVG(amount) FROM orders WHERE customer_id = o.customer_id
    )
""").explain()
# May produce inefficient nested loop plan for complex correlations
```

### Limitation 3: Stale or Missing Statistics

```python
# Without statistics, optimizer uses file size heuristics
# This can lead to wrong join strategy choices

# Table is 50MB compressed on disk → optimizer estimates 50MB
# But decompressed + deserialized = 500MB in memory
# Optimizer might broadcast a table that's actually too large!

# Fix: compute accurate statistics
spark.sql("ANALYZE TABLE orders COMPUTE STATISTICS FOR ALL COLUMNS")
```

### Limitation 4: Cross-Stage Optimization

```python
# Catalyst optimizes within a query, not across multiple queries
df1 = spark.read.parquet(path).filter(F.col("date") == "2024-01-15")
df1.cache()
df1.count()

# This second query doesn't benefit from knowing df1 is filtered
df2 = df1.groupBy("category").count()
# Catalyst doesn't know about the cache's contents for deeper optimization
```

---

## Whole-Stage Code Generation (Codegen)

Whole-stage codegen fuses multiple operators into a single optimized Java method:

```python
# View generated code
result.explain(mode="codegen")

# What codegen produces:
# Instead of: row → filter method → project method → aggregate method
# Generates: single tight loop that does filter + project + aggregate
# Eliminates virtual method dispatch overhead between operators

# Operations that support codegen:
# FileScan, Filter, Project, HashAggregate, BroadcastHashJoin, Sort
# Shown as *(N) in physical plan

# Operations that BREAK codegen:
# Python UDFs, SortMergeJoin (partially), External data sources
# Shown WITHOUT *(N) prefix
```

### Codegen Performance Impact

```python
# Disable codegen to measure impact
spark.conf.set("spark.sql.codegen.wholeStage", "false")
# Typical slowdown: 2-5x for CPU-bound operations

# Re-enable (default)
spark.conf.set("spark.sql.codegen.wholeStage", "true")

# Codegen threshold — minimum rows to trigger codegen
spark.conf.get("spark.sql.codegen.hugeMethodLimit")  # Default: 65536
# Very large generated methods may hit JVM limits
```

---

## Catalyst Extensions

```python
# Custom optimization rules (advanced — modifying optimizer behavior)
# Spark allows adding custom rules via SparkSessionExtensions

# Example: Custom rule to push filters through a specific UDF
# (requires Scala implementation)

# In practice, you influence the optimizer through:
# 1. Hints (BROADCAST, MERGE, COALESCE)
# 2. Configuration (autoBroadcastJoinThreshold, shuffle.partitions)
# 3. Statistics (ANALYZE TABLE)
# 4. Writing optimizer-friendly code (avoid UDFs, use native functions)
```

---

## Diagnosing Optimization Failures

```python
def diagnose_plan(df, name="query"):
    """Analyze plan for common optimization failures."""
    plan_str = df._jdf.queryExecution().executedPlan().toString()
    optimized_str = df._jdf.queryExecution().optimizedPlan().toString()
    
    issues = []
    
    # Check for missing pushdown
    if "Filter" in plan_str and "PushedFilters: []" in plan_str:
        issues.append("WARN: Filter not pushed to data source")
    
    # Check for unnecessary shuffles
    exchange_count = plan_str.count("Exchange")
    if exchange_count > 3:
        issues.append(f"WARN: {exchange_count} shuffles detected — consider reducing")
    
    # Check for SortMergeJoin where broadcast might be better
    if "SortMergeJoin" in plan_str:
        issues.append("INFO: SortMergeJoin used — check if broadcast is possible")
    
    # Check for cartesian product
    if "CartesianProduct" in plan_str or "BroadcastNestedLoopJoin" in plan_str:
        issues.append("CRITICAL: Cartesian product detected!")
    
    # Check for codegen breaks
    if "WholeStageCodegen" not in plan_str and "*(1)" not in plan_str:
        issues.append("WARN: No whole-stage codegen detected")
    
    print(f"\n=== Plan Diagnosis: {name} ===")
    if issues:
        for issue in issues:
            print(f"  {issue}")
    else:
        print("  All optimizations appear healthy")
    
    return issues
```

---

## Forcing Optimizer Behavior

```python
# When optimizer makes wrong choices, override with hints:

# Force broadcast
spark.sql("SELECT /*+ BROADCAST(small) */ * FROM large JOIN small ON large.id = small.id")

# Force shuffle join (when broadcast causes OOM)
spark.sql("SELECT /*+ MERGE(a, b) */ * FROM a JOIN b ON a.id = b.id")

# Force repartition hint
spark.sql("SELECT /*+ REPARTITION(100) */ * FROM large_table")

# Coalesce hint
spark.sql("SELECT /*+ COALESCE(10) */ * FROM filtered_result")

# Rebalance (Spark 3.2+ with AQE)
spark.sql("SELECT /*+ REBALANCE */ * FROM skewed_table")
```

---

## Interview Tips

> **Tip 1:** "How do you read an execution plan in detail?" — "Read bottom-up: start at the leaf nodes (FileScan), check PushedFilters and ReadSchema for pushdown and pruning. Exchange nodes indicate shuffles — each is expensive. The *(N) notation shows whole-stage codegen stages — operations within one stage are fused into a single optimized method. BroadcastExchange means a table is being sent to all executors. I look for: right join strategy, minimal shuffles, pushdown working, and codegen not broken."

> **Tip 2:** "What are the limitations of Catalyst?" — "Four main limitations: (1) UDFs are opaque — can't push predicates through them or include in codegen, (2) Statistics can be stale or missing — leading to wrong join strategy choices, (3) Can't optimize across separate queries or through caches, (4) Complex correlated subqueries may not decorrelate efficiently. The fix for most: avoid UDFs, keep statistics fresh, and write optimizer-friendly code."

> **Tip 3:** "What is whole-stage code generation?" — "Instead of executing each operator as a separate method call per row (virtual dispatch overhead), Spark generates a single Java method that fuses multiple operators together. A filter → project → aggregate pipeline becomes one tight loop. This eliminates method dispatch, enables CPU pipelining, and can be 2-5x faster for CPU-bound operations. UDFs and some complex operators break codegen boundaries, causing a performance cliff."

## ⚡ Cheat Sheet

**Catalyst Pipeline Stages**
1. Parsed Logical Plan → 2. Analyzed Logical Plan → 3. Optimized Logical Plan → 4. Physical Plan(s) → 5. Selected Physical Plan → 6. Code Generation

**Key Optimizer Rules (know these)**
- Predicate Pushdown — filters moved below joins/aggregations
- Column Pruning — unused columns dropped early
- Constant Folding — `1 + 1` → `2` at plan time
- Join Reordering — smaller tables moved to build side (with CBO enabled)

**UDF Barriers — The #1 Catalyst Gotcha**
- Python UDFs: Catalyst cannot inspect them; all optimizations stop at UDF boundary
- No predicate pushdown through UDF, no column pruning past it
- Fix: replace with native Spark SQL functions; if must UDF, use Pandas UDF (Arrow) for throughput

**CBO (Cost-Based Optimizer)**
- Enable: `spark.sql.cbo.enabled=true` + `spark.sql.statistics.histogram.enabled=true`
- Requires `ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS` — stale stats = bad plans
- CBO helps: join reordering, better build-side selection

**Reading explain() Output**
```
df.explain("extended")   # shows all 4 plan stages
df.explain("cost")       # shows estimated row counts (CBO)
df.explain("codegen")    # shows generated Java code
```
- Read bottom-up: leaves = data sources, root = final output
- `Exchange` = shuffle; `Sort` = sort; `BroadcastHashJoin` = broadcast join

**Code Generation**
- Whole-Stage CodeGen fuses operators into single JVM method (look for `*(1)` prefix in plan)
- Falls back to interpreted mode for: UDFs, complex expressions, very long pipelines
- `spark.sql.codegen.wholeStage=false` disables it (for debugging only)

**Interview Traps**
- Catalyst is rule-based + cost-based, not ML-based
- Adding `.filter()` after `.join()` is fine — Catalyst will push it before the join anyway
- Calling `.explain()` is free (no data scanned); calling `.count()` triggers full execution
