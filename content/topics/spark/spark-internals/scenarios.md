---
title: "Spark Internals Interview Scenarios"
description: "Scenarios covering Tungsten, Catalyst, memory management, and execution engine internals"
content_type: scenario_question
topic: spark
subtopic: spark-internals
tags: [spark, tungsten, catalyst, codegen, memory-management, off-heap, internals]
---

<article data-difficulty="junior">

## Scenario: Understanding Lazy Evaluation in Spark

Explain what "lazy evaluation" means in Spark and why it's beneficial. Give a concrete example where lazy evaluation saves unnecessary computation.

<details>
<summary>✅ Solution</summary>

### What is Lazy Evaluation?

In Spark, **lazy evaluation** means that transformations (like `filter`, `map`, `select`, `join`) are not executed immediately when you call them. Instead, Spark builds a logical plan — a blueprint of the work to do. The actual computation only begins when you call an **action** (like `show`, `count`, `collect`, `write`).

### Two Types of Operations

| Type | Examples | When it runs |
|------|----------|--------------|
| Transformation | `filter`, `select`, `groupBy`, `join`, `map` | Never immediately — recorded in the plan |
| Action | `show`, `count`, `collect`, `save`, `write` | Triggers execution of all pending transformations |

### Simple Example

```python
# Step 1: Read — no data is loaded yet
df = spark.read.parquet("/data/logs")  # Transformation

# Step 2: Filter — no computation happens
errors = df.filter(df.level == "ERROR")  # Transformation

# Step 3: Another filter — still nothing runs
critical = errors.filter(df.service == "payments")  # Transformation

# Step 4: Action — NOW Spark executes everything
critical.show(10)
```

Without lazy evaluation, Spark would read ALL data after step 1, filter ALL data after step 2, and filter again after step 3 — reading the full dataset three times.

With lazy evaluation, Spark waits until `show()` and then combines all filters into one pass over the data.

### Concrete Example: Lazy Evaluation Saves a Full Table Scan

```python
# Scenario: You want the first 10 error logs from a 1TB log file

# Without lazy eval: reads all 1TB, filters, then takes 10
# With lazy eval: Spark knows you only need 10 rows, so it can stop early

df = spark.read.parquet("/data/1tb_logs")
result = df.filter(df.level == "ERROR").limit(10)

result.show()  # Spark uses the limit to short-circuit — may only read a fraction of the data
```

### Why Lazy Evaluation is Beneficial

**1. Query optimization before execution**
Spark's Catalyst optimizer can see the entire chain of transformations before running anything. It can reorder, combine, and eliminate operations.

```python
# You wrote this:
df.select("*").filter("amount > 100").select("name", "amount")

# Catalyst optimizes it to:
# → Push filter before select (read less data)
# → Eliminate the select("*") (column pruning)
# → Equivalent to: df.filter("amount > 100").select("name", "amount")
```

**2. Avoid reading data you never use**
```python
df = spark.read.parquet("/huge_table")  # Would take 10 minutes to load

# If you realize you don't need this data and never call an action,
# Spark never reads the file at all
if some_condition:
    df.show()  # Only reads data if this line is reached
```

**3. Pipeline fusion**
Multiple transformations can be collapsed into a single pass, reducing intermediate data materialization.

### The DAG: Spark's Work Plan

When you chain transformations, Spark builds a **DAG (Directed Acyclic Graph)**:

```
Read("/data/logs")
    ↓
filter(level == "ERROR")
    ↓
filter(service == "payments")
    ↓
show(10)   ← action triggers execution of the entire DAG
```

The DAG is only executed when an action is called.

</details>

</article>

<article data-difficulty="mid">

## Scenario: Tracing Catalyst Optimizer Through a Query

Explain how Spark's Catalyst optimizer works. Given this query — `spark.table("orders").filter("amount > 100").join(spark.table("customers"), "customer_id").select("name", "amount")` — what optimizations would Catalyst apply and in what order?

<details>
<summary>✅ Solution</summary>

### What is Catalyst?

**Catalyst** is Spark SQL's query optimizer. It transforms a user's logical query into an efficient physical execution plan through a series of rule-based and cost-based optimization passes.

### The Four Phases of Catalyst

```
SQL / DataFrame API
        ↓
1. Unresolved Logical Plan   (parse)
        ↓
2. Resolved Logical Plan     (analyze — bind to catalog)
        ↓
3. Optimized Logical Plan    (optimize — apply rules)
        ↓
4. Physical Plan(s)          (plan — choose execution strategy)
        ↓
5. Code Generation           (codegen — Tungsten)
        ↓
Execute
```

### Phase-by-Phase Walkthrough for the Given Query

**Original query:**
```python
spark.table("orders") \
    .filter("amount > 100") \
    .join(spark.table("customers"), "customer_id") \
    .select("name", "amount")
```

---

**Phase 1: Unresolved Logical Plan**

Catalyst parses the code into a tree. Column names like `amount`, `name`, and `customer_id` are not yet resolved — they're just strings.

```
Project [name, amount]
└── Join [customer_id]
    ├── Filter [amount > 100]
    │   └── UnresolvedRelation [orders]
    └── UnresolvedRelation [customers]
```

---

**Phase 2: Analysis (Resolve)**

The **Analyzer** consults the **Catalog** (Hive metastore, Glue, etc.) to resolve table schemas and column types.

- `orders` → schema: `(order_id, customer_id, amount, created_at)`
- `customers` → schema: `(customer_id, name, email, country)`
- `amount` → resolved to `orders.amount` (type: Double)
- `name` → resolved to `customers.name` (type: String)

---

**Phase 3: Logical Optimization — Key Rules Applied**

**Rule 1: Predicate Pushdown**
Move the `filter(amount > 100)` as close to the data source as possible — before the join.

```
# Before: filter happens after join (more rows in join)
Project → Join → Filter → Scan(orders)

# After: filter pushed down (fewer rows enter the join)
Project → Join → Filter → Scan(orders)  ✓ (already correct here, but pushdown also goes into the scan)
```

For Parquet or ORC files, this means Spark tells the file reader to skip row groups where `amount <= 100` (predicate pushdown to storage).

**Rule 2: Column Pruning (Project Pushdown)**
Only read columns that are actually needed. The final `select("name", "amount")` means:
- From `orders`: only read `customer_id` (for join) and `amount`
- From `customers`: only read `customer_id` (for join) and `name`

Skip `order_id`, `created_at`, `email`, `country` — never read from disk.

**Rule 3: Constant Folding**
Any expressions with literals are pre-computed at plan time, not at execution time.

```python
# Written as:
.filter(col("amount") > 50 + 50)  # → evaluated to amount > 100 before execution
```

**Rule 4: Join Reordering (if stats available)**
If the catalog has table statistics, Catalyst may reorder joins to process smaller tables first.

---

**Phase 4: Physical Planning**

Catalyst generates one or more physical plans and selects the best one using a cost model.

For this query, it considers:
- `BroadcastHashJoin` vs `SortMergeJoin` — if `customers` is small enough (< `spark.sql.autoBroadcastJoinThreshold`), use broadcast
- Partition pruning if `orders` is partitioned by date or customer_id

```
*(2) Project [name, amount]
+- *(2) BroadcastHashJoin [customer_id], BuildRight
   :- *(1) Filter [amount > 100]
   :  +- *(1) ColumnarToRow
   :     +- Scan parquet orders [customer_id, amount]
   :        PushedFilters: [IsNotNull(amount), GreaterThan(amount,100.0)]
   +- BroadcastExchange HashedRelationBroadcastMode
      +- *(1) ColumnarToRow
         +- Scan parquet customers [customer_id, name]
```

### How to See the Plan Yourself

```python
query = spark.table("orders") \
    .filter("amount > 100") \
    .join(spark.table("customers"), "customer_id") \
    .select("name", "amount")

# See all four stages
query.explain(mode="extended")

# Formatted output (Spark 3.0+)
query.explain(mode="formatted")
```

### Summary Table

| Optimization | What it does | Benefit |
|-------------|--------------|---------|
| Predicate Pushdown | Moves filters toward the scan | Read less data from disk |
| Column Pruning | Only reads needed columns | Reduces I/O, especially on Parquet |
| Constant Folding | Pre-evaluates literals | Eliminates runtime computation |
| Broadcast Join | Sends small table to all executors | Eliminates shuffle |
| Join Reordering | Processes smaller inputs first | Reduces intermediate data |

</details>

</article>

<article data-difficulty="senior">

## Scenario: Diagnosing Spill Despite Sufficient Cluster Memory

A Spark job is spilling 200GB to disk during a groupBy aggregation despite the cluster having sufficient total memory. Walk through Spark's memory model (execution vs storage memory, unified memory manager), explain why spilling occurs, and propose 3 different fixes with trade-offs.

<details>
<summary>✅ Solution</summary>

### Spark's Unified Memory Model

Since Spark 1.6, memory management uses the **Unified Memory Manager**, which divides each executor's JVM heap into three regions:

```
Total Executor Heap (e.g., 16GB)
├── Reserved Memory (300MB fixed) — Spark internal objects
├── User Memory (spark.memory.fraction subtracted from remainder)
│   └── For user data structures, UDFs, non-Spark objects
└── Spark Memory Pool  [spark.memory.fraction = 0.6 default]
    ├── Execution Memory  — shuffle buffers, sort, aggregation hash maps
    └── Storage Memory    — cached DataFrames, broadcast variables
```

**Key property of Unified Memory Manager:** Execution and Storage memory share the same pool. Either side can borrow from the other — but:
- Execution memory can **evict** Storage memory (force cached data to disk)
- Storage memory **cannot** evict Execution memory

### Why Spill Occurs Despite "Sufficient" Cluster Memory

"Sufficient cluster memory" ≠ "sufficient executor memory." Here are the real causes:

**Cause 1: Per-executor memory is too small**
Total cluster RAM = 512GB, but 128 executors × 4GB each = heavy pressure per executor. A groupBy aggregation holds an in-memory hash map. When the hash map exceeds the executor's available execution memory, Spark spills the map to disk and starts a new one.

**Cause 2: Storage memory consuming Execution memory**
If you have cached DataFrames (`df.cache()`), the storage region grows and leaves less room for execution. The Unified Manager will evict cached blocks, but this takes time and may not free enough space before a spill decision is made.

**Cause 3: Too few partitions → too much data per task**
With `spark.sql.shuffle.partitions = 200` (default) and 800GB of data flowing through groupBy, each partition holds 4GB. A single task's aggregation hash map must fit the 4GB worth of distinct key/value pairs in the execution pool of ONE executor. This exceeds available memory → spill.

**Cause 4: High-cardinality groupBy on a non-compressible column**
Aggregating on UUID-like keys produces a hash map with millions of entries, each holding a full key string. Memory usage per entry is high; even moderate data sizes cause spill.

### Diagnosing in Spark UI

```
Stages tab → Click the spilling stage
→ Tasks table: "Shuffle Write", "Spill (Memory)", "Spill (Disk)" columns
→ Large "Spill (Memory)" means data that was in memory but had to be serialized to disk
```

---

### Fix 1: Increase Shuffle Partitions (Least Invasive)

**Rationale:** More partitions = less data per partition = smaller hash maps per task.

```python
# Default is 200. For 800GB of shuffle data targeting 128MB/partition:
# 800,000 MB / 128 MB = ~6250 partitions
spark.conf.set("spark.sql.shuffle.partitions", "6250")

# Or use AQE to auto-tune:
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
```

**Trade-offs:**
- Pro: Simple one-liner, no code changes, AQE can do it automatically
- Con: More partitions = more tasks = more scheduler overhead; very small tasks waste CPU
- When to use: Always try this first

---

### Fix 2: Increase Executor Memory and Tune Memory Fractions

**Rationale:** Give each executor more heap, and allocate more to execution memory.

```bash
spark-submit \
  --executor-memory 32g \
  --conf spark.memory.fraction=0.8 \
  --conf spark.memory.storageFraction=0.2 \
  ...
```

```python
# In code:
spark.conf.set("spark.memory.fraction", "0.8")       # 80% of heap for Spark (up from 60%)
spark.conf.set("spark.memory.storageFraction", "0.2") # Only 20% of Spark pool reserved for storage
                                                        # → more execution memory available
```

**What this does:**
- `spark.memory.fraction = 0.8` → 80% of (heap - 300MB) is the Spark memory pool
- `spark.memory.storageFraction = 0.2` → only 20% of that pool is "soft-reserved" for storage
- Execution memory can now use 64% of total heap instead of 42%

**Trade-offs:**
- Pro: Directly addresses memory pressure without code changes
- Con: Less room for user memory (UDFs, Python overhead, etc.); costs more on cloud per-executor
- When to use: When you have control over cluster configuration and can verify there's no user memory competition

---

### Fix 3: Use a Two-Phase Aggregation (Partial + Final)

**Rationale:** Reduce the data volume before the shuffle, so less data flows through the aggregation at each stage.

```python
from pyspark.sql.functions import col, sum as spark_sum, count

# Anti-pattern: single groupBy on full data
result = df.groupBy("product_category", "region") \
           .agg(spark_sum("revenue").alias("total_revenue"),
                count("*").alias("order_count"))

# Better: pre-aggregate on the original partitions first (map-side combine)
# Spark does this automatically for sum/count — but you can enforce it with:

# Repartition to increase parallelism BEFORE the groupBy
result = df.repartition(6250, "product_category", "region") \
           .groupBy("product_category", "region") \
           .agg(spark_sum("revenue").alias("total_revenue"),
                count("*").alias("order_count"))
```

**For truly custom aggregations, use UDAF or pandas_udf:**
```python
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

@pandas_udf(DoubleType())
def weighted_avg(values: pd.Series, weights: pd.Series) -> float:
    return (values * weights).sum() / weights.sum()

# pandas_udf operates on chunks and avoids full Java object overhead
result = df.groupBy("category").agg(weighted_avg("revenue", "weight"))
```

**Trade-offs:**
- Pro: Fundamentally reduces data volume before the expensive shuffle/aggregation
- Con: Requires understanding the aggregation semantics; not all aggregations are decomposable (e.g., `countDistinct`, `median`)
- When to use: When the aggregation itself is the bottleneck and the groupBy keys are high-cardinality

---

### Comparison Table

| Fix | Code Change | Memory Impact | Complexity | Best When |
|-----|------------|--------------|------------|-----------|
| More shuffle partitions | Config only | Moderate reduction per task | Low | Always try first |
| Larger executor + tuned fractions | Config only | Direct increase in budget | Low-Med | You control cluster sizing |
| Two-phase / repartition strategy | Code change | Reduces data before spill point | High | Aggregation is the root cause |

### Bonus: Verify the Fix

```python
# Before and after: check spill metrics programmatically
spark.sparkContext.statusTracker().getJobIdsForGroup(None)

# Or check the event log:
# $SPARK_HOME/bin/spark-history-server
# Look at Stage metrics: "Spill (Memory)" and "Spill (Disk)" should drop to 0
```

</details>

</article>
