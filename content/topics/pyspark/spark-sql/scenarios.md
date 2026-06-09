---
title: "PySpark Spark SQL - Interview Scenarios"
topic: pyspark
subtopic: spark-sql
content_type: scenario_question
tags: [pyspark, spark-sql, interview-scenarios, query-optimization, explain-plan, performance]
---

# PySpark Spark SQL — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: Scenario: Write a Spark SQL Query

**Scenario:** **Question:** "Given an orders table and a customers table, write a Spark SQL query to find the top 5 customers by total spend in Q1 2024, including their name and email."

### Setup

```python
from p

<details>
<summary>💡 Hint</summary>

Think carefully about the key concepts and consider the trade-offs.

</details>

<details>
<summary>✅ Solution</summary>

**Question:** "Given an orders table and a customers table, write a Spark SQL query to find the top 5 customers by total spend in Q1 2024, including their name and email."

### Setup

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SQLScenario").getOrCreate()

# Sample data
orders_data = [
    ("o1", "c1", 500.0, "2024-01-15"),
    ("o2", "c1", 300.0, "2024-02-20"),
    ("o3", "c2", 1200.0, "2024-01-10"),
    ("o4", "c3", 150.0, "2024-03-01"),
    ("o5", "c2", 800.0, "2024-03-15"),
    ("o6", "c4", 2000.0, "2024-04-01"),  # Outside Q1
]

customers_data = [
    ("c1", "Alice Smith", "alice@email.com"),
    ("c2", "Bob Jones", "bob@email.com"),
    ("c3", "Charlie Brown", "charlie@email.com"),
    ("c4", "Diana Ross", "diana@email.com"),
]

orders_df = spark.createDataFrame(orders_data, ["order_id", "customer_id", "amount", "order_date"])
customers_df = spark.createDataFrame(customers_data, ["customer_id", "name", "email"])

orders_df.createOrReplaceTempView("orders")
customers_df.createOrReplaceTempView("customers")
```

### Solution

```python
result = spark.sql("""
    SELECT
        c.name,
        c.email,
        SUM(o.amount) AS total_spend,
        COUNT(o.order_id) AS order_count
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    WHERE o.order_date >= '2024-01-01'
      AND o.order_date < '2024-04-01'
    GROUP BY c.name, c.email
    ORDER BY total_spend DESC
    LIMIT 5
""")

result.show()
# +------------+----------------+-----------+-----------+
# |        name|           email|total_spend|order_count|
# +------------+----------------+-----------+-----------+
# |   Bob Jones|  bob@email.com|     2000.0|          2|
# | Alice Smith|alice@email.com|      800.0|          2|
# |Charlie Brown|charlie@email.com|  150.0|          1|
# +------------+----------------+-----------+-----------+
```

**Expected Answer Points:**
- JOIN with proper ON clause
- Date filtering with inclusive start, exclusive end (avoids timezone issues)
- GROUP BY all non-aggregated columns
- ORDER BY with DESC for "top" results
- LIMIT for top N
- Bonus: mention that the optimizer will push the date filter before the join

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Scenario: Optimize a Slow Spark SQL Query

**Scenario:** **Question:** "This Spark SQL query takes 45 minutes to run on a 100-node cluster. The orders table has 2 billion rows partitioned by order_date. The products table has 50,000 rows. What's wrong and h

<details>
<summary>💡 Hint</summary>

Think carefully about the key concepts and consider the trade-offs.

</details>

<details>
<summary>✅ Solution</summary>

**Question:** "This Spark SQL query takes 45 minutes to run on a 100-node cluster. The orders table has 2 billion rows partitioned by order_date. The products table has 50,000 rows. What's wrong and how would you fix it?"

### The Slow Query

```python
# Takes 45 minutes — something is wrong
slow_result = spark.sql("""
    SELECT
        p.category,
        DATE_FORMAT(o.order_date, 'yyyy-MM') AS month,
        SUM(o.quantity * p.price) AS revenue,
        COUNT(DISTINCT o.customer_id) AS unique_customers
    FROM orders o
    JOIN products p ON o.product_id = p.product_id
    WHERE YEAR(o.order_date) = 2024
    GROUP BY p.category, DATE_FORMAT(o.order_date, 'yyyy-MM')
    ORDER BY revenue DESC
""")
```

### Diagnosis

```python
# Check the execution plan
slow_result.explain(mode="formatted")
```

```
== Physical Plan ==
Sort [revenue DESC]
+- HashAggregate [category, month], [sum, count(distinct)]
   +- Exchange hashpartitioning(category, month, 200)      ← HUGE SHUFFLE
      +- HashAggregate [category, month], [partial_sum, partial_count]
         +- SortMergeJoin [product_id = product_id]        ← WRONG JOIN!
            :- Sort [product_id ASC]
            :  +- Exchange hashpartitioning(product_id, 200)  ← SHUFFLE ORDERS
            :     +- Filter YEAR(order_date) = 2024           ← NO PUSHDOWN!
            :        +- FileScan parquet [ALL PARTITIONS]      ← FULL SCAN!
            +- Sort [product_id ASC]
               +- Exchange hashpartitioning(product_id, 200)
                  +- FileScan parquet products
```

**Problems identified:**
1. `YEAR(o.order_date) = 2024` — function on partition column prevents partition pruning
2. SortMergeJoin on products (50K rows) — should be BroadcastHashJoin
3. Full table scan of 2 billion rows instead of reading only 2024 partitions

### The Fix

```python
# Fixed query — 3 changes
fast_result = spark.sql("""
    SELECT /*+ BROADCAST(p) */
        p.category,
        DATE_FORMAT(o.order_date, 'yyyy-MM') AS month,
        SUM(o.quantity * p.price) AS revenue,
        COUNT(DISTINCT o.customer_id) AS unique_customers
    FROM orders o
    JOIN products p ON o.product_id = p.product_id
    WHERE o.order_date >= '2024-01-01'
      AND o.order_date < '2025-01-01'
    GROUP BY p.category, DATE_FORMAT(o.order_date, 'yyyy-MM')
    ORDER BY revenue DESC
""")

fast_result.explain(mode="formatted")
```

```
== Physical Plan ==
Sort [revenue DESC]
+- HashAggregate [category, month], [sum, count(distinct)]
   +- Exchange hashpartitioning(category, month, 200)
      +- HashAggregate [category, month], [partial_sum, partial_count]
         +- BroadcastHashJoin [product_id = product_id]     ← BROADCAST!
            :- FileScan parquet orders
            :     Partition Filters: [order_date >= 2024-01-01,  ← PRUNED!
            :                         order_date < 2025-01-01]
            +- BroadcastExchange
               +- FileScan parquet products
```

### Performance Comparison

| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| Partitions scanned | All (730 days) | 365 days only | 2x fewer |
| Join strategy | SortMergeJoin (shuffle both) | BroadcastHashJoin (no shuffle) | Eliminates shuffle of 2B rows |
| Shuffle data | ~800 GB | ~400 MB | 2000x less |
| Duration | 45 min | 2 min | 22x faster |

**Expected Answer Points:**
- Function on partition column (`YEAR()`) prevents pruning — use direct comparison
- Small table (50K rows) should be broadcast — add hint or check threshold config
- Explain plan reveals the problem — always check plan first
- Additional: increase `autoBroadcastJoinThreshold` if products table is under threshold

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Scenario: Compare SQL Plan vs DataFrame Plan

**Scenario:** **Question:** "Your team claims Spark SQL is slower than DataFrame API for a specific query. You need to prove or disprove this. Walk me through your investigation."

### Investigation Process

```pyt

<details>
<summary>💡 Hint</summary>

Think carefully about the key concepts and consider the trade-offs.

</details>

<details>
<summary>✅ Solution</summary>

**Question:** "Your team claims Spark SQL is slower than DataFrame API for a specific query. You need to prove or disprove this. Walk me through your investigation."

### Investigation Process

```python
from pyspark.sql import functions as F
import time

# The query in both forms
# SQL version
def run_sql():
    return spark.sql("""
        WITH ranked_orders AS (
            SELECT
                customer_id,
                order_id,
                amount,
                order_date,
                ROW_NUMBER() OVER (
                    PARTITION BY customer_id ORDER BY amount DESC
                ) AS rank
            FROM orders
            WHERE order_date >= '2024-01-01'
        )
        SELECT customer_id, order_id, amount, order_date
        FROM ranked_orders
        WHERE rank <= 3
    """)

# DataFrame version
def run_df():
    from pyspark.sql.window import Window
    w = Window.partitionBy("customer_id").orderBy(F.desc("amount"))
    return (orders_df
        .filter(F.col("order_date") >= "2024-01-01")
        .withColumn("rank", F.row_number().over(w))
        .filter(F.col("rank") <= 3)
        .drop("rank")
    )

# Step 1: Compare logical plans
sql_plan = run_sql()._jdf.queryExecution().optimizedPlan().toString()
df_plan = run_df()._jdf.queryExecution().optimizedPlan().toString()

print("Plans identical:", sql_plan == df_plan)
# Expected: True — both go through same optimizer

# Step 2: Compare physical plans
sql_physical = run_sql()._jdf.queryExecution().executedPlan().toString()
df_physical = run_df()._jdf.queryExecution().executedPlan().toString()

print("Physical plans identical:", sql_physical == df_physical)

# Step 3: If plans differ, compare explain output
run_sql().explain(mode="extended")
run_df().explain(mode="extended")

# Step 4: Benchmark with proper methodology
def benchmark(func, runs=5):
    times = []
    for _ in range(runs):
        start = time.time()
        func().write.mode("overwrite").parquet(f"/tmp/bench_{time.time()}")
        times.append(time.time() - start)
    times = sorted(times)[1:-1]  # Remove outliers
    return sum(times) / len(times)

sql_avg = benchmark(run_sql)
df_avg = benchmark(run_df)
print(f"SQL avg: {sql_avg:.2f}s, DF avg: {df_avg:.2f}s, Diff: {abs(sql_avg-df_avg):.2f}s")
```

### Common Reasons for Perceived Differences

| Cause | Explanation | Fix |
|-------|-------------|-----|
| Caching effect | First run caches metadata | Warm up before benchmark |
| Different queries | SQL and DF don't express the same logic | Verify with explain |
| UDF in DataFrame | Python UDF breaks optimization | Use native functions |
| Different configs | Session config changed between runs | Reset configs |
| Data skew randomness | Partition assignment varies | Average multiple runs |

**Expected Answer Points:**
- Start by comparing optimized logical plans — they should be identical
- If physical plans differ, one expression may not translate the same way
- Proper benchmarking: multiple runs, discard outliers, warm caches first
- Most likely cause of difference: the queries aren't logically equivalent
- If truly identical queries show different times, it's measurement noise

---

## Interview Tips

> **Tip 1:** "For basic SQL questions, show you understand the execution order." — "SQL is written SELECT-FROM-JOIN-WHERE-GROUP-HAVING-ORDER-LIMIT but executes as FROM-JOIN-WHERE-GROUP-HAVING-SELECT-ORDER-LIMIT. Understanding this helps predict where filters apply and what columns are available in each clause. Spark's optimizer may reorder operations but the logical semantics follow this order."

> **Tip 2:** "For optimization questions, always start with EXPLAIN." — "Don't guess — look at the plan. I check three things: Is partition pruning happening (Partition Filters in FileScan)? Is the join strategy appropriate (Broadcast for small tables)? Are there unnecessary shuffles (Exchange nodes)? Then I fix the most impactful issue first. Usually it's a filter that prevents pruning or a missing broadcast."

> **Tip 3:** "For SQL vs DataFrame comparisons, emphasize they're the same thing." — "Both APIs produce the same Catalyst logical plan. Any performance difference is either measurement error or the two versions aren't expressing the same query. I verify by comparing optimizedPlan().toString() — if they match, performance is identical. The choice between SQL and DataFrame is about team preference and code maintainability, not performance."

</details>

</article>



---

## Interview Tips