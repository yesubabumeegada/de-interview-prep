---
title: "Spark SQL and Datasets Interview Scenarios"
description: "Scenarios covering Spark SQL, Catalyst optimizer, window functions, and Dataset API"
content_type: scenario_question
topic: spark
subtopic: spark-sql-and-datasets
tags: [spark, spark-sql, catalyst, window-functions, dataset, dataframe, hive]
---

<article data-difficulty="junior">

## Scenario: Top 3 Products by Revenue Per Category Using Window Functions

Write a Spark SQL query to find the top 3 products by revenue for each category. The table `sales` has columns: `category`, `product_name`, `revenue`.

<details>
<summary>✅ Solution</summary>

### The SQL Query

```sql
SELECT category, product_name, revenue, rank
FROM (
    SELECT
        category,
        product_name,
        revenue,
        RANK() OVER (
            PARTITION BY category
            ORDER BY revenue DESC
        ) AS rank
    FROM sales
) ranked
WHERE rank <= 3
ORDER BY category, rank;
```

### Equivalent PySpark DataFrame API

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import rank, col
from pyspark.sql.window import Window

spark = SparkSession.builder.getOrCreate()

# Sample data
data = [
    ("Electronics", "Laptop", 1200.0),
    ("Electronics", "Phone", 800.0),
    ("Electronics", "Tablet", 600.0),
    ("Electronics", "Headphones", 200.0),
    ("Clothing", "Jacket", 150.0),
    ("Clothing", "Shoes", 120.0),
    ("Clothing", "T-Shirt", 30.0),
    ("Clothing", "Hat", 25.0),
]
df = spark.createDataFrame(data, ["category", "product_name", "revenue"])

# Define window: partition by category, order by revenue descending
window_spec = Window.partitionBy("category").orderBy(col("revenue").desc())

result = (
    df
    .withColumn("rank", rank().over(window_spec))
    .filter(col("rank") <= 3)
    .orderBy("category", "rank")
)

result.show()
# +-----------+------------+-------+----+
# |   category|product_name|revenue|rank|
# +-----------+------------+-------+----+
# |   Clothing|      Jacket|  150.0|   1|
# |   Clothing|       Shoes|  120.0|   2|
# |   Clothing|     T-Shirt|   30.0|   3|
# |Electronics|      Laptop| 1200.0|   1|
# |Electronics|       Phone|  800.0|   2|
# |Electronics|      Tablet|  600.0|   3|
```

### RANK vs DENSE_RANK vs ROW_NUMBER

Understanding which ranking function to use is a common follow-up:

```sql
-- If two products have the same revenue:
-- Product A: $500, Product B: $500, Product C: $400

-- RANK(): leaves gaps
-- 1, 1, 3   ← two products tied at 1, next is 3 (not 2)
RANK() OVER (PARTITION BY category ORDER BY revenue DESC)

-- DENSE_RANK(): no gaps
-- 1, 1, 2   ← two products tied at 1, next is 2
DENSE_RANK() OVER (PARTITION BY category ORDER BY revenue DESC)

-- ROW_NUMBER(): no ties (arbitrary tiebreak)
-- 1, 2, 3   ← unique ranks always
ROW_NUMBER() OVER (PARTITION BY category ORDER BY revenue DESC)
```

For "top N" queries:
- Use `RANK()` if you want to include **all tied products** at position N (may return more than N rows)
- Use `DENSE_RANK()` if tied products should count toward the N limit
- Use `ROW_NUMBER()` if you need **exactly N rows** (deterministic but arbitrary for ties)

### Register as Temp View for SQL

```python
df.createOrReplaceTempView("sales")

result = spark.sql("""
    SELECT category, product_name, revenue, rank
    FROM (
        SELECT
            category,
            product_name,
            revenue,
            RANK() OVER (PARTITION BY category ORDER BY revenue DESC) AS rank
        FROM sales
    ) t
    WHERE rank <= 3
    ORDER BY category, rank
""")
result.show()
```

</details>

</article>

<article data-difficulty="mid">

## Scenario: Slow Spark SQL Query Despite Date Partitioning — Full Table Scan

Your Spark SQL query reads from a Parquet table partitioned by `dt` (date). Despite filtering on `dt`, the EXPLAIN plan shows a full table scan reading all partitions. The query takes 45 minutes when it should take 3 minutes.

```sql
SELECT user_id, sum(amount) as total
FROM transactions
WHERE dt >= '2024-01-01' AND dt <= '2024-01-31'
GROUP BY user_id
```

What are the likely causes and how do you fix each one?

<details>
<summary>✅ Solution</summary>

### Diagnose First: Read the EXPLAIN Plan

```python
spark.sql("""
    SELECT user_id, sum(amount) as total
    FROM transactions
    WHERE dt >= '2024-01-01' AND dt <= '2024-01-31'
    GROUP BY user_id
""").explain(mode="extended")
```

Key things to look for in the physical plan:

```
# BAD — full scan:
FileScan parquet [user_id,amount,dt] Batched: true,
  PartitionFilters: [],                    ← empty! no partition pruning
  PushedFilters: [IsNotNull(dt), ...]

# GOOD — with pruning:
FileScan parquet [user_id,amount,dt] Batched: true,
  PartitionFilters: [isnotnull(dt), (dt >= 2024-01-01), (dt <= 2024-01-31)],
  PushedFilters: [IsNotNull(dt)]
```

### Cause 1: Type Mismatch — `dt` Column Type vs Filter Literal

Most common cause. If `dt` is stored as a `DATE` type but the filter uses a string literal without explicit casting:

```python
# Check the schema
spark.sql("DESCRIBE transactions").show()
# dt  date   (not string!)

# The optimizer may fail to push down string-to-date comparison
# Fix: cast explicitly or use date literals
spark.sql("""
    SELECT user_id, sum(amount) as total
    FROM transactions
    WHERE dt >= DATE '2024-01-01' AND dt <= DATE '2024-01-31'
    GROUP BY user_id
""")

# Or in DataFrame API:
from pyspark.sql.functions import col, lit, to_date
df.filter(
    (col("dt") >= to_date(lit("2024-01-01"))) &
    (col("dt") <= to_date(lit("2024-01-31")))
)
```

### Cause 2: Table Not Registered in Metastore (No Partition Statistics)

If the table was created with `spark.read.parquet(path)` rather than registered as a Hive/Glue table, Spark doesn't know about its partition structure:

```python
# BAD — Spark doesn't know about partition columns
df = spark.read.parquet("s3://bucket/transactions/")

# GOOD — register with partition awareness
df = spark.read.parquet("s3://bucket/transactions/")
df.createOrReplaceTempView("transactions")
# Still no partition pruning! Spark doesn't know dt is a partition column

# BEST — use a Hive/Glue catalog table with msck repair
spark.sql("MSCK REPAIR TABLE transactions")  # discover new partitions
# or in Glue: run Glue Crawler

# Then query normally — partition pruning works
spark.sql("SELECT * FROM transactions WHERE dt = '2024-01-15'")
```

### Cause 3: Function Applied to Partition Column

Wrapping a partition column in a function defeats predicate pushdown:

```sql
-- BAD: function on partition column — no pruning
WHERE CAST(dt AS STRING) >= '2024-01-01'
WHERE DATE_FORMAT(dt, 'yyyy-MM') = '2024-01'
WHERE YEAR(dt) = 2024 AND MONTH(dt) = 1

-- GOOD: direct comparison
WHERE dt >= '2024-01-01' AND dt <= '2024-01-31'
WHERE dt BETWEEN '2024-01-01' AND '2024-01-31'
```

### Cause 4: Subquery or CTE Hiding the Filter from the Optimizer

Catalyst can usually push filters through CTEs, but complex subqueries sometimes block it:

```sql
-- Potentially problematic
WITH base AS (SELECT * FROM transactions)
SELECT user_id, sum(amount)
FROM base
WHERE dt >= '2024-01-01'
GROUP BY user_id

-- Safer: filter early
WITH base AS (
    SELECT * FROM transactions
    WHERE dt >= '2024-01-01' AND dt <= '2024-01-31'   -- filter inside CTE
)
SELECT user_id, sum(amount) FROM base GROUP BY user_id
```

### Cause 5: Dynamic Partition Pruning (DPP) Not Enabled

For join queries, Dynamic Partition Pruning eliminates partitions at runtime based on the join:

```python
# Enable DPP (default in Spark 3.x)
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")
spark.conf.set("spark.sql.adaptive.enabled", "true")

# Example where DPP helps:
spark.sql("""
    SELECT t.user_id, sum(t.amount)
    FROM transactions t
    JOIN date_dim d ON t.dt = d.date_key
    WHERE d.month = '2024-01'    -- filter on dimension table
    GROUP BY t.user_id
""")
# Without DPP: full scan of transactions
# With DPP: Spark broadcasts the filtered date_dim keys and prunes transactions partitions
```

### Systematic Fix Checklist

```python
# 1. Verify partition column type
spark.sql("DESCRIBE FORMATTED transactions").filter("col_name == 'dt'").show()

# 2. Repair table (discover partitions in metastore)
spark.sql("MSCK REPAIR TABLE transactions")

# 3. Check partition statistics
spark.sql("ANALYZE TABLE transactions PARTITION (dt='2024-01-01') COMPUTE STATISTICS")

# 4. Enable AQE + DPP
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")

# 5. Verify pruning in EXPLAIN
query = "SELECT user_id, sum(amount) FROM transactions WHERE dt >= '2024-01-01' AND dt <= '2024-01-31' GROUP BY user_id"
spark.sql(query).explain()
# Look for PartitionFilters: [isnotnull(dt), ...]
```

</details>

</article>

<article data-difficulty="senior">

## Scenario: Sessionize 10 Billion Rows of Clickstream Data

Design a Spark SQL pipeline to sessionize clickstream data.

**Input table** `clickstream`: `(user_id STRING, event_time TIMESTAMP, page_url STRING)`
**Table size**: 10 billion rows

**Session definition**: Events for the same user are in the same session if they occur within **30 minutes** of each other. A new session starts when the gap between consecutive events exceeds 30 minutes.

**Output**: assign a `session_id` per session, compute `session_duration_minutes` and `page_count` per session.

<details>
<summary>✅ Solution</summary>

### Step 1: Understand the Scale and Algorithm

10 billion rows = significant shuffle and state requirements. We cannot use naive self-joins or sorted arrays — they won't scale.

The correct algorithm:
1. **Window function**: for each user, look at the previous event's timestamp
2. **Gap detection**: flag where gap > 30 minutes (session boundary)
3. **Cumulative sum**: number session boundaries to generate monotonically increasing session indices
4. **Aggregate**: compute metrics per (user_id, session_index)

### Step 2: The Sessionization Query

```sql
-- Step 1: Calculate time gap from previous event per user
WITH events_with_gap AS (
    SELECT
        user_id,
        event_time,
        page_url,
        LAG(event_time) OVER (
            PARTITION BY user_id
            ORDER BY event_time
        ) AS prev_event_time,
        DATEDIFF(
            MINUTE,
            LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time),
            event_time
        ) AS minutes_since_last_event
    FROM clickstream
),

-- Step 2: Flag session boundaries (gap > 30 min OR first event)
events_with_boundary AS (
    SELECT
        *,
        CASE
            WHEN prev_event_time IS NULL THEN 1          -- first event for user
            WHEN minutes_since_last_event > 30 THEN 1   -- new session
            ELSE 0
        END AS is_new_session
    FROM events_with_gap
),

-- Step 3: Assign session index via cumulative sum of boundaries
events_with_session_idx AS (
    SELECT
        *,
        SUM(is_new_session) OVER (
            PARTITION BY user_id
            ORDER BY event_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_idx
    FROM events_with_boundary
),

-- Step 4: Create globally unique session_id
events_with_session_id AS (
    SELECT
        *,
        CONCAT(user_id, '_', session_idx) AS session_id
    FROM events_with_session_idx
)

-- Step 5: Aggregate per session
SELECT
    user_id,
    session_id,
    MIN(event_time)                                              AS session_start,
    MAX(event_time)                                             AS session_end,
    DATEDIFF(MINUTE, MIN(event_time), MAX(event_time))         AS session_duration_minutes,
    COUNT(*)                                                    AS page_count,
    COLLECT_LIST(page_url)                                      AS pages_visited
FROM events_with_session_id
GROUP BY user_id, session_id
ORDER BY user_id, session_start;
```

### Step 3: PySpark Implementation with Performance Tuning

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lag, col, when, sum as spark_sum, min as spark_min,
    max as spark_max, count, concat, lit, collect_list,
    unix_timestamp
)
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .config("spark.sql.shuffle.partitions", "2000") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# Read — assume already partitioned by date in storage
df = spark.read.parquet("s3://bucket/clickstream/")

# Step 1 & 2: Gap detection
user_time_window = Window.partitionBy("user_id").orderBy("event_time")

df_gaps = df.withColumn(
    "prev_event_time",
    lag("event_time").over(user_time_window)
).withColumn(
    "gap_seconds",
    unix_timestamp("event_time") - unix_timestamp("prev_event_time")
).withColumn(
    "is_new_session",
    when(col("prev_event_time").isNull(), 1)
    .when(col("gap_seconds") > 1800, 1)   # 30 min = 1800 seconds
    .otherwise(0)
)

# Step 3: Cumulative session index
session_window = Window.partitionBy("user_id") \
    .orderBy("event_time") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_sessions = df_gaps.withColumn(
    "session_idx",
    spark_sum("is_new_session").over(session_window)
).withColumn(
    "session_id",
    concat(col("user_id"), lit("_"), col("session_idx").cast("string"))
)

# Step 4: Aggregate
result = df_sessions.groupBy("user_id", "session_id").agg(
    spark_min("event_time").alias("session_start"),
    spark_max("event_time").alias("session_end"),
    count("*").alias("page_count"),
    ((unix_timestamp(spark_max("event_time")) -
      unix_timestamp(spark_min("event_time"))) / 60).alias("session_duration_minutes")
)

result.write.partitionBy("user_id").parquet("s3://bucket/sessions/")
```

### Step 4: Performance Considerations for 10 Billion Rows

**Problem 1: Data skew in PARTITION BY user_id**

Some users have millions of events (bots, power users). Their partition will be huge and run on a single task:

```python
# Detect skewed users
df.groupBy("user_id").count().orderBy(col("count").desc()).show(20)

# Option A: Filter bots/power users before sessionization
BOT_THRESHOLD = 100_000  # events per day
user_counts = df.groupBy("user_id").count()
valid_users = user_counts.filter(col("count") < BOT_THRESHOLD).select("user_id")
df_clean = df.join(valid_users, "user_id")

# Option B: Use AQE skew join handling
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
```

**Problem 2: Window function requires sorting all events per user in memory**

For users with 10M+ events, the window sort stage can OOM:

```python
# Pre-partition the data by user_id before the window operation
# This avoids a global shuffle + sort
df_repartitioned = df.repartition(2000, "user_id") \
                     .sortWithinPartitions("user_id", "event_time")

# Now window operations are local — no shuffle needed
# (Spark may still shuffle for window if not partitioned correctly)
```

**Problem 3: `collect_list` of page_urls can be enormous**

```python
# Instead of collect_list (unbounded memory), compute only what you need
# Use array_join or limit the list
from pyspark.sql.functions import slice, collect_list

# Only keep first 100 pages per session
result = df_sessions.groupBy("user_id", "session_id").agg(
    spark_min("event_time").alias("session_start"),
    spark_max("event_time").alias("session_end"),
    count("*").alias("page_count"),
    slice(collect_list("page_url"), 1, 100).alias("first_100_pages")
)
```

**Recommended Spark config for this job:**

```python
spark.conf.set("spark.sql.shuffle.partitions", "2000")         # 10B / 5M per partition
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.executor.memory", "32g")
spark.conf.set("spark.executor.cores", "5")
spark.conf.set("spark.sql.windowExec.buffer.spill.threshold", "4096")  # spill window buffers to disk
```

### Output Schema

```
root
 |-- user_id: string
 |-- session_id: string         (user_id + "_" + sequential int)
 |-- session_start: timestamp
 |-- session_end: timestamp
 |-- session_duration_minutes: double
 |-- page_count: long
```

</details>

</article>
