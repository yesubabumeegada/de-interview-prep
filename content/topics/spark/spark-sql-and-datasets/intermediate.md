---
title: "Spark SQL & Datasets — Intermediate"
topic: spark
subtopic: spark-sql-and-datasets
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [spark, sql, window-functions, higher-order-functions, lateral-view, explode, pivot, cte]
---

# Spark SQL & Datasets — Intermediate

## Window Functions

Window functions compute a result for each row based on a set of related rows:

```python
from pyspark.sql import functions as F, Window

# Define the window specification
window = Window.partitionBy("region").orderBy("order_date")

# Running total
df = df.withColumn("running_total",
    F.sum("amount").over(window))

# Rank within partition
df = df.withColumn("rank",
    F.rank().over(Window.partitionBy("region").orderBy(F.desc("amount"))))

# Row number (unique, no ties)
df = df.withColumn("row_num",
    F.row_number().over(Window.partitionBy("region").orderBy(F.desc("amount"))))

# Dense rank (no gaps on ties)
df = df.withColumn("dense_rank",
    F.dense_rank().over(Window.partitionBy("region").orderBy(F.desc("amount"))))

# Lag/Lead: access previous/next row
df = df.withColumn("prev_amount",
    F.lag("amount", 1).over(Window.partitionBy("customer_id").orderBy("order_date")))

df = df.withColumn("next_amount",
    F.lead("amount", 1, 0.0).over(    # 0.0 default if no next row
        Window.partitionBy("customer_id").orderBy("order_date")))

# Frame specification: rolling 7-day sum
window_7d = Window.partitionBy("region") \
    .orderBy(F.col("order_date").cast("timestamp").cast("long")) \
    .rangeBetween(-7 * 86400, 0)    # 7 days in seconds
df = df.withColumn("rolling_7d_sum", F.sum("amount").over(window_7d))
```

---

## Higher-Order Functions (Spark 2.4+)

Process array and map columns without explode/collect:

```python
from pyspark.sql import functions as F

# Sample data with arrays
df = spark.createDataFrame([
    ("O1", [10.0, 25.0, 5.0], {"apple": 2, "banana": 3}),
    ("O2", [100.0, 50.0],      {"cherry": 1}),
], ["order_id", "item_prices", "item_counts"])

# transform: apply function to each array element
df.withColumn("doubled_prices",
    F.transform(F.col("item_prices"), lambda x: x * 2))

# filter: keep elements matching predicate
df.withColumn("expensive_items",
    F.filter(F.col("item_prices"), lambda x: x > 20))

# aggregate: fold array to single value
df.withColumn("total_price",
    F.aggregate(F.col("item_prices"), F.lit(0.0), lambda acc, x: acc + x))

# forall / exists
df.withColumn("all_expensive",
    F.forall(F.col("item_prices"), lambda x: x > 5))

df.withColumn("has_expensive",
    F.exists(F.col("item_prices"), lambda x: x > 50))

# map_keys / map_values
df.withColumn("items", F.map_keys(F.col("item_counts")))

# zip_with: combine two arrays
df.withColumn("price_with_count",
    F.zip_with(F.col("item_prices"), F.array(F.lit(1)), lambda p, c: p * c))
```

---

## Explode and Lateral View

```python
from pyspark.sql import functions as F

# explode: array → rows (null arrays produce no rows by default)
df_exploded = df.select("order_id", F.explode("item_prices").alias("price"))

# explode_outer: null arrays produce one row with null value
df_outer = df.select("order_id", F.explode_outer("item_prices").alias("price"))

# posexplode: includes position index
df_pos = df.select("order_id",
    F.posexplode("item_prices").alias("idx", "price"))

# Explode maps → (key, value) rows
df_kv = df.select("order_id",
    F.explode("item_counts").alias("item", "count"))

# SQL equivalent with LATERAL VIEW:
spark.sql("""
    SELECT order_id, price
    FROM orders
    LATERAL VIEW explode(item_prices) tmp AS price
""")
```

---

## PIVOT and UNPIVOT

```python
# Pivot: rows → columns
pivot_df = df.groupBy("region").pivot("status").agg(F.sum("amount"))
# Creates columns: region | completed | pending | cancelled

# Performance: specifying values avoids extra scan to discover distinct values
pivot_df = df.groupBy("region") \
    .pivot("status", ["completed", "pending", "cancelled"]) \
    .agg(F.sum("amount").alias("revenue"))

# SQL pivot:
spark.sql("""
    SELECT *
    FROM (SELECT region, status, amount FROM orders)
    PIVOT (SUM(amount) FOR status IN ('completed', 'pending', 'cancelled'))
""")

# Stack (UNPIVOT): columns → rows
stacked = spark.sql("""
    SELECT region,
           stack(3,
               'completed', completed,
               'pending', pending,
               'cancelled', cancelled
           ) AS (status, revenue)
    FROM pivot_result
""")
```

---

## CTEs and Subqueries

```python
# Common Table Expressions (CTEs)
spark.sql("""
    WITH
    regional_totals AS (
        SELECT region, SUM(amount) as total
        FROM orders
        WHERE status = 'completed'
        GROUP BY region
    ),
    global_total AS (
        SELECT SUM(total) as grand_total
        FROM regional_totals
    )
    SELECT
        r.region,
        r.total,
        ROUND(r.total / g.grand_total * 100, 2) as pct
    FROM regional_totals r
    CROSS JOIN global_total g
    ORDER BY pct DESC
""")

# Scalar subqueries
spark.sql("""
    SELECT *,
        amount / (SELECT AVG(amount) FROM orders WHERE status = 'completed') AS ratio
    FROM orders
""")

# IN / EXISTS with subqueries
spark.sql("""
    SELECT *
    FROM orders
    WHERE customer_id IN (
        SELECT id FROM customers WHERE tier = 'platinum'
    )
""")
```

---

## UDFs: User-Defined Functions

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, DoubleType

# Python UDF (slower — JVM ↔ Python serialization per row)
@udf(returnType=StringType())
def classify_amount(amount):
    if amount is None:
        return "unknown"
    if amount < 100:
        return "small"
    if amount < 1000:
        return "medium"
    return "large"

df.withColumn("size", classify_amount(F.col("amount")))

# Pandas UDF (fast — Arrow batch transfer, vectorized)
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(DoubleType())
def normalize_amount(s: pd.Series) -> pd.Series:
    return (s - s.mean()) / s.std()

df.withColumn("norm", normalize_amount(F.col("amount")))

# UDFs block Catalyst optimization!
# Prefer built-in functions whenever possible:
# BAD:  df.withColumn("year", get_year_udf(F.col("date")))
# GOOD: df.withColumn("year", F.year(F.col("date")))
```

---

## Interview Tips

> **Tip 1:** "How do window functions differ from GROUP BY?" — GROUP BY collapses rows into one row per group. Window functions compute a result for each row based on neighboring rows (the "window") — the original row count is preserved. You can compute running totals, rankings, or compare each row to its neighbors without losing row granularity. Common interview question: get the top N rows per group — use `row_number()` over a partition + filter `WHERE rn <= N`.

> **Tip 2:** "Why are Python UDFs slow and what's the alternative?" — Python UDFs require serializing each row from JVM to Python, processing, then deserializing back — typically 10-100× slower than built-in functions. Pandas UDFs (pandas_udf) use Apache Arrow to transfer whole column batches at once, reducing serialization overhead to near zero. Always check if a built-in function (`F.year()`, `F.regexp_replace()`, etc.) covers the use case before writing a UDF.

> **Tip 3:** "Explain the difference between `explode` and `explode_outer`." — `explode` converts an array/map column into multiple rows, but rows with null or empty arrays produce zero rows (the row disappears). `explode_outer` keeps the row with a null value for the exploded column, preserving row count for null arrays. Use `explode_outer` when you need to retain parent records even when the array is empty (similar to SQL LEFT JOIN vs INNER JOIN).
