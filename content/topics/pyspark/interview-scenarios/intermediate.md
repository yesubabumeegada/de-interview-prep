---
title: "Interview Scenarios - Intermediate"
topic: pyspark
subtopic: interview-scenarios
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [pyspark, interview, sessionization, window-functions, top-n, pivot, nested-json]
---

# PySpark Interview Scenarios — Intermediate

## The Mid-Level Interview Focus

Mid-level PySpark interviews test whether you can apply Window functions correctly, handle common data transformation patterns (sessionization, top-N, pivot), and work with complex data structures (nested JSON, arrays). These patterns come up repeatedly — learn them until they're fluent.

---

## Pattern 1: Sessionization (Gap-Based Session IDs)

**The problem:** Given a clickstream table with `(user_id, event_time)`, assign a `session_id` where a new session starts whenever the gap between consecutive events for the same user exceeds 30 minutes.

```python
from pyspark.sql.functions import col, lag, unix_timestamp, when, sum as spark_sum, concat_ws
from pyspark.sql.window import Window

# Step 1: Calculate gap between consecutive events per user
user_window = Window.partitionBy("user_id").orderBy("event_time")

df = df.withColumn(
    "prev_event_time",
    lag("event_time").over(user_window)
)

df = df.withColumn(
    "gap_seconds",
    unix_timestamp("event_time") - unix_timestamp("prev_event_time")
)

# Step 2: Flag session starts (first event OR gap > 30 minutes)
df = df.withColumn(
    "is_session_start",
    when(col("prev_event_time").isNull(), 1)   # first event for this user
    .when(col("gap_seconds") > 1800, 1)         # gap > 30 minutes
    .otherwise(0)
)

# Step 3: Cumulative sum of session starts = monotonically increasing session counter
df = df.withColumn(
    "session_counter",
    spark_sum("is_session_start").over(user_window)
)

# Step 4: Combine user_id + counter to make session_id globally unique
df = df.withColumn(
    "session_id",
    concat_ws("_", col("user_id").cast("string"), col("session_counter").cast("string"))
)

df = df.drop("prev_event_time", "gap_seconds", "is_session_start", "session_counter")
```

**Why `sum().over(window)` works here:** Each `is_session_start=1` increments the counter. Events within the same session keep the same counter value. The result is a dense integer that perfectly groups events into sessions.

---

## Pattern 2: Top-N Per Group

**The problem:** Return the top 3 products by revenue for each category.

```python
from pyspark.sql.functions import col, row_number, dense_rank
from pyspark.sql.window import Window

# Method 1: row_number (no ties — strictly N rows per group)
category_window = Window.partitionBy("category").orderBy(col("revenue").desc())

df_top3 = (
    df
    .withColumn("rn", row_number().over(category_window))
    .filter(col("rn") <= 3)
    .drop("rn")
)

# Method 2: dense_rank (includes ties at the Nth position)
df_top3_with_ties = (
    df
    .withColumn("rnk", dense_rank().over(category_window))
    .filter(col("rnk") <= 3)
    .drop("rnk")
)
```

**When to use which:**
- `row_number()` — exactly N rows per partition, ties broken arbitrarily
- `rank()` — ties get the same rank, next rank skips (1, 1, 3)
- `dense_rank()` — ties get the same rank, no skipping (1, 1, 2) — best for "top-N including ties"

---

## Pattern 3: Running Totals and Moving Averages

```python
from pyspark.sql.functions import sum as spark_sum, avg
from pyspark.sql.window import Window

# Running total (cumulative sum up to and including current row)
cumulative_window = (
    Window.partitionBy("user_id")
    .orderBy("transaction_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

df = df.withColumn("running_total", spark_sum("amount").over(cumulative_window))

# 7-day moving average (current row and 6 preceding rows)
moving_avg_window = (
    Window.partitionBy("store_id")
    .orderBy("sale_date")
    .rowsBetween(-6, 0)  # -6 = 6 rows before current, 0 = current row
)

df = df.withColumn("7day_avg_revenue", avg("revenue").over(moving_avg_window))
```

---

## Pattern 4: Deduplication Keeping Latest Record

A classic interview question combining Window functions with deduplication:

```python
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Keep the most recent record per user_id (by updated_at descending)
dedup_window = (
    Window.partitionBy("user_id")
    .orderBy(col("updated_at").desc(), col("record_id").desc())  # secondary sort for ties
)

df_latest = (
    df
    .withColumn("rn", row_number().over(dedup_window))
    .filter(col("rn") == 1)
    .drop("rn")
)
```

**Interview tip:** Adding a secondary sort key (`record_id`) makes deduplication deterministic when `updated_at` has ties. Mention this explicitly.

---

## Pattern 5: Flattening Nested JSON

```python
from pyspark.sql.functions import col, explode, from_json, get_json_object
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

# Method 1: Dot notation for struct fields
df_flat = df.select(
    "order_id",
    col("address.city").alias("city"),
    col("address.zip_code").alias("zip_code"),
)

# Method 2: explode for arrays
df_exploded = df.withColumn("item", explode("items")).select(
    "order_id",
    col("item.product_id"),
    col("item.quantity"),
    col("item.unit_price"),
)

# Method 3: getItem for map columns
df = df.withColumn("utm_source", col("utm_params").getItem("source"))

# Method 4: from_json for JSON strings (e.g., a STRING column containing JSON)
metadata_schema = StructType([
    StructField("campaign_id", LongType()),
    StructField("ab_variant", StringType()),
])
df = df.withColumn("metadata", from_json(col("metadata_json"), metadata_schema))
df = df.withColumn("campaign_id", col("metadata.campaign_id"))
```

---

## Pattern 6: Pivot and Unpivot

### Pivot (rows → columns)

```python
# Pivot: one row per (user, month) → one row per user with month columns
df_pivoted = (
    df
    .groupBy("user_id")
    .pivot("month", ["2024-01", "2024-02", "2024-03"])  # specify values to avoid scan
    .agg({"revenue": "sum"})
)
# Columns: user_id | 2024-01 | 2024-02 | 2024-03
```

**Always specify the pivot values explicitly** — omitting them forces Spark to scan all data to find unique values, which is a full extra pass.

### Unpivot / stack (columns → rows)

```python
# Stack: month columns → (month, revenue) rows using SQL stack() function
df_unpivoted = df_pivoted.selectExpr(
    "user_id",
    "stack(3, '2024-01', `2024-01`, '2024-02', `2024-02`, '2024-03', `2024-03`) as (month, revenue)"
)
```

---

## Pattern 7: Null-Safe Join (`eqNullSafe`)

Standard joins drop rows where the join key is null on either side. When nulls should match nulls:

```python
# Standard join: null keys never match (null != null)
df_standard = left.join(right, on="key", how="inner")  # rows with null keys are lost

# Null-safe join: null matches null
df_null_safe = left.join(
    right,
    left["key"].eqNullSafe(right["key"]),
    "inner"
)

# Or in SQL
df.createOrReplaceTempView("left_tbl")
right.createOrReplaceTempView("right_tbl")
spark.sql("SELECT * FROM left_tbl l JOIN right_tbl r ON l.key <=> r.key")
```

---

## Key Takeaways

- Sessionization = `lag` + gap flag + `cumsum` — memorize this pattern.
- For top-N, use `row_number()` for strict count, `dense_rank()` for ties-included.
- `rowsBetween(-6, 0)` is a 7-row window including current; `rangeBetween` uses value distance, not row count.
- Always specify pivot values explicitly — omitting them causes an extra full scan.
- `eqNullSafe` (or `<=>` in SQL) is the null-safe join operator — mention it when null join keys are possible.
