---
title: "Interview Scenarios - Scenario Questions"
topic: pyspark
subtopic: interview-scenarios
content_type: scenario_question
tags: [pyspark, interview, deduplication, sessionization, skew, window-functions, optimization]
---

# PySpark Interview Scenarios — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Deduplicate a User Events Table Keeping the Most Recent Record

You have a `user_events` table with the following columns:

```
user_id      LONG
event_type   STRING
event_time   TIMESTAMP
page_url     STRING
device_type  STRING
```

The table has duplicate records for some users (same `user_id`, different or same `event_time`). Write a PySpark job that returns **one row per `user_id`**, keeping the row with the **most recent `event_time`**. If two rows have the same `event_time`, keep the one with the longer `page_url` as a tiebreaker.

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, length
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("DeduplicateUserEvents").getOrCreate()

df = spark.read.format("delta").table("user_events")

# Define the Window: per user, order by event_time DESC, then page_url length DESC (tiebreaker)
dedup_window = (
    Window
    .partitionBy("user_id")
    .orderBy(
        col("event_time").desc(),
        length(col("page_url")).desc()  # tiebreaker: longer URL wins
    )
)

df_deduped = (
    df
    .withColumn("rn", row_number().over(dedup_window))
    .filter(col("rn") == 1)
    .drop("rn")
)

# Validate: should be exactly one row per user
# (Run this in development; comment out in production for performance)
assert df_deduped.count() == df_deduped.select("user_id").distinct().count(), \
    "Deduplication failed: multiple rows per user_id"

df_deduped.write.format("delta").mode("overwrite").saveAsTable("user_events_deduped")

df_deduped.show(5)
```

**Why `row_number()` instead of `dropDuplicates(["user_id"])`:**
- `dropDuplicates(["user_id"])` keeps an arbitrary row — you have no control over which one survives.
- `row_number()` with an explicit `orderBy` gives you a deterministic, reproducible result.
- The tiebreaker (`length(page_url).desc()`) makes the result fully deterministic even for exact timestamp ties.

**Null handling note:** If `event_time` can be null, null values sort LAST in descending order. If you need nulls to lose, the `orderBy(col("event_time").desc())` behavior is already correct — nulls will have `rn > 1` and be filtered out.

</details>
</article>

---

<article data-difficulty="mid">

## Scenario 2: Sessionize Clickstream Data

You have a `clickstream` table:

```
user_id      LONG
event_time   TIMESTAMP
page_url     STRING
event_type   STRING
```

A **session** is a sequence of events for the same user where no two consecutive events are separated by more than **30 minutes**. Write a PySpark job that:

1. Assigns a unique `session_id` to each event (format: `{user_id}_{session_number}`)
2. Calculates per-session aggregates: `session_start`, `session_end`, `session_duration_minutes`, `page_count`

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lag, unix_timestamp, when, sum as spark_sum,
    concat_ws, min as spark_min, max as spark_max,
    count, round as spark_round
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Sessionization").getOrCreate()

df = spark.read.format("delta").table("clickstream")

# ------- STEP 1: Calculate time gap to previous event per user -------
user_time_window = Window.partitionBy("user_id").orderBy("event_time")

df = df.withColumn(
    "prev_event_time",
    lag("event_time").over(user_time_window)
)

df = df.withColumn(
    "gap_seconds",
    unix_timestamp("event_time") - unix_timestamp("prev_event_time")
)

# ------- STEP 2: Flag where new sessions start -------
df = df.withColumn(
    "is_session_start",
    when(col("prev_event_time").isNull(), 1)  # first event for this user
    .when(col("gap_seconds") > 1800, 1)        # gap > 30 minutes (30 * 60 = 1800 seconds)
    .otherwise(0)
)

# ------- STEP 3: Cumulative sum = monotonically increasing session counter -------
df = df.withColumn(
    "session_number",
    spark_sum("is_session_start").over(user_time_window)
)

# ------- STEP 4: Build globally unique session_id -------
df = df.withColumn(
    "session_id",
    concat_ws("_", col("user_id").cast("string"), col("session_number").cast("string"))
)

# Drop intermediate columns
df_with_sessions = df.drop("prev_event_time", "gap_seconds", "is_session_start", "session_number")

df_with_sessions.show(10, truncate=False)

# ------- STEP 5: Calculate per-session aggregates -------
df_session_stats = (
    df_with_sessions
    .groupBy("user_id", "session_id")
    .agg(
        spark_min("event_time").alias("session_start"),
        spark_max("event_time").alias("session_end"),
        spark_round(
            (unix_timestamp(spark_max("event_time")) - unix_timestamp(spark_min("event_time"))) / 60.0,
            2
        ).alias("session_duration_minutes"),
        count("*").alias("page_count"),
    )
    .orderBy("user_id", "session_start")
)

df_session_stats.show(10)

# Write session-level data
df_session_stats.write.format("delta").mode("overwrite").saveAsTable("clickstream_sessions")
```

**Key design decisions:**

- The `lag + cumsum` pattern is the canonical sessionization approach. The cumulative sum is clever: every time `is_session_start=1`, the counter increments, and all subsequent events inherit the same counter value until the next session boundary.

- `unix_timestamp()` is used for arithmetic (subtraction gives seconds) since you cannot subtract `TimestampType` columns directly in PySpark.

- Session duration is `(max_time - min_time) / 60` — this is the wall-clock duration of the session. A single-event session has duration 0.

- The `session_id` uses `concat_ws("_", user_id, session_number)` rather than just `session_number` to ensure global uniqueness across users.

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3: Handle Skewed Orders Table for Revenue per Customer

You have an `orders` table (500GB, ~1 billion rows) with the following distribution problem: **80% of all rows have `customer_id = "GUEST"`** (unauthenticated users). You need to compute **total revenue per customer**.

Write an optimized PySpark job that:
1. Handles the skew on `customer_id = "GUEST"`
2. Produces correct results (total revenue per customer including GUEST)
3. Shows how to inspect the execution plan
4. Discusses the trade-offs of your approach

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, rand, floor, lit, concat_ws,
    sum as spark_sum
)

spark = (
    SparkSession.builder
    .appName("SkewedOrdersRevenue")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.shuffle.partitions", "2000")
    .getOrCreate()
)

df_orders = (
    spark.read
    .format("delta")
    .table("orders")
    .select("order_id", "customer_id", "revenue", "order_date")
)

# ------- INSPECT the skew first -------
print("Top 10 customer_id by count:")
df_orders.groupBy("customer_id").count().orderBy(col("count").desc()).show(10)
# customer_id=GUEST has ~800M of 1B rows → massive skew

# ------- SALT KEY APPROACH -------

SALT_FACTOR = 100  # distribute GUEST across 100 sub-partitions

# Step 1: Add a salt suffix to GUEST; non-GUEST keys get salt "0" (unchanged)
df_salted = df_orders.withColumn(
    "salted_customer_id",
    when(
        col("customer_id") == "GUEST",
        concat_ws("_", lit("GUEST"), floor(rand() * SALT_FACTOR).cast("string"))
    ).otherwise(col("customer_id"))  # non-skewed keys: no salt
)

# Step 2: First aggregation on salted key
# Each GUEST_0, GUEST_1, ... GUEST_99 is now processed by a different executor
df_agg_partial = (
    df_salted
    .groupBy("salted_customer_id", "customer_id")  # keep original for second agg
    .agg(spark_sum("revenue").alias("partial_revenue"))
)

# Step 3: Second aggregation to collapse salts back to original customer_id
# Sum of partial sums = total sum (commutative, associative — always correct)
df_revenue = (
    df_agg_partial
    .groupBy("customer_id")
    .agg(spark_sum("partial_revenue").alias("total_revenue"))
)

df_revenue.show(20)

# ------- INSPECT THE PLAN -------
# Before salting (shows the skewed SortMergeJoin or Exchange)
df_revenue.explain(mode="formatted")

# Metrics to compare:
# - With skew: one executor with 800M rows, others with ~100K rows → 1 straggler
# - With salt: each executor gets ~8M rows → balanced, 100x faster

# ------- WRITE OUTPUT -------
(
    df_revenue
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("gold.customer_revenue")
)
```

**Trade-offs and discussion points:**

**Why `SALT_FACTOR = 100`?**
- GUEST has 800M rows. With 100 salts, each GUEST sub-partition has ~8M rows.
- Non-GUEST customers have at most ~100K rows each.
- This balances the partition sizes so no executor is a straggler.
- Too high a salt factor (e.g., 10,000) creates too many small partitions and increases overhead.

**Why does this only work for commutative aggregations?**
- `SUM`, `COUNT`, `MAX`, `MIN` are all commutative and associative — partial results can be re-aggregated.
- `AVG` is NOT — you cannot average partial averages. Instead, use `SUM(revenue) / COUNT(*)` and aggregate those separately.
- `MEDIAN` and `PERCENTILE` are not commutative — salting doesn't work; use approximate functions (`approx_percentile`) instead.

**AQE as an alternative:**
```python
# AQE's skewJoin handles this automatically at runtime (Spark 3.0+)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

# AQE detects skewed partitions at runtime and splits them without code changes
# Downside: only works for joins, not groupBy. Manual salting covers groupBy too.
```

**When to use manual salt vs. AQE:**
- Use AQE skewJoin for join skew — zero code changes, automatic.
- Use manual salt for groupBy/agg skew (AQE doesn't handle this).
- Use both for skewed joins where you also need to aggregate the result.

</details>
</article>
