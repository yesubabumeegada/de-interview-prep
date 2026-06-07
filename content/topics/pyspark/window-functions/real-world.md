---
title: "PySpark Window Functions - Real-World Production Examples"
topic: pyspark
subtopic: window-functions
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [pyspark, window-functions, production, sessionization, scd, analytics]
---

# PySpark Window Functions — Real-World Production Examples

## Pattern 1: Customer Lifetime Value with Cohort Analysis

```python
from pyspark.sql.functions import (
    col, sum as spark_sum, count, datediff, min as spark_min,
    max as spark_max, months_between, row_number
)
from pyspark.sql.window import Window

# Calculate per-customer metrics using windows
user_window = Window.partitionBy("customer_id").orderBy("order_date")
user_full = Window.partitionBy("customer_id")

customer_metrics = orders \
    .withColumn("order_number", row_number().over(user_window)) \
    .withColumn("first_order", spark_min("order_date").over(user_full)) \
    .withColumn("last_order", spark_max("order_date").over(user_full)) \
    .withColumn("total_orders", count("*").over(user_full)) \
    .withColumn("total_spend", spark_sum("amount").over(user_full)) \
    .withColumn("running_spend", spark_sum("amount").over(
        user_window.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )) \
    .withColumn("days_since_first", datediff("order_date", "first_order")) \
    .withColumn("cohort_month", date_trunc("month", col("first_order")))

# Get one row per customer (latest metrics)
customer_summary = customer_metrics \
    .withColumn("rn", row_number().over(
        Window.partitionBy("customer_id").orderBy(col("order_date").desc())
    )) \
    .filter("rn = 1") \
    .select(
        "customer_id", "cohort_month", "total_orders", "total_spend",
        "first_order", "last_order",
        datediff("last_order", "first_order").alias("lifetime_days"),
        (col("total_spend") / col("total_orders")).alias("avg_order_value")
    )
```

---

## Pattern 2: Churn Detection Using Activity Windows

```python
from pyspark.sql.functions import lag, datediff, when, current_date

# Detect churned users: no activity in last 30 days after being active
activity_window = Window.partitionBy("user_id").orderBy("activity_date")

user_activity = daily_activity \
    .withColumn("prev_activity", lag("activity_date", 1).over(activity_window)) \
    .withColumn("days_inactive", datediff("activity_date", "prev_activity")) \
    .withColumn("max_inactivity", spark_max("days_inactive").over(
        Window.partitionBy("user_id")
    ))

# Current status: last activity date vs today
last_seen_window = Window.partitionBy("user_id").orderBy(col("activity_date").desc())

churn_status = user_activity \
    .withColumn("rn", row_number().over(last_seen_window)) \
    .filter("rn = 1") \
    .withColumn("days_since_last_activity", datediff(current_date(), col("activity_date"))) \
    .withColumn("churn_status",
        when(col("days_since_last_activity") > 30, "Churned")
        .when(col("days_since_last_activity") > 14, "At Risk")
        .otherwise("Active")
    )
```

---

## Pattern 3: SCD Type 2 Change Detection

```python
from pyspark.sql.functions import lag, when, col, lead, lit, current_date

# Detect changes in dimension attributes over time
change_window = Window.partitionBy("customer_id").orderBy("snapshot_date")

# Compare each snapshot to previous to find what changed
changes = customer_snapshots \
    .withColumn("prev_segment", lag("segment", 1).over(change_window)) \
    .withColumn("prev_city", lag("city", 1).over(change_window)) \
    .withColumn("prev_email", lag("email", 1).over(change_window)) \
    .withColumn("has_change",
        when(
            (col("segment") != col("prev_segment")) |
            (col("city") != col("prev_city")) |
            (col("email") != col("prev_email")),
            True
        ).otherwise(False)
    )

# Keep only change events (for SCD Type 2 loading)
change_events = changes.filter("has_change = true OR prev_segment IS NULL") \
    .withColumn("effective_from", col("snapshot_date")) \
    .withColumn("effective_to",
        lead("snapshot_date", 1).over(change_window)
    ) \
    .withColumn("effective_to",
        when(col("effective_to").isNull(), lit("9999-12-31").cast("date"))
        .otherwise(col("effective_to"))
    ) \
    .withColumn("is_current",
        when(col("effective_to") == "9999-12-31", True).otherwise(False)
    )
```

---

## Pattern 4: Funnel Analysis with Drop-Off

```python
from pyspark.sql.functions import min as spark_min, max as spark_max, countDistinct

# Define funnel steps with their expected order
funnel_steps = ["page_view", "add_to_cart", "checkout_start", "payment", "purchase"]

# For each user, find if and when they reached each funnel step
user_funnel_window = Window.partitionBy("user_id", "session_id")

# Assign step numbers
from pyspark.sql.functions import when as spark_when

events_with_steps = events
for i, step in enumerate(funnel_steps):
    events_with_steps = events_with_steps.withColumn(
        f"reached_{step}",
        spark_max(when(col("event_type") == step, 1).otherwise(0)).over(user_funnel_window)
    )

# Calculate conversion at each step
funnel_metrics = events_with_steps \
    .select("user_id", "session_id", *[f"reached_{s}" for s in funnel_steps]) \
    .distinct()

# Aggregate funnel
from pyspark.sql.functions import sum as spark_sum

funnel_summary = funnel_metrics.select(
    *[spark_sum(f"reached_{step}").alias(step) for step in funnel_steps]
)

funnel_summary.show()
# page_view: 50000, add_to_cart: 15000, checkout: 8000, payment: 6500, purchase: 5200
```

---

## Pattern 5: Time-Weighted Average for Financial Data

```python
from pyspark.sql.functions import lead, datediff, sum as spark_sum, col

# Calculate time-weighted average balance (not simple average of snapshots)
balance_window = Window.partitionBy("account_id").orderBy("effective_date")

time_weighted = account_balances \
    .withColumn("next_date", lead("effective_date", 1).over(balance_window)) \
    .withColumn("next_date",
        when(col("next_date").isNull(), current_date()).otherwise(col("next_date"))
    ) \
    .withColumn("days_at_balance", datediff("next_date", "effective_date")) \
    .withColumn("weighted_balance", col("balance") * col("days_at_balance"))

# Time-weighted average per account
twab = time_weighted.groupBy("account_id").agg(
    (spark_sum("weighted_balance") / spark_sum("days_at_balance")).alias("time_weighted_avg_balance"),
    spark_sum("days_at_balance").alias("total_days")
)
```

> **Why time-weighted:** A simple average of daily balances is wrong if balance changes are not evenly distributed. An account at $10K for 29 days and $1M for 1 day should NOT average to $505K. Time-weighted average = $10K * 29/30 + $1M * 1/30 = $43K (much more accurate).

---

## Pattern 6: Event Attribution (Multi-Touch)

```python
from pyspark.sql.functions import count, col, row_number, sum as spark_sum

# Attribute conversions to marketing touchpoints using window functions
touch_window = Window.partitionBy("user_id", "conversion_id") \
    .orderBy(col("touch_time").desc())

# Last-touch: most recent touchpoint gets 100% credit
last_touch = touchpoints \
    .withColumn("rn", row_number().over(touch_window)) \
    .filter("rn = 1") \
    .groupBy("campaign_id") \
    .agg(
        spark_sum("conversion_value").alias("attributed_revenue"),
        count("*").alias("conversions")
    )

# Linear attribution: split equally among all touchpoints for a conversion
touch_count_window = Window.partitionBy("user_id", "conversion_id")

linear = touchpoints \
    .withColumn("touch_count", count("*").over(touch_count_window)) \
    .withColumn("attributed_value", col("conversion_value") / col("touch_count")) \
    .groupBy("campaign_id") \
    .agg(
        spark_sum("attributed_value").alias("attributed_revenue"),
        count("*").alias("total_touches")
    )
```

---

## Production Performance Tips

| Scenario | Problem | Solution |
|----------|---------|----------|
| Window on 1B rows | Massive shuffle | Filter to relevant subset first |
| Multiple windows, same partition | Multiple shuffles | Reuse same partitionBy |
| Skewed partition key | One executor overloaded | Salt the key or filter outliers |
| Large partition (1 user with 10M rows) | Sort spills to disk | Increase executor memory or cap partition size |
| Window + join after | Double shuffle | Apply window, cache, then join |
| Complex window on streaming | State management overhead | Use watermarks to bound state |

---

## Interview Tips

> **Tip 1:** "Implement a real-world sessionization pipeline" — "Filter to relevant time range first (reduce data volume). Apply lag() to detect gaps per user. Flag boundaries with CASE WHEN. Cumulative sum assigns session IDs. Aggregate per session for metrics. The whole thing runs as a single Spark job with one shuffle on user_id."

> **Tip 2:** "How do you handle the SCD Type 2 change detection?" — "lag() on each tracked column to compare current vs previous snapshot. If any column differs, it's a change event. lead() on snapshot_date gives effective_to. Filter to only change rows. This generates the version history directly from periodic snapshots."

> **Tip 3:** "What's your approach to window function performance?" — "Three rules: (1) Filter before windowing to reduce data volume. (2) Reuse the same partitionBy key across multiple window functions to avoid multiple shuffles. (3) Cache the repartitioned DataFrame if applying many independent window operations on the same key. Monitor for partition skew in the Spark UI."
