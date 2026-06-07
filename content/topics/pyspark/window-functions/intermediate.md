---
title: "PySpark Window Functions - Intermediate"
topic: pyspark
subtopic: window-functions
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [pyspark, window-functions, frames, sessionization, dedup, gap-detection]
---

# PySpark Window Functions — Intermediate Concepts

## Window Frame Types

The frame defines WHICH rows within the partition the function operates on.

### Row-Based Frame (rowsBetween)

Counts physical rows relative to current position:

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, sum, col

# 3-row moving average (current row + 2 preceding)
window_3row = Window.partitionBy("department") \
    .orderBy("hire_date") \
    .rowsBetween(-2, 0)  # 2 rows before current, to current row

df.withColumn("moving_avg_3", avg("salary").over(window_3row)).show()
```

**Frame boundaries:**

| Value | Meaning |
|-------|---------|
| `Window.unboundedPreceding` | First row in partition |
| `-2` | 2 rows before current |
| `-1` | 1 row before current |
| `0` | Current row |
| `1` | 1 row after current |
| `2` | 2 rows after current |
| `Window.unboundedFollowing` | Last row in partition |

**Common frame patterns:**

```python
# Running total (from start to current)
Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

# Full partition (all rows in group)
Window.partitionBy("dept").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

# Sliding window: 7-day moving average
Window.orderBy("date").rowsBetween(-6, 0)

# Look-ahead: include 2 future rows
Window.orderBy("date").rowsBetween(0, 2)
```

### Range-Based Frame (rangeBetween)

Uses actual VALUES (not row positions). Requires numeric or date ordering column:

```python
# All rows within ±10000 salary of current row
window_range = Window.partitionBy("department") \
    .orderBy("salary") \
    .rangeBetween(-10000, 10000)

df.withColumn("peers_avg", avg("salary").over(window_range))
# Includes all employees whose salary is within $10K of current employee
```

> **Key difference:** `rowsBetween(-2, 0)` always includes exactly 3 rows. `rangeBetween(-10000, 10000)` includes a variable number of rows depending on the actual values.

---

## Sessionization — Assigning Session IDs

A classic DE interview problem: group continuous user activity into sessions based on time gaps.

```python
from pyspark.sql.functions import lag, when, sum as spark_sum, unix_timestamp, col
from pyspark.sql.window import Window

# Sample clickstream data
clicks = spark.createDataFrame([
    ("user1", "2024-01-15 10:00:00"),
    ("user1", "2024-01-15 10:05:00"),
    ("user1", "2024-01-15 10:08:00"),
    ("user1", "2024-01-15 11:00:00"),  # 52-min gap → new session
    ("user1", "2024-01-15 11:03:00"),
    ("user2", "2024-01-15 09:00:00"),
    ("user2", "2024-01-15 09:25:00"),
    ("user2", "2024-01-15 10:30:00"),  # 65-min gap → new session
], ["user_id", "event_time"])

clicks = clicks.withColumn("event_time", col("event_time").cast("timestamp"))

# Step 1: Calculate gap from previous event per user
user_window = Window.partitionBy("user_id").orderBy("event_time")

clicks = clicks.withColumn(
    "prev_time", lag("event_time", 1).over(user_window)
).withColumn(
    "gap_minutes",
    (unix_timestamp("event_time") - unix_timestamp("prev_time")) / 60
)

# Step 2: Flag new sessions (gap > 30 minutes or first event)
clicks = clicks.withColumn(
    "is_new_session",
    when(col("gap_minutes") > 30, 1)
    .when(col("prev_time").isNull(), 1)
    .otherwise(0)
)

# Step 3: Cumulative sum of flags = session number
session_window = Window.partitionBy("user_id") \
    .orderBy("event_time") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

clicks = clicks.withColumn(
    "session_id", spark_sum("is_new_session").over(session_window)
)

clicks.select("user_id", "event_time", "gap_minutes", "is_new_session", "session_id").show()
```

**Result:**

| user_id | event_time | gap_minutes | is_new_session | session_id |
|---------|-----------|------------|---------------|-----------|
| user1 | 10:00:00 | NULL | 1 | 1 |
| user1 | 10:05:00 | 5.0 | 0 | 1 |
| user1 | 10:08:00 | 3.0 | 0 | 1 |
| user1 | 11:00:00 | 52.0 | 1 | 2 |
| user1 | 11:03:00 | 3.0 | 0 | 2 |
| user2 | 09:00:00 | NULL | 1 | 1 |
| user2 | 09:25:00 | 25.0 | 0 | 1 |
| user2 | 10:30:00 | 65.0 | 1 | 2 |

> **The trick:** lag() detects gaps → CASE WHEN flags boundaries → cumulative sum() assigns session IDs. This three-step pattern works for any gap-based grouping problem.

---

## Deduplication with Window Functions

Keep only the latest version of each record:

```python
from pyspark.sql.functions import row_number, col

# Data has duplicates (same order_id, different updated_at)
orders = spark.createDataFrame([
    ("ORD001", 100.0, "2024-01-15 10:00:00"),
    ("ORD001", 110.0, "2024-01-15 14:00:00"),  # Updated amount
    ("ORD002", 50.0, "2024-01-15 09:00:00"),
    ("ORD002", 50.0, "2024-01-15 09:00:00"),   # Exact duplicate
    ("ORD003", 200.0, "2024-01-15 11:00:00"),
], ["order_id", "amount", "updated_at"])

# Keep only the latest version per order_id
dedup_window = Window.partitionBy("order_id").orderBy(col("updated_at").desc())

deduped = orders.withColumn("rn", row_number().over(dedup_window)) \
    .filter(col("rn") == 1) \
    .drop("rn")

deduped.show()
```

**Result:**

| order_id | amount | updated_at |
|---------|--------|-----------|
| ORD001 | 110.0 | 2024-01-15 14:00:00 |
| ORD002 | 50.0 | 2024-01-15 09:00:00 |
| ORD003 | 200.0 | 2024-01-15 11:00:00 |

> **This is the standard dedup pattern:** partition by the business key, order by timestamp descending, keep row_number = 1. Works for exact duplicates AND "keep latest version" scenarios.

---

## Gap Detection (Missing Data)

Find days with no events for each user:

```python
from pyspark.sql.functions import datediff, lead

events = spark.createDataFrame([
    ("user1", "2024-01-01"), ("user1", "2024-01-02"), ("user1", "2024-01-03"),
    ("user1", "2024-01-06"),  # Gap: missing Jan 4, 5
    ("user1", "2024-01-07"),
    ("user2", "2024-01-01"), ("user2", "2024-01-05"),  # Gap: missing Jan 2,3,4
], ["user_id", "event_date"])

events = events.withColumn("event_date", col("event_date").cast("date"))

# Find gaps: difference between current date and next date
window = Window.partitionBy("user_id").orderBy("event_date")

gaps = events.withColumn("next_date", lead("event_date", 1).over(window)) \
    .withColumn("days_to_next", datediff("next_date", "event_date")) \
    .filter(col("days_to_next") > 1)  # Gap if more than 1 day between events

gaps.select("user_id", "event_date", "next_date", "days_to_next").show()
```

**Result:**

| user_id | event_date | next_date | days_to_next |
|---------|-----------|-----------|-------------|
| user1 | 2024-01-03 | 2024-01-06 | 3 |
| user2 | 2024-01-01 | 2024-01-05 | 4 |

---

## Consecutive Event Detection (Streaks)

Find users with 3+ consecutive days of activity:

```python
from pyspark.sql.functions import row_number, count, datediff, date_sub

# The "date minus row_number" trick
window = Window.partitionBy("user_id").orderBy("event_date")

streaks = events.withColumn("rn", row_number().over(window)) \
    .withColumn("streak_group", date_sub("event_date", col("rn")))

# Consecutive dates produce the same streak_group value
streak_lengths = streaks.groupBy("user_id", "streak_group") \
    .agg(count("*").alias("streak_length")) \
    .filter(col("streak_length") >= 3)

streak_lengths.show()
```

---

## Multiple Windows in One Query

You can define multiple window specs and use them in the same select:

```python
from pyspark.sql.functions import sum, avg, row_number, max as spark_max

# Window 1: Per department ranking
rank_window = Window.partitionBy("department").orderBy(col("salary").desc())

# Window 2: Global statistics (no partition)
global_window = Window.orderBy(col("salary").desc())

# Window 3: Per department aggregates (no order)
dept_window = Window.partitionBy("department")

result = df.select(
    "name", "department", "salary",
    row_number().over(rank_window).alias("dept_rank"),
    row_number().over(global_window).alias("global_rank"),
    avg("salary").over(dept_window).alias("dept_avg"),
    (col("salary") / sum("salary").over(dept_window) * 100).alias("pct_of_dept_total"),
)
result.show()
```

> **Performance note:** If multiple windows have the same partitionBy and orderBy, Spark reuses the same shuffle/sort. Different partition keys = separate shuffles (expensive).

---

## Interview Tips

> **Tip 1:** "Implement sessionization" — This is a top-5 PySpark interview question. Know the three-step pattern: `lag()` for gap detection → `when()` for boundary flagging → cumulative `sum()` for session IDs. Practice writing it from memory.

> **Tip 2:** "How do you deduplicate with window functions?" — "partition by the business key, order by timestamp descending (latest first), row_number() = 1 keeps the most recent version. This handles both exact duplicates and 'keep latest' use cases."

> **Tip 3:** "What's the performance cost of window functions?" — "Each unique partitionBy triggers a shuffle (repartitioning data). The orderBy within each partition requires a sort. For 1B rows with 100M distinct partition keys, that's a massive shuffle. Optimize by: reusing the same window spec where possible, filtering data before windowing, and caching the repartitioned DataFrame if applying multiple windows with the same partition key."
