---
title: "PySpark Window Functions - Scenario Questions"
topic: pyspark
subtopic: window-functions
content_type: scenario_question
tags: [pyspark, window-functions, interview, scenarios, analytics, sql, big-data]
---

# Scenario Questions — PySpark Window Functions

---

## Junior Level

<article data-difficulty="junior">

## 🟢 Junior: Top-N Products per Category

**Scenario:** You work at an e-commerce company. The product team wants a report showing the top 3 products by revenue in each category for the last quarter. The source table `products_revenue` has columns: `product_id`, `category`, `product_name`, and `revenue`. Write PySpark code that returns only the top 3 products per category, ranked by revenue descending.

<details>
<summary>💡 Hint</summary>
Use `row_number()` over a window partitioned by `category` and ordered by `revenue` descending. Then filter where the row number is ≤ 3. Remember: `row_number()` assigns unique sequential integers even for ties, unlike `rank()` or `dense_rank()`.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

spark = SparkSession.builder.appName("TopNProducts").getOrCreate()

# Sample data
data = [
    ("P1", "Electronics", "Laptop", 50000),
    ("P2", "Electronics", "Phone", 45000),
    ("P3", "Electronics", "Tablet", 30000),
    ("P4", "Electronics", "Headphones", 12000),
    ("P5", "Clothing", "Jacket", 8000),
    ("P6", "Clothing", "Shoes", 15000),
    ("P7", "Clothing", "T-Shirt", 5000),
    ("P8", "Clothing", "Jeans", 12000),
]

df = spark.createDataFrame(data, ["product_id", "category", "product_name", "revenue"])

# Define window: partition by category, order by revenue descending
window_spec = Window.partitionBy("category").orderBy(col("revenue").desc())

# Add row number and filter top 3
top_products = (
    df.withColumn("rank", row_number().over(window_spec))
      .filter(col("rank") <= 3)
      .drop("rank")
)

top_products.show()
```

**Key Points:**
- `row_number()` guarantees unique ranks (no ties) — use `dense_rank()` if you want ties to share the same rank
- Partitioning ensures ranking is independent per category
- Filtering after the window operation is the standard Top-N pattern in Spark
- This generates a single shuffle (partition by category) — efficient for moderate cardinality

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Running Total / Cumulative Sum

**Scenario:** The finance team needs a daily cumulative transaction amount per customer account. Given a DataFrame with columns `account_id`, `transaction_date`, and `amount`, add a `cumulative_amount` column that shows the running total of `amount` ordered by date within each account.

<details>
<summary>💡 Hint</summary>
Use `sum("amount")` over a window partitioned by `account_id`, ordered by `transaction_date`, with `rowsBetween(Window.unboundedPreceding, Window.currentRow)` to accumulate from the first row to the current row.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as spark_sum, col

spark = SparkSession.builder.appName("RunningTotal").getOrCreate()

data = [
    ("A001", "2024-01-01", 100.0),
    ("A001", "2024-01-03", 250.0),
    ("A001", "2024-01-05", -50.0),
    ("A001", "2024-01-07", 300.0),
    ("A002", "2024-01-02", 500.0),
    ("A002", "2024-01-04", 150.0),
    ("A002", "2024-01-06", -200.0),
]

df = spark.createDataFrame(data, ["account_id", "transaction_date", "amount"])

# Define cumulative window
cumulative_window = (
    Window.partitionBy("account_id")
          .orderBy("transaction_date")
          .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

# Add running total
result = df.withColumn("cumulative_amount", spark_sum("amount").over(cumulative_window))

result.show()
# A001: 100, 350, 300, 600
# A002: 500, 650, 450
```

**Key Points:**
- `rowsBetween(Window.unboundedPreceding, Window.currentRow)` is actually the default when an orderBy is specified, but being explicit improves readability
- Alias `sum` as `spark_sum` to avoid conflict with Python's built-in `sum`
- The cumulative sum resets per partition (each account_id starts fresh)
- For production: ensure `transaction_date` is properly cast to DateType for correct ordering

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Previous Row Value with lag()

**Scenario:** A stock analytics dashboard needs to show each day's closing price alongside the previous day's closing price and the daily change (difference). Given a DataFrame with `ticker`, `trade_date`, and `close_price`, compute `prev_close` and `daily_change` columns.

<details>
<summary>💡 Hint</summary>
Use `lag("close_price", 1)` over a window partitioned by `ticker` and ordered by `trade_date`. The first row per ticker will have `null` for the previous value — handle it with `coalesce` or leave as null.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, col, coalesce, lit

spark = SparkSession.builder.appName("StockLag").getOrCreate()

data = [
    ("AAPL", "2024-01-02", 185.50),
    ("AAPL", "2024-01-03", 187.20),
    ("AAPL", "2024-01-04", 184.90),
    ("AAPL", "2024-01-05", 186.00),
    ("GOOG", "2024-01-02", 140.10),
    ("GOOG", "2024-01-03", 141.50),
    ("GOOG", "2024-01-04", 139.80),
]

df = spark.createDataFrame(data, ["ticker", "trade_date", "close_price"])

# Window partitioned by ticker, ordered by date
price_window = Window.partitionBy("ticker").orderBy("trade_date")

result = (
    df.withColumn("prev_close", lag("close_price", 1).over(price_window))
      .withColumn("daily_change", col("close_price") - coalesce(col("prev_close"), col("close_price")))
)

result.show()
```

**Key Points:**
- `lag(col, offset, default)` — offset=1 means one row back; optional default replaces null
- `lead()` is the opposite — looks forward instead of backward
- `coalesce(prev_close, close_price)` handles the first day gracefully (change = 0)
- The window order is critical: if dates aren't sorted correctly, lag returns wrong values

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Average Over a Window (Salary vs Department Average)

**Scenario:** HR wants to identify employees whose salary significantly deviates from their department average. Given a DataFrame with `employee_id`, `department`, `name`, and `salary`, add a `dept_avg_salary` column showing the department average, and a `deviation_pct` column showing how much each employee's salary differs from the average as a percentage.

<details>
<summary>💡 Hint</summary>
Use `avg("salary")` over a window partitioned by `department` (no orderBy needed since you want the full partition average). Calculate deviation as `(salary - dept_avg) / dept_avg * 100`.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, col, round as spark_round

spark = SparkSession.builder.appName("DeptAvg").getOrCreate()

data = [
    ("E1", "Engineering", "Alice", 120000),
    ("E2", "Engineering", "Bob", 95000),
    ("E3", "Engineering", "Charlie", 140000),
    ("E4", "Marketing", "Diana", 85000),
    ("E5", "Marketing", "Eve", 78000),
    ("E6", "Marketing", "Frank", 92000),
]

df = spark.createDataFrame(data, ["employee_id", "department", "name", "salary"])

# Window for department-level aggregate (no orderBy = full partition)
dept_window = Window.partitionBy("department")

result = (
    df.withColumn("dept_avg_salary", spark_round(avg("salary").over(dept_window), 2))
      .withColumn("deviation_pct", 
                  spark_round((col("salary") - col("dept_avg_salary")) / col("dept_avg_salary") * 100, 1))
)

result.show()
```

**Key Points:**
- Omitting `orderBy` in the window spec computes the aggregate over the entire partition (all rows in department)
- Adding `orderBy` would create a running average instead — a common mistake
- This avoids a self-join: window functions let you access group-level aggregates alongside row-level data
- `round()` is aliased to avoid conflicts with Python's built-in

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Ntile for Bucketing Customers

**Scenario:** The marketing team wants to segment customers into 4 spending quartiles (Q1 = lowest spenders, Q4 = highest spenders) for targeted campaigns. Given a DataFrame with `customer_id`, `name`, and `total_spending`, assign each customer to a quartile using `ntile`.

<details>
<summary>💡 Hint</summary>
Use `ntile(4)` over a window ordered by `total_spending`. No partition is needed since you want global quartiles across all customers. Ntile divides rows into approximately equal groups.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import ntile, col, when

spark = SparkSession.builder.appName("CustomerQuartiles").getOrCreate()

data = [
    ("C1", "Alice", 1200), ("C2", "Bob", 4500), ("C3", "Charlie", 890),
    ("C4", "Diana", 7800), ("C5", "Eve", 3200), ("C6", "Frank", 560),
    ("C7", "Grace", 9100), ("C8", "Hank", 2100), ("C9", "Ivy", 6400),
    ("C10", "Jack", 1800), ("C11", "Kim", 5500), ("C12", "Leo", 3800),
]

df = spark.createDataFrame(data, ["customer_id", "name", "total_spending"])

# Global window ordered by spending
spending_window = Window.orderBy("total_spending")

result = (
    df.withColumn("quartile", ntile(4).over(spending_window))
      .withColumn("segment", 
                  when(col("quartile") == 1, "Budget")
                  .when(col("quartile") == 2, "Standard")
                  .when(col("quartile") == 3, "Premium")
                  .otherwise("VIP"))
)

result.orderBy("total_spending").show()
```

**Key Points:**
- `ntile(n)` divides the ordered rows into `n` roughly equal buckets (1 to n)
- If rows don't divide evenly, earlier buckets get one extra row
- No `partitionBy` means global ranking — add partition if you need quartiles per segment/region
- Useful for percentile-based segmentation without computing exact percentile values

</details>

</article>

---

## Mid-Level

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Sessionize Clickstream Data

**Scenario:** You're building a clickstream analytics pipeline. A session is defined as a sequence of events from the same user where no two consecutive events are more than 30 minutes apart. Given a DataFrame with `user_id`, `event_timestamp`, and `page_url`, assign a unique `session_id` to each event.

<details>
<summary>💡 Hint</summary>
1. Use `lag()` to get the previous event timestamp per user.
2. Compute the time gap between consecutive events.
3. Flag rows where the gap exceeds 30 minutes (these are session boundaries).
4. Use a cumulative sum of these flags to generate incrementing session IDs per user.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    lag, col, unix_timestamp, when, sum as spark_sum, concat_ws, monotonically_increasing_id
)

spark = SparkSession.builder.appName("Sessionization").getOrCreate()

data = [
    ("user1", "2024-01-01 10:00:00", "/home"),
    ("user1", "2024-01-01 10:05:00", "/products"),
    ("user1", "2024-01-01 10:12:00", "/product/123"),
    ("user1", "2024-01-01 11:00:00", "/home"),        # 48 min gap → new session
    ("user1", "2024-01-01 11:10:00", "/checkout"),
    ("user2", "2024-01-01 09:00:00", "/home"),
    ("user2", "2024-01-01 09:20:00", "/search"),
    ("user2", "2024-01-01 14:00:00", "/home"),        # 4h40m gap → new session
]

df = spark.createDataFrame(data, ["user_id", "event_timestamp", "page_url"])
df = df.withColumn("event_ts", unix_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"))

# Step 1: Get previous event timestamp per user
user_window = Window.partitionBy("user_id").orderBy("event_ts")

df_with_prev = df.withColumn("prev_ts", lag("event_ts", 1).over(user_window))

# Step 2: Flag session boundaries (gap > 30 min = 1800 seconds)
df_flagged = df_with_prev.withColumn(
    "new_session_flag",
    when(
        (col("prev_ts").isNull()) | (col("event_ts") - col("prev_ts") > 1800),
        1
    ).otherwise(0)
)

# Step 3: Cumulative sum of flags = session number per user
session_window = Window.partitionBy("user_id").orderBy("event_ts") \
                       .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df_sessioned = df_flagged.withColumn(
    "session_num", spark_sum("new_session_flag").over(session_window)
)

# Step 4: Create unique session_id
result = df_sessioned.withColumn(
    "session_id", concat_ws("_", col("user_id"), col("session_num"))
).select("user_id", "event_timestamp", "page_url", "session_id")

result.show(truncate=False)
```

**Key Points:**
- The lag + cumsum pattern is the standard sessionization technique in both Spark and SQL
- Session gap threshold (30 min) is configurable — adjust based on business rules
- `unix_timestamp` converts strings to seconds for easy arithmetic
- For production: ensure event timestamps are properly deduplicated and sorted
- This pattern scales well — each user's data is processed independently within partitions

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Deduplicate with Window Functions

**Scenario:** Your CDC (Change Data Capture) pipeline ingests multiple versions of the same record. For each `user_id`, you need to keep only the most recent record based on `updated_at`. The DataFrame has columns: `user_id`, `email`, `name`, `updated_at`, and `source_system`.

<details>
<summary>💡 Hint</summary>
Use `row_number()` partitioned by `user_id` and ordered by `updated_at DESC`. The row with `row_number = 1` is the latest version. This is more flexible than `dropDuplicates()` because you control which record wins.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col

spark = SparkSession.builder.appName("Dedup").getOrCreate()

data = [
    ("U1", "alice@v1.com", "Alice", "2024-01-01 10:00:00", "crm"),
    ("U1", "alice@v2.com", "Alice Smith", "2024-01-03 14:00:00", "crm"),
    ("U1", "alice@v3.com", "Alice Smith", "2024-01-05 09:00:00", "web"),
    ("U2", "bob@old.com", "Bob", "2024-01-02 08:00:00", "crm"),
    ("U2", "bob@new.com", "Robert", "2024-01-04 16:00:00", "crm"),
    ("U3", "carol@test.com", "Carol", "2024-01-01 12:00:00", "web"),
]

df = spark.createDataFrame(data, ["user_id", "email", "name", "updated_at", "source_system"])

# Window: latest record per user_id
dedup_window = Window.partitionBy("user_id").orderBy(col("updated_at").desc())

deduped = (
    df.withColumn("rn", row_number().over(dedup_window))
      .filter(col("rn") == 1)
      .drop("rn")
)

deduped.show(truncate=False)
# Result: U1→alice@v3.com, U2→bob@new.com, U3→carol@test.com
```

**Key Points:**
- Prefer `row_number()` over `dropDuplicates()` when you need deterministic "latest wins" logic
- Add secondary sort columns (e.g., `source_system`) for tiebreaking when timestamps collide
- For very large datasets, this is more efficient than a self-join approach
- Common pattern in SCD Type 1 merges and CDC deduplication pipelines
- In production, add a secondary sort key to guarantee determinism: `.orderBy(col("updated_at").desc(), col("source_system").desc())`

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Gaps and Islands Problem

**Scenario:** Your user engagement team wants to identify consecutive sequences ("islands") of daily activity per user. Given a DataFrame with `user_id` and `activity_date` (one row per active day), find each user's continuous streaks — outputting `user_id`, `streak_start`, `streak_end`, and `streak_length`.

<details>
<summary>💡 Hint</summary>
The classic technique: subtract the row_number from the date. Consecutive dates will produce the same "group key" since both the date and row number increment by 1. Then group by this key to find streak boundaries.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, col, to_date, date_sub, min as spark_min,
    max as spark_max, datediff, lit
)

spark = SparkSession.builder.appName("GapsAndIslands").getOrCreate()

data = [
    ("user1", "2024-01-01"), ("user1", "2024-01-02"), ("user1", "2024-01-03"),
    ("user1", "2024-01-06"), ("user1", "2024-01-07"),  # gap on Jan 4-5
    ("user1", "2024-01-10"),                            # isolated day
    ("user2", "2024-01-01"), ("user2", "2024-01-02"),
    ("user2", "2024-01-03"), ("user2", "2024-01-04"), ("user2", "2024-01-05"),
]

df = spark.createDataFrame(data, ["user_id", "activity_date"])
df = df.withColumn("activity_date", to_date("activity_date"))

# Step 1: Assign row numbers per user ordered by date
user_window = Window.partitionBy("user_id").orderBy("activity_date")
df_numbered = df.withColumn("rn", row_number().over(user_window))

# Step 2: Subtract row_number days from date → consecutive dates get same group_key
df_grouped = df_numbered.withColumn(
    "group_key", date_sub(col("activity_date"), col("rn"))
)

# Step 3: Aggregate to find streak boundaries
streaks = (
    df_grouped.groupBy("user_id", "group_key")
    .agg(
        spark_min("activity_date").alias("streak_start"),
        spark_max("activity_date").alias("streak_end"),
    )
    .withColumn("streak_length", datediff(col("streak_end"), col("streak_start")) + 1)
    .drop("group_key")
    .orderBy("user_id", "streak_start")
)

streaks.show()
# user1: Jan 1-3 (3 days), Jan 6-7 (2 days), Jan 10 (1 day)
# user2: Jan 1-5 (5 days)
```

**Key Points:**
- The "date minus row_number" trick works because consecutive dates and sequential row numbers both increment by 1
- The resulting `group_key` is the same for all rows in a contiguous block
- This is a classic SQL/Spark pattern for detecting streaks, outages, or continuous periods
- Works with any sequence (timestamps rounded to day, integer sequences, etc.)
- For gaps: the inverse — find where `group_key` changes between consecutive rows

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Moving Average with Fixed Window

**Scenario:** The revenue analytics team wants a 7-day rolling average of daily revenue to smooth out day-of-week effects. Given a DataFrame with `date` and `daily_revenue`, compute a `rolling_7d_avg` that averages the current day and the 6 preceding days.

<details>
<summary>💡 Hint</summary>
Use `avg("daily_revenue")` over a window ordered by date with `rowsBetween(-6, 0)`. This creates a fixed-size window of 7 rows (current + 6 prior). For the first 6 days, the window will be smaller — decide whether that's acceptable or needs filtering.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import avg, col, count, round as spark_round, to_date

spark = SparkSession.builder.appName("RollingAvg").getOrCreate()

data = [
    ("2024-01-01", 10000), ("2024-01-02", 12000), ("2024-01-03", 9500),
    ("2024-01-04", 11000), ("2024-01-05", 15000), ("2024-01-06", 13500),
    ("2024-01-07", 14000), ("2024-01-08", 11500), ("2024-01-09", 12500),
    ("2024-01-10", 16000), ("2024-01-11", 13000), ("2024-01-12", 14500),
]

df = spark.createDataFrame(data, ["date", "daily_revenue"])
df = df.withColumn("date", to_date("date"))

# 7-day rolling window (current row + 6 preceding rows)
rolling_window = Window.orderBy("date").rowsBetween(-6, 0)

result = (
    df.withColumn("rolling_7d_avg", spark_round(avg("daily_revenue").over(rolling_window), 2))
      .withColumn("window_size", count("daily_revenue").over(rolling_window))
)

result.show()
```

**Key Points:**
- `rowsBetween(-6, 0)` = 7 rows total (6 before + current)
- For the first 6 rows, the window is smaller than 7 — the `window_size` column makes this visible
- Use `rangeBetween` instead of `rowsBetween` when you need time-based windows with potential gaps (e.g., missing days)
- `rowsBetween` is position-based (row count), `rangeBetween` is value-based (actual date range)
- For production: if days can be missing, use `rangeBetween(-6 * 86400, 0)` on a unix timestamp column

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Detect Longest Consecutive Login Streak

**Scenario:** The gamification team wants to award badges for login streaks. Find the longest consecutive day login streak per user. Input DataFrame has `user_id` and `login_date` (deduplicated — one row per user per day).

<details>
<summary>💡 Hint</summary>
Combine the gaps-and-islands technique with a final aggregation: after identifying streaks using date minus row_number, group by user and streak group, compute each streak's length, then pick the max streak per user.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, col, to_date, date_sub, datediff,
    min as spark_min, max as spark_max, max as spark_max_agg
)

spark = SparkSession.builder.appName("LoginStreaks").getOrCreate()

data = [
    ("user1", "2024-01-01"), ("user1", "2024-01-02"), ("user1", "2024-01-03"),
    ("user1", "2024-01-04"), ("user1", "2024-01-05"),  # 5-day streak
    ("user1", "2024-01-10"), ("user1", "2024-01-11"),  # 2-day streak
    ("user2", "2024-01-01"), ("user2", "2024-01-02"),  # 2-day streak
    ("user2", "2024-01-05"), ("user2", "2024-01-06"),
    ("user2", "2024-01-07"), ("user2", "2024-01-08"),  # 4-day streak (longest)
]

df = spark.createDataFrame(data, ["user_id", "login_date"])
df = df.withColumn("login_date", to_date("login_date"))

# Step 1: Row number per user by date
user_window = Window.partitionBy("user_id").orderBy("login_date")
df_rn = df.withColumn("rn", row_number().over(user_window))

# Step 2: Compute group key (consecutive dates get same key)
df_grouped = df_rn.withColumn("streak_group", date_sub(col("login_date"), col("rn")))

# Step 3: Compute streak length per group
streaks = (
    df_grouped.groupBy("user_id", "streak_group")
    .agg(
        spark_min("login_date").alias("streak_start"),
        spark_max("login_date").alias("streak_end"),
    )
    .withColumn("streak_length", datediff(col("streak_end"), col("streak_start")) + 1)
)

# Step 4: Get longest streak per user
longest_streaks = (
    streaks.groupBy("user_id")
    .agg(
        spark_max_agg("streak_length").alias("longest_streak"),
    )
)

longest_streaks.show()
# user1: 5, user2: 4
```

**Key Points:**
- Builds on the gaps-and-islands pattern with an additional max aggregation step
- The `date_sub(login_date, row_number)` trick groups consecutive dates together
- Two-phase aggregation: first find all streaks, then pick the longest per user
- For production: ensure login_date is deduplicated before running (one row per user per day)
- Alternative: use window-based approach with `lag()` and running counters, but the group-by approach is cleaner

</details>

</article>

---

## Senior Level

<article data-difficulty="senior">

## 🔴 Senior: Anomaly Detection with Rolling Statistics

**Scenario:** Your fraud detection system needs to flag transactions that are statistical outliers — specifically, any transaction whose amount exceeds 3 standard deviations above the 30-day rolling mean for that user. The DataFrame has `user_id`, `transaction_ts`, `amount`, and `merchant_category`. Implement this with proper handling of the time-based window and edge cases.

<details>
<summary>💡 Hint</summary>
Use `rangeBetween` with seconds to create a true time-based 30-day window (not row-based). Compute rolling `mean` and `stddev` over this window, then flag rows where `amount > mean + 3 * stddev`. Handle edge cases: users with few transactions (stddev = 0 or null) and ensure the window is based on actual time, not row count.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, avg, stddev, unix_timestamp, when, count, lit, round as spark_round
)

spark = SparkSession.builder.appName("AnomalyDetection").getOrCreate()

data = [
    ("U1", "2024-01-05 10:00:00", 50.0, "grocery"),
    ("U1", "2024-01-10 14:00:00", 45.0, "grocery"),
    ("U1", "2024-01-15 09:00:00", 55.0, "grocery"),
    ("U1", "2024-01-20 16:00:00", 60.0, "restaurant"),
    ("U1", "2024-01-25 11:00:00", 48.0, "grocery"),
    ("U1", "2024-01-30 20:00:00", 500.0, "electronics"),  # Anomaly!
    ("U1", "2024-02-01 08:00:00", 52.0, "grocery"),
    ("U2", "2024-01-01 10:00:00", 200.0, "travel"),
    ("U2", "2024-01-15 14:00:00", 210.0, "travel"),
    ("U2", "2024-01-28 09:00:00", 5000.0, "travel"),      # Anomaly!
]

df = spark.createDataFrame(data, ["user_id", "transaction_ts", "amount", "merchant_category"])
df = df.withColumn("ts_epoch", unix_timestamp("transaction_ts", "yyyy-MM-dd HH:mm:ss"))

# 30-day rolling window in seconds (30 * 24 * 60 * 60 = 2,592,000)
THIRTY_DAYS_SECONDS = 30 * 24 * 60 * 60

rolling_window = (
    Window.partitionBy("user_id")
          .orderBy("ts_epoch")
          .rangeBetween(-THIRTY_DAYS_SECONDS, -1)  # Exclude current row to avoid self-influence
)

# Compute rolling stats (excluding current transaction)
df_stats = (
    df.withColumn("rolling_mean", avg("amount").over(rolling_window))
      .withColumn("rolling_stddev", stddev("amount").over(rolling_window))
      .withColumn("rolling_count", count("amount").over(rolling_window))
)

# Flag anomalies: amount > mean + 3*stddev, with minimum sample size
STDDEV_THRESHOLD = 3
MIN_SAMPLES = 5

result = df_stats.withColumn(
    "is_anomaly",
    when(
        (col("rolling_count") >= MIN_SAMPLES) &
        (col("rolling_stddev") > 0) &
        (col("amount") > col("rolling_mean") + STDDEV_THRESHOLD * col("rolling_stddev")),
        True
    ).otherwise(False)
).withColumn(
    "z_score",
    when(
        (col("rolling_stddev") > 0) & (col("rolling_count") >= MIN_SAMPLES),
        spark_round((col("amount") - col("rolling_mean")) / col("rolling_stddev"), 2)
    )
)

result.select(
    "user_id", "transaction_ts", "amount", "merchant_category",
    "rolling_mean", "rolling_stddev", "is_anomaly", "z_score"
).show(truncate=False)
```

**Key Points:**
- `rangeBetween(-THIRTY_DAYS_SECONDS, -1)` creates a true time-based window excluding the current row — this prevents the anomaly from inflating its own statistics
- Minimum sample check (`rolling_count >= 5`) prevents false positives when a user has few transactions
- `stddev > 0` guard avoids division by zero when all historical amounts are identical
- **Performance consideration:** `rangeBetween` on a continuous column (epoch seconds) requires Spark to evaluate range boundaries per row — more expensive than `rowsBetween` but semantically correct for irregular time series
- **Production tip:** Consider separate thresholds per `merchant_category` for more precise detection
- For very active users, consider limiting the window to exclude very old data or use exponential decay weighting

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Event Sequence Pattern Matching

**Scenario:** The product analytics team wants to identify users who complete a specific conversion sequence: `page_view` → `add_to_cart` → `purchase` (in that exact order, with no more than 1 hour between each step). Given an events DataFrame with `user_id`, `event_type`, `event_timestamp`, and `product_id`, find all completed sequences and compute time-to-conversion.

<details>
<summary>💡 Hint</summary>
Use `lead()` to look ahead at the next 1-2 events per user per product. Check that the sequence matches the expected pattern and time constraints are satisfied. Alternatively, use multiple self-joins with window-generated row numbers, but `lead()` is more efficient.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, lead, unix_timestamp, when, lit, round as spark_round
)

spark = SparkSession.builder.appName("SequenceMatching").getOrCreate()

data = [
    ("U1", "page_view", "2024-01-01 10:00:00", "P100"),
    ("U1", "add_to_cart", "2024-01-01 10:15:00", "P100"),
    ("U1", "purchase", "2024-01-01 10:30:00", "P100"),      # ✓ Complete sequence
    ("U1", "page_view", "2024-01-01 14:00:00", "P200"),
    ("U1", "page_view", "2024-01-01 14:30:00", "P200"),     # Repeated view (not a sequence)
    ("U2", "page_view", "2024-01-02 09:00:00", "P100"),
    ("U2", "add_to_cart", "2024-01-02 09:10:00", "P100"),
    ("U2", "purchase", "2024-01-02 12:00:00", "P100"),      # ✗ > 1hr between add_to_cart and purchase
    ("U3", "page_view", "2024-01-03 08:00:00", "P300"),
    ("U3", "add_to_cart", "2024-01-03 08:20:00", "P300"),
    ("U3", "purchase", "2024-01-03 08:45:00", "P300"),      # ✓ Complete sequence
]

df = spark.createDataFrame(data, ["user_id", "event_type", "event_timestamp", "product_id"])
df = df.withColumn("event_epoch", unix_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"))

# Window per user+product ordered by time
sequence_window = Window.partitionBy("user_id", "product_id").orderBy("event_epoch")

# Look ahead at next 2 events
df_sequenced = (
    df.withColumn("next_event_1", lead("event_type", 1).over(sequence_window))
      .withColumn("next_event_1_ts", lead("event_epoch", 1).over(sequence_window))
      .withColumn("next_event_2", lead("event_type", 2).over(sequence_window))
      .withColumn("next_event_2_ts", lead("event_epoch", 2).over(sequence_window))
)

# Filter for complete sequences with time constraints
ONE_HOUR = 3600

conversions = (
    df_sequenced.filter(
        (col("event_type") == "page_view") &
        (col("next_event_1") == "add_to_cart") &
        (col("next_event_2") == "purchase") &
        # Time constraint: each step within 1 hour of previous
        ((col("next_event_1_ts") - col("event_epoch")) <= ONE_HOUR) &
        ((col("next_event_2_ts") - col("next_event_1_ts")) <= ONE_HOUR)
    )
    .withColumn("time_to_cart_sec", col("next_event_1_ts") - col("event_epoch"))
    .withColumn("time_to_purchase_sec", col("next_event_2_ts") - col("event_epoch"))
    .withColumn("total_conversion_min", spark_round((col("next_event_2_ts") - col("event_epoch")) / 60, 1))
    .select("user_id", "product_id", "event_timestamp", "total_conversion_min")
)

conversions.show(truncate=False)
# U1/P100: 30 min, U3/P300: 25 min
```

**Key Points:**
- `lead(col, n)` looks forward `n` rows — more efficient than self-join for sequence detection
- Partitioning by `user_id + product_id` ensures we match sequences for the same product
- Time constraints prevent matching events that are temporally unrelated
- **Limitation:** This approach assumes no intervening events between steps. For more complex patterns (allowing intermediate events), consider a state-machine approach or Spark's `SessionWindow`
- **Production considerations:** For very long event streams, add a date-range filter before the window operation to reduce partition sizes
- **Alternative:** For complex patterns, consider PySpark's SQL `MATCH_RECOGNIZE` (available in Spark 3.4+) for declarative pattern matching

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Time-Weighted Average

**Scenario:** An IoT platform collects sensor readings at irregular intervals. Each reading is valid until the next one arrives. Compute the time-weighted average temperature per sensor per day, where each reading's weight is the duration (in seconds) it was the "current" reading. This is critical for accurate SLA reporting.

<details>
<summary>💡 Hint</summary>
Use `lead()` to get the next reading's timestamp. The weight for each reading = `next_timestamp - current_timestamp`. For the last reading of the day, use end-of-day as the boundary. Then compute `sum(value * weight) / sum(weight)` per sensor per day.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, lead, unix_timestamp, to_date, when, sum as spark_sum,
    round as spark_round, least, lit, date_trunc, date_add,
    concat
)

spark = SparkSession.builder.appName("TimeWeightedAvg").getOrCreate()

data = [
    ("sensor_A", "2024-01-01 00:00:00", 20.0),
    ("sensor_A", "2024-01-01 06:00:00", 22.5),   # Held for 6 hours
    ("sensor_A", "2024-01-01 08:00:00", 25.0),   # Held for 2 hours
    ("sensor_A", "2024-01-01 18:00:00", 21.0),   # Held for 10 hours
    ("sensor_A", "2024-01-01 23:00:00", 19.5),   # Held for 1 hour until midnight
    ("sensor_A", "2024-01-02 03:00:00", 18.0),   # Next day
    ("sensor_B", "2024-01-01 02:00:00", 30.0),
    ("sensor_B", "2024-01-01 14:00:00", 32.5),   # Held for 12 hours
    ("sensor_B", "2024-01-01 20:00:00", 28.0),   # Held for 4 hours until midnight
]

df = spark.createDataFrame(data, ["sensor_id", "reading_ts", "temperature"])
df = (
    df.withColumn("reading_epoch", unix_timestamp("reading_ts", "yyyy-MM-dd HH:mm:ss"))
      .withColumn("reading_date", to_date("reading_ts"))
)

# Get next reading timestamp per sensor
sensor_window = Window.partitionBy("sensor_id").orderBy("reading_epoch")

df_with_next = df.withColumn("next_reading_epoch", lead("reading_epoch", 1).over(sensor_window))

# Compute end-of-day boundary (start of next day in epoch)
df_bounded = df_with_next.withColumn(
    "day_end_epoch",
    unix_timestamp(concat(col("reading_date").cast("string"), lit(" 23:59:59")), "yyyy-MM-dd HH:mm:ss") + 1
)

# Duration = min(next_reading, end_of_day) - current_reading
# If no next reading within same day, use end_of_day
df_weighted = df_bounded.withColumn(
    "effective_end",
    when(
        col("next_reading_epoch").isNull(),
        col("day_end_epoch")
    ).otherwise(
        least(col("next_reading_epoch"), col("day_end_epoch"))
    )
).withColumn(
    "duration_seconds",
    col("effective_end") - col("reading_epoch")
).withColumn(
    "weighted_value",
    col("temperature") * col("duration_seconds")
)

# Filter out readings where duration is negative (next day readings paired with wrong day)
df_valid = df_weighted.filter(col("duration_seconds") > 0)

# Time-weighted average per sensor per day
twa = (
    df_valid.groupBy("sensor_id", "reading_date")
    .agg(
        spark_round(
            spark_sum("weighted_value") / spark_sum("duration_seconds"), 2
        ).alias("time_weighted_avg_temp"),
        spark_sum("duration_seconds").alias("total_seconds_covered"),
    )
)

twa.show(truncate=False)
# sensor_A on 2024-01-01: weighted avg considering hold times
# sensor_B on 2024-01-01: weighted avg considering hold times
```

**Key Points:**
- Time-weighted average accounts for how long each value was "held" — simple mean ignores duration and biases toward frequent readings
- `lead()` gives the next measurement time; the difference is the weight
- End-of-day boundary prevents weights from spanning across days
- `least()` handles the case where the next reading is on the following day
- **Production considerations:**
  - Handle sensors with only one reading per day (weight = entire day)
  - Consider timezone implications — "day boundary" depends on business timezone
  - For sensors that go offline, decide whether to extrapolate or mark gaps
  - This pattern generalizes to any "step function" metric (inventory levels, pricing, etc.)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Funnel Analysis with Window Functions

**Scenario:** Build a multi-step conversion funnel (impression → click → signup → purchase) with strict constraints: each step must follow the previous step in order, each subsequent step must occur within 7 days of the first impression, and a user can have multiple funnels. Compute per-step conversion rates and median time between steps.

<details>
<summary>💡 Hint</summary>
Assign funnel instances using a sessionization-like approach on impressions. Then for each funnel instance, use conditional aggregation with `min()` over windows to find the earliest qualifying event at each step. Validate ordering and time constraints between steps.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, lead, lag, unix_timestamp, when, sum as spark_sum, min as spark_min,
    count, countDistinct, row_number, lit, expr, percentile_approx,
    round as spark_round, datediff, to_timestamp
)

spark = SparkSession.builder.appName("FunnelAnalysis").getOrCreate()

data = [
    # User 1: completes full funnel
    ("U1", "impression", "2024-01-01 10:00:00"),
    ("U1", "click", "2024-01-01 10:05:00"),
    ("U1", "signup", "2024-01-02 14:00:00"),
    ("U1", "purchase", "2024-01-03 09:00:00"),
    # User 2: drops off after signup
    ("U2", "impression", "2024-01-01 08:00:00"),
    ("U2", "click", "2024-01-01 08:30:00"),
    ("U2", "signup", "2024-01-01 09:00:00"),
    # User 3: completes but purchase too late (> 7 days)
    ("U3", "impression", "2024-01-01 12:00:00"),
    ("U3", "click", "2024-01-01 12:10:00"),
    ("U3", "signup", "2024-01-05 10:00:00"),
    ("U3", "purchase", "2024-01-10 16:00:00"),  # > 7 days from impression
    # User 4: completes full funnel
    ("U4", "impression", "2024-01-02 09:00:00"),
    ("U4", "click", "2024-01-02 09:15:00"),
    ("U4", "signup", "2024-01-02 10:00:00"),
    ("U4", "purchase", "2024-01-04 11:00:00"),
]

df = spark.createDataFrame(data, ["user_id", "event_type", "event_timestamp"])
df = df.withColumn("event_epoch", unix_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"))

SEVEN_DAYS = 7 * 24 * 3600  # seconds
FUNNEL_STEPS = ["impression", "click", "signup", "purchase"]

# Step 1: Identify funnel start points (impressions)
impressions = df.filter(col("event_type") == "impression").select(
    col("user_id"),
    col("event_epoch").alias("funnel_start_epoch"),
    col("event_timestamp").alias("funnel_start_ts")
)

# Step 2: For each impression, find the earliest qualifying event at each step
# Self-join events to impressions with time constraint
events_with_funnel = (
    impressions.join(df, on="user_id")
    .filter(
        (col("event_epoch") >= col("funnel_start_epoch")) &
        (col("event_epoch") - col("funnel_start_epoch") <= SEVEN_DAYS)
    )
)

# Step 3: For each user + funnel_start, find first event of each type after start
funnel_window = Window.partitionBy("user_id", "funnel_start_epoch", "event_type").orderBy("event_epoch")

first_events = (
    events_with_funnel
    .withColumn("event_rank", row_number().over(funnel_window))
    .filter(col("event_rank") == 1)
    .drop("event_rank")
)

# Step 4: Pivot to get one row per funnel instance
funnel_pivoted = (
    first_events.groupBy("user_id", "funnel_start_epoch")
    .pivot("event_type", FUNNEL_STEPS)
    .agg(spark_min("event_epoch"))
)

# Step 5: Validate ordering (each step must come after previous)
funnel_validated = funnel_pivoted.withColumn(
    "reached_click", col("click").isNotNull() & (col("click") > col("impression"))
).withColumn(
    "reached_signup",
    col("signup").isNotNull() & (col("signup") > col("click")) & col("reached_click")
).withColumn(
    "reached_purchase",
    col("purchase").isNotNull() & (col("purchase") > col("signup")) & col("reached_signup")
)

# Step 6: Compute conversion rates
total_funnels = funnel_validated.count()

conversion_rates = funnel_validated.select(
    lit(total_funnels).alias("step_1_impression"),
    spark_sum(col("reached_click").cast("int")).alias("step_2_click"),
    spark_sum(col("reached_signup").cast("int")).alias("step_3_signup"),
    spark_sum(col("reached_purchase").cast("int")).alias("step_4_purchase"),
)

conversion_rates.show()

# Step 7: Compute time between steps for completed funnels
time_analysis = (
    funnel_validated
    .filter(col("reached_purchase") == True)
    .withColumn("impression_to_click_min", spark_round((col("click") - col("impression")) / 60, 1))
    .withColumn("click_to_signup_hrs", spark_round((col("signup") - col("click")) / 3600, 1))
    .withColumn("signup_to_purchase_hrs", spark_round((col("purchase") - col("signup")) / 3600, 1))
)

time_analysis.select(
    "user_id", "impression_to_click_min", "click_to_signup_hrs", "signup_to_purchase_hrs"
).show()
```

**Key Points:**
- Multi-step funnels require careful ordering validation — each step must follow the previous
- The 7-day constraint uses epoch arithmetic for precision
- Pivoting creates a wide-format row per funnel instance for easy step comparison
- **Performance considerations:**
  - The self-join can be expensive; for large datasets, broadcast the impressions if they're small
  - Consider pre-filtering events to only relevant types before the join
  - Partition data by date range to limit the join scope
- **Production enhancements:**
  - Handle multiple products (partition by product_id)
  - Add attribution logic (which campaign drove the impression)
  - Consider "any-touch" vs "first-touch" attribution models
  - Use approximate percentiles (`percentile_approx`) for time-between-steps analysis at scale

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Optimize Window Functions for 500M+ Rows

**Scenario:** Your pipeline processes 500M+ daily transaction rows. It applies multiple window functions (running total, rank, lag, rolling 7-day average) partitioned by `customer_id` (50M unique customers). The job takes 4+ hours and frequently OOMs. Diagnose the performance issues and implement optimizations to bring runtime under 1 hour.

<details>
<summary>💡 Hint</summary>
Key issues to address: (1) Partition skew — some customers have millions of transactions while most have few, (2) Multiple window specs cause redundant shuffles, (3) Unbounded frames scan entire partitions, (4) No pre-filtering of unneeded data. Solutions involve: consolidating window specs, salting skewed partitions, limiting frame sizes, and strategic caching/checkpointing.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col, row_number, lag, sum as spark_sum, avg, count,
    lit, concat, floor, rand, broadcast, when, coalesce
)

spark = SparkSession.builder.appName("OptimizedWindows").getOrCreate()

# ============================================================
# PROBLEM: Naive implementation (DON'T DO THIS)
# ============================================================
"""
# This creates MULTIPLE shuffles and OOMs on skewed partitions:
w1 = Window.partitionBy("customer_id").orderBy("txn_date")
w2 = Window.partitionBy("customer_id").orderBy(col("txn_date").desc())
w3 = Window.partitionBy("customer_id").orderBy("txn_date").rowsBetween(-6, 0)

df_bad = (
    df.withColumn("running_total", spark_sum("amount").over(w1))
      .withColumn("rank_desc", row_number().over(w2))  # Different sort = new shuffle!
      .withColumn("prev_amount", lag("amount", 1).over(w1))
      .withColumn("rolling_avg", avg("amount").over(w3))
)
"""

# ============================================================
# SOLUTION: Optimized implementation
# ============================================================

# --- Optimization 1: Configure Spark for window operations ---
spark.conf.set("spark.sql.shuffle.partitions", "2000")  # More partitions for 500M rows
spark.conf.set("spark.sql.windowExec.buffer.in.memory.threshold", "4096")
spark.conf.set("spark.memory.fraction", "0.8")

# --- Optimization 2: Pre-filter and select only needed columns early ---
# Reduce data volume BEFORE window operations
df_filtered = (
    spark.read.parquet("/data/transactions/")
    .select("customer_id", "txn_date", "amount", "txn_id")  # Only needed cols
    .filter(col("txn_date") >= "2024-01-01")  # Date range filter
    .filter(col("amount").isNotNull())  # Remove nulls early
)

# --- Optimization 3: Handle partition skew with salting ---
# Identify heavy hitters (customers with > 10K transactions)
customer_counts = df_filtered.groupBy("customer_id").count()
heavy_hitters = customer_counts.filter(col("count") > 10000).select("customer_id")

# For non-window operations on skewed data, use salting
# For window operations, we can't salt the partition key, but we can:
# (a) Process heavy hitters separately with more memory
# (b) Repartition strategically

# Split processing: heavy hitters vs normal customers
df_normal = df_filtered.join(broadcast(heavy_hitters), on="customer_id", how="left_anti")
df_heavy = df_filtered.join(broadcast(heavy_hitters), on="customer_id", how="inner")

# --- Optimization 4: Consolidate window specs (same partition + sort = ONE shuffle) ---
# CRITICAL: Reuse the same window spec wherever possible
window_asc = Window.partitionBy("customer_id").orderBy("txn_date")
window_rolling = (
    Window.partitionBy("customer_id")
          .orderBy("txn_date")
          .rowsBetween(-6, 0)
)

def apply_windows(dataframe):
    """Apply all window functions that share the same partition + sort."""
    return (
        dataframe
        # These all use the same partition + sort = SINGLE shuffle
        .withColumn("running_total", spark_sum("amount").over(window_asc))
        .withColumn("prev_amount", lag("amount", 1).over(window_asc))
        .withColumn("rolling_7d_avg", avg("amount").over(window_rolling))
        .withColumn("txn_count_7d", count("amount").over(window_rolling))
    )

# --- Optimization 5: Process segments with appropriate parallelism ---
# Normal customers: standard processing
df_normal_result = apply_windows(df_normal)

# Heavy hitters: repartition for more parallelism within partitions
df_heavy_repartitioned = df_heavy.repartition(500, "customer_id")
df_heavy_result = apply_windows(df_heavy_repartitioned)

# --- Optimization 6: Checkpoint to break lineage (prevents OOM from long plans) ---
df_normal_result.checkpoint()

# Union results
final_result = df_normal_result.unionByName(df_heavy_result)

# --- Optimization 7: If you need a DIFFERENT sort order, do it separately ---
# This requires a new shuffle — minimize what goes into it
window_desc = Window.partitionBy("customer_id").orderBy(col("txn_date").desc())

# Only compute on the subset that needs it
final_with_rank = final_result.withColumn("recency_rank", row_number().over(window_desc))

# --- Optimization 8: Cache strategically ---
# Cache the intermediate result if used multiple times downstream
final_with_rank.persist()  # Use MEMORY_AND_DISK for large datasets

final_with_rank.write.parquet("/output/enriched_transactions/", mode="overwrite")
```

**Key Points:**
- **Consolidate window specs:** Same `partitionBy + orderBy` = single shuffle. Different sort orders force additional shuffles — minimize them
- **Pre-filter aggressively:** Select only needed columns and filter dates before window operations. Reducing row count from 500M to relevant subset saves massive compute
- **Handle skew explicitly:** Heavy hitters (power users with millions of rows) cause individual partitions to OOM. Process them separately with more resources or split their data across time ranges
- **Checkpoint long lineages:** Multiple window operations create deep execution plans. Checkpointing materializes intermediate results and breaks the lineage DAG, reducing driver memory pressure
- **rowsBetween vs unbounded:** `rowsBetween(-6, 0)` only scans 7 rows per output row. Unbounded frames scan the entire partition — catastrophic for heavy-hitter partitions
- **Monitor with Spark UI:**
  - Check "Shuffle Write" — multiple shuffles mean multiple expensive stages
  - Check "Task Duration" skew in the stage summary — max >> median indicates partition skew
  - Check "Spill (Memory)" and "Spill (Disk)" — indicates OOM pressure
- **Production checklist:**
  - Set `spark.sql.adaptive.enabled=true` (AQE) for automatic partition coalescing
  - Set `spark.sql.adaptive.skewJoin.enabled=true` for skew handling
  - Use `spark.sql.files.maxPartitionBytes` to control input partition sizes
  - Consider writing intermediate results to Delta/Parquet and reading back for very complex pipelines

</details>

</article>
