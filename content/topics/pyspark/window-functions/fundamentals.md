---
title: "PySpark Window Functions - Fundamentals"
topic: pyspark
subtopic: window-functions
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [pyspark, window-functions, ranking, analytics, partitioning, ordering]
---

# PySpark Window Functions — Fundamentals

## What Are Window Functions in PySpark?

Window functions compute a value for each row based on a "window" of related rows — without collapsing the DataFrame. Same concept as SQL window functions but expressed through PySpark's API.

**The analogy:** GROUP BY gives you one row per group. Window functions keep ALL rows but add a calculated column that "looks at" related rows in the same group.

```python
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number, rank, dense_rank, sum, avg, lag, lead

spark = SparkSession.builder.appName("WindowFunctions").getOrCreate()
```

> **Key Insight:** Every PySpark window function needs a `Window` specification that defines: (1) how to group rows (partitionBy), and (2) how to order them within each group (orderBy).

---

## Sample Data

```python
data = [
    ("Alice", "Engineering", 130000, "2020-01-15"),
    ("Bob", "Engineering", 95000, "2021-03-01"),
    ("Charlie", "Engineering", 110000, "2019-06-10"),
    ("Diana", "Marketing", 88000, "2021-04-01"),
    ("Eve", "Marketing", 105000, "2018-04-20"),
    ("Frank", "Sales", 72000, "2022-01-10"),
    ("Grace", "Sales", 82000, "2020-11-01"),
    ("Henry", "Sales", 78000, "2023-02-01"),
]

df = spark.createDataFrame(data, ["name", "department", "salary", "hire_date"])
df.show()
```

| name | department | salary | hire_date |
|------|-----------|--------|-----------|
| Alice | Engineering | 130000 | 2020-01-15 |
| Bob | Engineering | 95000 | 2021-03-01 |
| Charlie | Engineering | 110000 | 2019-06-10 |
| Diana | Marketing | 88000 | 2021-04-01 |
| Eve | Marketing | 105000 | 2018-04-20 |
| Frank | Sales | 72000 | 2022-01-10 |
| Grace | Sales | 82000 | 2020-11-01 |
| Henry | Sales | 78000 | 2023-02-01 |

---

## Window Specification — The Foundation

Every window function requires a `Window` spec:

```python
from pyspark.sql.window import Window

# Basic window: partition by department, order by salary descending
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())
```

**Components:**

| Component | Purpose | Example |
|-----------|---------|---------|
| `partitionBy()` | Divide rows into groups (like GROUP BY) | `partitionBy("department")` |
| `orderBy()` | Sort within each group | `orderBy(col("salary").desc())` |
| `rowsBetween()` | Define frame boundaries (optional) | `rowsBetween(-2, 0)` for last 3 rows |
| `rangeBetween()` | Define value-based frame (optional) | `rangeBetween(-1000, 1000)` |

---

## Ranking Functions

### row_number() — Unique Sequential Rank

```python
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

df.withColumn("rank", row_number().over(window_spec)).show()
```

**Result:**

| name | department | salary | rank |
|------|-----------|--------|------|
| Alice | Engineering | 130000 | 1 |
| Charlie | Engineering | 110000 | 2 |
| Bob | Engineering | 95000 | 3 |
| Eve | Marketing | 105000 | 1 |
| Diana | Marketing | 88000 | 2 |
| Grace | Sales | 82000 | 1 |
| Henry | Sales | 78000 | 2 |
| Frank | Sales | 72000 | 3 |

> **row_number()** always gives unique sequential numbers. If two rows have the same salary, the tie-break is arbitrary (non-deterministic). Use rank() or dense_rank() if ties should get the same number.

### rank() vs dense_rank()

```python
from pyspark.sql.functions import rank, dense_rank

# If Frank and Henry had the same salary (78000):
# rank():       Grace=1, Frank=2, Henry=2 (gap: next would be 4)
# dense_rank(): Grace=1, Frank=2, Henry=2 (no gap: next would be 3)
```

| Function | Ties get same rank? | Gaps after ties? | Use for |
|----------|:---:|:---:|------|
| `row_number()` | No (arbitrary tiebreak) | No | "Exactly N rows per group" |
| `rank()` | Yes | Yes (skip numbers) | "All tied values count as one rank" |
| `dense_rank()` | Yes | No | "Top N distinct values" |

---

## Top N Per Group — The #1 Interview Pattern

"Find the top 2 highest-paid employees per department:"

```python
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

top_2 = df.withColumn("rn", row_number().over(window_spec)) \
           .filter(col("rn") <= 2) \
           .drop("rn")

top_2.show()
```

**Result:**

| name | department | salary | hire_date |
|------|-----------|--------|-----------|
| Alice | Engineering | 130000 | 2020-01-15 |
| Charlie | Engineering | 110000 | 2019-06-10 |
| Eve | Marketing | 105000 | 2018-04-20 |
| Diana | Marketing | 88000 | 2021-04-01 |
| Grace | Sales | 82000 | 2020-11-01 |
| Henry | Sales | 78000 | 2023-02-01 |

> **This pattern:** window with row_number() + filter is the PySpark equivalent of SQL's `WHERE rn <= N` in a CTE. It's the most common window function interview question.

---

## Aggregate Window Functions

Standard aggregations (sum, avg, count, min, max) applied over a window:

```python
from pyspark.sql.functions import sum, avg, count, min, max

window_dept = Window.partitionBy("department")

result = df.select(
    "name", "department", "salary",
    sum("salary").over(window_dept).alias("dept_total"),
    avg("salary").over(window_dept).alias("dept_avg"),
    count("*").over(window_dept).alias("dept_count"),
    (col("salary") - avg("salary").over(window_dept)).alias("vs_dept_avg"),
)
result.show()
```

**Result (Engineering rows):**

| name | department | salary | dept_total | dept_avg | dept_count | vs_dept_avg |
|------|-----------|--------|-----------|---------|-----------|------------|
| Alice | Engineering | 130000 | 335000 | 111667 | 3 | +18333 |
| Bob | Engineering | 95000 | 335000 | 111667 | 3 | -16667 |
| Charlie | Engineering | 110000 | 335000 | 111667 | 3 | -1667 |

> **Key difference from groupBy:** Every row is preserved. The aggregate is computed per partition and repeated on each row in that partition.

---

## Running Totals (Cumulative Sum)

When you add `orderBy` to an aggregate window function, it becomes a running total:

```python
from pyspark.sql.functions import sum

window_running = Window.partitionBy("department") \
    .orderBy("hire_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("cumulative_salary", sum("salary").over(window_running)).show()
```

**Result (Engineering, ordered by hire_date):**

| name | department | salary | hire_date | cumulative_salary |
|------|-----------|--------|-----------|------------------|
| Charlie | Engineering | 110000 | 2019-06-10 | 110000 |
| Alice | Engineering | 130000 | 2020-01-15 | 240000 |
| Bob | Engineering | 95000 | 2021-03-01 | 335000 |

> **The frame clause** `rowsBetween(unboundedPreceding, currentRow)` means: "sum from the first row in the partition up to the current row." This is the default when ORDER BY is specified.

---

## Value Functions: lag() and lead()

Access values from previous or next rows without self-joining:

```python
from pyspark.sql.functions import lag, lead

window_ordered = Window.partitionBy("department").orderBy("hire_date")

result = df.select(
    "name", "department", "salary", "hire_date",
    lag("salary", 1).over(window_ordered).alias("prev_salary"),
    lead("salary", 1).over(window_ordered).alias("next_salary"),
    (col("salary") - lag("salary", 1).over(window_ordered)).alias("change"),
)
result.show()
```

**Result (Engineering):**

| name | hire_date | salary | prev_salary | next_salary | change |
|------|-----------|--------|------------|------------|--------|
| Charlie | 2019-06-10 | 110000 | NULL | 130000 | NULL |
| Alice | 2020-01-15 | 130000 | 110000 | 95000 | +20000 |
| Bob | 2021-03-01 | 95000 | 130000 | NULL | -35000 |

> **NULL for first/last:** lag() returns NULL for the first row (no previous). lead() returns NULL for the last. Use `lag("salary", 1, 0)` to provide a default value.

---

## Percent Rank and Percentiles

```python
from pyspark.sql.functions import percent_rank, ntile

window_global = Window.orderBy("salary")

df.select(
    "name", "salary",
    percent_rank().over(window_global).alias("percentile"),
    ntile(4).over(window_global).alias("quartile"),
).show()
```

| name | salary | percentile | quartile |
|------|--------|-----------|----------|
| Frank | 72000 | 0.0 | 1 |
| Henry | 78000 | 0.14 | 1 |
| Grace | 82000 | 0.29 | 2 |
| Diana | 88000 | 0.43 | 2 |
| Bob | 95000 | 0.57 | 3 |
| Eve | 105000 | 0.71 | 3 |
| Charlie | 110000 | 0.86 | 4 |
| Alice | 130000 | 1.0 | 4 |

---

## Common Patterns Summary

| Pattern | Window Spec | Function |
|---------|-------------|----------|
| Rank within group | `partitionBy(group).orderBy(metric.desc())` | `row_number()` |
| Top N per group | Same + `.filter(rn <= N)` | `row_number()` |
| Running total | `partitionBy(group).orderBy(date).rowsBetween(unbounded, current)` | `sum()` |
| Compare to previous | `partitionBy(group).orderBy(date)` | `lag(col, 1)` |
| Department average per row | `partitionBy(group)` (no orderBy) | `avg()` |
| Moving average (3-period) | `partitionBy(group).orderBy(date).rowsBetween(-2, 0)` | `avg()` |
| Quartile assignment | `orderBy(metric)` | `ntile(4)` |

---

## Interview Tips

> **Tip 1:** "How do you find top N per group in PySpark?" — "Window with row_number() partitioned by the group column, ordered by the metric descending, then filter where row_number <= N. This is the single most common PySpark interview question."

> **Tip 2:** "What's the difference between orderBy in the window vs orderBy at the end?" — "The window's orderBy defines the sequence for computing the window function (ranking, running totals). The final orderBy sorts the output for display. They're independent — you can rank by salary but output sorted by name."

> **Tip 3:** "When does a window function cause a shuffle?" — "partitionBy triggers a shuffle to group all rows with the same partition value on the same executor. If you partition by a high-cardinality column (user_id with 100M values), expect a massive shuffle. No partitionBy = entire dataset on one partition = no parallelism but no shuffle."
