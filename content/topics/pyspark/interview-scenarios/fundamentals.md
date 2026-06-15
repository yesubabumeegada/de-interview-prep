---
title: "Interview Scenarios - Fundamentals"
topic: pyspark
subtopic: interview-scenarios
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [pyspark, interview, deduplication, aggregation, joins, filtering, null-handling]
---

# PySpark Interview Scenarios — Fundamentals

## How PySpark Coding Interviews Work

PySpark interviews typically take one of two formats:

| Format | What to expect |
|--------|---------------|
| **Notebook (Databricks/Colab)** | Write and run code live. Interviewers see output immediately. Common for senior roles. |
| **Whiteboard / shared doc** | Write syntactically correct code without running it. Focus is on logic and API knowledge. |
| **Take-home** | 2-4 hour problem set. Emphasis on correctness, production-readiness, and test coverage. |

**Common problem types in order of frequency:**
1. Deduplication / finding latest record per group
2. Aggregation (groupBy, window functions)
3. Join with edge cases (nulls, many-to-many, missing keys)
4. Transformation chains (filter → withColumn → select)
5. Reading/writing with options (formats, schema, partition columns)

---

## Interview Tips

- **Use DataFrame API, not RDD.** Using RDD in a 2024+ interview signals you haven't kept up with the ecosystem. The only acceptable RDD use is explaining why DataFrame is better.
- **Chain transformations readably.** Use `(df\n    .filter(...)\n    .withColumn(...)\n    .select(...))` formatting.
- **Handle nulls explicitly.** Interviewers look for null awareness. Mention it even if you don't implement it.
- **Name your intermediate DataFrames** descriptively — `df_deduped`, `df_agg`, not `df2`, `df3`.
- **Comment on trade-offs**, not just code. "This uses a Window function which requires a full shuffle" signals senior thinking even at a junior level.

---

## Pattern 1: Deduplication

### `dropDuplicates()` — simplest form

```python
# Remove fully duplicate rows
df_deduped = df.dropDuplicates()

# Deduplicate on specific columns (keep one row per unique combination)
df_deduped = df.dropDuplicates(["user_id", "event_type"])
```

**Limitation:** When multiple rows match the dedup key, `dropDuplicates` keeps an arbitrary one — you cannot control which row is retained.

### `row_number()` — keep the latest/earliest record

```python
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# Keep the most recent event per user (by event_timestamp descending)
window_spec = Window.partitionBy("user_id").orderBy(col("event_timestamp").desc())

df_deduped = (
    df
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)
```

Use `row_number()` whenever you need to control which record survives deduplication (latest, earliest, highest value).

---

## Pattern 2: Aggregation

```python
from pyspark.sql.functions import count, sum, avg, max, min, countDistinct, collect_list

# Basic aggregation
df_agg = (
    df
    .groupBy("department", "job_level")
    .agg(
        count("*").alias("employee_count"),
        avg("salary").alias("avg_salary"),
        max("salary").alias("max_salary"),
        countDistinct("manager_id").alias("distinct_managers"),
    )
)

# Filter after aggregation (equivalent to SQL HAVING)
df_agg_filtered = df_agg.filter(col("employee_count") > 5)
```

**Null awareness in aggregations:**
- `count("*")` counts all rows including nulls.
- `count("salary")` counts only non-null salary rows.
- `sum`, `avg`, `max`, `min` all ignore null values automatically.

---

## Pattern 3: Filtering and Transformation Chains

```python
from pyspark.sql.functions import col, when, upper, trim, to_date, datediff, current_date

df_transformed = (
    df
    # Filter step
    .filter(col("status") == "active")
    .filter(col("created_at").isNotNull())
    # Transform step
    .withColumn("email", trim(upper(col("email"))))
    .withColumn("signup_date", to_date(col("created_at")))
    .withColumn("days_since_signup", datediff(current_date(), col("signup_date")))
    .withColumn(
        "user_tier",
        when(col("revenue_ltv") >= 1000, "gold")
        .when(col("revenue_ltv") >= 100, "silver")
        .otherwise("bronze")
    )
    # Select only needed columns
    .select("user_id", "email", "signup_date", "days_since_signup", "user_tier")
)
```

---

## Pattern 4: Basic Join Types

```python
# Inner join — only rows matching in both tables
df_inner = orders.join(customers, on="customer_id", how="inner")

# Left join — all orders, null for missing customers
df_left = orders.join(customers, on="customer_id", how="left")

# Anti join — orders with NO matching customer (orphaned records)
df_anti = orders.join(customers, on="customer_id", how="left_anti")

# Semi join — orders where a customer exists (no customer columns added)
df_semi = orders.join(customers, on="customer_id", how="left_semi")

# Joining on multiple columns
df_joined = orders.join(
    line_items,
    on=["order_id", "store_id"],
    how="inner"
)

# Joining on an expression (when column names differ)
df_joined = orders.join(
    customers,
    on=orders["cust_id"] == customers["customer_id"],
    how="left"
)
```

**Interview trap:** Forgetting that joining on a column with nulls never matches — `null != null` in SQL/Spark. If null keys should match, use `eqNullSafe`:

```python
df = df1.join(df2, df1["key"].eqNullSafe(df2["key"]), "inner")
```

---

## Pattern 5: Reading and Writing with Options

```python
# Read CSV with all common options
df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)        # always False in production
    .option("sep", ",")
    .option("quote", '"')
    .option("escape", "\\")
    .option("nullValue", "NULL")
    .option("mode", "PERMISSIVE")        # bad rows → null, not error
    .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    .schema(my_schema)
    .csv("s3://bucket/data.csv")
)

# Write partitioned Parquet
(
    df_transformed
    .write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("year", "month")
    .option("compression", "snappy")
    .save("s3://bucket/output/")
)

# Write Delta with merge schema
(
    df_transformed
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("silver.events")
)
```

---

## Key Takeaways

- Use `row_number()` when you need deterministic deduplication; use `dropDuplicates()` only when any surviving row is acceptable.
- `count("col_name")` ≠ `count("*")` — one ignores nulls, one doesn't.
- Null join keys never match — use `eqNullSafe` or filter nulls before joining.
- Always chain transformations in a readable, vertical format — interviewers read your style as a proxy for your production code quality.
- Always use explicit schema for reads — never `inferSchema=True` in code you're showing an interviewer.
