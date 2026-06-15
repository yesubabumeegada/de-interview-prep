---
title: "Interview Scenarios - Real World"
topic: pyspark
subtopic: interview-scenarios
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [pyspark, interview, mistakes, optimization, complexity, tips]
---

# PySpark Interview Scenarios — Real World

## Common Interview Mistakes That Immediately Signal Inexperience

These are the mistakes that interviewers specifically watch for. Each one is a signal about your production experience level.

---

### Mistake 1: Using `collect()` on a Large DataFrame

```python
# ❌ WRONG — pulls all data to the driver, causes OOM on large tables
all_users = df.collect()
for user in all_users:
    process(user)

# ✅ RIGHT — process data distributed on the cluster
df.foreachPartition(lambda rows: [process(row) for row in rows])

# ✅ RIGHT — use Spark transformations, not Python loops
df_processed = df.withColumn("processed_field", some_spark_function(col("field")))
```

**Why `collect()` is almost never right:** It moves gigabytes/terabytes of data to a single node (the driver). The driver has limited memory. On a 1TB dataset, `collect()` will kill your driver and fail the job.

**When `collect()` IS acceptable:**
- On a DataFrame you have explicitly aggregated down to < 1000 rows
- In unit tests with small test fixtures
- Reading a small config/lookup table that you'll use in a Python variable (even then, consider `broadcast` instead)

---

### Mistake 2: Writing UDFs for Operations That Have Built-in Equivalents

```python
# ❌ WRONG — Python UDF for string operations
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def clean_email(email):
    return email.strip().lower() if email else None

df = df.withColumn("email", clean_email(col("email")))

# ✅ RIGHT — built-in functions (10-100x faster, JVM-native)
from pyspark.sql.functions import trim, lower, when, col

df = df.withColumn("email", when(col("email").isNotNull(), trim(lower(col("email")))))
```

**The cost of Python UDFs:** Each row crosses the JVM-Python boundary twice (once for input, once for output). For a 1B-row DataFrame this boundary crossing alone can add minutes to your job. Built-in Spark functions execute entirely in the JVM using vectorized operations.

**Built-in equivalents interviewers expect you to know:**

| Task | Python UDF (don't use) | Built-in (use this) |
|------|------------------------|---------------------|
| String ops | custom `udf` | `lower`, `upper`, `trim`, `regexp_replace`, `split` |
| Date math | custom `udf` | `datediff`, `date_add`, `to_date`, `date_format` |
| Null handling | custom `udf` | `coalesce`, `when(...).otherwise(...)`, `isNull`, `fillna` |
| JSON parsing | custom `udf` | `from_json`, `get_json_object` |
| Array ops | custom `udf` | `explode`, `array_contains`, `transform`, `filter` |
| Math | custom `udf` | `round`, `abs`, `sqrt`, `pow`, `log` |

---

### Mistake 3: Not Caching a DataFrame Used Multiple Times

```python
# ❌ WRONG — Spark recomputes df_cleaned from scratch for EACH action
df_cleaned = (
    df_raw
    .filter(col("status") == "active")
    .withColumn("email", trim(lower(col("email"))))
)

count = df_cleaned.count()          # full scan 1
df_cleaned.write.parquet("/output") # full scan 2
df_cleaned.show()                   # full scan 3
# Each action re-reads df_raw and re-executes all transformations

# ✅ RIGHT — cache after expensive transformations, before multiple actions
df_cleaned = (
    df_raw
    .filter(col("status") == "active")
    .withColumn("email", trim(lower(col("email"))))
)
df_cleaned.cache()
df_cleaned.count()  # materializes the cache (forces computation)

count = df_cleaned.count()           # reads from cache
df_cleaned.write.parquet("/output")  # reads from cache
df_cleaned.show()                    # reads from cache

# Always unpersist when done to free executor memory
df_cleaned.unpersist()
```

**When to cache vs. not cache:**
- Cache when: the DataFrame is used in 2+ downstream actions OR in multiple branches of your pipeline
- Don't cache when: the DataFrame is used exactly once (caching adds overhead)
- Don't cache the raw/source DataFrame unless you need to re-read it multiple times (the read itself is usually the bottleneck, not the computation)

---

### Mistake 4: Wrong Join Type Causing Row Explosion or Row Loss

```python
# Row explosion: many-to-many join
# If orders and line_items both have multiple rows per order_id,
# joining them without deduplication first multiplies rows
df_exploded = orders.join(line_items, on="order_id", how="inner")
# Expected: 1M rows. Actual: 1M * 5 = 5M rows (each order × each line item)

# ✅ RIGHT — deduplicate or aggregate BEFORE joining
df_orders_deduped = orders.dropDuplicates(["order_id"])
df_joined = df_orders_deduped.join(line_items, on="order_id", how="inner")

# Row loss: inner join when you need all records
# ❌ WRONG for when you need all users even if they have no orders
df_lost = users.join(orders, on="user_id", how="inner")  # drops users with 0 orders

# ✅ RIGHT — left join preserves all users
df_all_users = users.join(orders, on="user_id", how="left")
df_all_users = df_all_users.fillna(0, subset=["order_count"])
```

---

## How to Recover Gracefully When Stuck

Interviewers expect candidates to get stuck — it's part of the test. What separates good candidates is how they handle it.

**Recovery framework:**

1. **Narrate your thinking.** "I know I need to find the latest record per user. I'm thinking of a Window function with `row_number()`, let me work through the window spec."

2. **Ask a clarifying question.** "To clarify — when there are two records with the same timestamp, does it matter which one we keep?" This shows production thinking, not uncertainty.

3. **Start with the simpler approach.** Write the naive solution first, get it correct, then optimize. "Let me start with `dropDuplicates` to get a correct answer, then we can talk about why `row_number` is better."

4. **Use SQL if you're blanking on the API.** `df.createOrReplaceTempView("t"); spark.sql("SELECT ...")` is fully valid and often clearer for complex logic.

---

## Discussing Time and Space Complexity for Spark Operations

Interviewers at senior levels expect you to reason about cost, not just correctness.

| Operation | Shuffle? | Notes |
|-----------|----------|-------|
| `filter`, `withColumn`, `select` | No | Narrow transformation, O(n) per partition |
| `groupBy`, `agg` | Yes | Two-stage: local partial agg + shuffle + global agg |
| `join` (sort-merge) | Yes | Both sides shuffled on join key; O(n log n) |
| `join` (broadcast) | No | Small side replicated to each executor; O(n) |
| `window` function | Yes | Partitioned shuffle + sort within partition |
| `distinct`, `dropDuplicates` | Yes | Shuffle to co-locate duplicates |
| `repartition(n)` | Yes | Full shuffle to n partitions |
| `coalesce(n)` | No (usually) | Reduces partitions without full shuffle |
| `cache()` | No | Stores result in executor memory after first action |

**Key talking point:** "The most expensive Spark operations are shuffles — they require network I/O across all executors. I try to minimize shuffles by pushing filters before joins, broadcasting small tables, and repartitioning once before multiple operations on the same key."

---

## What Interviewers Actually Look For

In priority order:

1. **Correctness** — Does the code produce the right answer? Include null handling, edge cases.
2. **API fluency** — DataFrame API, not RDD. Built-ins, not UDFs. Readable chaining.
3. **Performance awareness** — Can you identify and fix a shuffle, a skew, a missing cache?
4. **Production thinking** — Schema validation, error handling, idempotency, monitoring.
5. **Communication** — Can you explain what you're doing and why, and discuss trade-offs?

**The single highest-signal behavior:** When you get a working solution and then say unprompted, "This will have a performance problem if the data is skewed on X column — here's how I'd handle that." This signals that you've seen production data at scale, not just run notebooks on sample datasets.

---

## Key Takeaways

- `collect()` on large DataFrames is the most common interview red flag — avoid it.
- Python UDFs are 10-100x slower than built-in functions — know the built-in equivalents.
- Cache DataFrames used in multiple actions; unpersist when done.
- Wrong join type (inner vs. left, many-to-many) is a correctness bug before it's a performance bug.
- Narrate your thinking, ask clarifying questions, and propose optimizations after getting correctness right.
