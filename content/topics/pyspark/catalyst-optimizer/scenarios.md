---
title: "PySpark Catalyst Optimizer - Scenario Questions"
topic: pyspark
subtopic: catalyst-optimizer
content_type: scenario_question
tags: [pyspark, catalyst, optimizer, interview, scenarios]
---

# Scenario Questions — PySpark Catalyst Optimizer

<article data-difficulty="junior">

## 🟢 Junior: What Does Catalyst Do?

**Scenario:** A colleague asks: "Why is my Spark DataFrame query faster than my RDD code that does the same thing?" Explain the role of the Catalyst optimizer.

<details>
<summary>✅ Solution</summary>

**The Catalyst optimizer automatically applies optimizations that you'd have to code manually with RDDs:**

| Optimization | What Catalyst Does | RDD Equivalent (manual) |
|-------------|-------------------|------------------------|
| Predicate pushdown | Pushes filters to storage layer (reads less data) | You must filter early manually |
| Column pruning | Only reads needed columns from Parquet | You must select columns before processing |
| Broadcast join | Detects small table and broadcasts it | You must call `sc.broadcast()` manually |
| Join reordering | Finds optimal join order for multi-table queries | You must order joins yourself |
| Constant folding | Pre-computes constant expressions at compile time | N/A — RDDs compute everything at runtime |

**Example:**
```python
# DataFrame (Catalyst optimizes):
result = spark.read.parquet("s3://data/") \
    .filter("date = '2024-01-15'") \
    .select("user_id", "amount") \
    .groupBy("user_id").sum("amount")
# Catalyst: pushes date filter to Parquet reader, reads only 2 columns, 
# skips irrelevant row groups. Maybe 1% of data actually read.

# Equivalent RDD (no optimization):
rdd = sc.textFile("s3://data/")  # Reads EVERYTHING
    .map(parse_row)               # Parses ALL columns
    .filter(lambda r: r.date == "2024-01-15")  # Filters AFTER reading all data
    .map(lambda r: (r.user_id, r.amount))
    .reduceByKey(add)
# Reads 100% of data, parses all columns, then filters. 100x slower.
```

**Bottom line:** Catalyst makes DataFrames self-optimizing. The optimizer is why "write simple DataFrame code and let Spark optimize it" is the correct approach.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Filter Not Being Pushed Down

**Scenario:** Your query reads from a Parquet table on S3. You expect predicate pushdown to skip 90% of data, but `explain()` shows no `PushedFilters`. The query scans everything. Why?

```python
df = spark.read.parquet("s3://lake/events/")
result = df.filter(my_custom_udf(col("event_type")) == "purchase")
result.explain()
# No PushedFilters! Full scan of 5 TB!
```

<details>
<summary>✅ Solution</summary>

**Root cause:** The `my_custom_udf()` wrapping the column is opaque to Catalyst. It can't push an expression it doesn't understand to the Parquet reader.

**Fix: Replace UDF with native Spark expression:**

```python
# BAD: UDF blocks pushdown
@udf(returnType=StringType())
def my_custom_udf(event_type):
    return event_type.lower().strip()

df.filter(my_custom_udf(col("event_type")) == "purchase")  # No pushdown!

# GOOD: Native functions — Catalyst can push down
from pyspark.sql.functions import lower, trim

df.filter(lower(trim(col("event_type"))) == "purchase")
# Catalyst may partially push: PushedFilters: [IsNotNull(event_type)]
# And Parquet row-group-level filtering works on the raw column

# BEST: Filter on the raw column value (maximum pushdown)
df.filter(col("event_type") == "purchase")
# PushedFilters: [EqualTo(event_type, purchase)] ← Pushed to Parquet reader!
# Parquet skips entire row groups where event_type stats don't include "purchase"
```

**Other common reasons for no pushdown:**
1. **Filter on derived column:** `df.withColumn("x", col("a") + col("b")).filter("x > 10")` — derived columns can't push down
2. **OR conditions with unsupported expressions:** Complex OR logic may not push
3. **Data source doesn't support it:** JSON/CSV don't support pushdown (no row-group stats)
4. **Nested column filters:** `df.filter(col("struct_col.nested_field") == X)` — limited pushdown support

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: UDF Preventing Join Optimization

**Scenario:** Your pipeline applies a UDF to a join key before joining. This prevents Catalyst from choosing a broadcast join (because it can't determine the output size of the UDF). The job takes 2 hours with sort-merge join. The dimension table is only 50 MB. Redesign without the UDF.

```python
# Current (slow): UDF on join key blocks broadcast optimization
normalize_key = udf(lambda k: k.strip().lower().replace("-", ""))

fact_df = fact_df.withColumn("clean_key", normalize_key(col("product_code")))
dim_df = dim_df.withColumn("clean_key", normalize_key(col("product_code")))
result = fact_df.join(dim_df, "clean_key")  # Sort-merge (Catalyst can't see dim is small)
```

<details>
<summary>✅ Solution</summary>

**Why Catalyst can't optimize:** The UDF creates a new column whose statistics are unknown. Catalyst doesn't know `dim_df` after UDF transformation is still 50 MB → defaults to sort-merge join.

**Fix 1: Replace UDF with native functions (Catalyst can optimize)**

```python
from pyspark.sql.functions import lower, trim, regexp_replace

# Replace UDF with native Spark expressions
def clean_key(col_name):
    """Native expression — Catalyst can reason about output size."""
    return lower(trim(regexp_replace(col(col_name), "-", "")))

fact_df = fact_df.withColumn("clean_key", clean_key("product_code"))
dim_df = dim_df.withColumn("clean_key", clean_key("product_code"))

# Now Catalyst knows dim_df size is still ~50 MB → broadcasts it!
result = fact_df.join(dim_df, "clean_key")
# Plan shows: BroadcastHashJoin (no sort-merge!)
```

**Fix 2: Explicit broadcast hint (force it even with UDF)**

```python
# If you can't remove the UDF, force broadcast manually
from pyspark.sql.functions import broadcast

result = fact_df.join(broadcast(dim_df), "clean_key")
# Forces broadcast regardless of Catalyst's size estimate
# Works but: you're responsible for ensuring dim_df fits in executor memory
```

**Fix 3: Pre-compute the clean key in the dimension table (at ETL time)**

```python
# During dimension ETL: store the cleaned key as a physical column
# Then at query time: join on the pre-computed column (no runtime UDF!)

# Dimension ETL (runs once daily):
dim_df.withColumn("clean_key", clean_key("product_code")) \
    .write.mode("overwrite").parquet("s3://lake/dim_product_with_clean_key/")

# Query time: just read the pre-computed column
dim_clean = spark.read.parquet("s3://lake/dim_product_with_clean_key/")
result = fact_df.withColumn("clean_key", clean_key("product_code")) \
    .join(broadcast(dim_clean), "clean_key")
# Broadcast works because dim_clean's size is known from Parquet metadata
```

**Result:** 2 hours → 5 minutes (broadcast join eliminates the sort-merge shuffle of the billion-row fact table).

**Lesson:** UDFs on join keys are one of the worst performance anti-patterns in Spark. Always use native functions for key transformations, or pre-compute cleaned keys at ETL time.

</details>

</article>
