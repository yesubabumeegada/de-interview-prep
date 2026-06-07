---
title: "PySpark DataFrame API - Intermediate"
topic: pyspark
subtopic: dataframe-api
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [pyspark, dataframe, window-functions, complex-types, null-handling, caching]
---

# PySpark DataFrame API — Intermediate Concepts

## Window Functions in PySpark

Window functions let you compute values across rows without collapsing the DataFrame — identical concept to SQL window functions.

```python
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    row_number, rank, dense_rank, lag, lead,
    sum, avg, count, first, last, col
)

# Define a window specification
dept_window = Window.partitionBy("department").orderBy(col("salary").desc())

# Ranking within each department
df = df.withColumn("rank", rank().over(dept_window))
df = df.withColumn("row_num", row_number().over(dept_window))
df = df.withColumn("dense_rnk", dense_rank().over(dept_window))

# Running total
running_window = Window.partitionBy("department") \
    .orderBy("hire_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df = df.withColumn("running_salary_total", sum("salary").over(running_window))

# Previous/Next row values
df = df.withColumn("prev_salary", lag("salary", 1).over(dept_window))
df = df.withColumn("next_salary", lead("salary", 1).over(dept_window))
df = df.withColumn("salary_change", col("salary") - lag("salary", 1).over(dept_window))
```

### Window Frame Types

```python
# ROWS frame: physical row offset
rows_frame = Window.partitionBy("dept") \
    .orderBy("date") \
    .rowsBetween(-2, 0)  # Current row + 2 preceding rows

# RANGE frame: logical value range
range_frame = Window.partitionBy("dept") \
    .orderBy("salary") \
    .rangeBetween(-10000, 10000)  # ±$10K from current salary

# Unbounded frames
full_partition = Window.partitionBy("dept") \
    .orderBy("date") \
    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
```

## Working with Complex Types

### Arrays

```python
from pyspark.sql.functions import (
    array, explode, explode_outer, collect_list, collect_set,
    array_contains, size, array_distinct, array_union, flatten
)

# Create arrays
df = df.withColumn("skills", array(lit("python"), lit("sql"), lit("spark")))

# Explode: one row per array element (removes NULLs/empty arrays)
df_exploded = df.select("name", explode("skills").alias("skill"))

# Explode outer: keeps rows with NULL/empty arrays (NULL for the exploded column)
df_exploded = df.select("name", explode_outer("skills").alias("skill"))

# Aggregate back into arrays
df_grouped = df_exploded.groupBy("department").agg(
    collect_list("skill").alias("all_skills"),     # Keeps duplicates
    collect_set("skill").alias("unique_skills"),   # Deduplicates
)

# Array operations
df.filter(array_contains(col("skills"), "python"))
df.withColumn("skill_count", size("skills"))
df.withColumn("unique_skills", array_distinct("skills"))
```

### Structs (Nested Objects)

```python
from pyspark.sql.functions import struct, col

# Create struct
df = df.withColumn("address", struct(
    col("street"),
    col("city"),
    col("state"),
    col("zip")
))

# Access nested fields
df.select("address.city", "address.state")

# Flatten a struct into top-level columns
df.select("name", "address.*")  # Expands to street, city, state, zip
```

### Maps (Key-Value Pairs)

```python
from pyspark.sql.functions import create_map, map_keys, map_values, explode

# Create a map column
df = df.withColumn("metadata", create_map(
    lit("source"), col("source_system"),
    lit("version"), col("data_version")
))

# Access map values
df.select(col("metadata")["source"].alias("source"))

# Explode map into key-value rows
df.select("id", explode("metadata").alias("key", "value"))
```

## Null Handling

```python
from pyspark.sql.functions import coalesce, when, isnull, isnan

# Drop rows with any NULL
df.na.drop()                        # Any column has NULL
df.na.drop(subset=["name", "email"]) # Specific columns

# Drop rows where ALL specified columns are NULL
df.na.drop(how="all", subset=["col1", "col2"])

# Fill NULLs with default values
df.na.fill(0)                        # All numeric columns → 0
df.na.fill({"salary": 0, "department": "Unknown"})  # Per-column

# Coalesce: first non-null value
df.withColumn("display_name", coalesce(col("preferred_name"), col("full_name"), lit("Anonymous")))

# Conditional null handling
df.withColumn("salary_clean",
    when(col("salary").isNull() | isnan(col("salary")), lit(0))
    .otherwise(col("salary"))
)
```

## Caching and Persistence

```python
from pyspark import StorageLevel

# Cache in memory (lazy — only caches when action is triggered)
df.cache()      # Equivalent to persist(MEMORY_AND_DISK)
df.count()      # Triggers actual caching

# Persistence levels (control memory/disk trade-off)
df.persist(StorageLevel.MEMORY_ONLY)          # RAM only, recompute if evicted
df.persist(StorageLevel.MEMORY_AND_DISK)       # Spill to disk if RAM full
df.persist(StorageLevel.DISK_ONLY)             # Disk only (for very large DFs)
df.persist(StorageLevel.MEMORY_ONLY_SER)       # Serialized (less RAM, more CPU)

# Unpersist when done
df.unpersist()

# Check if cached
df.is_cached  # True/False
```

**When to cache:**
- DataFrame is reused multiple times in the same job
- After an expensive transformation (large join, complex aggregation)
- NEVER cache something only used once — it wastes memory

## Union and Set Operations

```python
# Union (append rows — columns must match in order and type)
combined = df1.union(df2)              # Positional matching
combined = df1.unionByName(df2)        # Match by column name (safer)

# Union with schema evolution
combined = df1.unionByName(df2, allowMissingColumns=True)  # Fills missing with NULL

# Distinct / Deduplicate
df.distinct()                          # All columns
df.dropDuplicates(["user_id", "event_date"])  # Specific columns (keeps first)

# Set operations
df1.intersect(df2)       # Rows in both
df1.subtract(df2)        # Rows in df1 but not df2
df1.exceptAll(df2)       # Like subtract but preserves duplicates
```

## Column Expressions and when/otherwise

```python
from pyspark.sql.functions import when, col, regexp_extract, split, trim, lower

# Multi-condition CASE WHEN
df = df.withColumn("tier",
    when(col("revenue") > 1000000, "Enterprise")
    .when(col("revenue") > 100000, "Mid-Market")
    .when(col("revenue") > 10000, "SMB")
    .otherwise("Startup")
)

# String operations
df = df.withColumn("domain", 
    regexp_extract(col("email"), r"@(.+)$", 1)
)
df = df.withColumn("first_name", split(col("full_name"), " ")[0])
df = df.withColumn("clean_name", trim(lower(col("name"))))

# Type casting
df = df.withColumn("salary_int", col("salary").cast("integer"))
df = df.withColumn("event_date", col("event_timestamp").cast("date"))
```

## Handling Schema Evolution

```python
# Read with schema enforcement (production-safe)
expected_schema = StructType([...])  # Your expected schema
df = spark.read.schema(expected_schema).parquet("s3://bucket/data/")
# Throws error if file doesn't match schema

# Read with merge schema (for evolving Parquet/Delta)
df = spark.read.option("mergeSchema", "true").parquet("s3://bucket/data/")

# Programmatic schema comparison
new_cols = set(incoming_df.columns) - set(target_df.columns)
missing_cols = set(target_df.columns) - set(incoming_df.columns)

# Add missing columns as NULL
for c in missing_cols:
    incoming_df = incoming_df.withColumn(c, lit(None).cast(target_df.schema[c].dataType))
```

## Interview Tip 💡

> Interviewers love to ask "how would you handle NULLs in this transformation?" or "what if the schema changes?" Have a standard pattern ready: (1) Use explicit schemas at read time, (2) Handle NULLs immediately after read with `coalesce`/`na.fill`, (3) Validate with assertions (`assert df.filter(col("id").isNull()).count() == 0`). This shows defensive engineering.
