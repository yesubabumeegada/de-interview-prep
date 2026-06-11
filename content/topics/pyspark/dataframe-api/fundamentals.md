---
title: "PySpark DataFrame API - Fundamentals"
topic: pyspark
subtopic: dataframe-api
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [pyspark, dataframe, spark, transformations, actions, schema]
---

# PySpark DataFrame API — Fundamentals


## 🎯 Analogy

Think of a Spark DataFrame like a giant spreadsheet that lives across hundreds of computers simultaneously. You describe the transformations you want (filter, join, group), and Spark figures out the most efficient way to apply them in parallel across all machines.

---
## What Is a PySpark DataFrame?

A PySpark DataFrame is a **distributed collection of data organized into named columns** — like a table in a database or a Pandas DataFrame, but designed to process terabytes of data across a cluster of machines.

**Key difference from Pandas:**

| Aspect | Pandas | PySpark |
|--------|--------|---------|
| Runs on | Single machine | Distributed cluster |
| Data size | Fits in RAM (GB) | Any size (TB+) |
| Execution | Immediate (eager) | Lazy (builds plan first) |
| Parallelism | None (single core*) | Automatic (many cores) |

> **Key Insight:** In PySpark, nothing actually executes when you write transformations. Spark builds an execution plan and only runs it when you call an "action" (like `.show()` or `.count()`). This lets the optimizer rearrange your operations for maximum efficiency.

---

## How Spark Processes Your Code

```mermaid
flowchart LR
    A["Your DataFrame code"] --> B["Logical Plan"]
    B --> C["Catalyst Optimizer"]
    C --> D["Optimized Physical Plan"]
    D --> E["Execute across cluster"]
```

**What this shows:**
- You write DataFrame operations (filter, join, group)
- Spark creates a logical plan (what you asked for)
- The Catalyst optimizer rewrites it (pushes filters early, removes unnecessary columns)
- Only when you call an action does the optimized plan actually execute

---

## Creating DataFrames

### Method 1: From Python Data (Testing/Development)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DE Prep").getOrCreate()

# From list of tuples
data = [
    ("Alice", "Engineering", 95000),
    ("Bob", "Marketing", 72000),
    ("Charlie", "Engineering", 110000),
]
df = spark.createDataFrame(data, ["name", "department", "salary"])
```

### Method 2: From Files (Most Common in Production)

```python
# Parquet — preferred for analytics (columnar, compressed)
df = spark.read.parquet("s3://bucket/data/events/")

# CSV with header
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)

# JSON
df = spark.read.json("s3://bucket/data/users.json")

# Delta Lake (with ACID transactions)
df = spark.read.format("delta").load("s3://bucket/delta-table/")
```

### Method 3: With Explicit Schema (Production Best Practice)

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("department", StringType(), nullable=True),
    StructField("salary", IntegerType(), nullable=True),
])

df = spark.read.schema(schema).parquet("s3://bucket/data/")
# Fails fast if file doesn't match schema — safer than inferSchema
```

> **Why explicit schema matters:** `inferSchema=True` reads the entire file first (slow for large files) and may guess wrong types. In production, always define your schema.

---

## Transformations vs Actions — The Most Important Concept

| Transformations (Lazy — build plan) | Actions (Trigger execution) |
|-------------------------------------|---------------------------|
| `select()`, `filter()`, `where()` | `show()`, `display()` |
| `groupBy()`, `join()` | `count()`, `first()` |
| `withColumn()`, `drop()` | `collect()`, `take(n)` |
| `orderBy()`, `distinct()` | `write.parquet()`, `write.csv()` |
| `union()`, `repartition()` | `toPandas()` |

```python
# NONE of this executes yet — only builds a plan:
filtered = df.filter(df.salary > 80000)          # Lazy
selected = filtered.select("name", "salary")     # Lazy
sorted_df = selected.orderBy("salary")           # Lazy

# THIS triggers execution of the entire chain:
sorted_df.show()  # ACTION — Spark now processes everything
```

**Result of `.show()`:**

```
+-------+------+
|   name|salary|
+-------+------+
|  Alice| 95000|  -- Bob (72000) was filtered out by salary > 80000
|Charlie|110000|  -- Only Alice and Charlie remain, sorted by salary
+-------+------+
```

> **Why lazy execution matters:** Spark can optimize the entire chain. Example: if you select only 2 columns but earlier created 10 columns with `withColumn`, Spark may skip computing the 8 unused columns entirely.

---

## Essential Transformations

### select() — Choose Columns

```python
from pyspark.sql.functions import col, upper, lit

# Select specific columns
df.select("name", "salary")

# With expressions and renaming
df.select(
    col("name"),
    col("salary"),
    (col("salary") * 12).alias("annual_salary"),
    upper(col("department")).alias("dept_upper"),
    lit("USD").alias("currency")
)
```

**Result:**

| name | salary | annual_salary | dept_upper | currency |
|------|--------|--------------|-----------|----------|
| Alice | 95000 | 1140000 | ENGINEERING | USD |
| Bob | 72000 | 864000 | MARKETING | USD |
| Charlie | 110000 | 1320000 | ENGINEERING | USD |

---

### filter() / where() — Row Filtering

```python
from pyspark.sql.functions import col

# Simple condition
df.filter(col("salary") > 80000)

# Multiple conditions (& = AND, | = OR, ~ = NOT)
df.filter(
    (col("salary") > 80000) & 
    (col("department") == "Engineering")
)

# String matching
df.filter(col("name").like("A%"))          # Starts with A
df.filter(col("department").isin("Engineering", "Sales"))

# NULL handling
df.filter(col("email").isNotNull())
```

> **Important:** Use `&` and `|` (not `and`/`or`). Wrap each condition in parentheses. This is because PySpark overloads bitwise operators.

---

### withColumn() — Add or Modify Columns

```python
from pyspark.sql.functions import when, coalesce, current_timestamp, lit

# Add a new column
df = df.withColumn("tax", col("salary") * 0.3)

# Conditional logic (like CASE WHEN in SQL)
df = df.withColumn("level",
    when(col("salary") > 120000, "Senior")
    .when(col("salary") > 90000, "Mid")
    .otherwise("Junior")
)

# Replace NULLs with a default
df = df.withColumn("department",
    coalesce(col("department"), lit("Unknown"))
)

# Add processing metadata
df = df.withColumn("processed_at", current_timestamp())
```

**Result (with level column):**

| name | salary | level |
|------|--------|-------|
| Alice | 95000 | Mid |
| Bob | 72000 | Junior |
| Charlie | 110000 | Mid |

---

### groupBy() + Aggregations

```python
from pyspark.sql.functions import count, sum as spark_sum, avg, max as spark_max

df.groupBy("department").agg(
    count("*").alias("headcount"),
    avg("salary").alias("avg_salary"),
    spark_max("salary").alias("max_salary"),
    spark_sum("salary").alias("total_payroll"),
)
```

**Result:**

| department | headcount | avg_salary | max_salary | total_payroll |
|-----------|-----------|-----------|-----------|--------------|
| Engineering | 2 | 102500 | 110000 | 205000 |
| Marketing | 1 | 72000 | 72000 | 72000 |

> **Note:** Import `sum` as `spark_sum` to avoid shadowing Python's built-in `sum`.

---

### join() — Combining DataFrames

```python
# Sample dimension table
departments = spark.createDataFrame([
    (10, "Engineering", "NYC"),
    (20, "Marketing", "London"),
], ["dept_id", "dept_name", "location"])

# Inner join
result = employees.join(departments, 
    employees.dept_id == departments.dept_id, 
    "inner"
)

# Left join (keep all employees)
result = employees.join(departments,
    employees.dept_id == departments.dept_id,
    "left"
)

# Available join types: "inner", "left", "right", "full", "semi", "anti", "cross"
```

> **Handling duplicate column names:** After a join, both tables may have columns with the same name. Use aliases:
> ```python
> result = employees.alias("e").join(
>     departments.alias("d"),
>     col("e.dept_id") == col("d.dept_id")
> ).select(col("e.name"), col("d.dept_name"))
> ```

---

## Writing Data

```python
# Parquet — preferred (columnar, compressed, schema-embedded)
df.write.mode("overwrite").parquet("s3://bucket/output/")

# Partitioned write (essential for large datasets)
df.write \
    .mode("overwrite") \
    .partitionBy("department", "year") \
    .parquet("s3://bucket/output/")

# Write modes:
# "overwrite" — replace existing data
# "append"    — add to existing data
# "ignore"    — do nothing if data already exists
# "error"     — fail if data already exists (default)
```

---

## Schema Inspection

```python
# Print the schema tree
df.printSchema()
# root
#  |-- name: string (nullable = false)
#  |-- department: string (nullable = true)
#  |-- salary: integer (nullable = true)

# Get column names and types
print(df.columns)    # ['name', 'department', 'salary']
print(df.dtypes)     # [('name', 'string'), ('department', 'string'), ('salary', 'int')]

# Row count (triggers execution!)
print(df.count())    # 3
```

---

## Common Mistakes to Avoid

| Mistake | Problem | Fix |
|---------|---------|-----|
| `df.collect()` on large data | OOM crash on driver | Use `df.show(20)` or `df.take(5)` |
| `df.count()` to check if empty | Scans ALL data | Use `len(df.head(1)) > 0` |
| `withColumn` in a for loop | Extremely slow plan building | Use single `select()` with all columns |
| `inferSchema=True` in production | Wrong types, slow reads | Define schema explicitly |
| Forgetting parentheses in filters | Wrong results | `(col("a") > 1) & (col("b") < 10)` |

---


## ▶️ Try It Yourself

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = SparkSession.builder.master("local[*]").appName("demo").getOrCreate()
data = [(1, "Alice", 300), (2, "Bob", 150), (3, "Alice", 200)]
df = spark.createDataFrame(data, ["id", "name", "amount"])
result = df.groupBy("name").agg(spark_sum("amount").alias("total"))
result.show()
# +-----+-----+
# | name|total|
# +-----+-----+
# |Alice|  500|
# |  Bob|  150|
# +-----+-----+
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** Structure your PySpark answers as: (1) Read with explicit schema, (2) Apply transformations (filter → select → transform → aggregate), (3) Write partitioned output. This shows you understand the pipeline pattern.

> **Tip 2:** Always mention lazy evaluation: "These transformations only build a plan. Spark optimizes it before execution, potentially skipping unnecessary columns or reordering filters for efficiency."

> **Tip 3:** When asked about joins, mention broadcast: "If the dimension table is small (under 10MB), I'd broadcast it to avoid shuffling the large fact table across the cluster."
