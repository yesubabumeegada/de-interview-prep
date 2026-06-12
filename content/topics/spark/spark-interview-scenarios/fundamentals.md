---
title: "Spark Interview Scenarios — Fundamentals"
topic: spark
subtopic: spark-interview-scenarios
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [spark, interview, scenarios, common-questions, concepts, word-count, etl]
---

# Spark Interview Scenarios — Fundamentals

## 🎯 How to Use This Section

This subtopic is scenario-based practice. Each scenario is a realistic interview question. Read the question, think through your answer, then read the solution. The goal is to build pattern recognition so you can recognize and solve common Spark problems during interviews.

---

## Scenario 1: Word Count (the "Hello World" of Spark)

**Question:** Write a Spark program to count word frequencies in a large text file.

**Solution:**

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[*]").getOrCreate()

# DataFrame approach (preferred):
df = spark.read.text("s3://bucket/logs/")   # each line is one row: {value: STRING}
word_counts = (
    df.select(F.explode(F.split(F.col("value"), r"\s+")).alias("word"))
    .filter(F.col("word") != "")
    .groupBy(F.lower(F.col("word")).alias("word"))
    .count()
    .orderBy(F.desc("count"))
)
word_counts.write.parquet("s3://bucket/word-counts/")

# RDD approach (shows fundamentals):
rdd = spark.sparkContext.textFile("s3://bucket/logs/")
counts = (
    rdd
    .flatMap(lambda line: line.lower().split())
    .filter(lambda w: w)
    .map(lambda w: (w, 1))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda kv: -kv[1])
)
counts.saveAsTextFile("output/")
```

**Follow-up: "Why is the DataFrame approach better than RDD?"**
DataFrame benefits from Catalyst optimization (predicate pushdown, column pruning) and Tungsten code generation. The RDD approach runs as Python bytecode on every row — slower and uses more memory.

---

## Scenario 2: Find the Top N Per Group

**Question:** Given a sales DataFrame (columns: salesperson_id, region, amount), find the top 3 salespersons per region by total sales.

**Solution:**

```python
from pyspark.sql import functions as F, Window

# Option 1: Window function (best approach)
window = Window.partitionBy("region").orderBy(F.desc("total_sales"))

result = (
    df.groupBy("region", "salesperson_id")
      .agg(F.sum("amount").alias("total_sales"))
      .withColumn("rank", F.dense_rank().over(window))
      .filter(F.col("rank") <= 3)
      .drop("rank")
      .orderBy("region", F.desc("total_sales"))
)

# Option 2: SQL
df.createOrReplaceTempView("sales")
spark.sql("""
    SELECT region, salesperson_id, total_sales
    FROM (
        SELECT
            region,
            salesperson_id,
            SUM(amount) as total_sales,
            DENSE_RANK() OVER (PARTITION BY region ORDER BY SUM(amount) DESC) as rk
        FROM sales
        GROUP BY region, salesperson_id
    )
    WHERE rk <= 3
    ORDER BY region, total_sales DESC
""").show()
```

**Follow-up: "What's the difference between rank(), dense_rank(), and row_number()?"**
- `rank()`: assigns the same rank to ties, with gaps (1, 1, 3)
- `dense_rank()`: same rank to ties, no gaps (1, 1, 2)
- `row_number()`: unique integer per row, no ties allowed (1, 2, 3 — arbitrary for ties)

For top-N-per-group, `dense_rank()` is usually correct — it ensures exactly N unique ranks even with ties.

---

## Scenario 3: Deduplicate Records

**Question:** A pipeline receives records that may be duplicated. Deduplicate by `event_id`, keeping the latest version by `updated_at`.

```python
from pyspark.sql import functions as F, Window

# Method 1: Window + row_number (for keeping latest)
window = Window.partitionBy("event_id").orderBy(F.desc("updated_at"))
deduped = (
    df.withColumn("rn", F.row_number().over(window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

# Method 2: dropDuplicates (keeps first, no control over which one)
df.dropDuplicates(["event_id"])   # any record for each event_id

# Method 3: For Delta Lake (MERGE — most efficient for streaming dedup)
from delta.tables import DeltaTable
DeltaTable.forPath(spark, "s3://bucket/delta/events/") \
    .alias("target") \
    .merge(df.alias("source"), "target.event_id = source.event_id") \
    .whenMatchedUpdate(
        condition="source.updated_at > target.updated_at",
        set={"*": "source.*"}
    ) \
    .whenNotMatchedInsertAll() \
    .execute()
```

---

## Scenario 4: Join Two Large DataFrames

**Question:** You need to join orders (1 TB) with customers (5 GB). What's your approach?

```python
# Check customer table size: 5 GB > default broadcast threshold (10 MB)
# → SortMergeJoin by default (two shuffles)

# Approach 1: Raise broadcast threshold (if 5 GB fits in executor memory)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(6 * 1024**3))  # 6 GB
result = orders.join(customers, "customer_id")
# Verify: result.explain() should show BroadcastHashJoin

# Approach 2: Explicit broadcast hint
from pyspark.sql.functions import broadcast
result = orders.join(broadcast(customers), "customer_id")

# Approach 3: Use AQE (Spark 3.0+)
# AQE measures actual customer table size after reading → may auto-broadcast
spark.conf.set("spark.sql.adaptive.enabled", "true")

# If 5 GB really can't broadcast (executor memory < 5 GB):
# Use SortMergeJoin with bucketing to avoid shuffle:
customers.write.bucketBy(200, "customer_id").sortBy("customer_id") \
    .saveAsTable("customers_bucketed")
orders.write.bucketBy(200, "customer_id").sortBy("customer_id") \
    .saveAsTable("orders_bucketed")
# Now join has no shuffle — both already sorted/partitioned by customer_id
```

---

## Scenario 5: Read CSV, Clean, Write Parquet

**Question:** Given a messy CSV with headers, null values, and inconsistent types — read, clean, and write as partitioned Parquet.

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *

# Define schema explicitly (never inferSchema in production — requires full scan)
schema = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("amount", StringType(), nullable=True),     # read as string, cast later
    StructField("order_date", StringType(), nullable=True),
    StructField("status", StringType(), nullable=True),
])

raw = spark.read \
    .option("header", "true") \
    .option("sep", ",") \
    .option("quote", '"') \
    .option("escape", "\\") \
    .schema(schema) \
    .csv("s3://bucket/raw/orders/*.csv")

cleaned = (
    raw
    .dropna(subset=["order_id"])                          # drop rows with no ID
    .withColumn("amount", F.col("amount").cast(DoubleType()))
    .withColumn("amount", F.when(F.col("amount") < 0, None).otherwise(F.col("amount")))
    .withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
    .withColumn("status", F.lower(F.trim(F.col("status"))))
    .withColumn("status", F.when(
        F.col("status").isin(["pending", "completed", "cancelled", "shipped"]),
        F.col("status")
    ).otherwise("unknown"))
    .withColumn("year", F.year(F.col("order_date")))
    .withColumn("month", F.month(F.col("order_date")))
    .dropDuplicates(["order_id"])
)

cleaned.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet("s3://bucket/processed/orders/")
```

---

## Interview Tips

> **Tip 1:** "Always explain your approach before writing code." — In a Spark interview, narrate your thinking: "This is a top-N-per-group problem, which I'd solve with a window function using dense_rank over a partition. Let me code that up." Interviewers care more about whether you understand the problem type than whether you remember exact API syntax.

> **Tip 2:** "Think about scale with every solution." — For every scenario, ask: "What if this data were 10× or 100× larger?" A groupBy that works at 10 GB might OOM at 1 TB if the result cardinality is high. Always mention partition strategy, broadcast join eligibility, and whether the result set could fit in memory.

> **Tip 3:** "Know the three deduplication patterns." — `dropDuplicates()` for simple dedup with no ordering requirement. Window `row_number()` for keeping a specific record (latest, first). Delta MERGE for streaming/upsert scenarios where you want idempotent dedup across batches. Interviewers often probe which one you'd choose and why.
