---
title: "PySpark Data Sources - Fundamentals"
topic: pyspark
subtopic: data-sources
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [pyspark, data-sources, parquet, csv, json, delta, reading, writing]
---

# PySpark Data Sources — Fundamentals

## The DataFrameReader API

Every Spark data read starts with `spark.read` — a `DataFrameReader` that provides a fluent interface for data ingestion.

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataSources").getOrCreate()

df = (
    spark.read
    .format("parquet")
    .option("key", "value")
    .schema(my_schema)
    .load("s3://bucket/path/")
)
```

> **Key Insight:** Spark is lazy — `.load()` creates a DataFrame but doesn't read data until an action triggers execution.

---

## Reading Common Formats

```python
# Parquet (default, recommended)
df = spark.read.parquet("s3://data-lake/events/")

# CSV
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("nullValue", "NA")
    .option("dateFormat", "yyyy-MM-dd")
    .load("s3://raw-data/users.csv")
)

# JSON
df = (
    spark.read.format("json")
    .option("multiLine", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt")
    .load("s3://raw-data/events/*.json")
)

# Delta Lake (ACID on top of Parquet)
df = spark.read.format("delta").load("s3://lakehouse/sales/")
df = spark.read.format("delta").option("versionAsOf", 5).load("s3://lakehouse/sales/")

# Avro
df = spark.read.format("avro").load("s3://kafka-archive/topic/")
```

## Format Comparison

| Format | Columnar | Schema Embedded | Splittable | Best For |
|--------|----------|-----------------|------------|----------|
| Parquet | Yes | Yes | Yes | Analytics, data lake |
| CSV | No | No | Uncompressed only | Data exchange |
| JSON | No | Inferred | Uncompressed only | APIs, logs |
| Delta | Yes | Yes + log | Yes | Lakehouse, ACID |
| Avro | No (row) | Yes | Yes | Streaming, Kafka |

---

## Schema Inference vs Explicit Schema

```python
# Inference (risky in production — reads data twice, can guess wrong)
df = spark.read.option("inferSchema", "true").csv("data.csv")

# Explicit schema (production best practice)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

schema = StructType([
    StructField("user_id", StringType(), nullable=False),
    StructField("amount", DoubleType(), nullable=True),
    StructField("created_at", TimestampType(), nullable=True),
])
df = spark.read.schema(schema).csv("s3://raw/events/")
```

> **Rule:** Always use explicit schemas in production. Inference is only for exploration.

---

## The DataFrameWriter API

```python
df.write.format("parquet").mode("overwrite").option("compression", "snappy").partitionBy("year", "month").save("s3://output/")
```

## Write Modes

| Mode | Behavior | Use Case |
|------|----------|----------|
| `overwrite` | Replace all existing data | Full refresh |
| `append` | Add new files alongside existing | Incremental loads |
| `ignore` | No-op if path exists | Idempotent first-run |
| `error` (default) | Exception if path exists | Safety net |

---

## Partitioning Output

```python
df.write.partitionBy("year", "month", "day").parquet("s3://output/events/")
# Creates: s3://output/events/year=2024/month=01/day=15/part-00000.parquet
```

**Why partition?** Enables partition pruning — a query filtering `WHERE year = 2024 AND month = 1` only reads those directories.

---

## Compression Options

| Compression | Speed | Ratio | Recommended |
|-------------|-------|-------|-------------|
| Snappy | Fast | Moderate | Default (speed) |
| ZSTD | Medium | High | Best ratio |
| LZ4 | Fastest | Low | Streaming |

```python
df.write.option("compression", "zstd").parquet("s3://output/")
```

---

## File Path Patterns

```python
df = spark.read.parquet("s3://data/events/year=2024/month=0[1-3]/")  # Glob
df = spark.read.parquet("s3://data/jan/", "s3://data/feb/")          # Multiple paths
df = spark.read.json("s3://logs/2024-01-*/app-*.json")               # Wildcards
```

---

## Complete Example

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import col, year, month

spark = SparkSession.builder.appName("ETL").getOrCreate()

schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("amount", DoubleType(), True),
    StructField("order_date", DateType(), True),
])

raw_df = spark.read.schema(schema).option("header", "true").csv("s3://raw/orders/")

(
    raw_df
    .withColumn("year", year(col("order_date")))
    .withColumn("month", month(col("order_date")))
    .write.mode("overwrite")
    .partitionBy("year", "month")
    .option("compression", "snappy")
    .parquet("s3://curated/orders/")
)
```

---

## Interview Tips

> **Tip 1:** "Why Parquet over CSV?" — "Parquet is columnar (reads only needed columns), embeds schema, supports predicate pushdown via row group stats, and compresses 5-10x better. CSV is row-based — even reading one column requires scanning entire rows."

> **Tip 2:** "When to use explicit schemas?" — "Always in production. Inference reads data twice, can guess wrong types, and is non-deterministic. Explicit StructType schemas catch quality issues immediately and are self-documenting."

> **Tip 3:** "Overwrite vs append mode?" — "Overwrite replaces the entire target — use for full-refresh. Append adds files without touching existing — use for incremental. Caution: overwrite with partitionBy replaces ALL partitions unless dynamic partition overwrite mode is enabled."
