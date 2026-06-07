---
title: "PySpark Data Sources - Scenario Questions"
topic: pyspark
subtopic: data-sources
content_type: scenario_question
tags: [pyspark, data-sources, interview, scenarios]
---

# Scenario Questions — PySpark Data Sources

<article data-difficulty="junior">

## 🟢 Junior: Read CSV and Write as Partitioned Parquet

**Scenario:** Read a daily CSV with headers (`order_id`, `customer_id`, `amount`, `order_date`, `region`). Write as Snappy-compressed Parquet partitioned by `year` and `month` from `order_date`.

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql.functions import col, year, month

spark = SparkSession.builder.appName("CSVToParquet").getOrCreate()

schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("amount", DoubleType(), True),
    StructField("order_date", DateType(), True),
    StructField("region", StringType(), True),
])

raw_df = (
    spark.read.schema(schema)
    .option("header", "true")
    .option("dateFormat", "yyyy-MM-dd")
    .csv("s3://raw-data/sales/daily_sales.csv")
)

(
    raw_df
    .withColumn("year", year(col("order_date")))
    .withColumn("month", month(col("order_date")))
    .write.mode("append")
    .partitionBy("year", "month")
    .option("compression", "snappy")
    .parquet("s3://datalake/curated/sales/")
)
```

**Key points:** Explicit schema avoids double-read from inferSchema. `partitionBy` creates directory structure enabling partition pruning on reads.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Parallel JDBC Extraction from 500M-Row Table

**Scenario:** Extract a 500M-row `transactions` table from PostgreSQL (ID range 1-500M). Cluster: 10 executors × 4 cores. DB allows max 50 connections (shared with app). Design the partition strategy.

<details>
<summary>✅ Solution</summary>

```python
# Decision: numPartitions = 20 (leaves 30 connections for the app)
bounds_df = spark.read.format("jdbc").option("url", url).option(
    "query", "SELECT MIN(id), MAX(id) FROM transactions").load()
bounds = bounds_df.collect()[0]

transactions_df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://prod-db:5432/app")
    .option("dbtable", "transactions")
    .option("user", "spark_reader").option("password", "secret")
    .option("partitionColumn", "id")
    .option("lowerBound", str(bounds[0]))
    .option("upperBound", str(bounds[1] + 1))
    .option("numPartitions", "20")
    .option("fetchSize", "50000")
    .load()
)

(
    transactions_df
    .withColumn("year", year("created_at"))
    .write.mode("overwrite")
    .partitionBy("year")
    .option("compression", "zstd")
    .parquet("s3://datalake/raw/transactions/")
)
```

**Handling ID gaps (skew):** If gaps cause extreme skew, use custom predicates based on percentile boundaries:

```python
predicates = [f"id >= {bounds[i]} AND id < {bounds[i+1]}" for i in range(len(bounds)-1)]
df = spark.read.jdbc(url=url, table="transactions", predicates=predicates, properties=props)
```

**Sizing rule:** `numPartitions = min(cluster_cores, db_connections / 2)`

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Handle Schema Evolution Without Breaking Downstream

**Scenario:** Your upstream API added 3 new columns without notice. Downstream pipelines (dbt, ML features) now fail. Design a strategy that: accommodates new columns, detects changes, and prevents breaking changes from propagating.

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

EXPECTED_SCHEMA = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("amount", DoubleType(), True),
])

def compare_schemas(expected, actual):
    expected_fields = {f.name: f for f in expected.fields}
    actual_fields = {f.name: f for f in actual.fields}
    added = set(actual_fields) - set(expected_fields)
    removed = set(expected_fields) - set(actual_fields)
    type_changes = {n for n in expected_fields.keys() & actual_fields.keys()
                    if expected_fields[n].dataType != actual_fields[n].dataType}
    return {"added": added, "removed": removed, "type_changes": type_changes,
            "is_breaking": bool(removed or type_changes)}

def safe_ingest(spark, source_path, target_path, expected_schema):
    raw_df = spark.read.option("mergeSchema", "true").parquet(source_path)
    diff = compare_schemas(expected_schema, raw_df.schema)

    if diff["is_breaking"]:
        raise ValueError(f"BREAKING CHANGE: removed={diff['removed']}, types={diff['type_changes']}")

    if diff["added"]:
        print(f"INFO: New columns detected: {diff['added']}")  # Alert team

    # Project only expected columns — shields downstream
    safe_df = raw_df.select([f.name for f in expected_schema.fields])
    safe_df.write.format("delta").mode("append").option("mergeSchema", "false").save(target_path)
```

**Architecture:** Expected schema as contract → classify changes (additive=safe, breaking=halt) → project expected columns → Delta enforcement as safety net → manual promotion of new columns after review.

</details>

</article>

---

## Interview Tips

> **Tip 1:** "How do you approach a data source scenario?" — "Start with requirements: volume, source type, downstream needs. Design for reliability (schema enforcement, error handling), performance (parallelism, partitioning), and operability (monitoring, alerts)."

> **Tip 2:** "What trade-offs for JDBC numPartitions?" — "Balance extraction speed vs database impact. More partitions = faster but more connections. Size as min(cluster_parallelism, db_pool / 2). Validate with the DBA."

> **Tip 3:** "How do you prevent schema evolution from breaking pipelines?" — "Three layers: schema comparison classifying additive vs breaking changes, column projection passing only expected columns downstream, and Delta schema enforcement as a safety net."
