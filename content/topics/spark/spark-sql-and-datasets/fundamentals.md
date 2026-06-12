---
title: "Spark SQL & Datasets — Fundamentals"
topic: spark
subtopic: spark-sql-and-datasets
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [spark, sql, dataframe, dataset, schema, temp-view, catalog, functions]
---

# Spark SQL & Datasets — Fundamentals

## 🎯 Analogy

Spark SQL turns Spark into a distributed SQL engine. Think of it as PostgreSQL stretched across hundreds of machines — you write the same SQL, but instead of querying a single server, you're querying petabytes of Parquet files on S3.

---

## SparkSession: The Entry Point

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyApp") \
    .config("spark.sql.warehouse.dir", "hdfs:///user/hive/warehouse") \
    .enableHiveSupport() \   # optional: connect to Hive metastore
    .getOrCreate()

# SparkSession exposes everything:
spark.sql(...)        # run SQL
spark.read(...)       # read data
spark.catalog(...)    # manage tables
spark.conf(...)       # read/set configuration
spark.sparkContext    # underlying RDD context
```

---

## Reading Data

```python
# Parquet (default, most efficient)
df = spark.read.parquet("s3://bucket/data/orders/")

# With options:
df = spark.read \
    .option("mergeSchema", "true") \
    .parquet("s3://bucket/data/orders/")

# JSON
df = spark.read \
    .option("multiLine", "true") \
    .json("data/orders.json")

# CSV
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("delimiter", ",") \
    .option("nullValue", "NULL") \
    .csv("data/orders.csv")

# JDBC (databases)
df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://host:5432/db") \
    .option("dbtable", "orders") \
    .option("user", "user") \
    .option("password", "pass") \
    .load()
```

---

## Writing Data

```python
# Parquet
df.write \
    .mode("overwrite")   \   # overwrite / append / ignore / errorIfExists
    .partitionBy("year", "month") \
    .parquet("s3://bucket/output/orders/")

# Single file (small result)
df.coalesce(1).write.mode("overwrite").csv("output/report.csv")

# Delta Lake
df.write.format("delta").mode("overwrite").save("s3://bucket/delta/orders/")

# Hive table
df.write.mode("overwrite").saveAsTable("analytics.orders_processed")
```

---

## Running SQL

```python
# Register DataFrame as a temp view
df.createOrReplaceTempView("orders")

# Run SQL against it
result = spark.sql("""
    SELECT
        region,
        COUNT(*) as order_count,
        SUM(amount) as total_revenue,
        AVG(amount) as avg_order_value
    FROM orders
    WHERE status = 'completed'
      AND order_date >= '2024-01-01'
    GROUP BY region
    ORDER BY total_revenue DESC
""")

# Global temp views (survive multiple SparkSessions)
df.createOrReplaceGlobalTempView("orders_global")
spark.sql("SELECT * FROM global_temp.orders_global")
```

---

## DataFrame Operations

```python
from pyspark.sql import functions as F

df = spark.table("orders")

# Selection
df.select("order_id", "amount", "status")
df.select(F.col("amount") * 1.1, F.col("status").alias("order_status"))

# Filtering
df.filter(F.col("amount") > 100)
df.filter("amount > 100 AND status = 'active'")   # SQL string
df.where(F.col("status").isin(["active", "pending"]))

# Aggregation
df.groupBy("region", "status") \
    .agg(
        F.count("*").alias("count"),
        F.sum("amount").alias("revenue"),
        F.avg("amount").alias("avg"),
        F.max("amount").alias("max_order"),
        F.collect_list("order_id").alias("order_ids"),
    )

# Joins
orders.join(customers, on="customer_id", how="inner")
orders.join(customers, orders.customer_id == customers.id, how="left")

# Sort
df.orderBy(F.desc("amount"), F.asc("order_date"))
df.sort("region", F.col("amount").desc())
```

---

## Built-in Functions

```python
from pyspark.sql import functions as F

# String functions
F.upper(F.col("name"))
F.lower(F.col("name"))
F.trim(F.col("email"))
F.concat(F.col("first"), F.lit(" "), F.col("last"))
F.regexp_replace(F.col("phone"), r"[^0-9]", "")
F.substring(F.col("code"), 1, 3)   # 1-indexed!

# Date functions
F.current_date()
F.year(F.col("order_date"))
F.month(F.col("order_date"))
F.date_diff(F.col("shipped_date"), F.col("order_date"))
F.date_add(F.col("order_date"), 30)
F.to_date(F.col("date_str"), "yyyy-MM-dd")

# Null handling
F.coalesce(F.col("amount"), F.lit(0.0))
F.when(F.col("status").isNull(), "unknown").otherwise(F.col("status"))
F.isnull(F.col("email"))
F.isnan(F.col("amount"))

# Math
F.round(F.col("amount"), 2)
F.abs(F.col("diff"))
F.log(F.col("value"))
F.pow(F.col("base"), F.lit(2))
```

---

## Schema and Catalog

```python
# Schema inspection
df.printSchema()
df.dtypes        # [("order_id", "string"), ("amount", "double"), ...]
df.schema        # StructType object

# Catalog operations
spark.catalog.listDatabases()
spark.catalog.listTables("analytics")
spark.catalog.tableExists("analytics", "orders")
spark.catalog.refreshTable("analytics.orders")  # invalidate cache

# Describe table
spark.sql("DESCRIBE TABLE analytics.orders")
spark.sql("DESCRIBE TABLE EXTENDED analytics.orders")
spark.sql("SHOW PARTITIONS analytics.orders")
```

---

## ▶️ Try It Yourself

```python
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.master("local[*]").appName("sql-demo").getOrCreate()

data = [
    ("O001", "US", 250.0, "completed"),
    ("O002", "EU", 75.0, "pending"),
    ("O003", "US", 1200.0, "completed"),
    ("O004", "APAC", 450.0, "cancelled"),
    ("O005", "EU", 890.0, "completed"),
]
df = spark.createDataFrame(data, ["order_id", "region", "amount", "status"])
df.createOrReplaceTempView("orders")

spark.sql("""
    SELECT region,
           COUNT(*) as orders,
           ROUND(SUM(amount), 2) as revenue
    FROM orders
    WHERE status = 'completed'
    GROUP BY region
    ORDER BY revenue DESC
""").show()
```

> **Run it:** Works with `local[*]` — no cluster needed.

---

## Interview Tips

> **Tip 1:** "What is the difference between a temp view and a table in Spark?" — A temp view (`createOrReplaceTempView`) is a named alias for a DataFrame that exists only for the duration of the SparkSession (session-scoped). A global temp view (`createOrReplaceGlobalTempView`) persists across sessions. A table (`saveAsTable`) writes data to the catalog (Hive metastore or Spark catalog) and persists after the session ends. Temp views are not backed by stored data; tables are.

> **Tip 2:** "How does Spark SQL compare to a database?" — Both accept SQL and return result sets, but Spark SQL processes data in parallel across a cluster (horizontal scale) vs a single database machine. Spark SQL has no indexes — it relies on partition pruning and predicate pushdown to skip data. For interactive, complex analytics on large datasets, Spark SQL wins. For low-latency point queries, a traditional database or columnar warehouse (Redshift, BigQuery) is better.

> **Tip 3:** "What are the write modes in Spark?" — `overwrite`: delete existing data and write new. `append`: add to existing data without deleting. `ignore`: if data exists, skip write (no error). `errorIfExists`: default — throw an error if data exists. For idempotent pipelines, `overwrite` is safest. For incremental loads, `append` is typical but requires deduplication logic downstream.
