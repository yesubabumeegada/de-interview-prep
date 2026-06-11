---
title: "Delta Lake Integration — Fundamentals"
topic: pyspark
subtopic: delta-lake-integration
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [delta-lake, pyspark, ACID, parquet, lakehouse, transactions]
---

# Delta Lake Integration — Fundamentals

Delta Lake is the storage layer that turns a data lake into a lakehouse. Before Delta, data lakes were fast to write to but unreliable — partial writes, no atomic updates, no way to read consistent data while a write was happening. Delta solves all of this while staying Parquet-compatible.

---


## 🎯 Analogy

Think of Delta Lake like a Google Doc for your data: every save creates a new version you can go back to, multiple writers don't corrupt each other, and ACID transactions mean either everything saves or nothing does.

---
## What Is Delta Lake?

Delta Lake is an open-source storage format built on top of Parquet files. It adds:

1. **ACID transactions** — reads and writes are atomic; partial writes never become visible.
2. **Versioned transaction log** — every change is recorded in a JSON/checkpoint log.
3. **Schema enforcement** — rejects writes that don't match the table schema.
4. **Schema evolution** — can add new columns without rewriting the table.
5. **Time travel** — query any previous version of the table.
6. **Scalable metadata** — handles tables with millions of files without listing overhead.

### Delta vs Plain Parquet

| Feature | Parquet | Delta Lake |
|---|---|---|
| ACID transactions | ❌ | ✅ |
| Partial write protection | ❌ | ✅ |
| Concurrent reads during write | ❌ (can read partial data) | ✅ (snapshot isolation) |
| Schema enforcement | ❌ | ✅ |
| `UPDATE` / `DELETE` / `MERGE` | ❌ (must rewrite files) | ✅ |
| Time travel | ❌ | ✅ |
| Table history | ❌ | ✅ |

---

## Creating Delta Tables

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable

spark = SparkSession.builder \
    .appName("delta-fundamentals") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Method 1: Write a DataFrame as a Delta table
df = spark.createDataFrame([
    (1, "Alice", "2024-01-15", 250.0),
    (2, "Bob",   "2024-01-15", 175.0),
    (3, "Carol", "2024-01-16", 320.0),
], ["order_id", "customer_name", "order_date", "amount"])

df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://my-lakehouse/tables/orders/")

# Method 2: Create a Delta table with SQL
spark.sql("""
    CREATE TABLE IF NOT EXISTS warehouse.orders (
        order_id      BIGINT,
        customer_name STRING,
        order_date    DATE,
        amount        DOUBLE
    )
    USING DELTA
    LOCATION 's3://my-lakehouse/tables/orders/'
    PARTITIONED BY (order_date)
""")

# Method 3: Convert existing Parquet to Delta (in-place)
spark.sql("""
    CONVERT TO DELTA parquet.`s3://my-lakehouse/old-parquet-table/`
""")
```

---

## Reading Delta Tables

```python
# Read as DataFrame
orders = spark.read.format("delta").load("s3://my-lakehouse/tables/orders/")
orders.show()

# Or via catalog (if table is registered)
orders = spark.table("warehouse.orders")

# Delta automatically reads the latest snapshot
# No stale reads — consistent as of when the read started
```

---

## Writing to Delta Tables

```python
# Append new records
new_orders = spark.createDataFrame([
    (4, "Dave", "2024-01-17", 400.0),
    (5, "Eve",  "2024-01-17", 150.0),
], ["order_id", "customer_name", "order_date", "amount"])

new_orders.write \
    .format("delta") \
    .mode("append") \
    .save("s3://my-lakehouse/tables/orders/")

# Overwrite entire table
new_orders.write \
    .format("delta") \
    .mode("overwrite") \
    .save("s3://my-lakehouse/tables/orders/")

# Overwrite a specific partition only (safe — only replaces that partition)
todays_data.write \
    .format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "order_date = '2024-01-17'") \
    .save("s3://my-lakehouse/tables/orders/")
# The replaceWhere option is safer than full overwrite — it's atomic
# and only affects the matched partition
```

---

## ACID Transactions: What They Actually Mean

ACID stands for Atomicity, Consistency, Isolation, Durability. Here's what each means in a Delta context:

```python
# ATOMICITY: A write either fully succeeds or leaves no trace
# Scenario: your Spark job writes 100 files, then the cluster crashes after 50 files

# Without Delta (plain Parquet): you have 50 corrupt files in the table
# With Delta: the transaction was never committed to the log
#             → readers see the previous complete version
#             → the 50 orphaned files are cleaned up by VACUUM

# ISOLATION: Readers see a consistent snapshot
# Scenario: Writer is appending 1M rows. Reader starts mid-write.

# Without Delta: reader may see 0 to 1M rows depending on timing
# With Delta: reader gets a snapshot — either the full previous version
#             or the full new version. Never a partial view.

# Verify by checking the transaction log:
delta_table = DeltaTable.forPath(spark, "s3://my-lakehouse/tables/orders/")
delta_table.history().show(truncate=False)
# +-------+---------+------+--------------------+
# |version|timestamp|userId|operation           |
# +-------+---------+------+--------------------+
# |      2|2024-01-17|...  |WRITE               |
# |      1|2024-01-16|...  |WRITE               |
# |      0|2024-01-15|...  |CREATE TABLE        |
```

---

## The Transaction Log: Delta's Secret Sauce

Every Delta table has a `_delta_log/` directory containing JSON files that record every transaction.

```
s3://my-lakehouse/tables/orders/
├── _delta_log/
│   ├── 00000000000000000000.json   ← transaction 0: table created
│   ├── 00000000000000000001.json   ← transaction 1: first append
│   ├── 00000000000000000002.json   ← transaction 2: second append
│   └── 00000000000000000010.checkpoint.parquet  ← checkpoint every 10 txns
├── part-00000-abc.snappy.parquet
├── part-00001-def.snappy.parquet
└── ...
```

Each JSON log entry contains:
- Which files were **added** (new Parquet files from this write)
- Which files were **removed** (for updates/deletes — old files are marked removed, not deleted)
- **Metadata**: schema, partitioning, statistics per file (min/max values)
- **Protocol version**: min reader/writer version required

```python
# Inspect the log directly
import json

# Read a transaction log entry
log_content = spark.read.text(
    "s3://my-lakehouse/tables/orders/_delta_log/00000000000000000001.json"
)
log_content.show(100, truncate=False)
# Each line is a JSON action: "add", "remove", "metaData", "commitInfo"
```

---

## Schema Enforcement and Evolution

```python
# Schema enforcement: Delta rejects incompatible writes by default
df_wrong_schema = spark.createDataFrame([
    ("not_a_number", "Alice"),
], ["order_id", "customer_name"])
# order_id should be BIGINT, not String

try:
    df_wrong_schema.write.format("delta").mode("append") \
        .save("s3://my-lakehouse/tables/orders/")
except Exception as e:
    print(f"Schema mismatch rejected: {e}")
# AnalysisException: Failed to merge fields 'order_id'

# Schema evolution: add a new column
df_with_new_column = spark.createDataFrame([
    (6, "Frank", "2024-01-18", 200.0, "premium"),
], ["order_id", "customer_name", "order_date", "amount", "tier"])

# Without option: fails (strict schema enforcement)
# With mergeSchema: adds the new column, nulls for existing rows
df_with_new_column.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("s3://my-lakehouse/tables/orders/")

# Existing rows now have tier = null
# New rows have tier = "premium"
spark.table("warehouse.orders").show()
```

---

## Time Travel: Querying Previous Versions

```python
# By version number
v0 = spark.read.format("delta") \
    .option("versionAsOf", 0) \
    .load("s3://my-lakehouse/tables/orders/")
v0.show()

# By timestamp
orders_yesterday = spark.read.format("delta") \
    .option("timestampAsOf", "2024-01-16 00:00:00") \
    .load("s3://my-lakehouse/tables/orders/")

# In SQL
spark.sql("""
    SELECT * FROM warehouse.orders VERSION AS OF 0
""")

spark.sql("""
    SELECT * FROM warehouse.orders TIMESTAMP AS OF '2024-01-15'
""")

# Use case: audit — what did the table look like before the bad write?
bad_write_version = 3
before_bad = spark.read.format("delta") \
    .option("versionAsOf", bad_write_version - 1) \
    .load("s3://my-lakehouse/tables/orders/")
# Compare to current to understand what changed
```

---

## Key Takeaways for Junior DEs

1. **Delta = Parquet + a transaction log.** The `_delta_log/` directory is what makes Delta different.
2. **ACID means** no partial reads, no partial writes, crash-safe appends.
3. **Schema enforcement** catches data quality issues at write time, not hours later when a query fails.
4. **`mergeSchema`** safely adds new columns; **`overwriteSchema`** replaces the schema entirely (destructive — use with caution).
5. **Time travel** is invaluable for audits and recovering from bad writes — but it requires keeping old files (`VACUUM` deletes them after the retention period).
6. **`replaceWhere`** is the safe way to overwrite a partition — atomic and partition-scoped.

## ▶️ Try It Yourself

```python
# Requires delta-spark package: pip install delta-spark
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]")     .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")     .appName("delta").getOrCreate()
data = [("Alice", 300), ("Bob", 150)]
df = spark.createDataFrame(data, ["name", "amount"])
df.write.format("delta").mode("overwrite").save("/tmp/delta_demo")
spark.read.format("delta").load("/tmp/delta_demo").show()
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
