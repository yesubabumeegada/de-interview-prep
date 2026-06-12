---
title: "Spark SQL & Datasets — Real World"
topic: spark
subtopic: spark-sql-and-datasets
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [spark, sql, production, schema-evolution, partition-pruning, dynamic-partition, real-world]
---

# Spark SQL & Datasets — Real World

## War Story: Dynamic Partition Overwrite Surprise

**Scenario:** An hourly pipeline wrote partitioned Parquet with `mode("overwrite")`. Engineers expected it to overwrite only the current hour's partition. Instead, it was deleting all existing partitions.

**Root cause:**
```python
# WRONG: Static partition overwrite — deletes ALL partitions
df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "hour") \
    .parquet("s3://bucket/events/")
# This deletes the ENTIRE s3://bucket/events/ directory before writing!
```

**Fix:** Enable dynamic partition overwrite:
```python
# Only overwrites partitions present in the new data
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df.write \
    .mode("overwrite") \
    .partitionBy("year", "month", "hour") \
    .parquet("s3://bucket/events/")
# Now: only year=2024/month=06/hour=14/ is replaced
```

```python
# Or use Delta Lake — MERGE handles this naturally:
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "s3://bucket/delta/events/")
delta_table.alias("target").merge(
    df.alias("source"),
    "target.event_id = source.event_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

---

## War Story: Schema Evolution Breaking Downstream

**Scenario:** An upstream team added two new columns to an orders table. The Parquet files had the new columns; the existing Parquet files didn't. Downstream Spark job threw:

```
AnalysisException: column 'shipping_address' not found in schema
```

**Root cause:** Spark's default behavior reads schema from the first file found. If that file predates the new columns, the schema is incomplete.

**Fix:**
```python
# Enable schema merging — Spark reads schema from ALL files and unions them
df = spark.read \
    .option("mergeSchema", "true") \
    .parquet("s3://bucket/orders/")

# For Delta Lake: schema evolution is explicit
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
df.write.format("delta").mode("append").option("mergeSchema", "true").save(path)

# For production: use explicit schema + fill defaults for missing columns
schema = StructType([...])   # full expected schema
df = spark.read.schema(schema).parquet(path)
df = df.fillna({"shipping_address": "", "shipping_cost": 0.0})
```

---

## Partition Pruning Pitfalls

```python
# BUG: Function on partition column disables pruning
df = spark.read.parquet("s3://bucket/events/")
# Schema: year INT, month INT, event_date DATE

# WRONG — Spark cannot prune; reads ALL partitions:
df.filter(F.year(F.col("event_date")) == 2024)    # function wraps partition col

# CORRECT — direct partition column filter → pruned:
df.filter(F.col("year") == 2024)

# Verify with explain():
df.filter(F.col("year") == 2024).explain()
# Look for: PartitionFilters: [isnotnull(year#0), (year#0 = 2024)]

# Common trap: casting disables pruning too
df.filter(F.col("year").cast("string") == "2024")  # WRONG — reads all
df.filter(F.col("year") == 2024)                    # CORRECT
```

---

## Efficient Incremental Loads

```python
from pyspark.sql import functions as F

# Track high-watermark in metadata table
last_run = spark.sql("SELECT MAX(run_ts) FROM etl_metadata WHERE job='orders'") \
                .collect()[0][0]

# Read only new data
new_data = spark.read.parquet("s3://bucket/orders/") \
    .filter(F.col("updated_at") > last_run)

# Process and write
result = transform(new_data)
result.write.mode("append").partitionBy("year", "month").parquet("output/")

# Update watermark
spark.sql(f"""
    INSERT INTO etl_metadata VALUES ('orders', current_timestamp(), {new_data.count()})
""")
```

---

## Interview Tips

> **Tip 1:** "What is dynamic partition overwrite and when do you need it?" — By default, Spark's `overwrite` mode replaces the entire output location — all partitions. Dynamic partition overwrite (`spark.sql.sources.partitionOverwriteMode=dynamic`) replaces only the partitions present in the current write. Essential for any pipeline writing to a date-partitioned table where you want idempotent partition-level rewrites without touching other partitions. Delta Lake handles this automatically via MERGE.

> **Tip 2:** "How do you handle schema evolution in Spark?" — Three options in increasing robustness: (1) `mergeSchema=true` on read — Spark union all file schemas, filling missing columns with null; simple but reads all files to build schema. (2) Explicit schema with defaults — define full expected schema, read with it, fill nulls for missing columns. (3) Delta Lake — tracks schema in transaction log, supports automatic or manual schema evolution with `mergeSchema` or `overwriteSchema` options. For production, Delta Lake is the right answer.
