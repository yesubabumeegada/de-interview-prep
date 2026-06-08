---
title: "Apache Iceberg — Intermediate"
topic: data-lakehouse
subtopic: apache-iceberg
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [iceberg, spark, trino, partitioning, compaction]
---

# Apache Iceberg — Intermediate

## Working with Iceberg Tables in Spark

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3://bucket/warehouse") \
    .getOrCreate()

# Create Iceberg table
spark.sql("""
  CREATE TABLE IF NOT EXISTS local.db.orders (
    order_id     BIGINT,
    customer_id  BIGINT,
    amount       DECIMAL(18,2),
    status       STRING,
    order_ts     TIMESTAMP
  )
  USING iceberg
  PARTITIONED BY (days(order_ts))
  TBLPROPERTIES (
    'write.target-file-size-bytes' = '134217728',  -- 128MB
    'write.parquet.compression-codec' = 'zstd',
    'history.expire.max-snapshot-age-ms' = '604800000'  -- 7 days
  )
""")

# Insert data
df = spark.read.csv("s3://bucket/raw/orders.csv", header=True, inferSchema=True)
df.writeTo("local.db.orders").append()

# Upsert (MERGE)
spark.sql("""
  MERGE INTO local.db.orders AS target
  USING new_orders AS source
  ON target.order_id = source.order_id
  WHEN MATCHED AND source.status != target.status
    THEN UPDATE SET target.status = source.status, target.amount = source.amount
  WHEN NOT MATCHED
    THEN INSERT *
""")
```

---

## Hidden Partitioning in Practice

```python
# Create table with hidden time partitioning
spark.sql("""
  CREATE TABLE local.db.events
  USING iceberg
  PARTITIONED BY (hours(event_ts))   -- hidden partition on timestamp hours
""")

# Query WITHOUT specifying partition value (Iceberg auto-prunes)
spark.sql("""
  SELECT user_id, COUNT(*) as event_count
  FROM local.db.events
  WHERE event_ts BETWEEN '2024-01-15 09:00:00' AND '2024-01-15 17:00:00'
  GROUP BY user_id
""")
-- Iceberg reads only the hour=09 through hour=17 partitions
-- No partition filter in WHERE clause needed!

# Change partition granularity (partition evolution)
spark.sql("""
  ALTER TABLE local.db.events
  ADD PARTITION FIELD days(event_ts)
""")
-- Old data: partitioned by hours (unchanged)
-- New data: partitioned by days
-- Queries transparently read both old and new partitions
```

---

## Iceberg Compaction (rewrite_data_files)

```python
from pyiceberg.catalog import load_catalog

catalog = load_catalog("glue", **{
    "type": "glue",
    "region_name": "us-east-1",
})

table = catalog.load_table("db.orders")

# Compact small files into 128MB target files
from pyiceberg.expressions import GreaterThanOrEqual
from datetime import datetime

table.rewrite_data_files(
    strategy="binpack",  # or "sort"
    options={
        "target-file-size-bytes": str(128 * 1024 * 1024),
        "partial-progress.enabled": "true",
    }
)

# Sort-based compaction (also optimizes for sort order)
table.rewrite_data_files(
    strategy="sort",
    sort_order=table.sort_order().asc("customer_id").asc("order_ts"),
    options={
        "target-file-size-bytes": str(128 * 1024 * 1024),
    }
)

# Via Spark SQL
spark.sql("""
  CALL local.system.rewrite_data_files(
    table => 'db.orders',
    strategy => 'sort',
    sort_order => 'customer_id ASC, order_ts ASC',
    options => map('target-file-size-bytes', '134217728')
  )
""")
```

---

## Time Travel and Rollback

```python
# Query historical snapshot
spark.read \
    .option("snapshot-id", "8440261290880314876") \
    .format("iceberg") \
    .load("local.db.orders")

# Via SQL
spark.sql("SELECT * FROM local.db.orders VERSION AS OF 42")
spark.sql("SELECT * FROM local.db.orders TIMESTAMP AS OF '2024-01-15 10:00:00'")

# Inspect history
spark.sql("SELECT * FROM local.db.orders.history").show()
# snapshot_id | committed_at | parent_id | is_current_ancestor

# Rollback to snapshot
spark.sql("""
  CALL local.system.rollback_to_snapshot(
    table => 'db.orders',
    snapshot_id => 8440261290880314876
  )
""")

# Expire old snapshots (maintenance)
spark.sql("""
  CALL local.system.expire_snapshots(
    table => 'db.orders',
    older_than => TIMESTAMP '2024-01-01 00:00:00',
    retain_last => 5
  )
""")
```

---

## Iceberg with Trino (AWS Athena or Open-Source)

```sql
-- Register Iceberg table in Glue/Hive catalog for Trino
-- (Trino reads Iceberg via Iceberg connector)

-- Query with time travel
SELECT *
FROM iceberg.db.orders
FOR VERSION AS OF 42;

-- Table maintenance via Trino
ALTER TABLE iceberg.db.orders EXECUTE expire_snapshots(retention_threshold => '7d');
ALTER TABLE iceberg.db.orders EXECUTE rewrite_data_files;
ALTER TABLE iceberg.db.orders EXECUTE rewrite_manifests;

-- Partition inspection
SELECT partition, record_count, file_count, total_size
FROM iceberg.db."orders$partitions"
ORDER BY partition DESC
LIMIT 20;
```

---

## Interview Tips

> **Tip 1:** "How does Iceberg handle concurrent writes from two Spark jobs?" — Iceberg uses optimistic concurrency control. Each writer reads the current snapshot, makes changes, then tries to commit by atomically updating the metadata pointer. If two writers commit simultaneously, one wins and the other gets a conflict exception. The losing writer retries. This is much better than Hive's file-level locking, and better than Delta's serializable isolation on Databricks which blocks writers.

> **Tip 2:** "What's the difference between `rewrite_data_files` and `rewrite_manifests`?" — `rewrite_data_files` compacts small Parquet files into larger ones (solves small files problem, also sorts data for better pruning). `rewrite_manifests` compacts the metadata manifest files (solves a different problem: too many manifest files slow metadata reads). Both are maintenance procedures. Run `rewrite_data_files` after heavy streaming ingestion; run `rewrite_manifests` periodically.

> **Tip 3:** "How do you use Iceberg with multiple engines simultaneously?" — Iceberg's catalog (Hive Metastore, Glue, Nessie, Polaris) is the coordination point. Register the table once in the catalog. Spark writes streaming data; Trino serves interactive queries; Flink reads for streaming joins — all on the same Iceberg table. The key: each engine must use an Iceberg-compatible reader/writer. Iceberg's spec guarantees compatibility across engines because all use the same metadata format.
