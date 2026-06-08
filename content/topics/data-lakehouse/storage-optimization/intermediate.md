---
title: "Storage Optimization — Intermediate"
topic: data-lakehouse
subtopic: storage-optimization
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [storage, optimize, zorder, lifecycle, partitioning, clustering]
---

# Storage Optimization — Intermediate

## Delta OPTIMIZE and Z-Ordering in Practice

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .getOrCreate()

# Basic OPTIMIZE: compact small files → 128MB
spark.sql("OPTIMIZE delta.`s3://bucket/silver/orders`")

# OPTIMIZE on specific partition (faster, less I/O)
spark.sql("""
  OPTIMIZE delta.`s3://bucket/silver/orders`
  WHERE order_date >= '2024-01-01' AND order_date < '2024-02-01'
""")

# OPTIMIZE + ZORDER: compact + sort for multi-column queries
spark.sql("""
  OPTIMIZE delta.`s3://bucket/silver/orders`
  ZORDER BY (customer_id, order_date)
""")
-- Z-order co-locates rows with same customer_id + order_date values
-- Query: WHERE customer_id=123 AND order_date='2024-01-15' → skips 95%+ of files

-- When to ZORDER: after your query pattern is known
-- ZORDER by columns that appear in WHERE clauses most often
-- Diminishing returns: ZORDER by 3+ columns is usually not worth it

# Check OPTIMIZE results
spark.sql("""
  SELECT version, timestamp, operationMetrics.numFilesAdded, 
         operationMetrics.numFilesRemoved,
         operationMetrics.numOutputBytes
  FROM (DESCRIBE HISTORY delta.`s3://bucket/silver/orders`)
  WHERE operation = 'OPTIMIZE'
  ORDER BY version DESC LIMIT 5
""").show()
-- Should see: many small files removed, few large files added
```

---

## Iceberg rewrite_data_files

```python
# Iceberg compaction with multiple strategies

# Strategy 1: bin-pack (basic compaction)
spark.sql("""
  CALL local.system.rewrite_data_files(
    table => 'db.orders',
    strategy => 'binpack',
    options => map(
      'target-file-size-bytes', '134217728',   -- 128MB target
      'min-file-size-bytes', '67108864',        -- compact if file < 64MB
      'max-file-size-bytes', '209715200',       -- don't compact files > 200MB
      'partial-progress.enabled', 'true',       -- commit partial progress
      'max-concurrent-file-group-rewrites', '5' -- 5 file groups in parallel
    )
  )
""")

# Strategy 2: sort-based (co-locate data by sort key)
spark.sql("""
  CALL local.system.rewrite_data_files(
    table => 'db.orders',
    strategy => 'sort',
    sort_order => 'zorder(customer_id, order_date)',  -- Z-order sort
    options => map('target-file-size-bytes', '134217728')
  )
""")

# How often to run:
# High-frequency streaming tables: daily compaction
# Medium-frequency batch tables: weekly compaction
# Low-frequency tables: monthly compaction

# Trigger condition (Python automation):
def needs_compaction(spark, table_path: str, threshold_mb: float = 64.0) -> bool:
    details = spark.sql(f"SELECT * FROM iceberg.`{table_path}`.files").agg(
        {"file_size_in_bytes": "avg"}
    ).collect()[0][0]
    avg_mb = (details or 0) / (1024**2)
    print(f"Average file size: {avg_mb:.1f}MB (threshold: {threshold_mb}MB)")
    return avg_mb < threshold_mb
```

---

## S3 Lifecycle Policies for Lakehouse Tiers

```python
import boto3

s3 = boto3.client("s3", region_name="us-east-1")

# Tiered storage lifecycle for lakehouse zones
lifecycle_config = {
    "Rules": [
        {
            "ID": "bronze-lifecycle",
            "Status": "Enabled",
            "Filter": {"Prefix": "bronze/"},
            "Transitions": [
                {"Days": 90,  "StorageClass": "STANDARD_IA"},   # < access after 90d
                {"Days": 365, "StorageClass": "GLACIER_IR"},     # archive after 1y
                {"Days": 1825, "StorageClass": "DEEP_ARCHIVE"}, # deep archive after 5y
            ],
            # Never expire (Bronze is permanent audit trail)
        },
        {
            "ID": "silver-lifecycle",
            "Status": "Enabled",
            "Filter": {"Prefix": "silver/"},
            "Transitions": [
                {"Days": 180, "StorageClass": "STANDARD_IA"},
                {"Days": 730, "StorageClass": "GLACIER_IR"},
            ],
        },
        {
            "ID": "gold-lifecycle",
            "Status": "Enabled",
            "Filter": {"Prefix": "gold/"},
            "Transitions": [
                {"Days": 90, "StorageClass": "STANDARD_IA"},
            ],
            # Gold is re-computed from Silver → can expire old versions
            "Expiration": {"Days": 365},
        },
        {
            "ID": "checkpoints-cleanup",
            "Status": "Enabled",
            "Filter": {"Prefix": "checkpoints/"},
            "Expiration": {"Days": 30},  # Spark/Flink checkpoints: 30 days
        },
    ]
}

s3.put_bucket_lifecycle_configuration(
    Bucket="my-lakehouse",
    LifecycleConfiguration=lifecycle_config
)

# Cost impact:
# Standard:    $0.023/GB/month
# Standard-IA: $0.0125/GB/month (46% savings, $0.01/GB retrieval)
# Glacier IR:  $0.004/GB/month (83% savings, $0.03/GB retrieval)
# Deep Archive: $0.00099/GB/month (96% savings, hours to retrieve)
```

---

## Bloom Filters and Column Statistics

```python
# Bloom filters: per-file probabilistic data structure for equality checks
# "Is value X in this file?" → "Definitely not" or "Probably yes"

# Delta bloom filter index
spark.sql("""
  CREATE BLOOMFILTER INDEX ON TABLE orders
  FOR COLUMNS (
    customer_id OPTIONS (fpp=0.01, numItems=5000000),
    order_uuid OPTIONS (fpp=0.001, numItems=5000000)
  )
""")
-- fpp: false positive probability (0.01 = 1% chance of false "yes")
-- numItems: expected unique values (affects filter size)
-- Lower fpp = larger filter but fewer wasted file reads

-- Effect: WHERE customer_id=12345 on 1000 files → bloom filter checks
-- Without bloom: might scan 100 files (those with customer_id range overlap)
-- With bloom: scan only files that bloom filter says "probably yes" → 5-10 files

# Column statistics in Parquet:
# Parquet files store per-row-group min/max statistics
# Query optimizer uses these for "range-based" file pruning
# WHERE amount BETWEEN 100 AND 200 → skip files where max(amount) < 100 OR min(amount) > 200

# Disable stats for very high cardinality columns (UUIDs) to reduce write overhead
spark.conf.set("spark.sql.parquet.filterPushdown", "true")  # enable stats push-down
spark.conf.set("spark.sql.parquet.recordLevelFilter.enabled", "true")
```

---

## Interview Tips

> **Tip 1:** "How does Z-ordering compare to partitioning?" — Partitioning creates physical directory-level separation (great for date/region queries, enables partition pruning). Z-ordering organizes data within each partition/file using a space-filling curve (great for multi-column equality/range queries within a partition). They complement each other: partition by date (prune to days), Z-order by customer_id (skip files within a day's partition). Z-ordering doesn't help if you have no date filter — you'd still scan all date partitions.

> **Tip 2:** "When should you move data to S3 Glacier vs just keeping it in Standard?" — Glacier when: data is accessed less than once per quarter. S3 Standard-IA when: data accessed occasionally (monthly). The math: $0.023/GB Standard → $0.004/GB Glacier IR. If you never access data again, Glacier saves 83%. If you access once/month for queries: Standard-IA wins (access cost on Glacier adds up). Bronze raw data: archive after 1 year → Glacier (audit trail, rarely accessed). Silver: Standard-IA after 6 months. Gold: re-computable, can expire after 1 year.

> **Tip 3:** "A Parquet file has no statistics. Why might that happen?" — Three scenarios: (1) Written with an old Parquet writer that didn't support statistics (pre-1.6 Parquet format); (2) Statistics were explicitly disabled at write time (`parquet.enable.statistics=false`); (3) Very high cardinality string column — some writers skip statistics for strings over a certain length. Fix: rewrite the files with statistics enabled (Delta OPTIMIZE rewrites files with current Parquet writer, including stats). Without statistics, no file-level pruning is possible — every query is a full scan.
