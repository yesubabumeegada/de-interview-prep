---
title: "Storage Optimization — Scenarios"
topic: data-lakehouse
subtopic: storage-optimization
content_type: scenario_question
tags: [storage, parquet, compaction, scenarios]
---

# Storage Optimization — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Parquet File Format Fundamentals

**Scenario:** Your team is moving from CSV files to Parquet for the data lake. Your manager asks you to explain why Parquet is better for analytical workloads and what settings you should configure when writing Parquet files.

<details>
<summary>💡 Hint</summary>

Focus on columnar storage (analytics reads few columns from many rows), compression (Snappy vs ZSTD vs GZIP trade-offs), and row group size. Also mention predicate pushdown via column statistics and dictionary encoding.

</details>

<details>
<summary>✅ Solution</summary>

**Why Parquet for Analytics?**

Parquet is columnar — data for each column is stored together. For analytical queries that read 3 of 100 columns, Parquet only reads those 3 columns from disk. CSV reads all 100.

```
CSV (row-oriented):
row1: id,name,age,city,...  ← must read entire row
row2: id,name,age,city,...

Parquet (columnar):
[id block][name block][age block][city block]
          ↑ skip if not needed
```

**Key Parquet Settings:**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Optimal settings for analytics workloads
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
spark.conf.set("spark.sql.parquet.block.size", str(128 * 1024 * 1024))  # 128MB row group
spark.conf.set("spark.sql.parquet.page.size", str(1 * 1024 * 1024))     # 1MB pages

df.write     .option("compression", "zstd")     .option("parquet.block.size", 134217728)     .parquet("s3://bucket/table/")
```

**Compression Comparison:**

| Codec | Ratio | Speed | Best For |
|-------|-------|-------|---------|
| SNAPPY | Medium | Fast | Hot data, frequent reads |
| ZSTD | High | Medium | Balanced — recommended default |
| GZIP | High | Slow | Archive/cold data |
| LZ4 | Low | Very Fast | Streaming, low-latency |

**Predicate Pushdown:**
Parquet stores min/max statistics per row group. Query engines can skip entire row groups without reading them:

```python
# This filter uses Parquet statistics to skip 95% of data
df = spark.read.parquet("s3://bucket/events/")     .filter("event_date = '2024-01-15'")
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Solving the Small File Problem

**Scenario:** Your Spark streaming job writes 1,000 micro-batches per day to an S3 data lake. Each batch produces 200 files of ~1MB each. After 30 days you have 6 million files totaling 6TB. Query performance has collapsed and S3 LIST operations are timing out. Design a compaction strategy.

<details>
<summary>💡 Hint</summary>

Small files are the most common data lake performance killer. Consider: target file size (128MB-1GB for Parquet), compaction scheduling, Iceberg's built-in rewrite procedures vs custom Spark jobs, and partitioning strategy to limit files per partition.

</details>

<details>
<summary>✅ Solution</summary>

**Root Cause:**

Each micro-batch writes `num_spark_partitions` files. With default `spark.sql.shuffle.partitions=200` and 1,000 batches/day:
- 200 files × 1,000 batches × 30 days = 6,000,000 files

**Solution 1: Fix at the Source — Coalesce Before Write**

```python
def write_batch(batch_df, batch_id):
    # Calculate target partitions: aim for 256MB files
    target_file_size_mb = 256
    avg_row_size_bytes = 500  # estimate
    
    target_rows_per_file = (target_file_size_mb * 1024 * 1024) // avg_row_size_bytes
    num_partitions = max(1, batch_df.count() // target_rows_per_file)
    
    batch_df.coalesce(num_partitions)         .write.format("iceberg")         .mode("append")         .saveAsTable("prod.events")
```

**Solution 2: Iceberg Compaction (Recommended)**

```python
# Schedule daily compaction via Airflow
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

compaction_task = SparkSubmitOperator(
    task_id='compact_events_table',
    application='/jobs/iceberg_compaction.py',
    conf={
        'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
    },
    schedule_interval='0 3 * * *'  # 3am daily
)
```

```python
# iceberg_compaction.py
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Compact all partitions modified in last 2 days
result = spark.sql("""
  CALL prod.system.rewrite_data_files(
    table => 'prod.events',
    strategy => 'binpack',
    where => 'event_date >= current_date - 2',
    options => map(
      'target-file-size-bytes', '268435456',
      'min-input-files', '10',
      'max-concurrent-file-group-rewrites', '50'
    )
  )
""")

print(f"Rewritten: {result.collect()[0]['rewritten_files_count']} files")
print(f"Added: {result.collect()[0]['added_files_count']} files")
```

**Solution 3: Partition Strategy to Limit Files/Partition**

```sql
-- Poor: date+hour partitioning with 200 spark tasks = 200 files per hour partition
-- 200 files × 24 hours × 365 days = 1.7M files/year

-- Better: date-only partitioning for daily compaction boundary
CREATE TABLE prod.events (
    event_id BIGINT,
    event_type STRING,
    event_time TIMESTAMP,
    payload STRING
) USING ICEBERG
PARTITIONED BY (days(event_time));  -- Iceberg hidden partition transform
```

**Solution 4: Monitor File Health**

```python
def file_health_report(table: str):
    df = spark.sql(f"""
        SELECT 
            partition,
            count(*) as file_count,
            round(avg(file_size_in_bytes)/1e6, 1) as avg_size_mb,
            round(sum(file_size_in_bytes)/1e9, 2) as total_size_gb,
            countif(file_size_in_bytes < 10*1024*1024) as small_files_under_10mb
        FROM {table}.files
        GROUP BY partition
        ORDER BY small_files_under_10mb DESC
        LIMIT 20
    """)
    return df

# Alert if any partition has >1000 small files
health = file_health_report("prod.events")
bad_partitions = health.filter("small_files_under_10mb > 1000")
if bad_partitions.count() > 0:
    send_alert("Small file issue detected", bad_partitions)
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Storage Cost Optimization for a Petabyte-Scale Data Lake

**Scenario:** Your data lake stores 5PB across S3. The annual storage bill is $1.4M. Your VP asks you to reduce costs by 40% without degrading query SLAs. Analyze the cost drivers and design a comprehensive optimization strategy.

<details>
<summary>💡 Hint</summary>

S3 costs: storage ($0.023/GB standard), requests (LIST/GET costs add up with small files), and data transfer. Optimization levers: storage tiering (S3 IA, Glacier), compression improvement (GZIP→ZSTD), deduplication, partition pruning to reduce scans, and data retention policies.

</details>

<details>
<summary>✅ Solution</summary>

**Cost Breakdown Analysis:**

```python
import boto3

def analyze_s3_costs(bucket: str, prefix: str):
    s3 = boto3.client('s3')
    cw = boto3.client('cloudwatch')
    
    # Get storage by class
    paginator = s3.get_paginator('list_objects_v2')
    
    storage_by_class = {}
    old_objects = []  # not accessed in 90+ days
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            storage_class = obj.get('StorageClass', 'STANDARD')
            storage_by_class[storage_class] =                 storage_by_class.get(storage_class, 0) + obj['Size']
            
            days_since_modified = (datetime.now() - obj['LastModified'].replace(tzinfo=None)).days
            if days_since_modified > 90:
                old_objects.append(obj['Key'])
    
    return storage_by_class, old_objects
```

**Optimization Strategy:**

**1. S3 Intelligent-Tiering (Immediate Win: ~30% savings)**

```python
# Apply Intelligent-Tiering to entire lake via lifecycle policy
s3 = boto3.client('s3')

s3.put_bucket_lifecycle_configuration(
    Bucket='datalake-prod',
    LifecycleConfiguration={
        'Rules': [{
            'ID': 'intelligent-tiering-all',
            'Status': 'Enabled',
            'Filter': {'Prefix': ''},
            'Transitions': [
                {'Days': 0, 'StorageClass': 'INTELLIGENT_TIERING'}
            ],
            # Archive infrequently accessed after 90 days
            'NoncurrentVersionExpiration': {'NoncurrentDays': 90}
        }]
    }
)
```

**2. Compression Upgrade: SNAPPY → ZSTD (~20% storage reduction)**

```python
# Identify SNAPPY-compressed files and recompress
def recompress_partition(source_path: str, target_path: str):
    df = spark.read         .option("mergeSchema", "true")         .parquet(source_path)
    
    # Rewrite with ZSTD + better row group size
    df.write         .option("compression", "zstd")         .option("parquet.block.size", 268435456) \  # 256MB row groups
        .mode("overwrite")         .parquet(target_path)
    
    # Validate
    original_count = df.count()
    new_count = spark.read.parquet(target_path).count()
    assert original_count == new_count

# Schedule for cold partitions (> 30 days old)
recompress_partition(
    "s3://datalake-prod/events/event_date=2023-01-01/",
    "s3://datalake-prod/events-zstd/event_date=2023-01-01/"
)
```

**3. Deduplication Detection**

```python
def find_duplicate_data(table: str):
    """Find partitions duplicated across raw/silver/gold without cleanup."""
    raw = spark.sql(f"SELECT count(*), sum(file_size_in_bytes) FROM {table}_raw.files")
    silver = spark.sql(f"SELECT count(*), sum(file_size_in_bytes) FROM {table}_silver.files")
    
    # If raw and silver have identical data, raw can be archived
    raw_size = raw.collect()[0][1]
    silver_size = silver.collect()[0][1]
    
    # Typically silver = 0.7x raw after dedup/compact
    print(f"Raw: {raw_size/1e12:.2f}TB, Silver: {silver_size/1e12:.2f}TB")
    print(f"Archive candidate: {raw_size/1e12:.2f}TB of raw data")
```

**4. Retention Policy Enforcement**

```sql
-- Audit data with no reads in 180+ days
SELECT 
    table_name,
    partition_path,
    round(sum(file_size)/1e12, 2) as size_tb,
    max(last_accessed) as last_read
FROM data_catalog.access_logs
GROUP BY 1, 2
HAVING last_read < CURRENT_DATE - 180
ORDER BY size_tb DESC;
```

**5. Request Cost Reduction (S3 GET costs)**

Small files generate expensive S3 GET requests. Post-compaction (Solution 2 above):
- 6M files → 12K files (500x reduction in GET requests)
- S3 request costs drop from ~$50K/month to ~$100/month

**Projected Savings Summary:**

| Initiative | Annual Savings | Effort |
|-----------|---------------|--------|
| S3 Intelligent-Tiering | $420K (30%) | Low |
| ZSTD recompression | $280K (20%) | Medium |
| Compaction (request costs) | $180K (13%) | Medium |
| Retention enforcement | $120K (9%) | Low |
| **Total** | **$1M (71%)** | |

Target: 40% reduction ($560K). Intelligent-Tiering + ZSTD alone achieves 50%.

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What is the ideal Parquet file size?" — 128MB to 1GB is the sweet spot. Below 128MB causes small file problems (too many S3 requests, query planning overhead). Above 1GB means less parallelism and slower individual task completion.
> **Tip 2:** "What is Z-ordering and when should you use it?" — Z-ordering (or Z-order clustering) sorts data by multiple columns simultaneously to co-locate related data in files. Use it for Iceberg/Delta tables where queries frequently filter on multiple columns (e.g., `WHERE customer_id = X AND event_type = Y`). It reduces data scanned by 10-100x for selective queries.
> **Tip 3:** "How do you handle the trade-off between compression ratio and query speed?" — ZSTD is the modern default: better compression than Snappy with similar read speed. Use Snappy only if write latency is critical (e.g., high-throughput streaming). Use GZIP only for archival data that's rarely queried.
