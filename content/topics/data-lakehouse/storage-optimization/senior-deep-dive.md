---
title: "Storage Optimization — Senior Deep Dive"
topic: data-lakehouse
subtopic: storage-optimization
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [storage, cost-optimization, tiered-storage, iceberg-v2, deletion-vectors]
---

# Storage Optimization — Senior Deep Dive

## Data Layout Strategy for Multi-Dimensional Queries

```
Problem: queries filter on many different columns depending on user
  Marketing: WHERE campaign_id = X AND event_date BETWEEN A AND B
  Finance:   WHERE customer_id = X AND order_date = Y
  Product:   WHERE product_id = X AND region = Y AND date = Z

Z-order handles 2 dimensions well, degrades for 3+

Advanced layout strategy: separate materialized tables per query pattern
  gold.orders_by_customer:  ZORDER BY (customer_id, order_date)
  gold.orders_by_product:   ZORDER BY (product_id, order_date)
  gold.orders_by_campaign:  ZORDER BY (campaign_id, event_date)

  Each table has same data, different sort order
  Trade-off: 3× storage cost vs 10× query performance improvement
  Decision: acceptable if storage cost < compute cost savings

Liquid Clustering (Delta 3.x):
  Adaptive: monitors actual query patterns, clusters accordingly
  Multi-column: clusters on multiple columns without degradation
  No re-run needed: incremental updates

Iceberg hidden partitioning with multiple levels:
  PARTITIONED BY (months(order_date), bucket(50, customer_id))
  Provides: time pruning (months) + customer clustering (bucket)
  Without partition evolution: would need full table rewrite to change
  With evolution: add new partition spec → old data stays with old partition
```

---

## Storage Cost Optimization Framework

```python
import boto3
from datetime import datetime, timedelta

def analyze_lakehouse_storage_costs(bucket: str, prefix: str = ""):
    """Analyze S3 storage distribution to identify optimization opportunities."""
    s3 = boto3.client("s3", region_name="us-east-1")
    
    # Collect file-level metadata
    paginator = s3.get_paginator("list_objects_v2")
    
    file_stats = {
        "total_size_gb": 0,
        "file_count": 0,
        "small_files": 0,     # < 32MB
        "medium_files": 0,    # 32MB - 256MB
        "large_files": 0,     # > 256MB
        "by_storage_class": {},
        "by_prefix": {},
    }
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            size_mb = obj["Size"] / (1024**2)
            size_gb = obj["Size"] / (1024**3)
            storage_class = obj.get("StorageClass", "STANDARD")
            key_prefix = "/".join(obj["Key"].split("/")[:3])
            
            file_stats["total_size_gb"] += size_gb
            file_stats["file_count"] += 1
            
            if size_mb < 32:
                file_stats["small_files"] += 1
            elif size_mb < 256:
                file_stats["medium_files"] += 1
            else:
                file_stats["large_files"] += 1
            
            file_stats["by_storage_class"][storage_class] = \
                file_stats["by_storage_class"].get(storage_class, 0) + size_gb
            
            file_stats["by_prefix"][key_prefix] = \
                file_stats["by_prefix"].get(key_prefix, 0) + size_gb
    
    # Calculate monthly cost
    prices = {
        "STANDARD": 0.023,
        "STANDARD_IA": 0.0125,
        "GLACIER_IR": 0.004,
        "DEEP_ARCHIVE": 0.00099,
    }
    
    monthly_cost = sum(
        gb * prices.get(sc, 0.023)
        for sc, gb in file_stats["by_storage_class"].items()
    )
    
    # Identify savings opportunities
    small_file_savings = file_stats["small_files"] * 32 / 1024 * 0.023  # estimate
    standard_to_ia_candidate = file_stats["by_storage_class"].get("STANDARD", 0) * 0.5
    potential_ia_savings = standard_to_ia_candidate * (0.023 - 0.0125)
    
    print(f"Total: {file_stats['total_size_gb']:.1f}GB in {file_stats['file_count']:,} files")
    print(f"Monthly cost: ${monthly_cost:.2f}")
    print(f"Small files: {file_stats['small_files']:,} → compact for ${small_file_savings:.2f}/mo savings")
    print(f"Standard → IA potential: ${potential_ia_savings:.2f}/mo if 50% eligible")
    
    return file_stats

# Run monthly as cost governance check
stats = analyze_lakehouse_storage_costs("my-lakehouse", "silver/")
```

---

## Orphan File Cleanup

```python
# Orphan files: Parquet files that exist on S3 but are NOT referenced by any table metadata
# Causes: failed writes, abandoned partial jobs, old table format migrations

# Delta: VACUUM handles this
spark.sql("VACUUM delta.`s3://bucket/silver/orders` RETAIN 168 HOURS")
-- Removes: files not referenced by any Delta commit older than 7 days
-- Safe: files within 7 days may be from in-progress transactions

# Iceberg: remove_orphan_files
spark.sql("""
  CALL local.system.remove_orphan_files(
    table => 'db.orders',
    older_than => TIMESTAMP '2024-01-08 00:00:00',  -- 7+ days ago
    location => 's3://bucket/warehouse/orders'
  )
""")

# Custom orphan detection (for non-Delta/Iceberg tables)
def find_orphan_files(spark, table_path: str, catalog_files: set) -> list:
    """Find S3 files not referenced in catalog."""
    import boto3
    
    s3 = boto3.client("s3")
    bucket = table_path.split("/")[2]
    prefix = "/".join(table_path.split("/")[3:])
    
    s3_files = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                s3_files.add(obj["Key"])
    
    orphans = s3_files - catalog_files
    orphan_size_gb = 0
    for orphan in orphans:
        resp = s3.head_object(Bucket=bucket, Key=orphan)
        orphan_size_gb += resp["ContentLength"] / (1024**3)
    
    print(f"Found {len(orphans)} orphan files ({orphan_size_gb:.1f}GB)")
    return list(orphans)
```

---

## Column Encoding Optimization

```
Parquet encoding strategies (automatic, but worth understanding):

Dictionary encoding:
  Applicable: low-cardinality string columns (status, region, product_type)
  How: replace repeated string with integer ID → dictionary lookup
  Savings: "delivered" stored as int(2), not 9-byte string
  Parquet auto-selects when cardinality < ~1,000 distinct values
  
  Force for specific columns in Spark:
  spark.conf.set("spark.sql.parquet.writeLegacyFormat", "false")
  -- Modern Parquet encoding enabled (better dictionary + delta encoding)

Delta encoding:
  Applicable: sorted numeric columns (timestamps, sequential IDs)
  How: store first value + differences from previous (small deltas)
  Savings: timestamps 2024-01-15 09:00:00, 09:00:01, 09:00:02 → store deltas (1,1)
  
RLE (Run-Length Encoding):
  Applicable: many repeated values in sequence
  How: store value + repetition count instead of repeating value
  After sorting: nearby rows have same column values → RLE very effective

Practical advice:
  Sort data before writing for better encoding:
  df.orderBy("status", "region").write.format("parquet")...
  -- status="delivered" rows are together → RLE + dict encoding at max efficiency
  -- Expect 20-40% additional compression vs unsorted

Zstd vs Snappy at the codec level:
  Snappy: fast encode/decode, moderate ratio (best for hot queries)
  Zstd:   medium encode, fast decode, better ratio (good for most cases)
  
  Zstd level tuning:
  spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
  spark.conf.set("parquet.zstd.level", "3")  # 1-22 (1=fast, 22=max ratio)
  -- Level 3: 10-15% better ratio than Snappy with same query latency
```

---

## Interview Tips

> **Tip 1:** "How do you balance compaction cost vs storage query performance?" — Compaction is an S3 read + S3 write operation (costs both compute and storage request fees). Run compaction on tables where: (1) query latency has increased (small files accumulating), (2) Spark scan reports many files scanned vs few bytes read. Don't compact every table every day — use a tiered schedule: streaming Silver tables daily, batch Gold tables weekly. Monitor: if compaction job cost > query savings, increase the trigger threshold.

> **Tip 2:** "When does dictionary encoding fail and hurt performance?" — Dictionary encoding fails when a column has high cardinality (millions of distinct values, e.g., UUID columns, customer_id with millions of customers). The dictionary grows large, requires dictionary page reads, and may fall back to plain encoding mid-file if the dictionary overflows. For UUID columns: disable dictionary encoding. For sequential integer IDs: delta encoding is better than dictionary. Profiling: check Parquet file footer statistics to see which encoding was actually used.

> **Tip 3:** "How do you handle storage optimization for a table that's both hot (recent data) and cold (3-year archive)?" — Partition-level compaction: compact only the last 7 days of data weekly (where streaming created small files). Don't compact historical partitions (already large files, no benefit). Apply S3 lifecycle: recent 90 days → Standard, 90 days–2 years → Standard-IA, 2+ years → Glacier IR. Use Delta/Iceberg partition filters for lifecycle: OPTIMIZE WHERE order_date >= current_date - 7. This minimizes compaction cost while keeping recent data fast.

## ⚡ Cheat Sheet

**Target file size**: 128 MB – 1 GB per Parquet/Delta file (sweet spot for parallelism vs metadata overhead)

**Compaction**
```python
# Delta
DeltaTable.forPath(spark, path).optimize().executeCompaction()
# Iceberg
spark.sql("CALL system.rewrite_data_files('prod.gold.orders')")
# Hudi: set hoodie.compact.inline=true or schedule async job
```

**Z-ordering (data skipping)**
```python
# Delta
DeltaTable.forPath(spark, path).optimize().executeZOrderBy("customer_id", "order_date")
# Iceberg: sort-order
spark.sql("ALTER TABLE prod.gold.orders WRITE ORDERED BY customer_id, order_date")
```

**Partitioning rules**
```python
# Good: low-medium cardinality, query predicate columns
df.write.partitionBy("region", "year", "month").format("delta").save(path)
# Bad: high cardinality (user_id) → millions of tiny files
# Bad: column never used in WHERE clause
```

**Parquet settings**
```python
spark.conf.set("spark.sql.parquet.compression.codec", "zstd")  # better than snappy
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")
spark.conf.set("spark.sql.parquet.filterPushdown", "true")
```

**Vacuum / expiry**
```python
DeltaTable.forPath(spark, path).vacuum(retentionHours=168)  # Delta
spark.sql("CALL system.expire_snapshots('table', TIMESTAMP '2024-01-01 00:00:00', 10)")  # Iceberg
```

**Key points**
- Bloom filters: per-column, enable for high-cardinality point lookups (order_id, user_id)
- Column statistics: min/max per file → data skipping without reading file
- ZSTD > Snappy: better compression ratio; prefer for cold/archival storage
- Partition evolution (Iceberg): change partition strategy without rewriting data
