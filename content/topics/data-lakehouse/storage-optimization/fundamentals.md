---
title: "Storage Optimization — Fundamentals"
topic: data-lakehouse
subtopic: storage-optimization
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [storage, compression, parquet, partitioning, small-files]
---

# Storage Optimization — Fundamentals


## 🎯 Analogy

Think of storage optimization like house cleaning for your data lake: small files are like scattered papers (thousands of tiny files slow down readers), OPTIMIZE/COMPACTION bundles them into neat stacks, and VACUUM cleans up the old papers (deleted file versions).

---
## Why Storage Optimization Matters

In a lakehouse, poor storage layout directly impacts: query performance (more files = more S3 GETs = slower), cost (S3 request charges + storage size), and compute cost (more data to scan = more CPU/memory on Spark/Trino).

```
S3 costs (us-east-1, 2024):
  Storage: $0.023/GB/month
  GET requests: $0.0004 per 1,000 requests
  PUT/COPY: $0.005 per 1,000 requests
  
100TB table × $0.023 = $2,300/month (storage)
Bad case: 1M small files × 100K daily GETs = 100M GETs × $0.0004 = $40/month (just requests)
Good case: 10K files × 100K daily GETs = 1B GETs → wait, this is the same query pattern
Reality: fewer files = fewer GET requests per query = lower latency + lower cost
```

---

## File Formats and Compression

```
File format impact on storage and query speed:

CSV:
  Size: 100GB (no compression, row-based)
  Query: must read all columns even if only 2 needed
  Use: data exchange, human-readable exports only

Parquet (columnar, compressed):
  Size: 10–20GB (5–10× compression vs CSV)
  Query: reads only requested columns from disk
  Compression: Snappy (fast, medium ratio), Zstd (slower write, better ratio), Gzip (best ratio, slowest)
  Use: all analytics workloads

Avro (row-based, schema evolution):
  Size: 30–50GB (moderate compression)
  Query: reads entire row (no columnar benefit)
  Use: Kafka messages, schema evolution pipelines

Compression comparison (1TB uncompressed):
  Parquet + Snappy: ~200GB (5×)
  Parquet + Zstd:   ~130GB (7×)
  Parquet + Gzip:   ~100GB (10×)

Rule of thumb: use Parquet + Snappy for performance, Zstd for cost-sensitive archival
```

---

## The Small Files Problem

```
Why small files hurt:
  Each Parquet file = one S3 GET request + one read task in Spark/Trino
  1M × 1MB files vs 1,000 × 1GB files:
    Same total data (1TB)
    1M files: 1M S3 GETs, 1M Spark tasks, 1M metadata entries
    1K files: 1K S3 GETs, 1K Spark tasks, 1K metadata entries
    100× difference in overhead

Common causes:
  1. Streaming micro-batches: Kafka → Spark → Delta, every 1-min batch = new files
  2. Over-partitioning: partitioned by (date, hour, region, product) = thousands of tiny partitions
  3. Many small INSERTs (row-by-row ETL)
  4. Failed partial writes leaving orphan files

Fix options:
  Delta: OPTIMIZE (bin-packs toward ~1GB target by default)
  Iceberg: rewrite_data_files (compact + optional sort)
  Hudi: compaction (MOR: merge base + logs; COW: compact small files)
  Spark: df.coalesce(N) before writing, or repartition(N)
  
Target file size: 128MB–1GB (Parquet sweet spot for columnar analytics)
Too small: overhead as described above
Too large: partial file reads waste time for small queries
```

---

## Partitioning Basics

```
Partitioning: physically separate data into subdirectories by column values
  orders/year=2024/month=01/day=15/part-00000.parquet

Benefits:
  Partition pruning: WHERE order_date = '2024-01-15' 
  → only read year=2024/month=01/day=15/ prefix
  → skip all other partitions

Choose partition columns:
  High cardinality: date, timestamp (too many partitions → too many small files)
  Low cardinality: year, month (too few partitions → large files, poor pruning)
  Sweet spot: 50–10,000 partitions, each > 100MB

Common partition strategies:
  By date: partitioned by (order_date)  → good for time-series queries
  By date + region: (order_date, region) → good if queries always filter both
  
Anti-patterns:
  By customer_id: 1M customers = 1M partitions → worst possible
  By hour: 365 days × 24 hours = 8,760 partitions → many small files
```

---


## ▶️ Try It Yourself

```sql
-- Delta Lake: OPTIMIZE compacts small files, ZORDER co-locates related data
OPTIMIZE silver.orders ZORDER BY (region, order_date);

-- VACUUM: delete old file versions older than retention period (default 7 days)
VACUUM silver.orders RETAIN 168 HOURS;  -- 7 days

-- Iceberg: equivalent operations
-- CALL catalog.system.rewrite_data_files('silver.orders');
-- CALL catalog.system.expire_snapshots('silver.orders', TIMESTAMP '2024-01-01 00:00:00.000');

-- Check small files problem before optimizing
SELECT input_file_name(), COUNT(*) FROM silver.orders GROUP BY 1 LIMIT 20;

-- Auto-optimization (Databricks / Delta 3.0+)
ALTER TABLE silver.orders SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What's the ideal file size for Parquet files in S3?" — 128MB to 1GB is the common recommendation. Below 64MB: the overhead of S3 metadata operations, file open calls, and Spark task scheduling dominates — query is I/O bound on file overhead rather than data. Above 1GB: reading a small range of data (single day's records) from a massive file wastes bytes. Note the two Delta defaults: optimized writes target ~128MB at write time, while OPTIMIZE bin-packs toward ~1GB. Adjust: 512MB–1GB for read-heavy, rarely-updated Gold tables.

> **Tip 2:** "How does columnar storage (Parquet) save compute?" — In a row-based format (CSV), reading 2 columns from a 100-column table still reads all 100 columns from disk. In Parquet, each column is stored separately in column chunks. A query selecting 2 columns reads only those 2 column chunks — 2% of the data. For analytical queries that aggregate 1-3 columns across billions of rows, this 50-100× I/O reduction is the primary reason Parquet is standard.

> **Tip 3:** "Why not always use Gzip compression if it gives the best ratio?" — Gzip is not splittable in its default form. If a compressed Gzip file is 10GB, Spark cannot split it across multiple tasks — one task reads the whole file, one CPU core does all the work. Snappy and Zstd produce splittable compressed blocks within Parquet (Parquet handles the splitting). Result: Gzip on a 10GB Parquet file → 1 Spark task = 1 core, very slow. Snappy on the same file → 80 tasks = 80 cores in parallel, 80× faster. Always use Snappy or Zstd for Parquet, not Gzip.
