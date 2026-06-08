---
title: "Scalability & Partitioning — Intermediate"
topic: system-design
subtopic: scalability
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [system-design, scalability, consistent-hashing, compaction, clustering, z-order]
---

# Scalability & Partitioning — Intermediate

## Consistent Hashing (Distributed Systems)

Standard hashing (`key % N`) breaks when N changes — all keys remap. Consistent hashing minimizes remapping:

```
Ring: keys and nodes both mapped to a circular hash space (0 → 2^32)
Assignment: each key is assigned to the next node clockwise on the ring

Adding a node: only keys between the new node and its predecessor remapped
Removing a node: only that node's keys remapped to its successor
Average: only K/N keys remapped (K = total keys, N = nodes)

Virtual nodes: each physical node owns multiple points on the ring
  Purpose: even distribution (prevents hot spots from uneven ring placement)
  Typical: 150 virtual nodes per physical node

Used by: Cassandra, DynamoDB, Kafka partition assignment, Consul
```

---

## File-Level Optimizations for Scale

### Small Files Problem
```python
# Problem: millions of small files (< 10MB) → S3 metadata overhead, slow listings
# Common cause: streaming pipelines writing micro-batches, partition explosion

# Diagnosis
files = s3.list_objects("s3://bucket/orders/")
sizes = [f['Size'] for f in files]
print(f"Files: {len(sizes)}, Avg size: {sum(sizes)/len(sizes)/1e6:.1f}MB")
# If avg < 64MB: small file problem

# Fix 1: Coalesce or repartition before writing
df.coalesce(10).write.parquet("s3://bucket/orders/")
# coalesce: reduces partitions without shuffle (cheap)
# repartition: shuffles to create evenly-sized partitions (more expensive but balanced)

# Fix 2: Delta Lake OPTIMIZE (compaction)
# Runs on a schedule; merges small files into 1GB files
spark.sql("OPTIMIZE delta.`s3://bucket/delta/orders`")

# Fix 3: Iceberg rewrite_data_files
spark.sql("""
    CALL system.rewrite_data_files(
        table => 'catalog.db.orders',
        strategy => 'binpack',
        options => map('target-file-size-bytes', '1073741824')  -- 1GB
    )
""")
```

### Z-Order Clustering (Delta Lake)
```python
# Z-Order: co-locate related data by multiple columns in the same files
# Reduces files read for multi-column filter queries

# Before OPTIMIZE + ZORDER: query on region + order_date reads all 500 files
# After OPTIMIZE ZORDER: query reads only ~10 files (data physically co-located)

spark.sql("""
    OPTIMIZE delta.`s3://bucket/delta/orders`
    ZORDER BY (region, order_date)
""")

# Check file stats (data skipping efficiency)
spark.sql("DESCRIBE DETAIL delta.`s3://bucket/delta/orders`").show()
# numFiles, sizeInBytes — monitor before/after

# Iceberg equivalent: sort order
spark.sql("""
    ALTER TABLE catalog.db.orders
    WRITE ORDERED BY (region, order_date)
""")
```

---

## Clustering Keys in Cloud Data Warehouses

```sql
-- Snowflake: cluster key for large tables
-- Automatically sorts and co-locates data by cluster key
ALTER TABLE orders CLUSTER BY (TO_DATE(order_date), region);

-- Check clustering effectiveness
SELECT SYSTEM$CLUSTERING_INFORMATION('orders', '(TO_DATE(order_date), region)');
-- average_depth close to 1.0 = perfectly clustered
-- average_depth > 3 = reclustering needed (auto-clustering handles this)

-- BigQuery: clustering + partitioning (different concepts)
CREATE TABLE orders
PARTITION BY DATE(order_date)       -- partition: eliminates partition scans
CLUSTER BY region, customer_id;     -- cluster: sorts within partitions (file-level pruning)

-- Redshift: sort key + distribution key
CREATE TABLE orders (
  order_id BIGINT,
  order_date DATE,
  region VARCHAR(20),
  customer_id BIGINT,
  amount DECIMAL(10,2)
)
DISTKEY(customer_id)    -- distribute by customer_id (joins with customers table)
SORTKEY(order_date);    -- sort on disk by order_date (range scans)
```

---

## Scaling Kafka

```
Key scaling levers:

1. Partitions (parallelism):
   - More partitions = more consumer parallelism
   - Rule of thumb: target partition size ~1GB/day of writes
   - Max consumers in a group = number of partitions
   - Can't reduce partitions after creation (only increase)

2. Replication factor (durability):
   - RF=3: standard (1 leader + 2 followers)
   - RF=1: dev only (no fault tolerance)
   - Higher RF = more disk, more network overhead

3. Retention (storage):
   - Default: 7 days or 1GB (whichever first)
   - Compacted topics: retain only latest value per key (changelog/CDC)
   - Storage sizing: avg message size × events/day × retention_days × RF

4. Brokers (throughput):
   - Add brokers to increase aggregate throughput
   - Trigger partition rebalancing after adding brokers:
     kafka-reassign-partitions.sh

Performance targets per broker:
  Read throughput: 600 MB/s (from page cache)
  Write throughput: 300 MB/s
  Single partition: ~10 MB/s sustained
```

---

## Interview Tips

> **Tip 1:** "How do you handle the small files problem in a data lake?" — Three approaches: (1) Compact files periodically using Delta OPTIMIZE or Iceberg rewrite_data_files — merges many small files into large 1GB files, (2) Tune the streaming writer to write larger batches (increase micro-batch interval or use buffer size limits), (3) Use file-size-aware partitioning (coarser partitions = fewer, larger files). Monitor: avg file size < 128MB = small file problem.

> **Tip 2:** "What is Z-ordering and when is it useful?" — Z-ordering is a multi-dimensional clustering technique that physically co-locates data with similar values across multiple columns in the same files. Unlike single-column sort, Z-order interleaves bits from multiple columns so that rows with similar values on ALL columns are stored nearby. Use it when queries frequently filter on 2-3 columns together (e.g., `WHERE region = 'US' AND order_date = '2024-01'`). It enables Delta Lake data skipping — queries can skip irrelevant files using min/max statistics.

> **Tip 3:** "How many Kafka partitions should a topic have?" — Start with: `max(target throughput / partition throughput, desired consumer parallelism)`. Each partition handles ~10MB/s. For 100MB/s throughput: 10+ partitions. For 20 parallel consumers: 20+ partitions. Round up to a power of 2 for even distribution (16, 32, 64). Don't over-partition — 10K partitions per broker is a practical limit before ZooKeeper/metadata becomes bottleneck (KRaft mode raises this).
