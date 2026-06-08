---
title: "Delta Lake Deep Dive — Senior Deep Dive"
topic: data-lakehouse
subtopic: delta-lake-deep-dive
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [delta-lake, deletion-vectors, liquid-clustering, universal-format, photon]
---

# Delta Lake Deep Dive — Senior Deep Dive

## Deletion Vectors (Delta Lake 2.0+)

```
Problem with Delta COW for row-level deletes:
  DELETE WHERE customer_id = 123
  Delta must: find all files containing customer_id=123, rewrite each file
  100GB table, customer in 50 files → rewrite 50 × 128MB = 6.4GB I/O

Deletion Vectors (DV) — row-level soft deletes:
  Instead of rewriting files: write a small DV file marking deleted rows
  DV file: bitmap of row positions to exclude (few KB per file)
  
  Cost: append DV file (< 1MB) vs rewrite large Parquet files
  Trade-off: reads must apply DV filter (small overhead)
  
  Enable:
  spark.sql("""
    ALTER TABLE orders
    SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'true')
  """)
  
  After DELETE, DV accumulates → run OPTIMIZE to physically remove deleted rows:
  spark.sql("OPTIMIZE orders")  -- materializes DVs, writes clean Parquet files

  Deletion Vector lifecycle:
    Write DELETE → DV file (fast, small)
    Read → Parquet + DV applied (small overhead)
    OPTIMIZE → rewrite Parquet without deleted rows (DV cleared)

Deletion Vector vs Iceberg V2 equality deletes:
  Similar concept (soft delete file + compaction materializes)
  DV uses row-position bitmap; Iceberg uses equality predicates
  Delta DV is currently Databricks-optimized; Iceberg V2 is multi-engine
```

---

## Liquid Clustering (Delta Lake 3.0+)

```
Problem with ZORDER:
  ZORDER requires knowing query patterns upfront
  Changing ZORDER requires full table recompute
  Can only ZORDER a few columns efficiently
  
Liquid Clustering — adaptive, incremental clustering:
  Define cluster keys: columns used in frequent WHERE clauses
  Delta AUTOMATICALLY clusters new data as it arrives
  No need to run OPTIMIZE ZORDER manually
  Cluster layout evolves as data changes

Enable liquid clustering:
  CREATE TABLE orders
  CLUSTER BY (customer_id, order_date)
  USING delta;

  -- Or convert existing table
  ALTER TABLE orders CLUSTER BY (customer_id, order_date);

Trigger clustering (periodic maintenance):
  spark.sql("OPTIMIZE orders")  -- applies liquid clustering without ZORDER specification

Change cluster columns (non-destructive):
  ALTER TABLE orders CLUSTER BY (region, order_date);
  -- Old data stays with old layout (not rewritten)
  -- New data uses new clustering
  -- Similar to Iceberg partition evolution

Liquid clustering vs ZORDER:
  ZORDER: runs once, need to re-run manually, full table recompute
  Liquid: incremental background, adapts to changes, lower maintenance
  Current state (2024): Liquid is Databricks Runtime only
              open-source Delta 3.x adds liquid support
```

---

## Delta Sharing and Universal Format

```
Delta Sharing:
  Protocol for sharing Delta tables with external consumers WITHOUT copying data
  Consumer: reads directly from your S3 via temporary signed URLs
  Use cases: sharing data with partners, cross-cloud data access
  
  Provider setup:
    CREATE SHARE partner_share;
    ADD TABLE silver.orders TO SHARE partner_share;
    CREATE RECIPIENT partner_company;
    GRANT SELECT ON SHARE partner_share TO RECIPIENT partner_company;
  
  Consumer (Python):
    import delta_sharing
    profile = delta_sharing.SharingClient("profile.json")
    df = delta_sharing.load_as_pandas("profile.json#share_name.schema.table")

UniForm (Universal Format):
  Write once as Delta, read as Iceberg or Hudi
  Databricks automatically generates Iceberg metadata alongside Delta log
  Enables: Trino/Flink/Athena to read Delta tables via Iceberg protocol
  
  Enable:
  spark.sql("""
    ALTER TABLE orders
    SET TBLPROPERTIES (
      'delta.universalFormat.enabledFormats' = 'iceberg'
    )
  """)
  
  Result: same S3 files serve both Delta readers and Iceberg readers
  No data duplication; Iceberg metadata auto-generated on each Delta commit
```

---

## Delta Lake Internals: Conflict Detection

```python
# How Delta detects and resolves write conflicts

# Scenario: Two Spark jobs start at t=0, both read version 5
# Job A: UPDATE orders SET status='shipped' WHERE status='pending'
# Job B: INSERT INTO orders VALUES (new_order)

# Job A writes Parquet files, commits at t=1 (version 6)
# Job B writes Parquet files, tries to commit at t=2

# Delta conflict resolution algorithm:
# "Does Job B's commit conflict with Job A's commit (version 6)?"
# Conflict = both jobs modified the same data files

# Job A modified: files containing pending orders
# Job B added: new file with new order (no overlap with Job A's files)
# Result: No conflict → Job B commits as version 7

# Conflict case:
# Job A: UPDATE orders SET status='shipped' WHERE order_date='2024-01-15'
# Job B: UPDATE orders SET amount=amount*1.1 WHERE order_date='2024-01-15'
# Both modified the same files (same partition, same records)
# Result: Job B gets TransactionAbortedException → retry from version 7

# Isolation levels:
# WriteSerializable (default): writes are serializable; reads may see stale state
# Serializable: both reads AND writes are serializable (slowest, most correct)

# Monitor conflicts (useful for tuning isolation):
spark.sql("""
  SELECT version, timestamp, operationMetrics.numConflictingTransactions
  FROM (DESCRIBE HISTORY orders)
  WHERE operationMetrics.numConflictingTransactions > 0
""")
```

---

## Delta Performance Tuning for Production

```python
# 1. Data skipping: ensure column stats are collected
# Delta collects stats by default on the first 32 columns
# For tables with many columns, increase or specify which columns

spark.sql("""
  ALTER TABLE orders
  SET TBLPROPERTIES (
    'delta.dataSkippingNumIndexedCols' = '5',  -- only first 5 columns (order_id, customer_id, ...)
    'delta.dataSkippingStatsColumns' = 'order_id,customer_id,order_date,status,amount'
  )
""")

# 2. Partition pruning verification
# Run EXPLAIN and look for "PartitionFilters" in the physical plan
spark.sql("EXPLAIN EXTENDED SELECT * FROM orders WHERE order_date='2024-01-15'")
# Should see: PartitionFilters: [isnotnull(order_date#0), (order_date#0 = 2024-01-15)]

# 3. File skipping verification
# Check how many files are scanned vs total
spark.sql("""
  SELECT
    operationMetrics.numFilesScanned,
    operationMetrics.numPartitionsScanned,
    numFiles AS totalFiles
  FROM (DESCRIBE HISTORY orders)
  LIMIT 5
""")

# 4. Bloom filter index (additional file-level index for point lookups)
spark.sql("""
  CREATE BLOOMFILTER INDEX ON TABLE orders
  FOR COLUMNS (order_id OPTIONS (fpp=0.1, numItems=1000000))
""")
-- Useful for: WHERE order_id = 'abc-123' (equality lookups, not range)
-- FPP: false positive probability (0.1 = 10% false positives)

# 5. Adaptive Query Execution (AQE) — works with Delta
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

---

## Interview Tips

> **Tip 1:** "How does Delta Lake's transaction log differ from Iceberg's metadata files?" — Delta uses JSON commit files in `_delta_log/` that are append-only and read sequentially (with checkpoint snapshots every 10 commits). Iceberg uses a hierarchy: table metadata JSON → snapshot → manifest list → manifest files → data files. Delta's model is simpler (one log directory, sequential commits). Iceberg's hierarchy enables more efficient metadata reads for large tables (read only relevant manifests, not all commit history).

> **Tip 2:** "What are deletion vectors and when do they outperform COW rewrites?" — DVs are bitmaps marking deleted/updated row positions, stored as tiny files alongside Parquet. They outperform COW when: deletes are sparse (scattered across many files), files are large (128MB+ — expensive to rewrite), and read traffic is manageable (reads must apply DV filter). DVs are like Iceberg V2 row-level deletes. The rule: for high-frequency targeted row deletions, DVs or MOR are faster than COW; for bulk partition-level deletes, COW (partition overwrite) is still fastest.

> **Tip 3:** "Explain UniForm and why it matters for the lakehouse ecosystem." — UniForm writes Delta and automatically generates Iceberg (and optionally Hudi) metadata from the same physical Parquet files. This means: one copy of data, readable by Spark (Delta native), Trino (via Iceberg connector), Flink (via Iceberg connector), and Athena (via Iceberg on Glue). It ends the "format wars" for Databricks users — you commit to Delta internally but expose an Iceberg interface to the rest of the ecosystem. This significantly reduces the "buy Databricks or lose portability" tension.
