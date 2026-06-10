---
title: "Apache Hudi — Senior Deep Dive"
topic: data-lakehouse
subtopic: apache-hudi
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [hudi, clustering, multi-table-transactions, index, performance]
---

# Apache Hudi — Senior Deep Dive

## Hudi Clustering (Data Layout Optimization)

```
Problem: Hudi writes scatter data by ingestion order (upserts land wherever the key is)
Result: queries filtering by non-key columns (e.g., customer_id) do full scans

Clustering: reorganizes data files for optimal query access patterns
  Sort data by high-cardinality query columns
  Co-locate related records (same customer, same region)
  Similar to Delta ZORDER, Iceberg sort-based rewrite

Clustering strategies:
  SpaceFillingCurveSortPartitioner: Z-order on multiple columns (best multi-column pruning)
  LinearSortPartitioner: simple sort on one column
  SizeBasedClusteringPlanStrategy: cluster based on file size balance

Configuration:
  "hoodie.clustering.plan.strategy.target.file.max.bytes": "1073741824",  # 1GB
  "hoodie.clustering.plan.strategy.small.file.limit": "629145600",        # 600MB min
  "hoodie.clustering.plan.strategy.sort.columns": "customer_id,order_date",
  "hoodie.layout.optimize.enable": "true",
  "hoodie.layout.optimize.strategy": "z-order",

Run clustering:
  spark.sql("CALL hudi.system.run_clustering(table => 'hudi.orders')")
```

---

## Multi-Table Transactions with Hudi

```python
# Hudi supports multi-table atomic commits via HoodieWriteClient
# This enables atomic writes across multiple tables (critical for consistency)

from pyspark.sql import SparkSession
from org.apache.hudi.client import SparkRDDWriteClient
from org.apache.hudi.common.model import HoodieRecord

# Multi-table write using same transaction context
# Requires Hudi 0.13+

def atomic_multi_table_write(spark, orders_df, inventory_df):
    """
    Atomically write to orders and inventory tables.
    If either fails, both are rolled back.
    """
    # This uses Hudi's multi-table commit API
    # (Simplified pattern — actual API uses Java SparkRDDWriteClient)
    
    try:
        # Write orders
        orders_df.write.format("hudi") \
            .options(**orders_options) \
            .mode("append") \
            .save("s3://bucket/hudi/orders")
        
        # Write inventory (in same "logical" transaction)
        inventory_df.write.format("hudi") \
            .options(**inventory_options) \
            .mode("append") \
            .save("s3://bucket/hudi/inventory")
        
        # If both succeed, commit both
        # Hudi records both in its timeline
        
    except Exception as e:
        # If either fails, rollback the successful one
        spark.sql("CALL hudi.system.rollback_to_savepoint(table => 'hudi.orders')")
        raise e

# Note: true cross-table atomicity is complex in Hudi.
# Better pattern: use a saga/compensation approach OR use same Hudi table
# with different record_type fields.
```

---

## Hudi Indexing Deep Dive

```
Index maps record keys → file groups (enables fast lookup for upserts)

BLOOM Index (default):
  Mechanism: bloom filter stored in Parquet file footer
  Lookup: check bloom filter to determine if key MAY be in a file
  False positives: bloom filter may return "maybe" (check actual file)
  False negatives: never (if bloom says "no", key definitely not there)
  Good for: medium-scale tables, no HBase dependency
  Config: hoodie.bloom.index.filter.dynamic.max.entries

SIMPLE Index:
  Mechanism: read all records to find matching keys
  Performance: O(n) — full scan on update
  Use: small tables, testing only
  Never use in production at scale

HBASE Index:
  Mechanism: HBase stores key → file_group_id mapping
  Lookup: HBase point lookup O(1) per key
  Performance: fastest for random upserts on huge tables
  Cost: requires HBase cluster
  Use: Uber-scale (billions of records, millions of upserts/hour)

RECORD_INDEX (Hudi 0.14+, new):
  Mechanism: index stored AS a Hudi table in .hoodie/metadata
  No external system needed
  Performance: near-HBase speed without HBase
  Replaces: BLOOM for large-scale without HBase dependency

Metadata Table (Hudi 0.11+):
  Hudi maintains an internal metadata table:
    - Files listing (replaces S3 LIST calls)
    - Column statistics (min/max per file per column → file pruning)
    - Bloom filters
    - Record index (0.14+)
  Enable: "hoodie.metadata.enable": "true"
  Dramatically reduces S3 API calls (expensive at scale)
```

---

## Hudi Multi-Writer Support

```
Problem: multiple Spark jobs writing to same Hudi table simultaneously

Hudi concurrency control options:

Option 1: Optimistic Concurrency Control (OCC, default)
  Each writer reads current state, applies changes, commits
  On conflict: retry from latest snapshot
  Config: "hoodie.write.concurrency.mode": "optimistic_concurrency_control"
         "hoodie.cleaner.policy.failed.writes": "LAZY"  # cleaner handles failed writes
  Use: when writes don't overlap (different partitions)

Option 2: Multi-Writer with Lock Provider
  Writers acquire a distributed lock before committing
  ZooKeeper or DynamoDB as lock backend
  Config:
    "hoodie.write.concurrency.mode": "optimistic_concurrency_control"
    "hoodie.write.lock.provider": "org.apache.hudi.client.transaction.lock.ZookeeperBasedLockProvider"
    "hoodie.write.lock.zookeeper.url": "zk:2181"
    "hoodie.write.lock.zookeeper.lock_key": "hudi_orders_lock"
  Use: overlapping writes to same partitions

Best practice: design pipelines so writers don't overlap on partitions.
  Job A: writes partition 2024-01-15
  Job B: writes partition 2024-01-16
  No lock needed if partitions are disjoint.
```

---

## Hudi on AWS: EMR Integration

```python
# EMR bootstrap action: install Hudi JARs
# Use EMR 6.x which bundles Hudi 0.12+

# EMR Spark configuration for Hudi
spark_conf = {
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.hudi.catalog.HoodieCatalog",
    "spark.sql.extensions": "org.apache.spark.sql.hudi.HoodieSparkSessionExtension",
    "spark.kryo.registrator": "org.apache.spark.HoodieSparkKryoRegistrar",
    # Enable metadata table for faster S3 file listing
    "hoodie.metadata.enable": "true",
    "hoodie.metadata.index.column.stats.enable": "true",
}

# Glue integration: Hudi auto-syncs schema to Glue Catalog
hudi_sync_options = {
    "hoodie.datasource.hive_sync.enable": "true",
    "hoodie.datasource.hive_sync.mode": "glue",
    "hoodie.datasource.hive_sync.database": "my_db",
    "hoodie.datasource.hive_sync.table": "orders",
    "hoodie.datasource.hive_sync.partition_fields": "order_date",
    "hoodie.datasource.hive_sync.use_jdbc": "false",
    "hoodie.datasource.hive_sync.auto_create_database": "true",
}

# After Hudi write + Glue sync, table is queryable via Athena immediately
# Athena reads Hudi COW tables natively
# For MOR: Athena reads read-optimized (base files only, not deltas)
```

---

## Interview Tips

> **Tip 1:** "How does Hudi clustering differ from compaction?" — Compaction is MOR-specific: it merges base Parquet files with delta log files to create clean base files (reduces read overhead from log merging). Clustering is layout optimization: it reorganizes data files for better query performance, similar to Delta ZORDER or Iceberg sort-based rewrite. Compaction is correctness maintenance (read latency degrades without it); clustering is performance tuning (queries are correct either way, just faster with clustering).

> **Tip 2:** "How does Hudi handle a record key that can change partition?" — By default, Hudi's BLOOM index doesn't handle cross-partition updates (if `customer_id=123` was in partition `date=2024-01-01` and an update moves it to `date=2024-01-15`, the old record won't be deleted). Fix: `hoodie.bloom.index.update.partition.path=true` OR use `GLOBAL_BLOOM` index (slower but cross-partition aware). Best practice: choose partition fields that never change per record to avoid this entirely.

> **Tip 3:** "Hudi vs Delta Lake: which is better for CDC at high frequency?" — Hudi wins for pure CDC use cases: incremental queries are more mature, MOR write performance is optimized for frequent updates, and Hudi's index types (especially HBase/Record Index) are purpose-built for fast key lookups. Delta Lake is better for: Databricks environments (tight integration), interactive analytics (OPTIMIZE ZORDER is excellent), and teams using Python/dbt (better tooling ecosystem). For AWS + high-frequency CDC + non-Databricks: Hudi or Iceberg V2.

## ⚡ Cheat Sheet

**Table types**
| Type | Write | Read | Use case |
|---|---|---|---|
| COW (Copy-on-Write) | Rewrites files on update | Fast reads | Batch BI/analytics |
| MOR (Merge-on-Read) | Appends to delta log | Reads merge on-the-fly | Low-latency ingestion |

**Key write options**
```python
hudi_options = {
    "hoodie.table.name": "orders",
    "hoodie.datasource.write.recordkey.field": "order_id",
    "hoodie.datasource.write.precombine.field": "updated_at",
    "hoodie.datasource.write.operation": "upsert",  # insert | bulk_insert | delete
    "hoodie.datasource.write.partitionpath.field": "dt",
}
df.write.format("hudi").options(**hudi_options).mode("append").save("s3://bucket/orders/")
```

**Incremental reads**
```python
spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", "20240101000000") \
    .load("s3://bucket/orders/")
```

**Hudi timeline instants**: commit, deltacommit, compaction, clean, rollback

**Key interview points**
- COW = read-optimized; MOR = write-optimized
- Hudi supports ACID transactions, time-travel, incremental pulls
- Clustering: reorganize files for read performance (like Delta OPTIMIZE)
- Compaction converts MOR delta files into COW Parquet files
