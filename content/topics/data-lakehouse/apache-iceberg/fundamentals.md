---
title: "Apache Iceberg — Fundamentals"
topic: data-lakehouse
subtopic: apache-iceberg
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [iceberg, table-format, lakehouse, parquet, schema-evolution]
---

# Apache Iceberg — Fundamentals

## What Is Apache Iceberg?

Apache Iceberg is an open table format for huge analytic datasets. It adds a metadata layer on top of Parquet (or ORC/Avro) files stored in object storage, enabling ACID transactions, schema evolution, and time-travel — for any query engine that supports the spec.

```
Iceberg is NOT:
  - A storage system (data lives in S3/GCS/ADLS)
  - A query engine (Spark/Trino/Flink reads the data)
  - A file format (Parquet/ORC are the file formats)

Iceberg IS:
  - A table format: a spec for how metadata describes a table
  - A metadata layer: tracks which files belong to the table
  - An open standard: any engine implementing the spec can read/write
```

---

## How Iceberg Stores Data

```
S3 bucket layout:
  s3://bucket/warehouse/db/orders/
  ├── metadata/
  │   ├── v1.metadata.json          ← table metadata (schema, partition spec)
  │   ├── v2.metadata.json          ← updated after schema change
  │   ├── snap-001.avro             ← snapshot: list of manifest files
  │   └── manifest-list-001.avro   ← manifest list
  ├── data/
  │   ├── year=2024/month=01/
  │   │   ├── 00000.parquet
  │   │   └── 00001.parquet
  │   └── year=2024/month=02/
  │       └── 00002.parquet

Metadata hierarchy:
  Table metadata → Snapshot → Manifest List → Manifest Files → Data Files
  
  Each layer:
    Table metadata: schema, partition spec, current snapshot pointer
    Snapshot: point-in-time state of all data files (used for time-travel)
    Manifest list: list of manifest files in this snapshot
    Manifest file: list of data files with min/max statistics per column
```

---

## Key Iceberg Features

```
1. ACID Transactions:
   Atomic commit: either all changes in a transaction are visible, or none
   Optimistic concurrency: writers detect conflicts on commit
   No file-level locking (unlike Hive)

2. Schema Evolution (without rewriting data):
   Add column: safe (old files treated as if column = null)
   Drop column: safe (column marked deleted, data files not rewritten)
   Rename column: safe (tracked via column IDs, not names)
   Change type: limited (widening only: int→long, float→double)
   
   Key difference from Hive: Iceberg uses column IDs, not names
   Renaming a column doesn't break existing queries

3. Hidden Partitioning:
   Hive: user writes WHERE dt = '2024-01-15' (must know partition format)
   Iceberg: user writes WHERE order_date = '2024-01-15' (Iceberg figures out partition)
   
   Partition transforms:
     years(ts), months(ts), days(ts), hours(ts)
     bucket(N, col)   → hash partitioning
     truncate(N, col) → string/integer prefix

4. Time Travel:
   SELECT * FROM orders FOR SYSTEM_TIME AS OF '2024-01-15 10:00:00';
   SELECT * FROM orders VERSION AS OF 42;  -- by snapshot ID

5. Partition Evolution:
   Can change partition strategy on existing table without rewriting data
   Old data stays with old partition; new data uses new partition
   Iceberg handles this transparently
```

---

## Iceberg vs Hive Table Format

| Feature | Hive | Iceberg |
|---|---|---|
| ACID | No (Hive 3 with ACID is slow) | Yes |
| Schema evolution | Name-based (rename breaks) | ID-based (rename safe) |
| Partition discovery | List all S3 prefixes | Read manifest (fast) |
| Time travel | No | Yes |
| Partition pruning | Manual partition filter required | Hidden (auto-detected) |
| Concurrent writes | File-level lock or overwrite | Optimistic concurrency |
| Engine support | Spark, Hive | Spark, Flink, Trino, Athena, Presto |

---

## Interview Tips

> **Tip 1:** "Why was Iceberg created when Parquet already existed?" — Parquet is a file format (how data is stored in a single file). Iceberg is a table format (how a collection of files form a consistent, transactional table). Parquet has no concept of ACID, schema evolution across files, or time-travel. Iceberg adds the metadata layer on top of Parquet files to provide these table-level guarantees.

> **Tip 2:** "What's the difference between Iceberg and Delta Lake?" — Both are table formats providing ACID + time-travel on Parquet. Key differences: Delta is tightly integrated with Databricks/Spark; Iceberg is a true open spec supported by more engines (Trino, Flink natively). Iceberg has hidden partitioning and partition evolution. Delta has DML on streaming (Databricks-optimized). For Databricks shops: Delta. For multi-engine environments: Iceberg.

> **Tip 3:** "How does Iceberg enable fast queries without listing all S3 files?" — Iceberg reads metadata files (manifests) that contain statistics (min/max per column, row counts) for each data file. The query engine reads the manifest and prunes files based on the WHERE clause — without ever listing S3 prefixes. This is "metadata-driven file pruning." S3 LIST is O(n) and expensive; reading a manifest file is O(1).
