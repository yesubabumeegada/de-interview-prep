---
title: "Table Format Comparison — Fundamentals"
topic: data-lakehouse
subtopic: table-format-comparison
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [delta-lake, iceberg, hudi, table-formats, comparison]
---

# Table Format Comparison — Fundamentals


## 🎯 Analogy

Think of Delta, Iceberg, and Hudi like competing operating systems for your data lake: all three provide ACID and time travel on top of object storage, but they differ in their metadata approach, ecosystem integrations, and performance characteristics.

---
## The Three Open Table Formats

All three table formats solve the same core problem: adding ACID transactions, schema evolution, and time-travel to plain Parquet files on object storage. They differ in design philosophy, ecosystem, and strengths.

```
Delta Lake:    Created by Databricks (2019). Open-sourced. Transaction log in _delta_log/.
Apache Iceberg: Created by Netflix (2018). Apache project. Metadata hierarchy: table/snapshot/manifest/data.
Apache Hudi:   Created by Uber (2016). Apache project. Timeline-based. Specializes in incremental reads.
```

---

## Feature Comparison Table

| Feature | Delta Lake | Apache Iceberg | Apache Hudi |
|---|---|---|---|
| **ACID transactions** | Yes | Yes | Yes |
| **Time travel** | Yes (version/timestamp) | Yes (snapshot/timestamp) | Yes (timestamp) |
| **Schema evolution** | Yes | Yes (ID-based) | Yes |
| **Partitioning** | Manual | Hidden + evolution | Manual |
| **Row-level updates** | COW + Deletion Vectors | COW (V1) / MOR (V2) | COW or MOR |
| **Incremental reads** | CDF | Changelog views | Native (first-class) |
| **Streaming** | Native (Spark) | Yes (Flink native) | Yes (Spark) |
| **Multi-engine** | Spark (best); Trino via UniForm | Spark, Trino, Flink, Athena | Spark (best) |
| **Catalog** | Unity Catalog (Databricks) | Glue, Hive, Nessie, Polaris | Hive, Glue |
| **Compaction** | OPTIMIZE | rewrite_data_files | compaction (MOR) |
| **Created by** | Databricks | Netflix | Uber |
| **Best fit** | Databricks shops | Multi-engine open lakehouse | High-frequency CDC/upserts |

---

## When to Choose Each Format

```
Choose Delta Lake when:
  ✓ Your team is on Databricks (native, optimized, Unity Catalog)
  ✓ Most engineers know Spark, not multi-engine SQL
  ✓ You want managed table format (Databricks handles optimization)
  ✓ dbt + Databricks integration (best-in-class)
  ✓ OPTIMIZE ZORDER / Liquid Clustering for query performance

Choose Apache Iceberg when:
  ✓ You need multi-engine (Spark writes, Trino/Athena queries)
  ✓ You're on AWS and use Athena/Glue + Spark/EMR
  ✓ You want open standard (not tied to Databricks)
  ✓ You need partition evolution (change partition strategy without rewrite)
  ✓ GCP BigQuery Iceberg support (read-only)

Choose Apache Hudi when:
  ✓ You have high-frequency CDC (millions of updates/hour)
  ✓ Incremental reads are critical for downstream pipeline efficiency
  ✓ You're on AWS EMR (good Hudi support)
  ✓ You need MOR write performance for frequent upserts
  ✓ You already have Hudi expertise on the team
```

---

## Metadata Architecture Comparison

```
Delta Lake metadata:
  _delta_log/
    00000000000000000001.json  ← append-only commit JSON files
    00000000000000000010.checkpoint.parquet  ← snapshot every 10 commits
  
  Commit JSON = list of "add file" / "remove file" operations
  Read: latest checkpoint + commits since checkpoint

Apache Iceberg metadata:
  metadata/
    v1.metadata.json          ← table schema + current snapshot pointer
    snap-001.avro             ← snapshot: list of manifests
    manifest-list-001.avro   ← manifest list: file stats for pruning
    manifest-001.avro        ← manifest: data file paths + column stats
  
  Read: table metadata → snapshot → manifest list → manifests → data files

Apache Hudi metadata:
  .hoodie/
    hoodie.properties         ← table config
    20240115120000.commit     ← timeline entry per commit
    .hoodie_partition_metadata
  metadata/ (optional table)
    files/     column_stats/   record_index/
  
  Read: timeline → latest commit → file listing

Key differences:
  Delta: linear commit log (simple, efficient for sequential reads)
  Iceberg: tree structure (efficient for large tables, multi-level pruning)
  Hudi: timeline (optimized for incremental: "what changed since commit X")
```

---


## ▶️ Try It Yourself

```bash
# Quick comparison — run against same data with different formats

# Delta Lake (Databricks native, best Spark integration)
# spark.read.format("delta").load("s3://bucket/delta/orders/")

# Apache Iceberg (best for multi-engine: Spark + Trino + Flink)
# spark.read.format("iceberg").load("catalog.silver.orders")

# Apache Hudi (best for CDC/streaming upserts)
# spark.read.format("hudi").load("s3://bucket/hudi/orders/")

# All three support:
# - ACID transactions
# - Time travel / versioning
# - Schema evolution
# - Partition evolution

# Key differences:
# Delta:   Databricks-optimized, best tooling, proprietary log format
# Iceberg: Open standard (ANSI), best multi-engine support, REST catalog
# Hudi:    Best for high-frequency upserts, built-in indexing for CDC

echo "Choose based on: primary engine (Delta), multi-engine (Iceberg), CDC (Hudi)
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "All three formats do the same thing — why does it matter which one you pick?" — The functional overlap is real, but the ecosystem fit is critical. Delta is deeply integrated with Databricks; choosing it means assuming Spark/Databricks as your compute. Iceberg is the open standard — Trino, Flink, Spark, Athena, BigQuery, DuckDB all support it. Hudi is optimized for CDC-heavy workloads with its incremental query model. The "right" choice depends on your existing stack more than the feature list.

> **Tip 2:** "Is one of these table formats 'winning' the market?" — As of 2024: Delta dominates in Databricks shops (which is a large market). Iceberg is growing fastest as the open standard — AWS, Google, Apple, LinkedIn, Netflix all use it. Hudi remains strong at Uber and Robinhood but has a smaller community. The trend: Databricks supports Iceberg reads via UniForm; Snowflake supports Iceberg tables; BigQuery supports Iceberg. Iceberg is becoming the interoperability standard even if Delta and Hudi remain as primary write formats.

> **Tip 3:** "Can I use multiple table formats in the same lakehouse?" — Yes, and it's common. Delta for Spark/Databricks workloads (Silver/Gold), Iceberg for tables that need multi-engine access (shared with Trino or Flink), Hudi for CDC ingestion tables (Bronze/Silver). Databricks UniForm lets you write Delta and expose Iceberg metadata automatically. The catalog (Unity Catalog or Glue) unifies the namespace — consumers don't need to know the underlying format.
