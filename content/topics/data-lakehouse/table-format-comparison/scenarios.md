---
title: "Table Format Comparison — Scenarios"
topic: data-lakehouse
subtopic: table-format-comparison
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [table-formats, scenarios, interview, selection, design]
---

# Table Format Comparison — Interview Scenarios

## Scenario 1: Choose a Table Format for a Healthcare Analytics Platform

**Question:** A healthcare company is building a data platform on AWS. Requirements: (1) Spark for ETL, (2) Athena for ad-hoc SQL by analysts, (3) Flink for real-time patient alerts, (4) compliance: 7-year data retention, audit trail for all data access, GDPR right-to-erasure for patient data. Which table format do you recommend?

**Answer:**

```
Analysis:

Compute engines: Spark (EMR), Athena, Flink → multi-engine requirement
  → Eliminates Delta as primary format (Athena + Delta = manifests, not native)
  → Iceberg: native Athena support (Iceberg connector), native Flink support, EMR Spark

GDPR right-to-erasure: DELETE WHERE patient_id = X
  → Need efficient row-level deletes
  → Iceberg V2 MOR: equality delete files (efficient deletes, compaction materializes)
  → Delta DV: similar, but Athena doesn't support DVs natively
  → Hudi: efficient deletes, but Athena support for Hudi is limited
  → Winner: Iceberg V2

7-year retention + audit trail:
  → Iceberg: snapshot history tracks all changes
  → Iceberg table branching (Nessie): immutable tags for each year-end state
  → Athena audit: S3 access logs + Athena query history in CloudTrail

Recommendation: Apache Iceberg

Full architecture:
  Catalog: AWS Glue (managed, integrates with all three engines)
  Table format: Iceberg V2 (MOR for efficient deletes)
  Storage: S3 with Object Lock (WORM for 7-year retention compliance)
  
  Engine mapping:
    Flink → Iceberg sink (patient alerts: streaming inserts to Bronze)
    Spark/EMR → Iceberg MERGE (Silver: ETL transforms, upserts by patient_id)
    Athena → Iceberg SELECT (read-only analytics for compliance team)
  
  GDPR process:
    1. DELETE WHERE patient_id = X (Spark → Iceberg V2 equality delete)
    2. Schedule rewrite_data_files (physical removal from Parquet files)
    3. expire_snapshots (removes historical snapshots referencing patient data)
    4. Object Lock ensures deletion is final and auditable

  Audit trail:
    CloudTrail: all Athena + EMR access logged
    Iceberg table history (DESCRIBE HISTORY): all commits with timestamp and operation
    Glue Catalog: schema change history
```

---

## Scenario 2: Migrate from Hive to a Modern Table Format

**Question:** Your team has 500 Hive tables on HDFS (Parquet format, Hive Metastore catalog). You're migrating to AWS S3 and want to modernize the table format. Choose between Delta, Iceberg, and Hudi, and outline the migration plan.

**Answer:**

```
Choose: Apache Iceberg
  Reasoning:
  1. Migration path: Iceberg can migrate Hive Parquet tables WITHOUT data copy
     (snapshot migration: Iceberg reads existing Parquet files, wraps with Iceberg metadata)
  2. Hive Metastore is Iceberg-compatible (Hive catalog → Iceberg works natively)
  3. No Databricks lock-in (team doesn't use Databricks)
  4. Athena + Iceberg: simpler than Delta manifests for ad-hoc SQL
  5. Schema evolution: ID-based (rename columns safely — important after migration)

Migration Plan:

Phase 1: Infrastructure (Week 1-2)
  - Copy HDFS Parquet files to S3: s3-dist-cp or AWS DataSync
  - Verify checksums (md5/sha256 comparison before and after copy)
  - Install Iceberg JARs on EMR clusters

Phase 2: Migrate High-Value Tables (Week 3-4)
  - Start with 10 most-used tables (80% of query volume)
  - Use Iceberg snapshot migration (no data copy, metadata only):
    CALL local.system.snapshot(
      source_table => 'hive.db.orders',
      table => 'iceberg.db.orders',
      location => 's3://bucket/iceberg/orders'
    )
  - Validate: row count, column distributions, query results match

Phase 3: Parallel Operation (Week 5-8)
  - New writes go to Iceberg tables
  - Hive tables become read-only (for rollback safety)
  - Update downstream jobs to read from Iceberg catalog

Phase 4: Migrate Remaining Tables (Week 9-16)
  - 490 remaining tables via batch migration scripts
  - Categorize: active (migrate now), archive (migrate to cold storage), deprecated (drop)

Phase 5: Decommission HDFS (Week 17-20)
  - All tables validated in Iceberg
  - HDFS kept read-only for 30 days
  - Delete HDFS data and Hadoop cluster
  
Cost: primary cost is migration compute (S3-dist-cp + EMR spot instances)
      Expect $2,000-5,000 one-time migration cost for 500 tables
```

---

## Scenario 3: Debug Format-Specific Query Performance Issue

**Question:** Your Delta table answers queries in 3 seconds from Spark. Your colleague's Iceberg table on the same data answers similar queries in 15 seconds from the same Spark cluster. What do you investigate?

**Answer:**

```
Hypothesis checklist:

1. Check file sizes
   -- Both tables: DESCRIBE DETAIL / DESCRIBE TABLE
   Delta: average file size 128MB (OPTIMIZE ran recently)
   Iceberg: average file size 8MB (rewrite_data_files never ran, streaming writes)
   → Root cause found: Iceberg has 16× more files, 16× more S3 GET requests
   Fix: CALL system.rewrite_data_files(table => 'db.orders', strategy => 'binpack',
         options => map('target-file-size-bytes', '134217728'))

2. Check column statistics collection
   Delta: stats collected on all columns by default (32 columns)
   Iceberg: stats require explicitly enabling column statistics at write time
   
   Check: spark.sql("SELECT * FROM iceberg.db.orders$files LIMIT 5")
   -- Look at: lower_bounds, upper_bounds — if null, stats not collected
   -- Fix: set write.metadata.metrics.default=truncate(16) in table properties

3. Check partitioning
   Delta: partition by order_date (explicit)
   Iceberg: hidden partition by days(order_ts) — but query might not hit it
   
   Explain plan: both should show partition pruning
   If Iceberg doesn't prune: query uses wrong timestamp column format
   Fix: ensure WHERE clause uses the column configured in PARTITIONED BY

4. Check number of manifest files
   SELECT count(*) FROM iceberg.db."orders$manifests";
   -- 10,000 manifests (one per streaming micro-batch)
   Fix: CALL system.rewrite_manifests(table => 'db.orders')
   
Result after fix:
  After rewrite_data_files + rewrite_manifests: Iceberg query drops to 3-4 seconds
  Conclusion: format difference was operational (no maintenance), not inherent
```
