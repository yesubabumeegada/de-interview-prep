---
title: "Apache Iceberg — Real World"
topic: data-lakehouse
subtopic: apache-iceberg
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [iceberg, production, netflix, maintenance, migration]
---

# Apache Iceberg — Real World

## Netflix's Iceberg Usage (Origin Story)

```
Iceberg was created at Netflix to solve problems with Hive tables at petabyte scale:

Problem 1: S3 LIST latency
  Hive partitions stored as S3 prefixes → discovering all files requires LIST all prefixes
  At petabyte scale: millions of prefixes → LIST takes minutes
  Iceberg solution: manifest files track all data files → no S3 LIST needed

Problem 2: Schema evolution breaking queries
  Hive column names are positional: renaming a column breaks all downstream SQL
  Iceberg: column IDs (not names) → renaming safe, column ordering changes safe

Problem 3: Concurrent writer corruption
  Multiple Spark jobs writing to same table → partial writes, corrupt state
  Iceberg: atomic snapshot commits → partial write never visible

Problem 4: No time travel
  Debugging data issues required manual S3 snapshots
  Iceberg: built-in time travel via snapshot history

Result: Netflix runs 10,000+ Iceberg tables at exabyte scale.
        Spark, Trino, Flink, and Presto all read from the same Iceberg catalog.
```

---

## Pattern: Hive-to-Iceberg Migration

```python
# In-place migration: convert Hive table to Iceberg (no data copy)
# Only works if data is already in Parquet format on S3

# Step 1: Validate source is Parquet
spark.sql("DESCRIBE EXTENDED hive.db.orders").show()
# Check: serde = ParquetHiveSerDe, location = s3://...

# Step 2: Snapshot migration (creates new Iceberg table from Hive metadata)
spark.sql("""
  CALL spark_catalog.system.snapshot(
    source_table => 'hive.db.orders',
    table => 'iceberg.db.orders',
    location => 's3://bucket/iceberg/orders'
  )
""")
-- This MIGRATES the metadata, NOT the data files
-- Data files stay in place; Iceberg metadata points to same Parquet files

# Step 3: Validate row counts
hive_count = spark.sql("SELECT COUNT(*) FROM hive.db.orders").collect()[0][0]
iceberg_count = spark.sql("SELECT COUNT(*) FROM iceberg.db.orders").collect()[0][0]
assert hive_count == iceberg_count, f"Count mismatch: {hive_count} vs {iceberg_count}"

# Step 4: Run new writes against Iceberg; Hive table becomes read-only
# Step 5: Cut downstream consumers to Iceberg table
# Step 6: Drop Hive table (90 days after cutover, with audit)

# Full migration (data copy — for non-Parquet sources or layout change)
spark.sql("""
  CREATE TABLE iceberg.db.orders
  USING iceberg
  PARTITIONED BY (days(order_ts))
  AS SELECT * FROM hive.db.orders
""")
```

---

## Pattern: Multi-Engine Setup (Spark + Trino + Flink)

```
Setup: same Iceberg tables read/written by three engines

Catalog: AWS Glue (supported by all three)
Storage: s3://bucket/warehouse/

Spark (batch ETL + large transforms):
  Config: spark.sql.catalog.glue_catalog = org.apache.iceberg.spark.SparkCatalog
          spark.sql.catalog.glue_catalog.catalog-impl = org.apache.iceberg.aws.glue.GlueCatalog
          spark.sql.catalog.glue_catalog.warehouse = s3://bucket/warehouse
  Write: daily batch transforms, Silver → Gold aggregations

Trino (interactive analytics, BI dashboards):
  Connector: iceberg connector with Glue catalog
  Config: hive.metastore = glue
  Read: SELECT queries from analysts and Tableau
  
Flink (streaming CDC ingestion):
  Catalog: HiveCatalog pointing to Glue (HMS-compatible API)
  Write: Bronze and Silver streaming inserts

Coordination:
  All three engines commit snapshots independently to Glue
  Iceberg optimistic concurrency handles conflicts
  No engine "owns" the table — all are peers

Key operational note:
  Each engine may have different Iceberg library versions
  Always use Iceberg >= 1.3 on all engines for full V2 compatibility
  Test engine compatibility before production multi-write
```

---

## Pattern: Iceberg for GDPR Right-to-Erasure

```python
def gdpr_erasure(spark, customer_id: str, tables_to_erase: list):
    """
    GDPR Article 17: right to erasure.
    Iceberg V2 equality deletes enable efficient row-level deletion.
    """
    from datetime import datetime
    
    erasure_log = []
    
    for table_path in tables_to_erase:
        # Count rows before deletion
        before_count = spark.sql(f"""
            SELECT COUNT(*) FROM iceberg.`{table_path}`
            WHERE customer_id = '{customer_id}'
        """).collect()[0][0]
        
        if before_count > 0:
            # Delete by equality predicate (Iceberg V2 equality delete file)
            spark.sql(f"""
                DELETE FROM iceberg.`{table_path}`
                WHERE customer_id = '{customer_id}'
            """)
            
            erasure_log.append({
                "table": table_path,
                "customer_id": customer_id,
                "rows_deleted": before_count,
                "deleted_at": datetime.now().isoformat(),
                "snapshot_before": spark.sql(f"""
                    SELECT snapshot_id FROM iceberg.`{table_path}`.history
                    ORDER BY made_current_at DESC LIMIT 1
                """).collect()[0][0]
            })
    
    # Store erasure audit log
    spark.createDataFrame(erasure_log).write \
        .format("iceberg") \
        .mode("append") \
        .save("s3://bucket/audit/gdpr_erasure_log")
    
    # Note: run rewrite_data_files periodically to physically remove rows
    # Until then, rows are "logically deleted" via equality delete files
    # Physical deletion required for full GDPR compliance
    print(f"Logical deletion complete for {customer_id}. Run compaction for physical deletion.")
```

---

## Interview Tips

> **Tip 1:** "How do you perform truly physical deletion of data in Iceberg for GDPR?" — Iceberg V2 equality deletes are *logical* deletes — the row data still exists in the original Parquet files, just marked as deleted via a separate delete file. For physical deletion (required by GDPR), you must run `rewrite_data_files` which creates new Parquet files that genuinely exclude the deleted rows. After compaction, run `expire_snapshots` to remove the old snapshot that references the original files. Only then are the physical Parquet bytes gone.

> **Tip 2:** "What's the snapshot isolation model in Iceberg?" — Iceberg provides snapshot isolation: each read transaction sees a consistent snapshot of the table. Concurrent writers use optimistic concurrency — the first committed write wins; the second gets a `CommitFailedException` and must retry from the latest snapshot. This means readers never see partial writes, and writers don't block readers. It's the same guarantee as PostgreSQL's MVCC model.

> **Tip 3:** "How does Iceberg differ from what Hive with ACID mode provides?" — Hive ACID uses a merge-on-read approach with delta files written to HDFS. It requires compaction (the "major compaction" step), has known performance issues at scale, and is tightly coupled to Hive SerDe. Iceberg was designed from scratch for cloud object storage, is engine-agnostic, has a formal spec, and separates the file format (Parquet) from the table format (Iceberg metadata). Hive ACID is essentially deprecated for new workloads in favor of Iceberg.
