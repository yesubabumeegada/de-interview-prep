---
title: "Apache Iceberg — Senior Deep Dive"
topic: data-lakehouse
subtopic: apache-iceberg
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [iceberg, v2, row-level-deletes, branching, nessie, catalog]
---

# Apache Iceberg — Senior Deep Dive

## Iceberg V2: Row-Level Deletes

```
Iceberg V1: Copy-on-Write (COW)
  UPDATE / DELETE: rewrites entire data files
  Read performance: optimal (no delete tracking needed on read)
  Write amplification: high (updating 1 row in 128MB file → rewrite full file)
  Use case: read-heavy, infrequent updates

Iceberg V2: Merge-on-Read (MOR)
  UPDATE: writes a "position delete file" or "equality delete file"
  DELETE: same — a delete file marks which rows are deleted
  Read: merge base data files with delete files at read time
  Write performance: optimal (append-only delete files, no rewrite)
  Read overhead: reader must apply deletes (handled transparently by engine)
  Use case: write-heavy, frequent CDC updates

Delete file types in V2:
  Position deletes: (file_path, row_position) pairs
    - Mark specific row positions as deleted
    - Efficient for targeted row deletes
  Equality deletes: column value predicates
    - Mark all rows matching predicate as deleted
    - Used for DELETE WHERE customer_id = 123
    - Less efficient at read time (scan all files for matches)

Auto-compaction converts MOR → COW:
  After accumulating many delete files, run rewrite_data_files
  This materializes deletes into new clean Parquet files (COW state)
  Read performance restored
```

---

## Iceberg Branching and Tagging (Nessie Catalog)

```python
# Iceberg supports Git-like branches for table isolation
# Requires: Nessie catalog or Iceberg REST catalog with branching support

# Via Spark SQL (Iceberg native branching)
spark.sql("ALTER TABLE db.orders CREATE BRANCH dev")
spark.sql("ALTER TABLE db.orders CREATE BRANCH staging")

# Write to dev branch only (main is unchanged)
spark.sql("""
  INSERT INTO db.orders.branch_dev
  SELECT * FROM new_orders WHERE is_test = true
""")

# Merge dev branch to main (after validation)
spark.sql("""
  CALL local.system.fast_forward(
    table => 'db.orders',
    branch => 'main',
    to => 'dev'
  )
""")

# Tags (immutable pointers to snapshots — good for audit)
spark.sql("""
  ALTER TABLE db.orders CREATE TAG monthly_close_2024_01
  AS OF VERSION 42
""")

# Nessie multi-table branching (ACID across multiple tables)
# Nessie supports Git-like commits for the entire catalog
# This enables:
#   - Isolated dev/staging/prod environments sharing same S3 data
#   - Zero-copy table clones for testing
#   - Atomic multi-table schema migrations
```

---

## Iceberg Catalog Options Deep Dive

```
Hive Metastore:
  Mechanism: HMS stores table metadata, Iceberg stores snapshot pointer in HMS
  Pros: works with existing Hive infrastructure, Spark/Trino/Hive support
  Cons: HMS is a SPOF, not cloud-native, no multi-table atomicity

AWS Glue:
  Mechanism: Glue acts as HMS-compatible catalog; Iceberg metadata in S3
  Pros: managed, serverless, integrates with Athena/EMR natively
  Cons: no branching, eventual consistency on catalog operations

Apache Nessie:
  Mechanism: Git-like catalog with commit history, branches, tags
  Pros: multi-table atomicity, data isolation (branches), audit trail
  Cons: new project, smaller community, requires hosting

Apache Polaris (Iceberg REST Catalog):
  Mechanism: open-source REST API spec for Iceberg catalog
  Pros: vendor-neutral, Snowflake Open Catalog implements this spec
  Cons: newer standard, ecosystem still maturing

Snowflake Open Catalog (formerly Polaris):
  Mechanism: managed Iceberg REST catalog by Snowflake
  Pros: access Iceberg tables from Spark, Trino, Flink via REST
  Cons: Snowflake vendor dependency

Recommendation:
  AWS + EMR/Athena: Glue (easiest, managed)
  Multi-engine + Databricks: Unity Catalog with Iceberg (Databricks 13+)
  Open lakehouse, multi-team isolation: Nessie
  Snowflake + external engines: Snowflake Open Catalog
```

---

## Iceberg in Streaming Pipelines

```python
# Flink + Iceberg: native streaming sink (better than Spark for low latency)
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# Create Iceberg table in Flink
t_env.execute_sql("""
  CREATE TABLE orders_iceberg (
    order_id     BIGINT,
    customer_id  BIGINT,
    amount       DOUBLE,
    order_ts     TIMESTAMP(3),
    WATERMARK FOR order_ts AS order_ts - INTERVAL '5' SECOND
  )
  WITH (
    'connector'             = 'iceberg',
    'catalog-type'          = 'hive',
    'catalog-name'          = 'hive_prod',
    'database'              = 'db',
    'table-name'            = 'orders_iceberg',
    'warehouse'             = 's3://bucket/warehouse',
    'write.upsert.enabled'  = 'true',
    'write.distribution-mode' = 'hash'
  )
""")

# Iceberg streaming optimizations:
# write.upsert.enabled = true → UPSERT mode (equality deletes for updates)
# write.distribution-mode = hash → data distributed by primary key (fewer small files)
# sink.parallelism = N → control write parallelism

# Snapshot commits in streaming:
# Flink commits an Iceberg snapshot every checkpoint interval
# This determines read visibility: data visible after each checkpoint
```

---

## Production Iceberg Maintenance Schedule

```python
# Automated maintenance via Spark (run as daily Databricks/EMR job)
def iceberg_maintenance(spark, table: str, catalog: str = "local"):
    full_table = f"{catalog}.{table}"
    
    # 1. Expire old snapshots (keep last 7 days)
    spark.sql(f"""
        CALL {catalog}.system.expire_snapshots(
            table => '{full_table}',
            older_than => TIMESTAMP '{(datetime.now() - timedelta(days=7)).isoformat()}',
            retain_last => 5
        )
    """)
    
    # 2. Compact small files → 128MB
    result = spark.sql(f"""
        CALL {catalog}.system.rewrite_data_files(
            table => '{full_table}',
            strategy => 'binpack',
            options => map(
                'target-file-size-bytes', '134217728',
                'partial-progress.enabled', 'true',
                'max-concurrent-file-group-rewrites', '5'
            )
        )
    """).collect()[0]
    print(f"Compacted: {result['rewritten_data_files_count']} files → {result['added_data_files_count']} files")
    
    # 3. Compact manifests
    spark.sql(f"""
        CALL {catalog}.system.rewrite_manifests(table => '{full_table}')
    """)
    
    # 4. Remove orphan files (S3 files not in any snapshot)
    spark.sql(f"""
        CALL {catalog}.system.remove_orphan_files(
            table => '{full_table}',
            older_than => TIMESTAMP '{(datetime.now() - timedelta(days=3)).isoformat()}'
        )
    """)

# Run daily for high-volume tables, weekly for low-volume
from datetime import datetime, timedelta
tables_to_maintain = [
    "db.orders",
    "db.clickstream",
    "db.customer_features",
]
for table in tables_to_maintain:
    iceberg_maintenance(spark, table)
```

---

## Interview Tips

> **Tip 1:** "When would you choose Iceberg V2 (MOR) over V1 (COW)?" — V2 MOR is the right choice when you have frequent updates/deletes (e.g., CDC from OLTP systems). V1 COW writes are expensive when updating rows scattered across large files. V2 writes are cheap (append delete files) but reads pay a merge cost. The pattern: use V2 for ingest, then run periodic compaction to convert to COW state and restore read performance. Delta Lake uses a similar approach with its deletion vectors.

> **Tip 2:** "How does Iceberg ensure atomic cross-table commits?" — Standard Iceberg (Hive/Glue catalog) doesn't support multi-table atomic commits. For that you need Nessie, which provides a Git-like catalog with branch-level atomic commits across any number of tables. A Nessie commit atomically updates multiple table pointers in the catalog. This enables: atomic schema migrations across related tables, A/B testing with isolated table states, rollback of a full pipeline run (not just one table).

> **Tip 3:** "How do you monitor Iceberg table health in production?" — Track: (1) snapshot count — if growing, expire_snapshots isn't running; (2) average data file size — if < 32MB, compaction is needed; (3) delete file ratio — if > 10% of read I/O comes from delete files, run rewrite_data_files; (4) manifest file count — if > 1000 manifests, run rewrite_manifests. Alert thresholds: average file < 32MB → trigger compaction, delete file overhead > 20% → urgent compaction.
