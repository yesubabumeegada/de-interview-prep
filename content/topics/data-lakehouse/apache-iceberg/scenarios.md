---
title: "Apache Iceberg — Scenarios"
topic: data-lakehouse
subtopic: apache-iceberg
content_type: scenario_question
tags: [iceberg, scenarios, interview, design, maintenance]
---

# Apache Iceberg — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Understanding Iceberg Table Format

**Scenario:** Your team is migrating from a traditional Hive-partitioned table to Apache Iceberg. A junior colleague asks why Iceberg is better than just using Hive partitions on S3. How do you explain the core advantages?

<details>
<summary>💡 Hint</summary>

Focus on hidden partitioning, ACID transactions, schema evolution, and time travel. Compare what happens when you run a query against a Hive table vs an Iceberg table — specifically around partition pruning and small files.

</details>

<details>
<summary>✅ Solution</summary>

**Key Iceberg advantages over Hive partitioned tables:**

**1. Hidden Partitioning**
Hive requires users to manually specify partition columns in queries (`WHERE dt = '2024-01-01'`). Iceberg stores partition metadata in its own catalog, so queries automatically prune partitions without users needing to know the physical layout.

```sql
-- Hive: must specify partition explicitly or full scan
SELECT * FROM hive_table WHERE dt = '2024-01-01';

-- Iceberg: partition pruning is automatic even on derived columns
SELECT * FROM iceberg_table WHERE event_time >= '2024-01-01';
```

**2. ACID Transactions**
Iceberg supports snapshot isolation — concurrent reads and writes don't block each other. Hive on S3 has no atomic operations.

**3. Schema Evolution**
Add, drop, rename, or reorder columns without rewriting data files.

```sql
ALTER TABLE orders ADD COLUMN discount DECIMAL(10,2);
-- Old files still readable; new column returns NULL for old rows
```

**4. Time Travel**
Query historical snapshots:
```sql
SELECT * FROM orders FOR SYSTEM_TIME AS OF '2024-01-15 10:00:00';
SELECT * FROM orders FOR VERSION AS OF 12345;
```

**5. Metadata-driven operations**
Iceberg maintains a metadata layer (manifest lists → manifests → data files), enabling efficient partition and column-level statistics for query planning.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Snapshot Expiration and File Compaction

**Scenario:** Your Iceberg table on S3 has been running for 6 months with hourly micro-batch writes. Storage costs are climbing and query performance is degrading. How do you diagnose the issue and implement a maintenance strategy?

<details>
<summary>💡 Hint</summary>

Think about small file accumulation from frequent writes, orphan files from failed jobs, and old snapshots piling up. Iceberg provides built-in procedures for maintenance — `expire_snapshots`, `rewrite_data_files`, and `remove_orphan_files`.

</details>

<details>
<summary>✅ Solution</summary>

**Diagnosis:**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Check snapshot count
spark.sql("SELECT count(*) FROM prod.orders.snapshots").show()

# Check file size distribution
spark.sql("""
  SELECT file_size_in_bytes, count(*) as cnt
  FROM prod.orders.files
  GROUP BY 1
  ORDER BY 1
""").show()

# Check number of data files
spark.sql("SELECT count(*) FROM prod.orders.files").show()
```

**Maintenance Strategy:**

```python
# 1. Expire old snapshots (keep last 7 days)
spark.sql("""
  CALL prod.system.expire_snapshots(
    table => 'prod.orders',
    older_than => TIMESTAMP '2024-01-01 00:00:00',
    retain_last => 10
  )
""")

# 2. Compact small files into target size (512MB)
spark.sql("""
  CALL prod.system.rewrite_data_files(
    table => 'prod.orders',
    strategy => 'binpack',
    options => map(
      'target-file-size-bytes', '536870912',
      'min-input-files', '5'
    )
  )
""")

# 3. Remove orphan files (failed write leftovers)
spark.sql("""
  CALL prod.system.remove_orphan_files(
    table => 'prod.orders',
    older_than => TIMESTAMP '2024-01-08 00:00:00'
  )
""")

# 4. Rewrite manifests for better planning
spark.sql("""
  CALL prod.system.rewrite_manifests('prod.orders')
""")
```

**Schedule via Airflow DAG:**
```python
from airflow.operators.python import PythonOperator

def run_iceberg_maintenance():
    # Run the above Spark procedures
    pass

maintenance_task = PythonOperator(
    task_id='iceberg_maintenance',
    python_callable=run_iceberg_maintenance,
    schedule_interval='0 2 * * *'  # daily at 2am
)
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Multi-Engine Catalog Architecture with Iceberg

**Scenario:** Your organization runs Spark for batch ETL, Trino for ad-hoc analytics, and Flink for streaming. You need a unified Iceberg catalog strategy that supports concurrent reads/writes from all three engines while guaranteeing consistency. Design the architecture.

<details>
<summary>💡 Hint</summary>

Consider catalog choices (Hive Metastore, AWS Glue, Nessie, REST catalog), optimistic concurrency control in Iceberg, and how each engine connects to the catalog. Think about write isolation guarantees and failure scenarios.

</details>

<details>
<summary>✅ Solution</summary>

**Architecture:**

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Apache     │  │   Trino     │  │  Apache     │
│  Spark      │  │  (Ad-hoc)   │  │  Flink      │
│  (Batch)    │  │             │  │ (Streaming) │
└──────┬──────┘  └──────┬──────┘  └──────┬──────┘
       │                │                │
       └────────────────┼────────────────┘
                        │
              ┌─────────▼──────────┐
              │   REST Catalog     │
              │  (e.g. Nessie /    │
              │   Polaris / Lakeformation)│
              └─────────┬──────────┘
                        │
              ┌─────────▼──────────┐
              │   Object Storage   │
              │   (S3 / GCS / ADLS)│
              └────────────────────┘
```

**Catalog Configuration — Spark:**
```python
spark = (
    SparkSession.builder
    .config("spark.sql.catalog.prod", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.prod.type", "rest")
    .config("spark.sql.catalog.prod.uri", "https://catalog.internal/api/catalog")
    .config("spark.sql.catalog.prod.credential", "client:secret")
    .getOrCreate()
)
```

**Catalog Configuration — Trino:**
```properties
# /etc/trino/catalog/iceberg.properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=https://catalog.internal/api/catalog
iceberg.rest-catalog.security=OAUTH2
```

**Catalog Configuration — Flink:**
```java
CatalogLoader catalogLoader = CatalogLoader.rest(
    "prod",
    ImmutableMap.of(
        CatalogProperties.URI, "https://catalog.internal/api/catalog"
    ),
    new Configuration()
);
```

**Concurrency Control:**
Iceberg uses optimistic concurrency — each write attempts a CAS (compare-and-swap) on the metadata pointer. If two writers conflict:
- Iceberg retries appends automatically
- For overwrites, the second writer fails → application must handle retry

```python
# Spark: set retry on commit conflict
spark.conf.set("spark.sql.iceberg.handle-merge-schema", "true")

# For Flink streaming writes, use exactly-once with checkpointing
env.enableCheckpointing(60_000)  # checkpoint every 60s
```

**Branch strategy for Flink streaming:**
```sql
-- Create a write branch for Flink, merge periodically
ALTER TABLE prod.events CREATE BRANCH streaming_write;

-- Flink writes to branch
-- Scheduled job merges branch to main
CALL prod.system.fast_forward('prod.events', 'main', 'streaming_write');
```

**Key Design Decisions:**
1. REST catalog (Nessie/Polaris) over HMS for multi-engine neutrality
2. S3 with strong consistency (post-2020 S3) eliminates need for DynamoDB lock table
3. Table branching isolates streaming writes from batch reads
4. Separate IAM roles per engine for fine-grained access control

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What's the difference between Iceberg and Delta Lake?" — Focus on open standards: Iceberg is engine-agnostic by design, while Delta originated with Databricks. Both now have open specs, but Iceberg has broader multi-engine adoption (Trino, Flink, Spark, Hive).
> **Tip 2:** "How does Iceberg handle schema evolution?" — Iceberg uses column IDs internally, not names, so renaming/reordering columns doesn't break existing files.
> **Tip 3:** "What happens during a concurrent write conflict?" — Iceberg uses optimistic concurrency: writers read the current snapshot, apply changes, then attempt to commit. If another writer committed first, the operation retries or fails depending on the operation type.
