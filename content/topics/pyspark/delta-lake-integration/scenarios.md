---
title: "Delta Lake Integration — Scenarios"
topic: pyspark
subtopic: delta-lake-integration
content_type: scenario_question
tags: [delta-lake, MERGE, CDF, medallion, ACID, scenarios, interview]
---

# Delta Lake Integration — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: What Is Delta Lake and How Does ACID Work?

**Scenario:** You're in a system design interview. The interviewer says: "Your team is building a data lake on S3 using Parquet files. A colleague suggests using Delta Lake instead. What would you tell them? What does ACID mean in this context, and what concrete problems does it solve?"

<details>
<summary>💡 Hint</summary>

Think about what happens with plain Parquet when: (1) a write job crashes halfway through, (2) two jobs write at the same time, (3) you need to undo a bad write. Then explain how Delta's transaction log solves each problem.

</details>

<details>
<summary>✅ Solution</summary>

```
Plain Parquet problems Delta solves:

1. ATOMICITY (partial write protection)
   Problem: Spark writes 100 Parquet files. Cluster crashes after file 50.
   Parquet result: 50 corrupt files are now part of the "table"
   Delta result: Write never committed to _delta_log/ → readers see previous complete version
                 50 orphaned files are cleaned up by VACUUM

2. CONSISTENCY (schema enforcement)
   Problem: Someone pushes a bad ETL that writes "amount" as String instead of Double
   Parquet result: Bad files silently mixed with correct files → queries fail hours later
   Delta result: AnalysisException at write time: "Failed to merge incompatible data types"
                 Bad data never enters the table

3. ISOLATION (concurrent reader/writer safety)
   Problem: Daily ETL appends 1M rows. Analytics query starts mid-write.
   Parquet result: Query reads partial data — some old, some new rows mixed
   Delta result: Query gets a consistent snapshot — either pre-write or post-write version

4. DURABILITY (every committed write survives)
   Problem: No way to verify what was committed vs. what's just files sitting in S3
   Delta result: Every commit is recorded in JSON log files in _delta_log/
                 Can inspect all historical operations with DeltaTable.history()
```

```python
# Concrete demo of what Delta's ACID gives you:
from delta.tables import DeltaTable

# After a bad write, check what happened
table = DeltaTable.forPath(spark, "s3://my-lake/customers/")
table.history().select("version", "timestamp", "operation", "operationMetrics").show()
# +-------+--------------------+-----------+-------------------------------+
# |version|timestamp           |operation  |operationMetrics               |
# +-------+--------------------+-----------+-------------------------------+
# |      3|2024-01-20 14:30:00 |WRITE      |{numOutputRows: 0, ...}  ← bad write
# |      2|2024-01-20 08:00:00 |WRITE      |{numOutputRows: 1500000}       |

# Roll back to pre-bad-write state (time travel + overwrite)
spark.read.format("delta").option("versionAsOf", 2) \
    .load("s3://my-lake/customers/") \
    .write.format("delta").mode("overwrite") \
    .save("s3://my-lake/customers/")
# Atomically restored — takes seconds, not a re-ingest
```

**Summary for the interviewer:** Delta Lake adds a transaction log to Parquet files. This enables atomic writes (all or nothing), schema enforcement (bad data rejected at write time), consistent reads (snapshot isolation), and time travel (any previous version accessible). For a production data lake, these aren't nice-to-haves — they're the difference between a reliable data platform and a pile of files that may or may not be correct.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Implement a CDC Upsert Pipeline Using MERGE

**Scenario:** You're receiving hourly CDC (Change Data Capture) events from a MySQL database via Debezium. The events land in S3 as Parquet files with this schema:

```
customer_id: int
name: string
email: string
updated_at: timestamp
op: string  -- 'c' (create), 'u' (update), 'd' (delete)
```

The target is a Delta table `warehouse.customers`. Write the full pipeline function that processes one batch of CDC events correctly — including deduplication within the batch.

<details>
<summary>💡 Hint</summary>

The tricky parts: (1) a single batch may contain multiple events for the same `customer_id` — you need to keep only the latest. (2) The MERGE condition needs to handle all three op types differently. (3) Deleted records should be removed from the Delta table, not kept with a flag.

</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def process_cdc_batch(spark: SparkSession, batch_path: str):
    """
    Process one hour's CDC events from Debezium into the customers Delta table.
    """
    # 1. Read the CDC batch
    raw_events = spark.read.parquet(batch_path)

    # 2. Deduplicate within the batch
    # Same customer_id may appear multiple times: c then u, or u then d
    # Keep only the LAST event per customer_id
    window = Window.partitionBy("customer_id").orderBy(col("updated_at").desc())

    latest_events = raw_events \
        .withColumn("rn", row_number().over(window)) \
        .filter(col("rn") == 1) \
        .drop("rn")

    # 3. Verify we have events to process
    if latest_events.isEmpty():
        print("Empty batch — nothing to process")
        return

    print(f"Processing {latest_events.count()} unique customer events")
    latest_events.groupBy("op").count().show()
    # op | count
    #  c |   150
    #  u |  1200
    #  d |    30

    # 4. MERGE into target Delta table
    target = DeltaTable.forName(spark, "warehouse.customers")

    target.alias("t").merge(
        latest_events.alias("s"),
        "t.customer_id = s.customer_id"
    ).whenMatchedDelete(
        condition="s.op = 'd'"
    ).whenMatchedUpdate(
        condition="s.op IN ('u', 'c')",  # 'c' can be a match if record already exists
        set={
            "name":       "s.name",
            "email":      "s.email",
            "updated_at": "s.updated_at",
        }
    ).whenNotMatchedInsert(
        condition="s.op IN ('c', 'u')",  # Insert if new, even if op='u' (late-arriving create)
        values={
            "customer_id": "s.customer_id",
            "name":        "s.name",
            "email":       "s.email",
            "updated_at":  "s.updated_at",
        }
    ).execute()

    # 5. Verify with operation metrics
    history = target.history(1)
    metrics = history.select("operationMetrics").collect()[0][0]
    print(f"Rows inserted: {metrics.get('numTargetRowsInserted', 0)}")
    print(f"Rows updated:  {metrics.get('numTargetRowsUpdated', 0)}")
    print(f"Rows deleted:  {metrics.get('numTargetRowsDeleted', 0)}")

# Run the pipeline
process_cdc_batch(spark, "s3://landing/cdc/customers/dt=2024-01-20/hr=14/")
```

**Key decisions explained:**
- **Dedup within batch:** Debezium guarantees at-least-once delivery. Within one Parquet file, you may get `c` then `u` for the same customer. The `row_number().over(window)` pattern keeps only the final state.
- **`whenNotMatchedInsert` with `op='u'`:** protects against late-arriving creates. If we only ever see `u` events but the `c` was dropped, we still want to create the record rather than silently ignoring it.
- **`whenMatchedDelete` first:** MERGE executes clauses in order. Put DELETE first so an `op='d'` row matches the delete clause, not the update clause.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Lakehouse with CDF-Based Incremental Processing

**Scenario:** Your team is building a three-layer medallion lakehouse (Bronze → Silver → Gold) for 50 event types with ~100M events/day total. The current architecture re-reads and re-processes the full Bronze table every time Silver is updated. This takes 4 hours. You need to reduce it to under 30 minutes. Design a CDF-based incremental architecture, addressing: how CDF works at each layer, how you handle schema evolution in Bronze without breaking Silver, and how Gold is kept up to date.

<details>
<summary>💡 Hint</summary>

CDF gives you row-level changes between versions. The key design questions: how do you track "last processed version" reliably? What happens if the Silver job fails mid-run — do you reprocess? How does schema evolution in Bronze propagate safely to Silver? For Gold, can you do incremental aggregation or must you always recompute?

</details>

<details>
<summary>✅ Solution</summary>

```python
from delta.tables import DeltaTable
from pyspark.sql.functions import col, current_timestamp, trim, lower, row_number
from pyspark.sql.window import Window
import json, time

# ── Architecture Overview ─────────────────────────────────────────────────
# Bronze: raw append-only, CDF enabled, schema mergeSchema allowed
# Silver: cleaned/deduped, upsert via MERGE, CDF enabled
# Gold:   aggregated, incremental where possible, else partition overwrite
#
# State tracking: Delta table `pipeline_state` stores last_bronze_version,
# last_silver_version per pipeline name. Transactionally updated with MERGE.

# ── State Management ──────────────────────────────────────────────────────

def get_pipeline_state(pipeline_name: str) -> dict:
    """Read current pipeline state from a dedicated state Delta table."""
    state = spark.sql(f"""
        SELECT last_bronze_version, last_silver_version, last_gold_version
        FROM pipeline.state
        WHERE pipeline_name = '{pipeline_name}'
    """).collect()
    if state:
        return state[0].asDict()
    return {"last_bronze_version": 0, "last_silver_version": 0, "last_gold_version": 0}

def update_pipeline_state(pipeline_name: str, **versions):
    """Atomically update pipeline state."""
    updates = ", ".join([f"{k} = {v}" for k, v in versions.items()])
    spark.sql(f"""
        MERGE INTO pipeline.state AS t
        USING (SELECT '{pipeline_name}' AS pipeline_name) AS s
        ON t.pipeline_name = s.pipeline_name
        WHEN MATCHED THEN UPDATE SET {updates}
        WHEN NOT MATCHED THEN INSERT *
    """)

# ── Bronze Ingestion ───────────────────────────────────────────────────────

def ingest_bronze(event_type: str, raw_df):
    """
    Ingest raw events to Bronze.
    Bronze is append-only with mergeSchema to handle upstream changes.
    CDF is enabled to power incremental Silver promotion.
    """
    raw_df \
        .withColumn("_ingested_at", current_timestamp()) \
        .write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(f"s3://lakehouse/bronze/{event_type}/")

# ── Bronze → Silver Incremental ──────────────────────────────────────────

def promote_silver_incremental(event_type: str, pk_col: str):
    """
    Read only changed records from Bronze using CDF.
    Apply transformations and upsert to Silver.
    """
    state = get_pipeline_state(f"silver_{event_type}")
    start_version = state["last_bronze_version"] + 1

    # Get current Bronze version
    bronze_table = DeltaTable.forPath(spark, f"s3://lakehouse/bronze/{event_type}/")
    current_bronze_version = bronze_table.history(1).select("version").collect()[0][0]

    if start_version > current_bronze_version:
        print(f"[{event_type}] Silver up to date at Bronze v{current_bronze_version}")
        return

    print(f"[{event_type}] Processing Bronze v{start_version} → v{current_bronze_version}")

    # Read CDF changes
    bronze_changes = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", start_version) \
        .option("endingVersion", current_bronze_version) \
        .load(f"s3://lakehouse/bronze/{event_type}/") \
        .filter(col("_change_type").isin(["insert", "update_postimage"]))

    if bronze_changes.isEmpty():
        update_pipeline_state(f"silver_{event_type}",
                              last_bronze_version=current_bronze_version)
        return

    # Apply Silver transformations
    silver_ready = (
        bronze_changes
        .drop("_change_type", "_commit_version", "_commit_timestamp", "_ingested_at")
        # Clean and validate
        .withColumn("email", lower(trim(col("email"))))
        .filter(col(pk_col).isNotNull())
        .filter(col("email").contains("@") if "email" in bronze_changes.columns else lit(True))
        # Dedup within the CDF batch (CDF can return multiple changes per key)
        .withColumn("rn", row_number().over(
            Window.partitionBy(pk_col).orderBy(col("_commit_version").desc())
        ))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    # Schema safety: only allow new columns that are nullable
    # (non-nullable columns in Silver would break CDF inserts from Bronze)
    silver_table = DeltaTable.forPath(spark, f"s3://lakehouse/silver/{event_type}/")
    silver_table.alias("t") \
        .merge(silver_ready.alias("s"), f"t.{pk_col} = s.{pk_col}") \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

    # Update state — ONLY after successful MERGE
    update_pipeline_state(f"silver_{event_type}",
                          last_bronze_version=current_bronze_version)
    print(f"[{event_type}] Silver updated. Processed {silver_ready.count()} records.")

# ── Silver → Gold Incremental ─────────────────────────────────────────────

def build_gold_incremental(metric_name: str, silver_table: str,
                           partition_col: str, agg_query: str):
    """
    For Gold metrics that can be computed per-partition,
    only recompute partitions that have changed in Silver since last Gold run.
    """
    state = get_pipeline_state(f"gold_{metric_name}")
    start_version = state["last_silver_version"] + 1

    silver = DeltaTable.forName(spark, silver_table)
    current_silver_version = silver.history(1).select("version").collect()[0][0]

    if start_version > current_silver_version:
        return

    # Find which partitions changed in Silver since last Gold run
    silver_changes = spark.read.format("delta") \
        .option("readChangeFeed", "true") \
        .option("startingVersion", start_version) \
        .option("endingVersion", current_silver_version) \
        .table(silver_table)

    changed_partitions = silver_changes \
        .select(partition_col).distinct() \
        .collect()
    changed_values = [r[0] for r in changed_partitions]

    if not changed_values:
        update_pipeline_state(f"gold_{metric_name}",
                              last_silver_version=current_silver_version)
        return

    print(f"[{metric_name}] Recomputing Gold for {len(changed_values)} partitions: {changed_values[:5]}")

    # Recompute Gold only for affected partitions
    for partition_value in changed_values:
        partition_result = spark.sql(
            agg_query + f" WHERE {partition_col} = '{partition_value}'"
        )
        partition_result.write \
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", f"{partition_col} = '{partition_value}'") \
            .save(f"s3://lakehouse/gold/{metric_name}/")

    update_pipeline_state(f"gold_{metric_name}",
                          last_silver_version=current_silver_version)

# ── Performance Results ────────────────────────────────────────────────────
# Before: re-process full Bronze (100M rows/day × 50 event types) = 4 hours
# After:  CDF reads only changed rows (typically 1-5% of Bronze per hour)
#         + Silver MERGE on ~1M rows + Gold partition recompute on 1-2 dates
#         = 15-25 minutes total ✓
```

**Architecture decisions:**
- **State in Delta table** (not a file): atomic, concurrent-safe, auditable.
- **Dedup within CDF batch**: CDF can return `update_preimage` + `update_postimage` for the same key — filter to `postimage` only and dedup by commit version.
- **Schema evolution safety**: Bronze uses `mergeSchema` (any new column allowed); Silver uses `ALTER TABLE ADD COLUMN` for controlled, nullable additions only.
- **Gold partial recompute**: use CDF to identify which partition values changed in Silver, then `replaceWhere` only those partitions in Gold — avoids full Gold recompute.
- **Idempotency**: if Silver job fails after the MERGE but before `update_pipeline_state`, the next run re-reads the same versions. MERGE is idempotent (upsert on primary key).

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What's the difference between Delta MERGE and just overwriting the table?" — Overwrite rewrites all files; MERGE only rewrites files containing matched rows. For a 500 GB table where only 1% of rows change daily, MERGE rewrites ~5 GB; overwrite rewrites 500 GB. MERGE is also atomic and records the exact number of inserts/updates/deletes in the operation history.

> **Tip 2:** "How does time travel work under the hood?" — Delta never deletes files on write. When you update a record, the old Parquet file is marked "removed" in the transaction log, and a new file with the updated record is marked "added." The old file physically exists until VACUUM cleans it. Time travel simply reads the version of the log where the old file was still "active."

> **Tip 3:** "When would you NOT use Delta Lake?" — When your use case is pure append-only with no updates, deletes, or schema evolution (e.g., raw log archival to cold storage), plain Parquet may be simpler and cheaper — no transaction log overhead, compatible with every tool. Also consider: Delta requires Spark or compatible readers; if your consumers are plain S3 + Athena with no Spark, the `_delta_log` metadata overhead may not be worth it. (Though Delta-RS and Delta Kernel are addressing this.)
