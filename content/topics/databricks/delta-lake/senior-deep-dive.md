---
title: "Delta Lake - Senior Deep Dive"
topic: databricks
subtopic: delta-lake
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [databricks, delta-lake, internals, transaction-log, concurrency, performance, optimization]
---

# Delta Lake — Senior-Level Deep Dive

## Transaction Log Internals

The `_delta_log/` directory is the heart of Delta Lake. Understanding it deeply explains all Delta behavior.

### Log Structure

```
s3://lake/tables/fact_orders/
├── _delta_log/
│   ├── 00000000000000000000.json   # Version 0: initial write
│   ├── 00000000000000000001.json   # Version 1: append
│   ├── 00000000000000000002.json   # Version 2: update
│   ├── 00000000000000000010.checkpoint.parquet  # Checkpoint (every 10 versions)
│   └── _last_checkpoint                         # Points to latest checkpoint
├── part-00000-*.parquet            # Data files (current + historical)
├── part-00001-*.parquet
└── ...
```

### What's Inside a Log Entry

```json
// 00000000000000000002.json (an UPDATE operation)
{
  "commitInfo": {
    "timestamp": 1705334400000,
    "operation": "UPDATE",
    "operationMetrics": {"numUpdatedRows": "150", "numCopiedRows": "850"}
  },
  "remove": {
    "path": "part-00003-old.parquet",
    "deletionTimestamp": 1705334400000,
    "dataChange": true
  },
  "add": {
    "path": "part-00003-new.parquet",
    "size": 52428800,
    "modificationTime": 1705334400000,
    "dataChange": true,
    "stats": "{\"numRecords\":1000,\"minValues\":{\"amount\":1.5},\"maxValues\":{\"amount\":9999.99}}"
  }
}
```

**What this reveals:**
- An UPDATE doesn't modify files in-place. It REMOVES the old file and ADDS a new file with updated rows.
- Statistics (min/max per column) are stored in the log for data skipping.
- Each commit is a single atomic JSON file write (S3 PUT is atomic for single objects).

### Checkpoints (Performance Optimization)

Reading 1000 JSON log files on every query is slow. Checkpoints consolidate log state:

```
After every 10 commits (configurable):
  - Replay all log entries since last checkpoint
  - Write a single Parquet file with the current table state (active files list)
  - Future readers start from this checkpoint, not from version 0
```

> **Read path:** Find latest checkpoint → read checkpoint Parquet → apply only log entries AFTER the checkpoint → now you know which data files are current.

---

## Concurrency Control

Delta uses **optimistic concurrency control (OCC)** — multiple writers can proceed in parallel, and conflicts are detected at commit time.

### How Commits Work

```
Writer A:                         Writer B:
1. Read current version (v5)      1. Read current version (v5)
2. Write new Parquet files        2. Write new Parquet files
3. Try to commit as v6            3. Try to commit as v6
   → Success! (first to commit)      → Conflict! v6 already exists
                                  4. Re-read v6, check for conflicts
                                  5. If no logical conflict → commit as v7
                                  6. If logical conflict → retry or fail
```

### Conflict Detection Rules

| Writer A does | Writer B does | Conflict? |
|--------------|--------------|-----------|
| Append to partition 1 | Append to partition 2 | No — different partitions |
| Append to partition 1 | Append to partition 1 | No — both only add files |
| UPDATE rows in file X | Append new file Y | No — different files |
| UPDATE rows in file X | UPDATE rows in file X | YES — same file modified |
| DELETE rows in file X | UPDATE rows in file X | YES — same file |
| OPTIMIZE (rewrite files) | Append new file | No — append wins |

> **Key insight:** Conflicts only occur when two writers touch the SAME physical files. Since Delta is copy-on-write (UPDATE = remove old file + add new file), conflicts happen when two writers try to remove the same file.

---

## MERGE Performance Optimization

MERGE is the most complex (and most important) operation in Delta ETL. Understanding its execution is critical.

### How MERGE Executes

```
1. Read the SOURCE data (incoming records)
2. Inner join SOURCE with TARGET on match condition
3. Determine which target files contain matching rows
4. For each affected file:
   a. Read the file
   b. Apply updates to matched rows
   c. Write a new file with changes
5. Append any unmatched source rows as new files
6. Commit: remove old files, add new files (atomically)
```

### MERGE Optimization Techniques

```python
# 1. PARTITION PRUNING: add partition filter to reduce target scan
dt.alias("t").merge(
    source.alias("s"),
    "t.order_id = s.order_id AND t.order_date = s.order_date"  # Include partition column!
    # Delta only reads partitions that could match, not the entire table
)

# 2. Z-ORDER on merge key: colocates matching rows
# OPTIMIZE table ZORDER BY (order_id) → fewer files to rewrite per merge

# 3. Reduce source data volume
# Filter source to only truly new/changed records before MERGE
new_records = source.filter("updated_at > last_watermark")
dt.merge(new_records, ...)  # Much less data to join
```

### MERGE vs DELETE+INSERT

| Approach | When to Use | Trade-off |
|----------|-------------|-----------|
| MERGE | Few changes relative to table size | Rewrites only affected files |
| DELETE + INSERT | Replacing entire partitions | Simpler, faster for full partition refreshes |
| INSERT OVERWRITE | Full partition replacement | Fastest for partition-level idempotent loads |

```python
# For partition-level overwrites, this is faster than MERGE:
df.write.format("delta") \
    .mode("overwrite") \
    .option("replaceWhere", "order_date = '2024-01-15'") \
    .save("s3://lake/tables/fact_orders")
# Atomically replaces all files in that partition
```

---

## Data Skipping and Statistics

Delta maintains per-file statistics in the transaction log for data skipping (similar to Snowflake's partition pruning):

```
File: part-00042.parquet (1000 rows)
Stats: {
  "numRecords": 1000,
  "minValues": {"order_date": "2024-01-15", "amount": 5.99, "customer_id": "C0001"},
  "maxValues": {"order_date": "2024-01-15", "amount": 4500.00, "customer_id": "C0500"},
  "nullCount": {"amount": 0, "customer_id": 2}
}
```

**Query: `WHERE customer_id = 'C9999'`**
- Delta checks: file's max customer_id is "C0500" → C9999 > C0500 → **SKIP this file**
- Only files whose min/max range includes "C9999" are actually read

**Optimizing data skipping:**
- Z-ORDER on filter columns → narrows min/max ranges within files
- Statistics collected on first 32 columns by default
- Set `delta.dataSkippingNumIndexedCols` to include more columns if needed

---

## Streaming with Delta

Delta tables work as both streaming sources and sinks:

### Streaming Write (Sink)

```python
# Stream from Kafka → Delta table
spark.readStream.format("kafka") \
    .option("subscribe", "orders") \
    .load() \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://lake/checkpoints/orders") \
    .trigger(processingTime="30 seconds") \
    .start("s3://lake/tables/fact_orders")
```

### Streaming Read (Source)

```python
# Stream changes from a Delta table to another table
spark.readStream.format("delta") \
    .load("s3://lake/tables/fact_orders") \
    .writeStream \
    .format("delta") \
    .option("checkpointLocation", "s3://lake/checkpoints/downstream") \
    .start("s3://lake/tables/fact_orders_enriched")
```

> **How streaming from Delta works:** It reads the transaction log incrementally — processing only NEW files added since the last checkpoint. It's essentially CDC from the transaction log.

---

## VACUUM Strategy

VACUUM removes old files no longer referenced by any version within the retention period.

```sql
-- See what would be deleted (dry run)
VACUUM delta.`s3://lake/tables/fact_orders` DRY RUN;

-- Actually delete old files (default: 7-day retention)
VACUUM delta.`s3://lake/tables/fact_orders` RETAIN 168 HOURS;
```

**VACUUM scheduling strategy:**

| Table Type | VACUUM Frequency | Retention |
|-----------|-----------------|-----------|
| High-churn (many updates/merges) | Daily | 7 days |
| Append-only (facts) | Weekly | 30 days |
| Slowly changing (dimensions) | Monthly | 90 days |
| Compliance/audit tables | Never (or very long) | 365+ days |

> **WARNING:** After VACUUM, time travel to versions older than the retention period PERMANENTLY fails. Set retention based on your recovery needs.

---

## Performance Tuning Checklist

| Issue | Diagnostic | Solution |
|-------|-----------|----------|
| Slow reads | Many small files (check file count) | OPTIMIZE + Z-ORDER |
| Slow MERGE | Large target scan (check operationMetrics) | Include partition column in match condition |
| Slow writes | Auto-optimize disabled | Enable autoOptimize.optimizeWrite |
| High storage cost | Old files accumulating | Schedule VACUUM |
| Frequent conflicts | Multiple concurrent writers | Partition writes by time window |
| Poor data skipping | Wide min/max ranges | Z-ORDER on filter columns |

---

## Interview Tips

> **Tip 1:** "How does Delta Lake provide ACID on object storage?" — "Through the transaction log (_delta_log). Each commit writes a new JSON file atomically (S3 PUT is atomic for single objects). The log defines which Parquet files are 'current.' Reads replay the log to determine the valid file set. Concurrent writes use optimistic concurrency — conflict detected at commit time."

> **Tip 2:** "When would you use MERGE vs INSERT OVERWRITE?" — "MERGE for surgical updates (few rows changed, many unchanged). INSERT OVERWRITE with replaceWhere for full partition replacement (idempotent, faster when replacing entire partitions). Rule of thumb: if changing >30% of a partition's rows, partition overwrite is faster."

> **Tip 3:** "How does time travel work under the hood?" — "Delta keeps old Parquet files even after UPDATE/DELETE (they're just removed from the active file list in the log). Time travel reads an old version of the log to find which files were active at that point. VACUUM eventually cleans up old files — after that, those versions are gone forever."

## ⚡ Cheat Sheet

**ACID guarantees**
- Atomicity: write produces single `_delta_log` JSON entry; readers never see partial write
- Isolation: optimistic concurrency; conflict = retry or fail (no write-write conflicts on disjoint partitions)
- `SERIALIZABLE` default for DML; `WRITE_SERIALIZABLE` default for streaming (weaker but more concurrent)

**Key operations**
```sql
OPTIMIZE delta.`/path` ZORDER BY (col1, col2)  -- compaction + data skipping
VACUUM delta.`/path` RETAIN 168 HOURS           -- default 7-day retention
RESTORE TABLE t TO VERSION AS OF 10             -- time travel rollback
ALTER TABLE t SET TBLPROPERTIES ('delta.targetFileSize'='128mb')
```

**Time travel limits**
- Default retention: 7 days (168 hours); set `delta.logRetentionDuration`
- `VACUUM` enforces retention; run weekly or after large deletes
- `AS OF VERSION` / `AS OF TIMESTAMP` for point-in-time reads

**Write patterns**
- `replaceWhere`: partition-scoped overwrite; much faster than full overwrite
- `mergeSchema`: add new columns without rewrite; `overwriteSchema`: full schema replace
- `MERGE INTO`: upsert; CDC pattern; must include `ON` key + `WHEN MATCHED/NOT MATCHED`

**Performance tuning**
- Target file size: 128 MB–1 GB (default 128 MB); tune with `delta.targetFileSize`
- ZORDER: reorders data within files; best for 1–3 high-cardinality filter columns
- Data skipping: min/max stats stored in `_delta_log` JSON; skips files not matching predicates
- Bloom filters: `delta.bloomFilter.enabled=true` on high-cardinality equality columns

**Streaming**
- `readStream` on Delta: tracks `_delta_log` for new commits; no Kafka needed
- `trigger(availableNow=True)` → process batch of changes and stop
- `ignoreChanges` / `ignoreDeletes`: handle updates/deletes in streaming source
