---
title: "Apache Iceberg — Scenarios"
topic: data-lakehouse
subtopic: apache-iceberg
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [iceberg, scenarios, interview, design, maintenance]
---

# Apache Iceberg — Interview Scenarios

## Scenario 1: Design an Iceberg-Based CDC Pipeline

**Question:** You need to replicate changes from a PostgreSQL orders table (INSERT/UPDATE/DELETE) to an Iceberg table for analytics. The orders table has 500M rows and receives 100K changes per minute. Design the CDC pipeline.

**Answer:**

```
Architecture:
  PostgreSQL (WAL) → Debezium → Kafka → Flink → Iceberg (V2, MOR)

Component Design:

1. Debezium (Source Connector):
   connector.class = PostgresConnector
   database.hostname = postgres-prod
   table.include.list = public.orders
   plugin.name = pgoutput
   slot.name = orders_iceberg_slot
   publication.name = orders_iceberg_pub
   Output: Kafka topic "orders-cdc" with Avro messages

2. Kafka Topic:
   Partitions: 20 (100K events/min = ~1,700 events/sec → 85 events/sec per partition)
   Retention: 7 days
   Replication: 3

3. Flink Processing:
   Source: Kafka (orders-cdc topic)
   Logic:
     - Deserialize Avro with Schema Registry
     - Route by op field: 'c' (create), 'u' (update), 'd' (delete), 'r' (snapshot)
     - Emit UPSERT for c/u/r; DELETE for d
   Sink: Iceberg with write.upsert.enabled=true
   Checkpoint interval: 60 seconds (snapshot commit every minute)
   Parallelism: 20 (matches Kafka partitions)

4. Iceberg Table (V2, MOR):
   CREATE TABLE db.orders (
     order_id     BIGINT,
     customer_id  BIGINT,
     amount       DECIMAL(18,2),
     status       STRING,
     updated_at   TIMESTAMP,
     _deleted     BOOLEAN  -- soft delete flag (alternate approach)
   )
   USING iceberg
   PARTITIONED BY (months(updated_at))
   TBLPROPERTIES (
     'format-version' = '2',
     'write.upsert.enabled' = 'true',
     'write.merge.mode' = 'merge-on-read'
   )

5. Compaction Schedule (daily Spark job):
   CALL system.rewrite_data_files(table => 'db.orders',
     strategy => 'sort',
     sort_order => 'order_id ASC')
   -- Converts MOR delete files → COW clean files
   -- Run at low-traffic window (e.g., 3 AM)

Latency: < 2 minutes end-to-end (Debezium → Kafka → Flink checkpoint → Iceberg snapshot)
```

---

## Scenario 2: Debug Iceberg Table Performance Degradation

**Question:** An Iceberg table that ran queries in 5 seconds is now taking 90 seconds. No schema changes were made. The table ingests 1M rows per hour via Flink streaming. What do you investigate?

**Answer:**

```
Step 1: Check file count and average size
  spark.sql("DESCRIBE DETAIL iceberg.db.events").show()
  -- numFiles: 50,000  ← was 2,000 last month
  -- sizeInBytes: 100GB
  -- Average file: 100GB / 50,000 = 2MB  ← far below 128MB target
  Diagnosis: small files problem (Flink writing many small files)

Step 2: Check manifest file count
  SELECT count(*) FROM iceberg.db."events$manifests";
  -- 5,000 manifests  ← high (causes slow metadata reads)
  Diagnosis: metadata bloat (too many manifest files)

Step 3: Check delete file ratio (V2 tables)
  SELECT * FROM iceberg.db."events$files" LIMIT 10;
  -- content = 1 (DELETE file): 80% of file entries are delete files
  Diagnosis: MOR deletes accumulating without compaction

Fix 1: Compact data files (urgent)
  CALL local.system.rewrite_data_files(
    table => 'db.events',
    strategy => 'binpack',
    options => map('target-file-size-bytes', '134217728',
                   'delete-file-threshold', '2')  -- compact if >= 2 delete files
  )
  Expected: 50,000 files → ~800 files, query back to 5 seconds

Fix 2: Compact manifests
  CALL local.system.rewrite_manifests(table => 'db.events')
  
Fix 3: Add compaction to Flink pipeline (prevent recurrence)
  -- Configure Flink sink with compaction enabled:
  'write.distribution-mode' = 'hash'   -- co-locate same keys
  'sink.parallelism' = '10'            -- fewer writers = larger files

Fix 4: Schedule nightly compaction job
  -- Add to Airflow/Databricks Workflows as daily task
```

---

## Scenario 3: Iceberg Schema Evolution Without Downtime

**Question:** You have a production Iceberg table `db.orders` with 100+ downstream Spark and Trino consumers. You need to: (1) add a `coupon_code STRING` column, (2) rename `amt` to `amount`, (3) drop the deprecated `legacy_flag BOOLEAN` column. How do you do this with zero downtime?

**Answer:**

```
Key insight: Iceberg uses column IDs (not names) internally.
Schema changes update metadata only — no data files are rewritten.
Iceberg readers handle old files transparently (missing column = null).

Step 1: Add new column (safest — zero impact)
  ALTER TABLE db.orders ADD COLUMN coupon_code STRING;
  -- Immediately visible to all consumers
  -- Old Parquet files return null for coupon_code
  -- Zero data rewrite
  -- Zero downtime

Step 2: Rename column (safe in Iceberg — unsafe in Hive)
  ALTER TABLE db.orders RENAME COLUMN amt TO amount;
  -- Iceberg tracks column by ID (not name)
  -- Old files still read correctly (file says col_id=5, metadata says col_id=5 = amount)
  -- Consumers using SELECT amt → break (SQL uses name, not ID)
  
  Safest migration path for rename:
  a) Add new column: ALTER TABLE db.orders ADD COLUMN amount DECIMAL(18,2);
  b) Populate via backfill: UPDATE db.orders SET amount = amt;  (NOT recommended for 500M rows)
     Better: in Spark, new SELECT uses COALESCE(amount, amt) alias
  c) Notify consumers: give 30-day deprecation window to update queries
  d) Drop old column after all consumers migrated

Step 3: Drop deprecated column (after consumers migrated)
  ALTER TABLE db.orders DROP COLUMN legacy_flag;
  -- Iceberg marks column as dropped in metadata
  -- Existing Parquet files still contain legacy_flag bytes (not reclaimed until compaction)
  -- Consumers selecting legacy_flag → AnalysisException (column not found)
  -- After compaction: physical bytes reclaimed

Timeline:
  Day 0: Add coupon_code (immediate)
  Day 0: Rename amt → amount (mark amt as deprecated, add amount as alias view)
  Day 0–30: Notify all consumers, monitor usage of amt
  Day 30: Drop amt (after all consumers migrated)
  Day 30: Drop legacy_flag
  Day 31: Run rewrite_data_files to reclaim physical space
```
