---
title: "ORC & Parquet Formats - Scenario Questions"
topic: hadoop
subtopic: orc-parquet-formats
content_type: scenario_question
tags: [hadoop, orc, parquet, columnar, storage]
---

# ORC & Parquet Formats — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Choosing the Right File Format

Your team is building a new data lake on HDFS. You have three datasets:

1. **Clickstream events**: 50 million events/day appended via Kafka → S3 Sink. Queries are run in Spark SQL by analysts for aggregations and funnel analysis.
2. **Customer dimension**: 5 million rows, updated daily with INSERT/UPDATE/DELETE via an ETL from a source OLTP system. Queried exclusively by Hive.
3. **Raw sensor data**: Binary time-series readings from IoT devices. Schema changes frequently as new sensor types are added. Data is also consumed by Kafka consumers and REST APIs.

For each dataset, recommend a file format and justify your choice.

<details>
<summary>✅ Solution</summary>

**Dataset 1: Clickstream events → Parquet + Snappy (or ZSTD)**

- Spark is the primary engine, and Spark's vectorized Parquet reader is heavily optimized.
- Append-only workload — no need for ORC ACID.
- Cross-tool compatibility: Parquet works with Athena, Presto/Trino, and Spark equally well.
- Analytical aggregations benefit from columnar layout (scan only needed columns).
- ZSTD if the cluster has spare CPU; Snappy if CPU is tight.

```python
df.write \
  .option("compression", "snappy") \
  .partitionBy("event_date", "event_type") \
  .parquet("hdfs:///data/clickstream/")
```

**Dataset 2: Customer dimension → ORC (with ACID)**

- Hive is the exclusive engine — ORC is the native Hive format with the best performance inside LLAP.
- Requires row-level UPDATE/DELETE: ORC is the only Hadoop-native format supporting Hive ACID transactions.
- Statistics and bloom filters integrate natively with the Hive metastore and CBO.

```sql
CREATE TABLE customer_dim (
  customer_id BIGINT,
  name        STRING,
  email       STRING,
  status      STRING
)
CLUSTERED BY (customer_id) INTO 16 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true');
```

**Dataset 3: Raw sensor data → Avro**

- Schema changes frequently: Avro stores the full schema in every file and supports robust schema evolution (add fields with defaults, remove optional fields).
- Consumed by Kafka ecosystem: Avro + Confluent Schema Registry is the standard for Kafka event serialization.
- REST API consumption: Avro's row-oriented layout makes it easy to serialize individual records.
- Note: for analytical queries on this data, consider converting to Parquet in a downstream processing step.

```python
# Kafka → Avro → HDFS (using Confluent Schema Registry)
df.write.format("avro").save("hdfs:///data/sensor_raw/")
```

</details>
</article>

---

<article data-difficulty="mid">

## Scenario 2: Predicate Pushdown Not Working

A data engineer notices that a Hive query on an ORC table is reading 500 GB of data even though the result set is only 200 MB. The query:

```sql
SELECT customer_id, SUM(amount) AS total
FROM   transactions
WHERE  UPPER(status) = 'COMPLETED'
  AND  transaction_date >= '2024-01-01'
GROUP BY customer_id;
```

The `transactions` table is:
- ORC format, Snappy compressed
- Partitioned by `transaction_date` (daily partitions)
- Has bloom filters on `status` and `customer_id`
- Has ORC stripe statistics (min/max) on all columns

`EXPLAIN` shows the TableScan is reading all 500 GB (all partitions, all stripes).

Identify all the reasons predicate pushdown is failing and fix the query.

<details>
<summary>✅ Solution</summary>

**Problem 1: `UPPER(status)` defeats bloom filter and statistics pushdown**

`UPPER(status)` is a UDF transformation applied to the stored value. The ORC reader stores raw values (`Completed`, `completed`, `COMPLETED`) in statistics and bloom filters. When Hive evaluates `UPPER(status) = 'COMPLETED'`, it cannot push this down because the filter is applied *after* the UDF — the reader doesn't know what `UPPER` will produce.

**Problem 2: No partition filter → all partitions scanned**

`transaction_date >= '2024-01-01'` is a range predicate on the partition key. However, EXPLAIN shows all partitions are scanned. This happens because the partition key type may be STRING and the comparison is between STRING values using lexicographic ordering — which works correctly here — but if the column was not recognized as a partition filter during planning, it falls through to a post-scan filter.

**Diagnosis:**

```sql
-- Check partition filter is being applied
EXPLAIN
SELECT customer_id, SUM(amount)
FROM   transactions
WHERE  UPPER(status) = 'COMPLETED'
  AND  transaction_date >= '2024-01-01'
GROUP BY customer_id;

-- Look for: "Partition Filters: (transaction_date >= '2024-01-01')"
-- vs.      "Filter Operator" after TableScan (means no pruning)
```

**Fix 1: Remove UDF from the filter side**

If data is stored with mixed case, normalize at write time, not query time:

```sql
-- Normalize status on write (ETL fix)
INSERT INTO transactions PARTITION(transaction_date)
SELECT customer_id, amount, UPPER(status) AS status, transaction_date
FROM   raw_transactions;

-- Now the query can push down the predicate
SELECT customer_id, SUM(amount) AS total
FROM   transactions
WHERE  status = 'COMPLETED'                 -- no UDF → bloom filter + stats work
  AND  transaction_date >= '2024-01-01'
GROUP BY customer_id;
```

**Fix 2: If normalization isn't possible, rewrite the filter**

```sql
-- Use IN to enumerate all known variants (bloom filter works for equality)
SELECT customer_id, SUM(amount) AS total
FROM   transactions
WHERE  status IN ('COMPLETED', 'Completed', 'completed')
  AND  transaction_date >= '2024-01-01'
GROUP BY customer_id;
```

**Fix 3: Verify partition pruning is enabled**

```sql
SET hive.optimize.ppd=true;
SET hive.optimize.index.filter=true;
SET hive.spark.dynamic.partition.pruning=true;  -- if using Spark engine
```

**Fix 4: Verify bloom filters are actually present**

```bash
# Use ORC tools to inspect a file
hive --orcfiledump hdfs:///warehouse/transactions/transaction_date=2024-01-15/000000_0 | grep -i bloom
# Should show: Bloom filters for column status
```

**Expected outcome after fix:**
- Partition pruning eliminates partitions before `2024-01-01` (~historical 95% of data).
- ORC bloom filters on `status = 'COMPLETED'` skip stripes without that value.
- Data read drops from 500 GB to ~5–10 GB (the 2024 partitions containing COMPLETED rows).

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3: Format Migration from CSV to ORC at Petabyte Scale

Your company has 5 PB of historical data stored as gzipped CSV files on HDFS, partitioned by `event_date` (daily, 5 years of data). The data is queried by both Hive (ETL) and Spark (analytics). Current pain points:

- Queries take 10–30 minutes; SLA requires < 3 minutes.
- GZIP is not splittable — each file is processed by a single mapper.
- No predicate pushdown (CSV has no statistics).
- Schema changes require full rewrites.

Design a complete migration strategy to ORC (for Hive) and Parquet (for Spark), including: execution plan, risk mitigation, rollback strategy, and performance validation.

<details>
<summary>✅ Solution</summary>

**Phase 0: Assessment (Week 1)**

```bash
# Inventory current state
hdfs dfs -count /data/events/
# Total files, total size, average file size

# Sample query baseline
hive -e "SELECT COUNT(*), SUM(amount) FROM events_csv WHERE event_date='2024-01-15';"
# Record: runtime, bytes read (from counters), CPU seconds

# Identify hottest partitions (queried most)
# → prioritize migrating recent 90 days first
```

**Phase 1: Target Schema Design**

```sql
-- ORC table (Hive-primary workloads)
CREATE TABLE events_orc (
  event_id    BIGINT,
  user_id     BIGINT,
  event_type  STRING,
  amount      DOUBLE,
  metadata    MAP<STRING, STRING>
)
PARTITIONED BY (event_date STRING)
CLUSTERED BY (user_id) INTO 32 BUCKETS
STORED AS ORC
TBLPROPERTIES (
  'orc.compress'             = 'SNAPPY',
  'orc.stripe.size'          = '67108864',
  'orc.row.index.stride'     = '10000',
  'orc.bloom.filter.columns' = 'user_id,event_type',
  'orc.bloom.filter.fpp'     = '0.05',
  'transactional'            = 'false'  -- enable later if ACID needed
);

-- Parquet table (Spark/Trino workloads) — same schema, different format
CREATE TABLE events_parquet
USING PARQUET
PARTITIONED BY (event_date)
OPTIONS ('compression' 'snappy')
AS SELECT * FROM events_orc WHERE 1=0;  -- empty skeleton
```

**Phase 2: Incremental Migration (Weeks 2–6)**

Strategy: migrate 30 days at a time, oldest first, in parallel Spark jobs:

```python
# Spark migration job (run one job per 30-day window)
from pyspark.sql import SparkSession
from datetime import date, timedelta

spark = SparkSession.builder \
    .config("spark.sql.parquet.compression.codec", "snappy") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256m") \
    .getOrCreate()

def migrate_window(start_date, end_date):
    df = spark.read \
        .option("inferSchema", "false") \
        .schema(source_schema) \
        .csv(f"hdfs:///data/events_csv/event_date={start_date}/")

    # Cast and clean
    df_clean = df \
        .withColumn("event_id", df["event_id"].cast("bigint")) \
        .withColumn("user_id",  df["user_id"].cast("bigint")) \
        .withColumn("amount",   df["amount"].cast("double"))

    # Write ORC
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .orc("hdfs:///data/events_orc/")

    # Write Parquet (dual-write to same data)
    df_clean.write \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .parquet("hdfs:///data/events_parquet/")

# Run for each 30-day window
```

**Phase 3: Dual-Read Validation**

Before cutting over traffic, validate row counts and aggregate checksums:

```sql
-- Row count validation per partition
SELECT 'csv'     AS src, COUNT(*) AS cnt FROM events_csv     WHERE event_date='2024-01-15'
UNION ALL
SELECT 'orc'     AS src, COUNT(*) AS cnt FROM events_orc     WHERE event_date='2024-01-15'
UNION ALL
SELECT 'parquet' AS src, COUNT(*) AS cnt FROM events_parquet WHERE event_date='2024-01-15';

-- Aggregate checksum
SELECT 'csv'     AS src, SUM(amount), AVG(amount), COUNT(DISTINCT user_id) FROM events_csv     WHERE event_date='2024-01-15'
UNION ALL
SELECT 'orc'     AS src, SUM(amount), AVG(amount), COUNT(DISTINCT user_id) FROM events_orc     WHERE event_date='2024-01-15';
```

**Phase 4: Cutover**

```sql
-- Swap the default table via view (zero-downtime cutover)
CREATE OR REPLACE VIEW events AS SELECT * FROM events_orc;
-- Spark users: swap their reads to events_parquet

-- Keep CSV table for 30 days post-cutover as rollback option
-- Then archive to cold storage (S3 Glacier equivalent)
hdfs dfs -mv /data/events_csv /data/archive/events_csv_backup
```

**Rollback Strategy:**

```sql
-- Instant rollback: redirect view back to CSV
CREATE OR REPLACE VIEW events AS SELECT * FROM events_csv;
```
CSV files are never deleted until validation is complete and the migration is stable for 30 days.

**Expected Performance Gains:**

| Metric | CSV + GZIP | ORC + Snappy | Improvement |
|---|---|---|---|
| Storage | 5 PB | ~1.5 PB | 67% reduction |
| Query time (single partition) | 10–30 min | 30–90 sec | 10–20× |
| Splittability | No (1 mapper/file) | Yes (1 mapper/stripe) | Full parallelism |
| Predicate pushdown | None | Stripe + row index + bloom | Skips 90%+ of I/O |

**Risk Mitigation:**

- Run migration during off-peak hours.
- Validate each 30-day window before proceeding to the next.
- Monitor HDFS NameNode heap during migration (file count increases before CSV deletion).
- Keep dual tables for 30 days post-cutover before deleting CSV.
- Circuit breaker: if validation fails for any partition, reprocess that window before continuing.

</details>
</article>
