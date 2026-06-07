---
title: "Hive - Intermediate"
topic: hadoop
subtopic: hive
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [hadoop, hive, optimization, vectorization, cbo, acid, serde]
---

# Hive Intermediate Concepts

## Query Optimization

### Cost-Based Optimizer (CBO)
Hive CBO uses table/column statistics to choose the optimal join order and strategy:

```sql
-- Enable CBO
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;

-- Collect statistics (required for CBO)
ANALYZE TABLE orders COMPUTE STATISTICS;
ANALYZE TABLE orders COMPUTE STATISTICS FOR COLUMNS order_id, customer_id, amount;
ANALYZE TABLE orders PARTITION(order_date) COMPUTE STATISTICS;

-- View statistics
DESCRIBE EXTENDED orders;
-- Look for: "numRows=1000000, rawDataSize=5000000000"

-- CBO then chooses join order:
-- Small table × large table (not large × small) for better broadcast join decisions
SELECT o.*, c.name
FROM orders o  -- 1B rows
JOIN customers c  -- 100K rows
ON o.customer_id = c.id;
-- CBO detects customers is small → recommends map join (broadcast)
```

### Map Join (Broadcast Join)
For small tables, broadcast the entire table to every mapper (eliminates shuffle):

```sql
-- Automatic map join (enabled when small table < threshold)
SET hive.auto.convert.join=true;
SET hive.mapjoin.smalltable.filesize=25000000;  -- 25 MB threshold

-- Force map join with hint
SELECT /*+ MAPJOIN(c) */ o.*, c.name
FROM orders o
JOIN customers c ON o.customer_id = c.id;

-- For sorted and bucketed tables: sort-merge join (very efficient)
SET hive.auto.convert.sortmerge.join=true;
SET hive.optimize.bucketmapjoin=true;
SET hive.optimize.bucketmapjoin.sortedmerge=true;
```

### Vectorized Query Execution
Processes 1024 rows at a time using CPU SIMD instructions instead of row-by-row:

```sql
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;

-- Vectorization works best with:
-- ORC format (native vectorized reader)
-- Simple filter/aggregate/arithmetic queries
-- Avoid: complex UDFs (break vectorization)

-- Check if query used vectorization
EXPLAIN VECTORIZATION SELECT COUNT(*), SUM(amount) FROM orders WHERE status = 'COMPLETED';
```

## ACID Transactions

Hive 3.x supports full ACID transactions for INSERT, UPDATE, DELETE, MERGE:

```sql
-- Enable ACID (required settings)
SET hive.support.concurrency=true;
SET hive.enforce.bucketing=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
SET hive.compactor.initiator.on=true;
SET hive.compactor.worker.threads=1;

-- Create ACID table (must be ORC, bucketed)
CREATE TABLE customer_preferences (
    customer_id STRING,
    preference_key STRING,
    preference_value STRING,
    updated_at TIMESTAMP
)
CLUSTERED BY (customer_id) INTO 8 BUCKETS
STORED AS ORC
TBLPROPERTIES ('transactional'='true');

-- DML on ACID table
INSERT INTO customer_preferences VALUES ('C001', 'theme', 'dark', CURRENT_TIMESTAMP);

UPDATE customer_preferences
SET preference_value = 'light', updated_at = CURRENT_TIMESTAMP
WHERE customer_id = 'C001' AND preference_key = 'theme';

DELETE FROM customer_preferences
WHERE customer_id = 'C001' AND preference_key = 'old_feature';

-- MERGE (upsert pattern)
MERGE INTO customer_preferences AS target
USING incoming_prefs AS source
ON target.customer_id = source.customer_id
  AND target.preference_key = source.preference_key
WHEN MATCHED THEN
  UPDATE SET preference_value = source.preference_value, updated_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
  INSERT VALUES (source.customer_id, source.preference_key, source.preference_value, CURRENT_TIMESTAMP);
```

### ACID Compaction
ACID tables use delta files for updates. Compaction merges delta files for performance:

```sql
-- Check compaction status
SHOW COMPACTIONS;

-- Manual compaction
ALTER TABLE customer_preferences COMPACT 'minor';  -- Merge delta files
ALTER TABLE customer_preferences COMPACT 'major';  -- Rewrite base + all deltas

-- Configure automatic compaction thresholds
SET hive.compactor.delta.num.threshold=10;  -- Trigger minor after 10 delta files
SET hive.compactor.delta.pct.threshold=0.1; -- Trigger major when deltas > 10% of base
```

## SerDe (Serializer/Deserializer)

SerDe defines how Hive reads/writes data in custom formats:

```sql
-- JSON SerDe
CREATE TABLE json_logs (
    user_id STRING,
    action STRING,
    metadata MAP<STRING, STRING>
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE;

-- RegEx SerDe (for unstructured logs)
CREATE TABLE apache_logs (
    host STRING,
    identity STRING,
    user STRING,
    time STRING,
    request STRING,
    status INT,
    size INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
WITH SERDEPROPERTIES (
    "input.regex" = "([^ ]*) ([^ ]*) ([^ ]*) \\[([^\\]]*)\\] \"([^\"]*)\"\\ ([0-9]*) ([0-9]*)"
)
STORED AS TEXTFILE;

-- CSV SerDe with custom delimiter
CREATE TABLE csv_data (
    col1 STRING,
    col2 INT,
    col3 DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar" = "\"",
    "escapeChar" = "\\"
)
STORED AS TEXTFILE;

-- Avro SerDe
CREATE TABLE avro_table
STORED AS AVRO
TBLPROPERTIES ('avro.schema.url'='hdfs:///schemas/my_schema.avsc');
```

## Hive Views

```sql
-- Create view (logical, no data copied)
CREATE VIEW daily_revenue AS
SELECT
    DATE(order_time) as order_date,
    product_category,
    SUM(amount) as revenue,
    COUNT(*) as order_count
FROM orders
GROUP BY DATE(order_time), product_category;

-- Materialized view (data is stored, updated periodically)
CREATE MATERIALIZED VIEW revenue_summary
TBLPROPERTIES ('rewrite.enabled'='true')
AS SELECT
    order_date,
    SUM(amount) as total_revenue
FROM orders
GROUP BY order_date;

-- Rebuild materialized view
ALTER MATERIALIZED VIEW revenue_summary REBUILD;

-- Query rewriting: Hive automatically uses materialized view
-- when query matches the view definition
SELECT order_date, SUM(amount) FROM orders GROUP BY order_date;
-- → Hive rewrites to: SELECT * FROM revenue_summary;
```

## HiveServer2 Configuration

```xml
<!-- hive-site.xml -->
<property>
  <name>hive.server2.thrift.port</name>
  <value>10000</value>
</property>
<property>
  <name>hive.server2.thrift.bind.host</name>
  <value>0.0.0.0</value>
</property>

<!-- Enable impersonation (run as submitting user) -->
<property>
  <name>hive.server2.enable.doAs</name>
  <value>true</value>
</property>

<!-- Connection pool for Metastore -->
<property>
  <name>datanucleus.connectionPoolingType</name>
  <value>HikariCP</value>
</property>
<property>
  <name>datanucleus.connectionPool.maxPoolSize</name>
  <value>20</value>
</property>
```

```bash
# Connect with Beeline
beeline -u "jdbc:hive2://hiveserver2-host:10000/default" \
  -n username -p password

# Connect with SSL
beeline -u "jdbc:hive2://hiveserver2-host:10000/default;ssl=true;sslTrustStore=/path/to/truststore.jks"

# Execute query file
beeline -u "jdbc:hive2://localhost:10000" \
  -f /path/to/query.hql

# Execute inline query
beeline -u "jdbc:hive2://localhost:10000" \
  -e "SELECT COUNT(*) FROM orders WHERE order_date='2024-01-15'"
```

## Hive vs Spark SQL

| Feature | Hive | Spark SQL |
|---------|------|-----------|
| Speed | Slow (MR) / Fast (Tez/Spark) | Fast (in-memory) |
| SQL compliance | HiveQL (most SQL-92) | More ANSI SQL compliant |
| Streaming | No | Yes (Structured Streaming) |
| ACID | Yes (ORC only) | No native ACID |
| Metastore | Owns the metastore | Can use Hive Metastore |
| UDFs | Java/Python | Scala/Python/Java |
| Execution | Server-based (HS2) | Client-side or server (Thrift) |
| Best for | Stable ETL, SQL users | Data science, complex analytics |
| Fault tolerance | High (disk-based) | RDD lineage (may OOM) |

## Optimization Settings Reference

```sql
-- Query execution optimizations
SET hive.optimize.ppd=true;               -- Predicate pushdown
SET hive.optimize.index.filter=true;      -- Use indexes
SET hive.optimize.correlation=true;       -- Correlation optimizer (reduce redundant passes)
SET hive.optimize.reducededuplication=true; -- Remove redundant reducers

-- Join optimizations
SET hive.auto.convert.join=true;          -- Auto map join
SET hive.mapjoin.smalltable.filesize=25000000; -- 25 MB broadcast threshold
SET hive.auto.convert.join.noconditionaltask=true;
SET hive.auto.convert.join.noconditionaltask.size=20971520; -- 20 MB

-- Tez settings
SET hive.execution.engine=tez;
SET hive.tez.container.size=4096;         -- Container memory for Tez tasks
SET hive.tez.java.opts=-Xmx3277m;

-- Parallelism
SET hive.exec.parallel=true;             -- Run independent stages in parallel
SET hive.exec.parallel.thread.number=8;
SET hive.merge.mapfiles=true;            -- Merge small output files
SET hive.merge.mapredfiles=true;
SET hive.merge.size.per.task=256000000;  -- Target 256 MB merged file size
```

## Interview Tips

> **Tip 1:** CBO requires statistics. A common interview question is "your Hive query picks a bad join order." The answer is: run `ANALYZE TABLE COMPUTE STATISTICS FOR COLUMNS` on all join tables, enable CBO, and check the EXPLAIN plan to see the new join order. Without stats, Hive uses heuristics.

> **Tip 2:** ACID tables require ORC format and bucketing — both mandatory. This is a frequent misconception. You cannot have an ACID table in Parquet format in Hive (though Iceberg/Delta Lake solve this).

> **Tip 3:** Know the three types of Hive joins: Map Join (small table broadcast, no reducer), Sort-Merge Bucket Join (both tables sorted+bucketed on same key, most efficient for large-large joins), and Reduce-Side Join (default, uses shuffle). Mentioning SMB join shows depth.

> **Tip 4:** SerDe is important for real-world data ingestion. JSON logs, regex-parsed access logs, CSV with custom delimiters — all require appropriate SerDes. Knowing the common SerDes (JsonSerDe, RegexSerDe, OpenCSVSerde, LazySimpleSerDe) and when to use them is practical knowledge interviewers value.

> **Tip 5:** Materialized views with rewrite optimization are a Hive 3.x feature that can dramatically speed up dashboards. If users run the same aggregation frequently, a materialized view makes Hive automatically rewrite their queries to use cached results. This is an advanced concept that shows familiarity with modern Hive.
