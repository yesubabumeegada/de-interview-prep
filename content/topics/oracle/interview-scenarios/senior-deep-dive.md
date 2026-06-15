---
title: "Oracle Interview Scenarios - Senior Deep Dive"
topic: oracle
subtopic: interview-scenarios
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [oracle, interview, scenarios, sql-tuning, pl-sql]
---

# Oracle Interview Scenarios – Senior Deep Dive

## Overview

This guide covers advanced Oracle topics expected at the senior engineer and principal architect level: RAC troubleshooting, Data Guard failover operations, Exadata Smart Scan internals, and Oracle-to-cloud migration patterns (Snowflake, BigQuery).

---

## 1. RAC Troubleshooting

### RAC Architecture Refresher

Oracle Real Application Clusters (RAC) allows multiple instances to mount a single database concurrently. Each node has its own SGA and redo thread. The Global Cache Services (GCS) and Global Enqueue Services (GES) coordinate cache coherency via the interconnect.

```
Node 1 (Instance 1)          Node 2 (Instance 2)
  SGA / Buffer Cache   ←→    SGA / Buffer Cache
       ↕                           ↕
   Private Interconnect (low-latency, e.g., InfiniBand)
       ↕                           ↕
         Shared Storage (ASM / SAN)
```

### Diagnosing RAC Wait Events

```sql
-- Check global cache waits across all instances
SELECT inst_id, event, total_waits, time_waited_micro / 1e6 time_waited_sec,
       ROUND(time_waited_micro / NULLIF(total_waits, 0) / 1000) avg_wait_ms
FROM   gv$system_event
WHERE  event LIKE 'gc%'
AND    wait_class <> 'Idle'
ORDER BY time_waited_micro DESC;
```

**Key RAC wait events and their meaning:**

| Event | Description | Root Cause |
|-------|-------------|------------|
| `gc current block 2-way` | Node requested a dirty block; source node sent it | Normal, but high latency = interconnect bottleneck |
| `gc cr block 2-way` | Consistent-read block transfer | Cross-instance reads; acceptable if latency < 2ms |
| `gc buffer busy acquire` | Session waiting to acquire a block being transferred | Hot blocks / heavy cross-node contention |
| `gc current block lost` | Block transfer failed | Interconnect packet loss; serious |
| `gc current grant 2-way` | Lock grant received in 2 messages | Normal; high avg = interconnect latency |

### Identifying Hot Blocks (Block Contention)

```sql
-- Find the segments with the most cross-instance block traffic
SELECT o.object_name, o.object_type,
       s.gc_current_block_receive_time + s.gc_cr_block_receive_time total_gc_time,
       s.gc_current_blocks_received + s.gc_cr_blocks_received       total_blocks
FROM   v$segment_statistics s
JOIN   dba_objects o ON o.data_object_id = s.obj#
WHERE  s.statistic_name IN ('gc current blocks received', 'gc cr blocks received')
ORDER BY total_gc_time DESC
FETCH FIRST 20 ROWS ONLY;
```

### RAC Sequence Contention

A common RAC anti-pattern: a sequence with `CACHE 20` causes every 20th nextval to update the sequence header block — broadcasting that block across all nodes. Fix:

```sql
-- Increase cache size dramatically
ALTER SEQUENCE order_seq CACHE 10000;

-- Or use NOORDER if sequence ordering across nodes is not required
ALTER SEQUENCE order_seq CACHE 1000 NOORDER;
```

### Interview Q&A

**Q: A RAC cluster experiences extreme `gc buffer busy` waits on a single segment. What steps do you take?**

**A:**
1. Identify the hot segment using `v$segment_statistics`.
2. Determine whether the hot object is a sequence, an index, or a table block.
3. For **sequences**: increase `CACHE` size, use `NOORDER`.
4. For **index right-hand inserts** (monotonically increasing key): use a **reverse key index** or **hash partitioned index** to spread inserts across blocks.
5. For **table hot rows**: check for row-level lock contention (`enq: TX`) and review application logic for unnecessarily long lock holds.
6. Consider application-level partitioning of workload so each instance owns a subset of rows.

---

## 2. Data Guard Failover Operations

### Data Guard Topology

```
Primary DB (ORCL_P)        Standby DB (ORCL_S)
  Redo Logs ──────────────→  Apply Redo (MRP process)
  (LGWR/FAL)                 (Real-time apply or delay)
```

**Protection modes:**

| Mode | Data Loss Risk | Redo Shipping |
|------|---------------|--------------|
| Maximum Protection | Zero | Synchronous; primary hangs if standby unavailable |
| Maximum Availability | Near-zero | Synchronous; falls back to async if standby unavailable |
| Maximum Performance | Some seconds | Asynchronous (default) |

### Performing a Planned Switchover

```sql
-- On PRIMARY: initiate switchover
ALTER DATABASE COMMIT TO SWITCHOVER TO PHYSICAL STANDBY WITH SESSION SHUTDOWN;

-- On STANDBY: complete switchover
ALTER DATABASE COMMIT TO SWITCHOVER TO PRIMARY WITH SESSION SHUTDOWN;
ALTER DATABASE OPEN;

-- On new standby (former primary): start apply
ALTER DATABASE RECOVER MANAGED STANDBY DATABASE USING CURRENT LOGFILE DISCONNECT;
```

### Performing an Emergency Failover

Used when the primary is unavailable and data loss is acceptable:

```sql
-- On STANDBY: check apply lag first
SELECT name, value, datum_time FROM v$dataguard_stats WHERE name = 'apply lag';

-- Failover (with possible data loss)
ALTER DATABASE RECOVER MANAGED STANDBY DATABASE FINISH;
ALTER DATABASE ACTIVATE PHYSICAL STANDBY DATABASE;
ALTER DATABASE OPEN;
```

### Monitoring with Data Guard Broker

```bash
# Connect to DGMGRL
dgmgrl sys/password@primary

DGMGRL> SHOW CONFIGURATION;
DGMGRL> SHOW DATABASE VERBOSE standby_db;
DGMGRL> SHOW DATABASE VERBOSE primary_db;

# Check lag
DGMGRL> SHOW DATABASE standby_db 'ApplyLag';

# Switchover via broker (preferred for planned maintenance)
DGMGRL> SWITCHOVER TO standby_db;
```

### Interview Q&A

**Q: How do you verify zero data loss after a failover?**

**A:**
1. Check `v$dataguard_stats` for `apply lag = +00 00:00:00` before failover — confirms all redo was applied.
2. After activation, compare the standby SCN to the last known primary SCN from `v$database.current_scn`.
3. Use **Flashback Database** on the former primary to re-synchronize it as a new standby once it is brought back online — this avoids a full resync.

---

## 3. Exadata Smart Scan

### What is Smart Scan?

Exadata's Storage Servers (cells) contain their own CPUs. Smart Scan offloads SQL filtering, column projection, and predicate evaluation to the storage layer, returning only qualifying rows and columns to the database node — instead of shipping entire blocks.

**Requirements for Smart Scan to activate:**
1. Full table scan or full partition scan (not index range scan)
2. Direct-path read (bypasses buffer cache — look for `cell smart table scan` wait event)
3. Object stored on Exadata cells
4. No active result cache on the object

### Verifying Smart Scan usage

```sql
-- Check Smart Scan cell offload efficiency
SELECT metric_name, value
FROM   v$sysmetric
WHERE  metric_name LIKE 'Cell%';

-- In the execution plan
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR(format => 'ALL'));
-- Look for: "cell smart table scan" in the Note section
-- And: "TABLE ACCESS STORAGE FULL"
```

### Smart Scan statistics in AWR

```sql
SELECT stat_name, value
FROM   v$sysstat
WHERE  stat_name IN (
    'cell physical IO bytes eligible for predicate offload',
    'cell physical IO bytes saved by storage index',
    'cell physical IO interconnect bytes',
    'cell physical IO interconnect bytes returned by smart scan'
);
```

**Efficiency metric:**
```
Offload efficiency = 1 - (interconnect bytes / eligible bytes)
```
A value > 90% means Exadata is eliminating >90% of data before it reaches the DB node.

### Storage Indexes

Storage indexes are automatically maintained in Exadata cell memory. They track the MIN and MAX of column values per 1MB storage region. A predicate like `WHERE region = 'WEST'` can skip entire storage regions whose MIN/MAX range excludes 'WEST'.

```sql
-- Storage index savings visible here:
SELECT name, value FROM v$sysstat
WHERE  name = 'cell physical IO bytes saved by storage index';
```

### Interview Q&A

**Q: A query on Exadata runs slower than expected and Smart Scan is not activating. What do you check?**

**A:**
1. Verify the table resides on Exadata cells (not regular storage attached to DB nodes).
2. Check whether the plan uses an index — Smart Scan requires a full scan. If the index is not selective enough, add `/*+ NO_INDEX(t) */` or drop the misleading index.
3. Check for `CACHE` table attribute: `ALTER TABLE t NOCACHE` to allow direct-path reads.
4. Check `_serial_direct_read` parameter — it controls whether serial full scans bypass the buffer cache. On Exadata it defaults to `ALWAYS`.
5. Look for `cell smart table scan` in wait events; if absent, Smart Scan is not being used.

---

## 4. Oracle → Snowflake / BigQuery Migration Patterns

### Migration Assessment Checklist

Before migrating:
- Catalog all Oracle objects: tables, views, materialized views, synonyms, sequences, triggers, stored procedures, packages, jobs (DBMS_SCHEDULER)
- Identify Oracle-specific SQL constructs: `CONNECT BY PRIOR` (hierarchical), `MODEL` clause, `PIVOT`/`UNPIVOT`, `LISTAGG`, `XMLTABLE`
- Identify data types with no direct equivalents: `CLOB`/`BLOB`, `INTERVAL`, `XMLType`, `SDO_GEOMETRY`
- Map Oracle date semantics: `DATE` in Oracle includes time component; `DATE` in Snowflake/BigQuery is date-only

### Oracle → Snowflake Mapping

| Oracle | Snowflake Equivalent |
|--------|---------------------|
| `VARCHAR2(n)` | `VARCHAR(n)` |
| `NUMBER(p,s)` | `NUMBER(p,s)` |
| `DATE` (with time) | `TIMESTAMP_NTZ` |
| `CLOB` | `VARCHAR(16777216)` |
| `ROWNUM` | `ROW_NUMBER() OVER (ORDER BY ...)` |
| `CONNECT BY PRIOR` | Recursive CTE (`WITH RECURSIVE`) |
| `NVL(x, y)` | `COALESCE(x, y)` or `IFF(x IS NULL, y, x)` |
| `DECODE` | `CASE WHEN ... END` |
| `DUAL` table | Omit or use `SELECT 1` |
| Sequences | `AUTO_INCREMENT` or Snowflake sequences |
| Materialized views | Snowflake dynamic tables (continuous) or tasks |
| DBMS_SCHEDULER jobs | Snowflake Tasks |

### Oracle → BigQuery Mapping

| Oracle | BigQuery Equivalent |
|--------|-------------------|
| `VARCHAR2(n)` | `STRING` |
| `NUMBER(p,s)` | `NUMERIC(p,s)` or `BIGNUMERIC` |
| `DATE` (with time) | `DATETIME` |
| `CLOB` | `STRING` |
| `CONNECT BY PRIOR` | Recursive CTE (BigQuery supports since 2023) |
| `ROWNUM` | `ROW_NUMBER() OVER (ORDER BY ...)` |
| Partitioned table (RANGE by date) | Partitioned table (column or ingestion-time) |
| Bitmap index | Clustered table (implicit) |
| Materialized views | BigQuery materialized views or scheduled queries |

### ETL Migration Pattern for Large Tables

For a 50TB Oracle data warehouse, a direct single-shot extract is impractical. Use partitioned export:

```bash
# Step 1: Export from Oracle in parallel using Data Pump (per partition)
expdp system/password \
    DUMPFILE=sales_2023_%U.dmp \
    PARALLEL=8 \
    QUERY=sales:'"WHERE sale_date >= DATE ''2023-01-01'' AND sale_date < DATE ''2024-01-01''"' \
    TABLES=sales

# Step 2: Convert to Parquet using Python/Spark
# (Oracle → Parquet is the most portable intermediate format)
```

```python
# PySpark export pattern
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.jars", "ojdbc8.jar") \
    .getOrCreate()

# Partition export for parallelism
df = spark.read.format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//host:1521/orcl") \
    .option("dbtable", "SALES") \
    .option("user", "etl_user") \
    .option("password", "secret") \
    .option("partitionColumn", "SALE_ID") \
    .option("lowerBound", "1") \
    .option("upperBound", "500000000") \
    .option("numPartitions", "100") \
    .load()

# Write to GCS/S3 as Parquet
df.write.mode("overwrite").partitionBy("SALE_YEAR").parquet("gs://bucket/sales/")
```

```sql
-- BigQuery: load from GCS
LOAD DATA INTO mydataset.sales
FROM FILES (
    format = 'PARQUET',
    uris = ['gs://bucket/sales/*.parquet']
);

-- Snowflake: load from S3/GCS via external stage
COPY INTO sales
FROM @my_gcs_stage/sales/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

### Handling Oracle-Specific SQL

**Hierarchical queries (CONNECT BY):**
```sql
-- Oracle
SELECT employee_id, manager_id, LEVEL, SYS_CONNECT_BY_PATH(last_name, '/') path
FROM   employees
START WITH manager_id IS NULL
CONNECT BY PRIOR employee_id = manager_id;

-- BigQuery / Snowflake equivalent (recursive CTE)
WITH RECURSIVE emp_hierarchy AS (
    -- Anchor: top-level managers
    SELECT employee_id, manager_id, last_name, 1 AS lvl,
           CAST(last_name AS STRING) AS path
    FROM   employees
    WHERE  manager_id IS NULL

    UNION ALL

    -- Recursive: subordinates
    SELECT e.employee_id, e.manager_id, e.last_name, h.lvl + 1,
           h.path || '/' || e.last_name
    FROM   employees e
    JOIN   emp_hierarchy h ON e.manager_id = h.employee_id
)
SELECT * FROM emp_hierarchy;
```

### Interview Q&A

**Q: How would you validate data integrity after migrating a 50TB Oracle warehouse to BigQuery?**

**A:**
1. **Row count checks** at partition/table level:
```sql
-- Oracle
SELECT TO_CHAR(sale_date, 'YYYY-MM') month, COUNT(*) cnt, SUM(amount) total
FROM sales GROUP BY TO_CHAR(sale_date, 'YYYY-MM');

-- BigQuery — compare output
SELECT FORMAT_DATE('%Y-%m', sale_date) month, COUNT(*) cnt, SUM(amount) total
FROM `project.dataset.sales` GROUP BY 1;
```
2. **Column-level checksums** on critical numeric columns using SUM/COUNT/MAX/MIN across both systems.
3. **Sample reconciliation**: random-sample 0.1% of rows and compare field by field.
4. **Business metric validation**: re-run key reports in both systems and compare outputs.
5. **Null/type drift checks**: verify that Oracle's implicit type coercions (NUMBER → VARCHAR on concatenation) are handled explicitly in BigQuery.

**Q: What Oracle features have no equivalent in Snowflake/BigQuery and require redesign?**

**A:**
- **DBMS_SCHEDULER** jobs → Snowflake Tasks or Cloud Scheduler + Cloud Run
- **Database Triggers** (DML triggers) → application-level logic or Snowflake Streams + Tasks
- **Row-Level Security (VPD/FGAC)** → Snowflake Row Access Policies or BigQuery row-level security
- **Oracle TEXT (full-text search)** → BigQuery Search Indexes or Snowflake full-text search (Cortex)
- **XMLType columns** → parse to JSON first, store as VARIANT (Snowflake) or JSON (BigQuery)
- **Autonomous Transactions** (PRAGMA AUTONOMOUS_TRANSACTION) → no direct equivalent; redesign logging/auditing patterns
