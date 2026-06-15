---
title: "Oracle Interview Scenarios - Real World"
topic: oracle
subtopic: interview-scenarios
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [oracle, interview, scenarios, sql-tuning, pl-sql]
---

# Oracle Interview Scenarios – Real World

## Overview

This guide presents production-grade incident narratives, AWR-driven diagnosis workflows, and migration case studies that reflect the kinds of problems senior Oracle engineers and data engineers actually solve. Each section is structured as a realistic scenario with root cause analysis and resolution.

---

## 1. Production Tuning Incident: The Vanishing Index

### Incident Description

**Symptom:** A nightly ETL job that normally completes in 45 minutes starts running for 8 hours. The on-call engineer is paged at 3 AM. The job loads data into a 2-billion-row `TRANSACTIONS` table.

### Step 1: Check Active Sessions

```sql
-- What is the ETL session doing right now?
SELECT s.sid, s.serial#, s.status, s.event, s.wait_class,
       s.seconds_in_wait, s.sql_id, s.prev_sql_id, s.module
FROM   v$session s
WHERE  s.module LIKE 'ETL%'
AND    s.status  = 'ACTIVE';
```

Output shows `db file sequential read` wait, 6-second avg wait. The current `sql_id` maps to an UPDATE statement.

### Step 2: Pull the Current Execution Plan

```sql
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR('abc123xyz', 0, 'ALLSTATS LAST'));
```

```
| Id | Operation          | Name         | E-Rows |  A-Rows | Buffers |
|  1 |  UPDATE            | TRANSACTIONS |        |         |         |
|  2 |   TABLE ACCESS FULL| TRANSACTIONS |   1500 | 2000000 |  980000 |
```

`TABLE ACCESS FULL` on a 2B-row table — the index is not being used. E-Rows = 1,500 but A-Rows = 2,000,000 (massive cardinality underestimate).

### Step 3: Check Index Health

```sql
-- Is the index there?
SELECT index_name, status, visibility, last_analyzed
FROM   dba_indexes
WHERE  table_name = 'TRANSACTIONS'
AND    index_name = 'TXN_STATUS_IDX';
```

**Finding:** `STATUS = 'UNUSABLE'`

A partition maintenance operation earlier that day (`ALTER TABLE ... SPLIT PARTITION`) invalidated the local index partition but nobody rebuilt it.

### Step 4: Resolution

```sql
-- Rebuild only the affected partition's index
ALTER INDEX txn_status_idx REBUILD PARTITION p_current ONLINE;

-- Verify
SELECT status FROM dba_ind_partitions
WHERE  index_name = 'TXN_STATUS_IDX'
AND    partition_name = 'P_CURRENT';
-- Should return: VALID
```

ETL job performance immediately returned to normal after the rebuild.

### Post-Mortem Actions

```sql
-- Add a monitoring query to the daily health check
SELECT i.index_name, ip.partition_name, ip.status
FROM   dba_ind_partitions ip
JOIN   dba_indexes i ON i.index_name = ip.index_name
WHERE  i.table_name IN ('TRANSACTIONS', 'ORDERS', 'PRODUCTS')
AND    ip.status <> 'USABLE';

-- Automate with DBMS_SCHEDULER
BEGIN
    DBMS_SCHEDULER.CREATE_JOB(
        job_name        => 'CHECK_UNUSABLE_INDEXES',
        job_type        => 'PLSQL_BLOCK',
        job_action      => q'[
            DECLARE v_cnt NUMBER;
            BEGIN
                SELECT COUNT(*) INTO v_cnt FROM dba_ind_partitions
                WHERE status = 'UNUSABLE';
                IF v_cnt > 0 THEN
                    -- Send alert via UTL_MAIL or custom alerting proc
                    RAISE_APPLICATION_ERROR(-20001, v_cnt || ' unusable index partitions found');
                END IF;
            END;
        ]',
        repeat_interval => 'FREQ=HOURLY',
        enabled         => TRUE
    );
END;
/
```

---

## 2. AWR-Based Diagnosis: The Log File Sync Storm

### Incident Description

**Symptom:** Application response times degrade from 50ms to 3 seconds between 9–11 AM every business day. The database is an OLTP system with thousands of short transactions per second.

### AWR Report Analysis (9–10 AM snapshot)

**Top 5 Timed Events:**
```
Event                          Waits      Time(s)  Avg Wait(ms)  % DB Time
------------------------------ ---------- --------  ------------ ---------
log file sync                  1,234,000    6,200        5.0        52.3
CPU time                               -    3,100           -        26.2
db file sequential read          450,000      890        2.0         7.5
log file parallel write           890,000      780        0.9         6.6
enq: TX - row lock contention       1,200      320      266.7         2.7
```

`log file sync` consuming 52% of DB time is a red flag. This event fires every time a session issues a COMMIT — the foreground process waits for LGWR to flush redo to disk.

**Instance Efficiency:**
```
Redo NoWait %:     99.1  ← normal
Buffer Hit %:      98.5  ← normal
Soft Parse %:      72.3  ← PROBLEM (target > 95)
```

**SQL Ordered by Parse Calls:**
```
SQL Id           Parse Calls   Executions   Soft/Hard Ratio
---------------- -----------   ----------   ---------------
7x9abc123        980,000         980,000          0/100%
```

100% hard parse rate on a high-frequency SQL! The application is generating literal SQL instead of using bind variables.

### Root Cause

```sql
-- Find the literal-SQL culprit
SELECT sql_text, parse_calls, executions, hard_parses
FROM   v$sql
WHERE  executions = parse_calls
AND    parse_calls > 10000
ORDER BY parse_calls DESC
FETCH FIRST 5 ROWS ONLY;
```

Output reveals:
```sql
SELECT * FROM accounts WHERE account_id = 12345678
SELECT * FROM accounts WHERE account_id = 12345679
SELECT * FROM accounts WHERE account_id = 12345680
...
```

The application is building SQL with literal account IDs. Each unique SQL text requires a hard parse: parse → optimize → generate plan → cache. At 1M executions/hour, this causes massive CPU and latch contention, which also backs up LGWR (redo generation is proportional to work done, including parse work).

### Resolution

**Short-term (same day):** Set `CURSOR_SHARING = FORCE` at the session or system level to have Oracle automatically replace literals with bind variables:

```sql
-- Session level (for the app connection pool):
ALTER SESSION SET CURSOR_SHARING = FORCE;

-- Or system-wide (use cautiously — can cause plan instability):
ALTER SYSTEM SET CURSOR_SHARING = FORCE;
```

**Long-term:** Fix the application to use parameterized queries / bind variables:

```python
# Before (bad)
cursor.execute(f"SELECT * FROM accounts WHERE account_id = {account_id}")

# After (good)
cursor.execute("SELECT * FROM accounts WHERE account_id = :1", [account_id])
```

**Result:** After applying `CURSOR_SHARING = FORCE`, `log file sync` waits dropped from 5ms average to 0.3ms, and response times returned to baseline within 15 minutes.

---

## 3. Migration Case Study: Oracle 19c → BigQuery (Retail Data Warehouse)

### Context

A retail company runs a 40TB Oracle 19c data warehouse on-premises. Queries on the `FACT_SALES` table (3.5B rows) take 45–90 minutes for monthly aggregations. The migration target is BigQuery for cost savings and serverless scalability.

### Phase 1: Discovery and Inventory

```sql
-- Catalog all tables by size
SELECT owner, segment_name, segment_type,
       ROUND(bytes / 1024 / 1024 / 1024, 2) size_gb
FROM   dba_segments
WHERE  segment_type = 'TABLE'
AND    owner = 'RETAIL_DW'
ORDER BY bytes DESC
FETCH FIRST 20 ROWS ONLY;

-- Find all dependencies (views, MVs, synonyms)
SELECT name, type, referenced_name, referenced_type
FROM   dba_dependencies
WHERE  owner = 'RETAIL_DW'
ORDER BY name;

-- Identify Oracle-only SQL features in views
SELECT view_name, text
FROM   dba_views
WHERE  owner = 'RETAIL_DW'
AND    (UPPER(text) LIKE '%CONNECT BY%'
     OR UPPER(text) LIKE '%ROWNUM%'
     OR UPPER(text) LIKE '%NVL(%'
     OR UPPER(text) LIKE '%DECODE(%');
```

### Phase 2: Schema Conversion

**Key conversion decisions:**

1. **Oracle `DATE` → BigQuery `DATETIME`** (Oracle DATE has a time component)
2. **Oracle `NUMBER` → BigQuery `NUMERIC(29,9)` or `BIGNUMERIC`** for financial columns
3. **Bitmap indexes → BigQuery clustered tables** (no explicit indexes in BQ)
4. **Range-list composite partitioning → BigQuery date partitioning + clustering**

```sql
-- Oracle DDL
CREATE TABLE fact_sales (
    sale_id     NUMBER(15)      NOT NULL,
    sale_date   DATE            NOT NULL,
    product_id  NUMBER(10),
    store_id    NUMBER(10),
    amount      NUMBER(15,2),
    quantity    NUMBER(8)
)
PARTITION BY RANGE (sale_date)
SUBPARTITION BY LIST (region_code) (...);

-- BigQuery DDL equivalent
CREATE TABLE `project.retail_dw.fact_sales` (
    sale_id     INT64         NOT NULL,
    sale_date   DATE          NOT NULL,
    product_id  INT64,
    store_id    INT64,
    amount      NUMERIC,
    quantity    INT64
)
PARTITION BY sale_date
CLUSTER BY store_id, product_id
OPTIONS (partition_expiration_days = 3650);
```

### Phase 3: Data Extraction

For 40TB, a streaming approach is used with Apache Spark on Dataproc:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month

spark = SparkSession.builder \
    .appName("oracle-to-bq") \
    .config("spark.jars", "/opt/jars/ojdbc8.jar,/opt/jars/spark-bigquery-connector.jar") \
    .getOrCreate()

# Extract one year at a time (partition pruning on Oracle side)
for yr in range(2015, 2024):
    df = spark.read.format("jdbc") \
        .option("url", "jdbc:oracle:thin:@//oracle-host:1521/RETAIL") \
        .option("dbtable", f"(SELECT * FROM RETAIL_DW.FACT_SALES WHERE EXTRACT(YEAR FROM SALE_DATE) = {yr})") \
        .option("user", "etl_svc") \
        .option("password", "secret") \
        .option("numPartitions", "32") \
        .option("partitionColumn", "SALE_ID") \
        .option("lowerBound", str(yr * 1_000_000)) \
        .option("upperBound", str((yr + 1) * 1_000_000)) \
        .option("fetchsize", "10000") \
        .load()

    # Write to BigQuery
    df.write \
        .format("bigquery") \
        .option("table", f"project.retail_dw.fact_sales") \
        .option("temporaryGcsBucket", "my-temp-bucket") \
        .option("partitionField", "sale_date") \
        .option("clusteredFields", "store_id,product_id") \
        .mode("append") \
        .save()

    print(f"Year {yr}: {df.count()} rows migrated")
```

### Phase 4: SQL Translation Examples

**CONNECT BY query → Recursive CTE:**
```sql
-- Oracle: product category hierarchy
SELECT product_id, category_id, LEVEL, SYS_CONNECT_BY_PATH(category_name, ' > ') path
FROM   product_categories
START WITH parent_id IS NULL
CONNECT BY PRIOR category_id = parent_id;

-- BigQuery equivalent
WITH RECURSIVE cat_tree AS (
    SELECT category_id, parent_id, category_name,
           1 AS depth,
           category_name AS path
    FROM   `project.retail_dw.product_categories`
    WHERE  parent_id IS NULL

    UNION ALL

    SELECT c.category_id, c.parent_id, c.category_name,
           t.depth + 1,
           t.path || ' > ' || c.category_name
    FROM   `project.retail_dw.product_categories` c
    JOIN   cat_tree t ON c.parent_id = t.category_id
)
SELECT * FROM cat_tree;
```

**LISTAGG → STRING_AGG:**
```sql
-- Oracle
SELECT department_id, LISTAGG(last_name, ', ') WITHIN GROUP (ORDER BY last_name) names
FROM   employees
GROUP BY department_id;

-- BigQuery
SELECT department_id, STRING_AGG(last_name, ', ' ORDER BY last_name) AS names
FROM   `project.hr.employees`
GROUP BY department_id;
```

### Phase 5: Validation and Cutover

```python
# Row count reconciliation script
import oracledb
from google.cloud import bigquery

oracle_conn = oracledb.connect(user="etl_svc", password="secret", dsn="oracle-host/RETAIL")
bq_client   = bigquery.Client(project="my-project")

def reconcile_monthly(year, month):
    # Oracle count
    with oracle_conn.cursor() as cur:
        cur.execute("""
            SELECT COUNT(*), SUM(amount)
            FROM RETAIL_DW.FACT_SALES
            WHERE EXTRACT(YEAR FROM SALE_DATE)  = :1
            AND   EXTRACT(MONTH FROM SALE_DATE) = :2
        """, [year, month])
        oracle_count, oracle_sum = cur.fetchone()

    # BigQuery count
    bq_result = bq_client.query(f"""
        SELECT COUNT(*) AS cnt, SUM(amount) AS total
        FROM `my-project.retail_dw.fact_sales`
        WHERE EXTRACT(YEAR  FROM sale_date) = {year}
        AND   EXTRACT(MONTH FROM sale_date) = {month}
    """).result()

    for row in bq_result:
        bq_count, bq_sum = row.cnt, row.total

    match = (oracle_count == bq_count) and (abs(oracle_sum - bq_sum) < 0.01)
    print(f"{year}-{month:02d}: Oracle={oracle_count} BQ={bq_count} Match={match}")
    return match

# Run reconciliation for all months
for yr in range(2015, 2024):
    for mo in range(1, 13):
        reconcile_monthly(yr, mo)
```

### Outcome

- Migration completed in 6 weeks (4 weeks for historical data, 2 weeks for validation and cutover)
- Query performance improved from 45–90 minutes to 3–8 seconds on BigQuery (leveraging columnar storage and distributed execution)
- Cost reduced by 60% (no Oracle license, no on-premises hardware maintenance)

---

## 4. Interview Q&A: Real-World Scenarios

**Q: Walk me through a production Oracle performance issue you diagnosed using AWR.**

**A (structured answer):**
1. Identified the problem time window from monitoring alerts
2. Generated an AWR report for the impacted snapshot pair
3. Located the top wait event (`log file sync` in our case, 52% of DB time)
4. Cross-referenced with "SQL Ordered by Parse Calls" — found 100% hard parse rate
5. Traced the hard parses to a specific application module using `v$sql.module`
6. Applied `CURSOR_SHARING = FORCE` as an emergency fix
7. Coordinated with the application team for a proper bind variable fix in the next release

**Q: How do you handle a migration cutover for a 24/7 Oracle system with minimal downtime?**

**A:**
1. Use **Oracle GoldenGate** (or Debezium for open-source) for CDC replication from Oracle to the target system during migration
2. Run both systems in parallel; validate data continuously
3. Schedule a short maintenance window (typically 1–4 hours) for final catch-up and cutover
4. During cutover: stop writes to Oracle, let CDC drain the remaining lag, switch application connection strings, validate
5. Keep Oracle on standby for 2–4 weeks as a rollback option

**Q: What would cause an Oracle query to suddenly use a different execution plan after a statistics gather?**

**A:** Statistics gathering updates the optimizer's model of the data. A plan change occurs when the new statistics change the optimizer's cardinality estimates enough to make a different join order, join method, or access path appear cheaper. Common causes:
- Data skew was previously hidden (uniform distribution assumed); new histogram reveals it
- Row count changed dramatically (table grew or shrank after bulk delete/load)
- Column value distribution shifted (e.g., most orders now have `status = 'COMPLETE'` instead of `'PENDING'`)

**Mitigations:**
- Use SQL Plan Management (SPM) baselines to pin stable plans
- Enable `OPTIMIZER_ADAPTIVE_PLANS` and `OPTIMIZER_ADAPTIVE_STATISTICS` (Oracle 12.2+)
- Use `DBMS_STATS.SET_TABLE_PREFS` to lock statistics on volatile tables during business hours
