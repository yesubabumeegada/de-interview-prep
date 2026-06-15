---
title: "Oracle Interview Scenarios - Scenario Questions"
topic: oracle
subtopic: interview-scenarios
content_type: scenario_question
tags: [oracle, interview, scenarios, sql-tuning, pl-sql]
---

# Oracle Interview Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Read and Interpret an EXPLAIN PLAN

**Context:** You are a junior Oracle developer at a financial services company. Your team lead asks you to investigate why the following query is slow on a 50-million-row `TRANSACTIONS` table:

```sql
SELECT t.txn_id, t.amount, t.txn_date, a.account_name
FROM   transactions t
JOIN   accounts a ON t.account_id = a.account_id
WHERE  t.txn_date >= DATE '2024-01-01'
AND    t.amount > 10000;
```

You run `EXPLAIN PLAN FOR` followed by `SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY)` and get this output:

```
Plan hash value: 3892017450

----------------------------------------------------------------------
| Id | Operation                    | Name         | Rows  | Cost   |
----------------------------------------------------------------------
|  0 | SELECT STATEMENT             |              | 95000 | 48200  |
|  1 |  HASH JOIN                   |              | 95000 | 48200  |
|  2 |   TABLE ACCESS FULL          | ACCOUNTS     |  5000 |    42  |
|  3 |   TABLE ACCESS FULL          | TRANSACTIONS |  95000| 48158  |
----------------------------------------------------------------------

Note
-----
- dynamic statistics used: dynamic sampling (level=2)
- 1 index found but rejected (TXN_DATE_IDX: not selective enough)
```

**Questions:**
1. What do each of the operations (HASH JOIN, TABLE ACCESS FULL) mean?
2. Why did Oracle reject the index on `txn_date`?
3. The query is still slow. What hint or index design change would you try first?
4. What does the `Note` section telling you about "dynamic statistics" suggest about the table's statistics?

<details>
<summary>✅ Solution</summary>

### Answer 1: What do the operations mean?

- **TABLE ACCESS FULL (ACCOUNTS):** Oracle reads every block of the `ACCOUNTS` table sequentially from disk into the buffer cache. No index is used. This is acceptable here because `ACCOUNTS` is small (5,000 rows) — a full scan of a small table is often the right choice.

- **TABLE ACCESS FULL (TRANSACTIONS):** Oracle reads every block of the 50M-row `TRANSACTIONS` table. At cost 48,158 this is the dominant cost. This is the operation we want to improve.

- **HASH JOIN:** Oracle builds a hash table in memory from the smaller input (ACCOUNTS, 5,000 rows), then probes it for each row from the larger input (TRANSACTIONS). Hash join is optimal when one side is small and fits in memory. The join order is correct here (small table as build input).

### Answer 2: Why was the index rejected?

The note says the index was "not selective enough." Oracle estimates that 95,000 rows out of 50 million qualify — roughly **0.19%** selectivity. However, Oracle's threshold for preferring an index over a full scan is typically around **5–15%** depending on clustering factor.

The key question is **clustering factor**: if `TXN_DATE_IDX` has a high clustering factor (rows for the same date are scattered across many different blocks), each ROWID lookup in the index would require a separate block read. At 95,000 rows, that could mean 95,000 random I/Os — far worse than the sequential full scan.

To check:
```sql
SELECT index_name, clustering_factor, num_rows
FROM   dba_indexes
WHERE  table_name = 'TRANSACTIONS'
AND    index_name = 'TXN_DATE_IDX';
```

If `clustering_factor` is close to `num_rows` (50M), the data is completely unsorted by date, and the full scan is genuinely faster.

### Answer 3: What to try first?

**Option A — Composite index with covering columns:**
```sql
CREATE INDEX txn_date_amount_idx
ON transactions (txn_date, amount)
INCLUDE (txn_id, account_id);  -- covering index reduces ROWID lookups
```
A covering index lets Oracle satisfy the query from the index alone (INDEX FAST FULL SCAN or INDEX RANGE SCAN with no table access).

**Option B — Partition the table by txn_date:**
```sql
-- Partition pruning would limit the scan to only the 2024 partition
ALTER TABLE transactions
PARTITION BY RANGE (txn_date) (
    PARTITION p_2023 VALUES LESS THAN (DATE '2024-01-01'),
    PARTITION p_2024 VALUES LESS THAN (DATE '2025-01-01'),
    PARTITION p_max  VALUES LESS THAN (MAXVALUE)
);
```
With partitioning, a `WHERE txn_date >= DATE '2024-01-01'` predicate scans only the 2024 partition — potentially 1/3 of the data.

**Hint to force index (for testing only):**
```sql
SELECT /*+ INDEX(t TXN_DATE_IDX) */ t.txn_id, t.amount, t.txn_date, a.account_name
FROM   transactions t
JOIN   accounts a ON t.account_id = a.account_id
WHERE  t.txn_date >= DATE '2024-01-01'
AND    t.amount > 10000;
```
Compare elapsed time and consistent gets with and without the hint to validate.

### Answer 4: What does "dynamic statistics" mean?

The note `dynamic statistics used: dynamic sampling (level=2)` indicates that Oracle **did not have up-to-date statistics on the TRANSACTIONS table** and had to sample the table at parse time to estimate cardinality. This is a warning sign:

- The estimated 95,000 rows may be inaccurate — if the actual result is very different, the plan choice may be wrong.
- Fix by gathering fresh statistics:

```sql
EXEC DBMS_STATS.GATHER_TABLE_STATS(
    ownname          => 'FINDB',
    tabname          => 'TRANSACTIONS',
    estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE,
    method_opt       => 'FOR COLUMNS txn_date SIZE 254 amount SIZE 254',
    cascade          => TRUE
);
```

After regathering, recheck whether the optimizer now makes a better choice.

</details>

</article>

---

<article data-difficulty="mid">

## Scenario 2: Diagnose a Slow PL/SQL Batch Job Using AWR

**Context:** You are a mid-level Oracle data engineer. A nightly batch job `PKG_BILLING.PROCESS_INVOICES` that normally completes in 30 minutes has been running for 4 hours. The AWR report for the affected time window shows:

**Top 5 Timed Events:**
```
Event                          Waits       Time(s)  Avg Wait  % DB Time
------------------------------ ----------- -------  --------  ---------
db file sequential read        18,500,000   14,400    0.78ms      62.1
CPU time                                -    5,200        -        22.4
log file sync                     920,000    2,100    2.28ms       9.1
enq: TX - row lock contention         450      680  1511.1ms       2.9
latch: cache buffers chains           120       90   750.0ms       0.4
```

**SQL Ordered by Buffer Gets (top entry):**
```
SQL Id: f8x9k2mno1q
Buffer Gets: 890,000,000
Executions:       18,500,000
Gets/Exec:               48
SQL Text: UPDATE invoices SET status = :B1 WHERE invoice_id = :B2
```

**Instance Efficiency:**
```
Buffer Hit %:   94.2
Soft Parse %:   99.8
```

**Questions:**
1. What is the primary bottleneck and what pattern in the batch job code is causing it?
2. The `enq: TX - row lock contention` avg wait is 1511ms. What does this suggest and how do you investigate it?
3. How would you rewrite the batch job to eliminate the dominant bottleneck?
4. Buffer Hit % is 94.2%. Is this a concern given the scenario?

<details>
<summary>✅ Solution</summary>

### Answer 1: Primary Bottleneck — Row-by-Row Processing

The top SQL executes **18.5 million times** with only **48 buffer gets per execution**. This is the classic "slow-by-slow" anti-pattern: the PL/SQL procedure is updating invoices one row at a time inside a cursor loop, with each iteration being an individual UPDATE.

**Estimated code pattern:**
```plsql
-- Anti-pattern (what's almost certainly happening)
FOR rec IN (SELECT invoice_id FROM invoices WHERE batch_id = p_batch) LOOP
    UPDATE invoices SET status = 'PROCESSED' WHERE invoice_id = rec.invoice_id;
    COMMIT;  -- possibly committing every row
END LOOP;
```

18.5M single-row UPDATEs × 2 round trips per update (context switch PL/SQL → SQL → PL/SQL) = ~37M context switches. The `db file sequential read` waits are the index lookups for each individual `WHERE invoice_id = :B2`.

### Answer 2: Row Lock Contention

`enq: TX - row lock contention` with 1,511ms average wait on 450 waits means some sessions waited on average 1.5 seconds for a row lock. This indicates:

**Likely causes:**
1. Another session (or the batch job's own previous iterations due to missing commits) is holding locks on invoice rows.
2. If `COMMIT` is inside the loop (row by row), each commit releases locks but also generates a `log file sync` wait — matching the elevated `log file sync` we see.
3. A concurrent process (e.g., a web application allowing invoice edits) is locking the same rows the batch is trying to update.

**Investigation queries:**
```sql
-- Find who is blocking whom
SELECT blocking_session, sid, serial#, wait_class, seconds_in_wait, event
FROM   v$session
WHERE  blocking_session IS NOT NULL;

-- See what the blocker is doing
SELECT s.sid, s.sql_id, s.event, s.seconds_in_wait,
       q.sql_text
FROM   v$session s
JOIN   v$sql q ON q.sql_id = s.sql_id
WHERE  s.sid = <blocking_sid>;

-- Check for long-running uncommitted transactions
SELECT s.sid, s.serial#, s.username, t.used_ublk, t.used_urec,
       s.last_call_et seconds_active
FROM   v$session s
JOIN   v$transaction t ON t.addr = s.taddr
ORDER BY t.used_ublk DESC;
```

### Answer 3: Rewrite Using BULK COLLECT + FORALL

```plsql
CREATE OR REPLACE PROCEDURE process_invoices_fast(p_batch_id IN NUMBER) IS
    TYPE t_invoice_ids IS TABLE OF invoices.invoice_id%TYPE;
    v_ids    t_invoice_ids;
    v_status VARCHAR2(20) := 'PROCESSED';
    c_limit  CONSTANT PLS_INTEGER := 10000;

    CURSOR c_invoices IS
        SELECT invoice_id
        FROM   invoices
        WHERE  batch_id = p_batch_id
        AND    status   = 'PENDING'
        ORDER BY invoice_id;
BEGIN
    OPEN c_invoices;
    LOOP
        -- Fetch in chunks to limit memory usage
        FETCH c_invoices BULK COLLECT INTO v_ids LIMIT c_limit;
        EXIT WHEN v_ids.COUNT = 0;

        -- Single array DML — one context switch for all rows
        FORALL i IN 1..v_ids.COUNT SAVE EXCEPTIONS
            UPDATE invoices
            SET    status     = v_status,
                   updated_at = SYSDATE
            WHERE  invoice_id = v_ids(i);

        COMMIT;  -- commit per chunk, not per row
        DBMS_OUTPUT.PUT_LINE('Committed chunk of ' || v_ids.COUNT || ' rows');
    END LOOP;
    CLOSE c_invoices;

EXCEPTION
    WHEN OTHERS THEN
        IF c_invoices%ISOPEN THEN CLOSE c_invoices; END IF;
        ROLLBACK;
        RAISE;
END;
/
```

**Alternatively — pure SQL single-statement approach (fastest):**
```sql
-- If no per-row logic is needed, avoid PL/SQL entirely
UPDATE invoices
SET    status     = 'PROCESSED',
       updated_at = SYSDATE
WHERE  batch_id = :p_batch_id
AND    status   = 'PENDING';

COMMIT;
```

A single UPDATE statement eliminates all context switching, reduces `log file sync` waits from 920,000 to 1, and reduces `db file sequential read` from 18.5M to a single index range scan.

### Answer 4: Buffer Hit % of 94.2%

In isolation, 94.2% sounds reasonable (target is >95%), but **in this specific scenario it is a concern**. With 890M buffer gets on a single UPDATE statement, a 5.8% miss rate translates to **~51.6M physical reads** — extremely high. This is a consequence of the row-by-row pattern: the buffer cache cannot keep all the randomly accessed index blocks hot when there are 18.5M individual lookups scattered across a large index.

After switching to BULK/FORALL or a single UPDATE:
- Buffer gets drop from 890M to a small fraction
- Physical reads drop proportionally
- Buffer Hit % becomes irrelevant because the I/O is minimal

The fix is not to increase `db_cache_size` — it is to eliminate the inefficient access pattern.

</details>

</article>

---

<article data-difficulty="senior">

## Scenario 3: Design Oracle to BigQuery Migration for a 50TB Data Warehouse

**Context:** You are a senior data engineer at an enterprise retail company. The CTO has tasked you with migrating a 50TB Oracle 19c on-premises data warehouse to Google BigQuery. The warehouse has:

- 3 fact tables (FACT_SALES 3.5B rows, FACT_INVENTORY 1.2B rows, FACT_RETURNS 450M rows)
- 47 dimension tables (DIM_PRODUCT, DIM_STORE, DIM_CUSTOMER, etc.)
- 23 materialized views refreshed nightly
- 15 PL/SQL packages with ~8,000 lines of procedural ETL logic
- 120 DBMS_SCHEDULER jobs
- Complex SQL including CONNECT BY hierarchies, MODEL clause analytics, and PIVOT queries
- SLA: < 4 hours downtime during cutover; zero data loss

**Questions:**
1. What is your overall migration strategy and phasing plan?
2. How do you handle the CONNECT BY hierarchies and MODEL clause in BigQuery?
3. Design the BigQuery table structure for FACT_SALES to maximize query performance.
4. How do you achieve < 4 hours downtime with zero data loss on a 50TB dataset?
5. What are the top 3 risks and how do you mitigate each?

<details>
<summary>✅ Solution</summary>

### Answer 1: Migration Strategy and Phasing

**Phase 0 — Discovery (weeks 1–2):**
```sql
-- Automated Oracle inventory
SELECT 'TABLE'   AS obj_type, table_name AS obj_name,
       num_rows, last_analyzed
FROM   dba_tables WHERE owner = 'RETAIL_DW'
UNION ALL
SELECT 'INDEX', index_name, num_rows, last_analyzed
FROM   dba_indexes WHERE table_owner = 'RETAIL_DW'
UNION ALL
SELECT 'PACKAGE', object_name, NULL, last_ddl_time
FROM   dba_objects WHERE owner = 'RETAIL_DW' AND object_type = 'PACKAGE'
ORDER BY obj_type, obj_name;
```

Classify each object:
- **Lift-and-shift compatible**: standard SQL, no Oracle-specific syntax
- **Needs translation**: CONNECT BY, DECODE, NVL, ROWNUM, PIVOT, MODEL
- **Needs redesign**: PL/SQL packages, DBMS_SCHEDULER jobs, materialized views

**Phase 1 — Schema and dimension migration (weeks 3–6):**
- Convert all 47 dimension tables (small, fast)
- Validate with row count and checksum comparison
- Set up BigQuery dataset structure, IAM, and networking

**Phase 2 — Historical fact table migration (weeks 7–14):**
- Use Spark on Dataproc for parallelized extraction
- Export fact tables partition by partition (year/month) to GCS as Parquet
- Load into BigQuery; validate each partition before proceeding

**Phase 3 — ETL re-implementation (weeks 7–18, parallel with Phase 2):**
- Rewrite PL/SQL packages as dbt models or Dataform SQL
- Rewrite DBMS_SCHEDULER jobs as Cloud Composer (Airflow) DAGs or BigQuery Scheduled Queries
- Rewrite materialized views as BigQuery dynamic tables

**Phase 4 — CDC and cutover (weeks 18–20):**
- Set up Oracle GoldenGate (or Debezium + Kafka) for CDC from Oracle to BigQuery
- Run dual-write period; validate continuously
- Perform cutover during scheduled maintenance window

### Answer 2: CONNECT BY and MODEL Clause Translation

**CONNECT BY → Recursive CTE:**
```sql
-- Oracle: product category tree
SELECT category_id, parent_id, category_name,
       LEVEL,
       SYS_CONNECT_BY_PATH(category_name, ' > ') AS full_path,
       CONNECT_BY_ISLEAF AS is_leaf
FROM   dim_product_category
START WITH parent_id IS NULL
CONNECT BY PRIOR category_id = parent_id;

-- BigQuery equivalent
WITH RECURSIVE category_tree AS (
    -- Anchor: root categories
    SELECT category_id, parent_id, category_name,
           1 AS depth,
           category_name AS full_path,
           (SELECT COUNT(*) = 0 FROM dim_product_category child
            WHERE child.parent_id = c.category_id) AS is_leaf
    FROM   `project.retail_dw.dim_product_category` c
    WHERE  parent_id IS NULL

    UNION ALL

    SELECT c.category_id, c.parent_id, c.category_name,
           t.depth + 1,
           t.full_path || ' > ' || c.category_name,
           (SELECT COUNT(*) = 0 FROM `project.retail_dw.dim_product_category` child
            WHERE child.parent_id = c.category_id)
    FROM   `project.retail_dw.dim_product_category` c
    JOIN   category_tree t ON c.parent_id = t.category_id
)
SELECT * FROM category_tree;
```

**Oracle MODEL clause → BigQuery equivalent using window functions:**
```sql
-- Oracle MODEL: calculate running sales with carry-forward logic
SELECT product_id, month_num, sales_amount, forecast
FROM   monthly_sales
MODEL
    PARTITION BY (product_id)
    DIMENSION BY (month_num)
    MEASURES (sales_amount, 0 AS forecast)
    RULES (
        forecast[month_num > 12] = forecast[CV() - 1] * 1.05
    );

-- BigQuery equivalent using window functions and GENERATE_ARRAY
WITH base AS (
    SELECT product_id, month_num, sales_amount
    FROM   `project.retail_dw.monthly_sales`
),
future_months AS (
    SELECT product_id, month_num
    FROM   (SELECT DISTINCT product_id FROM base),
           UNNEST(GENERATE_ARRAY(13, 24)) AS month_num
),
combined AS (
    SELECT product_id, month_num, sales_amount FROM base
    UNION ALL
    SELECT product_id, month_num, NULL AS sales_amount FROM future_months
),
with_forecast AS (
    SELECT product_id, month_num, sales_amount,
           LAST_VALUE(sales_amount IGNORE NULLS)
               OVER (PARTITION BY product_id ORDER BY month_num
                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_actual,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY month_num) - 12 AS periods_ahead
    FROM combined
)
SELECT product_id, month_num,
       COALESCE(sales_amount, last_actual * POW(1.05, GREATEST(periods_ahead, 0))) AS value
FROM   with_forecast
ORDER BY product_id, month_num;
```

### Answer 3: BigQuery Table Design for FACT_SALES

```sql
CREATE TABLE `project.retail_dw.fact_sales` (
    -- Surrogate keys
    sale_sk         INT64     NOT NULL,
    -- Dimension foreign keys (for clustering)
    store_sk        INT64     NOT NULL,
    product_sk      INT64     NOT NULL,
    customer_sk     INT64,
    date_sk         INT64     NOT NULL,
    -- Degenerate dimensions
    transaction_id  STRING    NOT NULL,
    channel         STRING,
    -- Measures
    quantity_sold   INT64,
    unit_price      NUMERIC,
    discount_amount NUMERIC,
    net_amount      NUMERIC,
    tax_amount      NUMERIC,
    -- Partition key (must be DATE/DATETIME/TIMESTAMP for date partitioning)
    sale_date       DATE      NOT NULL,
    -- Audit
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY sale_date
CLUSTER BY store_sk, product_sk, customer_sk
OPTIONS (
    description          = 'Retail sales fact table - partitioned by sale_date, clustered by store/product/customer',
    partition_expiration_days = 3650,  -- 10 years
    require_partition_filter  = TRUE   -- prevent full table scans in production
);
```

**Design rationale:**
- **Date partitioning on `sale_date`**: most queries filter by date range, eliminating irrelevant partitions
- **Clustering on `store_sk, product_sk, customer_sk`**: these are the most common GROUP BY / filter columns in retail analytics; BigQuery sorts data within each partition by these columns, enabling block pruning
- **`require_partition_filter = TRUE`**: prevents accidental full 3.5B-row scans in production — forces analysts to specify a date range
- **NUMERIC for monetary columns**: avoids floating-point rounding errors on financial data

### Answer 4: < 4 Hours Downtime with Zero Data Loss

The strategy uses **Change Data Capture (CDC)** to eliminate the downtime dependency on data volume:

```
Timeline:
Weeks 1–14:  Historical bulk migration (50TB) — no downtime
Week 15–19:  CDC running in parallel — changes replicated in near-real-time
Week 20:     Cutover window (< 4 hours)
```

**Cutover steps:**
```
T-0:00  Announce maintenance window; freeze Oracle writes at app layer
T-0:05  Verify CDC lag = 0 (all changes replicated to BigQuery)
T-0:10  Run final reconciliation checksums (automated script)
T-0:30  Switch application connection strings to BigQuery
T-0:45  Smoke test critical reports in BigQuery
T-1:00  Open system to users on BigQuery
T-4:00  Maintenance window closed; Oracle kept in read-only mode for 2 weeks as fallback
```

**CDC tooling options:**
- **Oracle GoldenGate → BigQuery**: native Oracle CDC with official BQ connector; lowest risk
- **Debezium (open-source) + Kafka + Dataflow**: lower cost; requires more engineering
- **Striim**: commercial alternative with Oracle-native log mining

**Zero data loss assurance:**
Oracle GoldenGate reads Oracle redo logs at the source — it captures every committed transaction regardless of when it occurred. As long as CDC lag reaches 0 before application cutover, BigQuery is guaranteed to have every committed Oracle transaction.

### Answer 5: Top 3 Risks and Mitigations

**Risk 1: SQL Compatibility — Oracle-specific features break in BigQuery**

- **Severity:** High — could block business reports
- **Mitigation:**
  1. Run automated SQL compatibility scanner across all 120+ queries and views
  2. Maintain an Oracle-to-BigQuery SQL translation runbook
  3. Use dbt's `{{ source() }}` abstraction so queries reference logical names, not physical dialects
  4. Run BigQuery query in parallel with Oracle for 4 weeks before cutover; diff outputs

**Risk 2: Performance Regression — BigQuery queries slower than Oracle**

- **Severity:** Medium — BigQuery's distributed architecture excels at large aggregations but can be slower for low-latency point lookups
- **Mitigation:**
  1. Apply `require_partition_filter` and appropriate clustering from day one
  2. Identify Oracle queries using indexes for point lookups; rewrite as partitioned range scans in BQ
  3. Use BigQuery BI Engine for sub-second dashboard queries
  4. Benchmark top 20 business-critical queries in BigQuery before cutover; define acceptance criteria

**Risk 3: Oracle PL/SQL Procedural Logic Cannot Be Directly Translated**

- **Severity:** High for complex ETL packages with row-by-row logic, autonomous transactions, or dynamic SQL
- **Mitigation:**
  1. Audit all 8,000 lines of PL/SQL; categorize as (a) pure SQL logic → dbt model, (b) procedural orchestration → Airflow DAG, (c) complex business rules → Python/Spark job
  2. For Oracle PRAGMA AUTONOMOUS_TRANSACTION (used for audit logging): redesign as a separate BigQuery table with streaming inserts, decoupled from the main transaction
  3. Allocate 30% buffer in the timeline for PL/SQL rewrite — it is consistently underestimated
  4. Keep the Oracle system on standby for 30 days post-cutover as a rollback option

</details>

</article>
