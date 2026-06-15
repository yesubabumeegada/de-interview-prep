---
title: "Teradata Interview Scenarios - Intermediate"
topic: teradata
subtopic: interview-scenarios
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [teradata, interview, scenarios, primary-index, bteq]
---

# Teradata Interview Scenarios — Intermediate

This guide covers mid-level Teradata topics: diagnosing and fixing skew, debugging BTEQ scripts, handling FastLoad/MultiLoad error tables, and statistics collection best practices.

---

## 1. Skew — Diagnosis and Fix

### What Is Skew?

Skew is uneven data distribution across AMPs. Because Teradata's performance depends on parallel processing, any AMP with a disproportionate share of rows becomes the query bottleneck.

**Skew factor formula (used by Teradata internally):**

```
Skew % = (Max AMP rows − Average AMP rows) / Average AMP rows × 100
```

A skew factor above 10–15% warrants investigation. Above 50% is a serious performance problem.

### Diagnosing Skew

**Step 1: Check the EXPLAIN output**

```sql
EXPLAIN
SELECT customer_id, COUNT(*) FROM sales_fact GROUP BY customer_id;
```

Look for `"Skew:"` in the explain text, or a note like `"spool is not empty... confidence: none"` which can signal the optimizer does not have good row estimates.

**Step 2: Query AMP-level row distribution directly**

```sql
SELECT
    HASHAMP(HASHBUCKET(HASHROW(customer_id))) AS amp_number,
    COUNT(*) AS row_count
FROM sales_fact
GROUP BY 1
ORDER BY 2 DESC;
```

If one AMP has 5 million rows while the average is 50,000, `customer_id` is a poor PI for this table.

**Step 3: Check the `TABLESIZE` system view**

```sql
SELECT
    TableName,
    SUM(CurrentPerm) / 1e9 AS total_gb,
    MAX(CurrentPerm) / AVG(CurrentPerm) AS skew_ratio
FROM DBC.TableSizeV
WHERE DatabaseName = 'PROD_DB'
  AND TableName = 'sales_fact'
GROUP BY 1
ORDER BY skew_ratio DESC;
```

A `skew_ratio` significantly above 1.0 indicates uneven distribution.

### Root Causes of Skew

| Root Cause | Example |
|---|---|
| Hot key — one PI value has millions of rows | `customer_id = 99999` (a corporate account) has 40% of all orders |
| NULL PI values | Many rows with `customer_id IS NULL` all hash to the same AMP |
| Low-cardinality PI | Using `state_code` (50 values) as PI on a 500M-row table — some states have far more rows |
| Poor composite PI | `(year, month)` PI on 10 years of data — only 120 distinct hash values for all AMPs |

### Fixing Skew

**Option 1: Change the Primary Index**

The most permanent fix. Requires recreating the table or using `ALTER TABLE ... PRIMARY INDEX`.

```sql
-- Original: NUPI on customer_id — hot key skew
-- Fix: use order_id (high cardinality, uniform distribution) as UPI
CREATE TABLE sales_fact_new AS sales_fact WITH DATA
PRIMARY INDEX (order_id);

-- Rename
RENAME TABLE sales_fact TO sales_fact_old;
RENAME TABLE sales_fact_new TO sales_fact;
```

**Option 2: Use a Composite PI**

If neither column alone is uniform, combine them:

```sql
CREATE TABLE sales_fact (
    sale_id     INTEGER NOT NULL,
    customer_id INTEGER,
    region_id   SMALLINT,
    sale_date   DATE,
    amount      DECIMAL(15,2)
) PRIMARY INDEX (customer_id, region_id);  -- combined cardinality is higher
```

**Option 3: Add a Partitioned Primary Index (PPI)**

For large fact tables, PPI reduces per-partition skew and enables partition elimination:

```sql
PRIMARY INDEX (customer_id)
PARTITION BY RANGE_N(
    sale_date BETWEEN DATE '2020-01-01' AND DATE '2025-12-31'
    EACH INTERVAL '1' MONTH
);
```

**Option 4: Redistribute via SPOOL in a query**

For a one-time query workaround without table restructuring:

```sql
-- Force redistribution in a derived table
SELECT /*+ HASHBYT */ t.*
FROM (
    SELECT *, (HASHROW(sale_id)) AS dist_key FROM sales_fact
) t;
```

---

## 2. BTEQ Script Debugging

BTEQ (Basic Teradata Query) is the primary CLI/scripting tool for Teradata batch operations. Understanding error handling and session control is essential for mid-level engineers.

### BTEQ Script Structure

```bteq
.LOGON tdserver/username,password;

.SET ERROROUT STDOUT
.SET MAXERROR 1

DATABASE prod_db;

-- Main query
INSERT INTO staging_orders
SELECT * FROM source_orders WHERE load_date = CURRENT_DATE;

.IF ACTIVITYCOUNT = 0 THEN .GOTO NO_ROWS;

COLLECT STATISTICS COLUMN (load_date) ON staging_orders;

.LABEL NO_ROWS;
.QUIT 0;

.LOGOFF;
```

### Key BTEQ Directives

| Directive | Purpose |
|---|---|
| `.SET MAXERROR n` | Abort script if error code ≥ n. Use `1` for strict mode. |
| `.SET ERROROUT STDOUT` | Write error messages to stdout (piped to logs). |
| `.IF ERRORCODE <> 0 THEN .QUIT 12;` | Exit with code 12 on any SQL error — signals failure to the scheduler. |
| `.ACTIVITYCOUNT` | Row count affected by the last statement. |
| `.SET WIDTH 200` | Prevent line wrapping in output reports. |
| `.EXPORT FILE=output.csv` | Redirect SELECT output to a file. |

### Common BTEQ Debugging Scenarios

**Scenario A: Script exits silently with code 0 but no rows inserted**

```bteq
.LOGON tdserver/etl_user,secret;
DATABASE staging;

INSERT INTO daily_sales
SELECT * FROM source.sales WHERE sale_date = :v_date;

-- Missing check:
.IF ACTIVITYCOUNT = 0 THEN .QUIT 8;   -- Add this to detect empty loads

.LOGOFF;
```

**Root cause:** The `:v_date` variable was not exported to the BTEQ session. `sale_date = :v_date` silently resolved to `NULL`, the WHERE clause returned no rows, ACTIVITYCOUNT was 0, but since there was no exit check, BTEQ returned 0 (success).

**Fix:** Always validate ACTIVITYCOUNT after critical inserts; pass variables explicitly via shell export.

**Scenario B: Intermittent "transaction aborted" errors**

```sql
-- Inside BTEQ
BT;  -- Begin Transaction
UPDATE account SET balance = balance - 500 WHERE acct_id = 101;
UPDATE account SET balance = balance + 500 WHERE acct_id = 202;
ET;  -- End Transaction
```

If a deadlock or timeout occurs mid-transaction, Teradata rolls back automatically and returns error 2631 (deadlock) or 2644 (timeout). BTEQ continues unless `.SET MAXERROR` is configured.

**Fix:**

```bteq
.SET MAXERROR 1

BT;
UPDATE account SET balance = balance - 500 WHERE acct_id = 101;
.IF ERRORCODE <> 0 THEN .GOTO ROLLBACK_HANDLER;

UPDATE account SET balance = balance + 500 WHERE acct_id = 202;
.IF ERRORCODE <> 0 THEN .GOTO ROLLBACK_HANDLER;

ET;
.GOTO END_SCRIPT;

.LABEL ROLLBACK_HANDLER;
BT;  -- Teradata already rolled back, this starts a clean transaction
-- Log the error, send alert
INSERT INTO etl_error_log VALUES (CURRENT_TIMESTAMP, 'Transfer failed', :ERRORCODE);
ET;
.QUIT 12;

.LABEL END_SCRIPT;
.QUIT 0;
```

---

## 3. FastLoad and MultiLoad — Error Table Handling

### FastLoad Overview

FastLoad bulk-loads data into an **empty** table using a two-phase protocol. It bypasses the AMP transaction manager for speed but does not support duplicate PI values during the load session.

```
-- FastLoad script skeleton
LOGON tdserver/etl_user,secret;
SESSIONS 8;
ERRLIMIT 100;

DATABASE staging;
DROP TABLE orders_err1;
DROP TABLE orders_err2;

BEGIN LOADING orders_staging
    ERRORFILES orders_err1, orders_err2
    CHECKPOINT 50000;

DEFINE
    order_id    (INTEGER),
    customer_id (INTEGER),
    order_date  (CHAR(10)),
    amount      (DECIMAL(15,2))
INFILE orders_input.dat;

INSERT INTO orders_staging (
    order_id, customer_id, order_date, amount
) VALUES (
    :order_id, :customer_id, :order_date, :amount
);

END LOADING;
LOGOFF;
```

### FastLoad Error Tables

| Error Table | What It Contains |
|---|---|
| `orders_err1` | Conversion errors (wrong data type, overflow, format mismatch) |
| `orders_err2` | Uniqueness violations (duplicate UPI rows when using FastLoad into a UPI table) |

**Inspecting error tables after a load:**

```sql
-- Check conversion errors
SELECT * FROM orders_err1 ORDER BY ErrorCode;

-- Common error codes
-- ErrorCode 2679: Invalid date
-- ErrorCode 2673: Data size mismatch / truncation
-- ErrorCode 6706: Character set conversion failure

-- Check duplicate violations
SELECT * FROM orders_err2;
```

**What to do with error rows:**

1. **Correct and reload** — Fix the source data and re-run FastLoad (must be into the same empty table or drop/recreate).
2. **Quarantine** — Move error rows to a reject table for investigation:

```sql
INSERT INTO orders_reject
SELECT
    CURRENT_TIMESTAMP AS rejected_at,
    'FASTLOAD_ERR1'   AS error_source,
    e.ErrorCode,
    e.ErrorField,
    e.DataParcel
FROM orders_err1 e;
```

3. **Log and alert** — Track error counts in a control table:

```sql
INSERT INTO etl_load_control
    (load_id, table_name, load_date, error_count, status)
VALUES
    (SEQ.NEXTVAL, 'orders_staging', CURRENT_DATE,
     (SELECT COUNT(*) FROM orders_err1) + (SELECT COUNT(*) FROM orders_err2),
     'PARTIAL');
```

### MultiLoad Overview

MultiLoad loads into **non-empty** tables and supports `INSERT`, `UPDATE`, `DELETE`, and `UPSERT` in a single pass. It uses a more complex three-phase protocol and creates **work tables** internally.

```
-- MultiLoad upsert example
LOGON tdserver/etl_user,secret;
SESSIONS 8;

DATABASE staging;
DROP TABLE customer_wt;   -- work table (MultiLoad creates this)
DROP TABLE customer_log;  -- log table
DROP TABLE customer_err1;
DROP TABLE customer_err2;

BEGIN MLOAD INTO customer_dim
    WORKTABLE customer_wt
    LOGTABLE  customer_log
    ERRORFILES customer_err1, customer_err2;

DEFINE
    customer_id   (INTEGER),
    customer_name (VARCHAR(100)),
    email         (VARCHAR(200)),
    updated_at    (TIMESTAMP)
INFILE customer_delta.dat;

APPLY
    ('INSERT' = :action) TO OPERATOR ($INSERT)
    WHERE :action = 'I',
    ('UPSERT' = :action) TO OPERATOR ($UPDATE
        SET customer_name = :customer_name,
            email = :email,
            updated_at = :updated_at
        WHERE customer_id = :customer_id
    )
    WHERE :action = 'U';

END MLOAD;
LOGOFF;
```

**Key MultiLoad error table differences from FastLoad:**

- `err1`: Constraint violations (e.g., NOT NULL, CHECK constraints)
- `err2`: Duplicate rows that violate UPI during the apply phase
- The work table (`customer_wt`) is left in place after a failed load — you **must drop it** before restarting.

---

## 4. Statistics Collection Best Practices

### Why Collect Statistics?

Teradata's optimizer is cost-based. Without statistics, it defaults to demographic estimates that are often wildly inaccurate, leading to poor join order, wrong join strategy (hash vs. merge vs. nested), and bad row estimates.

### COLLECT STATISTICS Syntax

```sql
-- Collect on a single column
COLLECT STATISTICS COLUMN (sale_date) ON sales_fact;

-- Collect on a composite (important for multi-column predicates)
COLLECT STATISTICS COLUMN (customer_id, region_id) ON sales_fact;

-- Collect on an index (PI columns)
COLLECT STATISTICS INDEX (customer_id) ON sales_fact;

-- Collect on a partition expression (critical for PPI tables)
COLLECT STATISTICS COLUMN (PARTITION) ON orders_ppi;
```

### When to Collect (or Refresh) Statistics

| Trigger | Action |
|---|---|
| Table newly created or populated | Collect immediately before queries run |
| More than 10% of rows changed | Refresh statistics |
| After adding a column used in WHERE or JOIN | Collect on that column |
| After a PPI reorganization | Collect on `PARTITION` column |
| Before running EXPLAIN for optimization | Ensure stats are current |

### Statistics in Interview Q&A

**Q: A query that used to run in 2 minutes now takes 45 minutes. No schema changes were made. What do you check first?**

> **A:** Check if statistics are stale. Run `HELP STATISTICS sales_fact;` to see the last collection date and row counts. If the table has grown significantly since the last `COLLECT STATISTICS`, the optimizer is working from outdated estimates and may have picked a suboptimal plan. Refresh stats and re-run the query — this resolves ~40% of "suddenly slow query" incidents.

```sql
HELP STATISTICS sales_fact;
-- Shows: Column/Index, Last Collection Date, Row Count at collection time

COLLECT STATISTICS
    COLUMN (sale_date),
    COLUMN (customer_id),
    INDEX (customer_id)
ON sales_fact;
```

**Q: What is the difference between `COLLECT STATISTICS` and `COLLECT STATISTICS USING SAMPLE`?**

> **A:** `USING SAMPLE` collects stats from a random sample (default 20%) of the table rather than a full scan. It is faster but less accurate. Use it for very large tables (hundreds of billions of rows) where the cost of a full stats collection scan exceeds acceptable maintenance windows. For most production tables, full statistics collection is preferred.
