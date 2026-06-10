---
title: "Teradata - BTEQ Senior Deep Dive"
topic: teradata
subtopic: bteq
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [teradata, bteq, production-etl, error-handling, restart-logic, tpt-migration]
---

# BTEQ — Senior Deep Dive

## Production ETL Script Architecture

Enterprise BTEQ scripts follow a pattern with startup checks, error handling, audit logging, and clean shutdown:

```bteq
-- ============================================================
-- Script: daily_sales_load.bteq
-- Purpose: Load daily sales from staging to fact table
-- Restartable: Yes (via delete-then-insert pattern)
-- ============================================================

.LOGON ${TD_HOST}/${TD_USER},${TD_PASS};

-- === SETUP ===
.SET WIDTH 200
.SET SESSION TRANSACTION ANSI;
.SET MAXERROR 1;

-- === AUDIT LOG: START ===
INSERT INTO etl.pipeline_log VALUES (
    CURRENT_TIMESTAMP(0), 'daily_sales_load', 'STARTED', USER, NULL, NULL
);
.IF ERRORCODE <> 0 THEN .GOTO ABORT;

-- === STEP 1: Validate staging has data ===
SEL COUNT(*) FROM stg.sales_today;
.IF ACTIVITYCOUNT = 0 THEN .GOTO NO_DATA;
.IF ERRORCODE <> 0 THEN .GOTO ABORT;

-- === STEP 2: Delete today's rows from fact (idempotent restart) ===
DELETE FROM fact.sales WHERE sale_date = ${LOAD_DATE (DATE, FORMAT 'YYYY-MM-DD')};
.IF ERRORCODE <> 0 THEN .GOTO ABORT;

-- === STEP 3: Load ===
INSERT INTO fact.sales
SELECT * FROM stg.sales_today;
.IF ERRORCODE <> 0 THEN .GOTO ABORT;

-- === STEP 4: Collect stats ===
COLLECT STATISTICS ON fact.sales COLUMN (sale_date);
COLLECT STATISTICS ON fact.sales COLUMN (PARTITION);

-- === AUDIT LOG: SUCCESS ===
UPDATE etl.pipeline_log
SET status = 'COMPLETED', end_ts = CURRENT_TIMESTAMP(0)
WHERE pipeline_name = 'daily_sales_load'
  AND start_ts = (SEL MAX(start_ts) FROM etl.pipeline_log
                   WHERE pipeline_name = 'daily_sales_load');

.LOGOFF;
.QUIT 0;

-- === ERROR HANDLERS ===
.LABEL NO_DATA
INSERT INTO etl.pipeline_log VALUES (
    CURRENT_TIMESTAMP(0), 'daily_sales_load', 'NO_DATA', USER, NULL, NULL
);
.LOGOFF;
.QUIT 0;  -- Not a failure — expected scenario

.LABEL ABORT
INSERT INTO etl.pipeline_log VALUES (
    CURRENT_TIMESTAMP(0), 'daily_sales_load', 'FAILED',
    USER, CAST(ERRORCODE AS VARCHAR(10)), NULL
);
.LOGOFF;
.QUIT 8;
```

---

## Restart Logic and Idempotency

Production ETL must be **restartable** — if it fails midway, re-running should be safe:

**Pattern: Delete-then-Insert**
```sql
-- Step 1: Remove any partial data from this run
DELETE FROM fact.sales WHERE sale_date = :load_date;

-- Step 2: Reload from staging
INSERT INTO fact.sales SELECT * FROM stg.sales WHERE load_date = :load_date;
```

Re-running after failure: the DELETE removes any partial inserts from the previous failed run, then the INSERT does a clean load. No duplicate data.

**Pattern: Truncate-then-Insert (for daily-replace fact tables)**
```sql
-- Truncate the entire target (if full-refresh pattern)
DELETE FROM fact.daily_summary ALL;
INSERT INTO fact.daily_summary SELECT * FROM stg.daily_summary;
```

**Anti-pattern: Append without check**
```sql
-- DANGEROUS: No guard against duplicate loads
INSERT INTO fact.sales SELECT * FROM stg.sales;
-- If this runs twice, you get duplicate rows!
```

---

## BTEQ for Complex ETL Orchestration

```bteq
-- Orchestrate multiple dependent steps with checkpoint tracking

.LOGON server/user,pass;

-- Check if this step already completed (checkpoint-based restart)
SEL COUNT(*) FROM etl.checkpoints
WHERE pipeline = 'sales_etl' AND step = 'STAGE_LOAD' AND status = 'OK'
  AND run_date = ${LOAD_DATE};

.IF ACTIVITYCOUNT > 0 THEN .GOTO SKIP_STAGE_LOAD;

-- === Stage Load ===
DELETE FROM stg.orders ALL;
INSERT INTO stg.orders SELECT * FROM src.orders_delta;
.IF ERRORCODE <> 0 THEN .GOTO ABORT;

-- Mark checkpoint
INSERT INTO etl.checkpoints VALUES ('sales_etl', 'STAGE_LOAD', 'OK', ${LOAD_DATE}, CURRENT_TIMESTAMP);

.LABEL SKIP_STAGE_LOAD
.REMARK "Stage load: complete (skipped or done)";

-- === Transform and Load ===
-- ... next steps ...

.LOGOFF;
.QUIT 0;

.LABEL ABORT
.LOGOFF;
.QUIT 8;
```

---

## Performance Tuning BTEQ Scripts

### Avoid Row-by-Row Processing
```bteq
-- BAD: Row-by-row via .IMPORT (one INSERT per row = 10M individual inserts)
.IMPORT DATA FILE = bigfile.dat
USING (col1 INTEGER, col2 VARCHAR(100))
INSERT INTO target VALUES (:col1, :col2);

-- GOOD: Use INSERT/SELECT for set-based operations
-- Or use FastLoad for large files
```

### Use Multi-Statement Requests for Batched INSERTs
```bteq
-- Bundle small inserts into multi-statement request
BT;
INSERT INTO audit_log VALUES (CURRENT_TIMESTAMP, 'step1', 'start');
DELETE FROM staging ALL;
INSERT INTO staging SELECT * FROM source;
INSERT INTO audit_log VALUES (CURRENT_TIMESTAMP, 'step1', 'done');
ET;
```

### Maximize Parallelism in BTEQ
BTEQ itself is single-threaded, but the SQL it submits runs in parallel on AMPs. Write SQL that maximizes AMP parallelism:
- Use set-based INSERT/SELECT (not row-by-row)
- Avoid cursors and procedural loops
- Structure JOINs to be AMP-local where possible

---

## BTEQ Limitations and When to Migrate to TPT

| Limitation | Impact | TPT Alternative |
|---|---|---|
| Single-threaded connection | Lower throughput than parallel tools | TPT Load operator (parallel) |
| No restart for data loads | Full reload on failure | TPT checkpointing |
| No incremental insert for large files | Slow for > 1M row imports | TPT Stream operator |
| No secondary index bypass for exports | Slower exports | FastExport / TPT Export |
| Session-level only | Can't run as multi-session | TPT multi-session jobs |

**When to keep BTEQ:**
- Complex conditional logic (BTEQ's .IF/.GOTO is more flexible than TPT)
- DDL scripts (CREATE TABLE, GRANT, etc.)
- Admin tasks and one-off queries
- Small data volumes
- Systems where TPT isn't installed

---

## Security Best Practices for BTEQ Scripts

```bash
# NEVER hardcode credentials in scripts
# BAD:
bteq <<EOF
.LOGON server/my_etl_user,my_password123;
...

# GOOD: Use environment variables (set by secrets manager)
export TD_USER=$(vault read -field=username secret/teradata/etl)
export TD_PASS=$(vault read -field=password secret/teradata/etl)

bteq <<EOF
.LOGON ${TD_HOST}/${TD_USER},${TD_PASS};
...
EOF
```

```bash
# Protect script files with credentials
chmod 600 /secure/bteq_credentials.conf

# Use .btq files with restricted permissions
# Never commit credentials to version control
```

---

## Interview Tips

> **Tip 1:** "How do you make a BTEQ ETL script restartable?" — "Use the delete-then-insert pattern: at the start of each step, delete any data written by the current run (e.g., WHERE load_date = :today), then re-insert. This makes the script idempotent — running it twice produces the same result as running it once. For complex multi-step pipelines, add a checkpoints table."

> **Tip 2:** "What are BTEQ's main limitations for production ETL?" — "BTEQ is single-threaded, slow for large file imports (row-by-row INSERT), has no built-in restart/checkpoint for data loads, and can't leverage Teradata's parallel load mechanisms. For high-volume loads, use FastLoad or TPT; BTEQ is best for orchestration logic, DDL, and small volumes."

> **Tip 3:** "How do you handle credentials securely in BTEQ scripts?" — "Never hardcode credentials. Use environment variables populated by a secrets manager (Vault, AWS Secrets Manager). The script references ${TD_USER},${TD_PASS}. Protect the scripts themselves with restricted filesystem permissions (chmod 600) and exclude them from version control."

> **Tip 4:** "When would you choose to migrate from BTEQ to TPT?" — "When data volumes exceed what BTEQ can process within the batch window, when you need parallel loading with restart capability, or when FastLoad/MultiLoad functionality is needed. Keep BTEQ for orchestration, DDL, admin tasks, and complex conditional logic that TPT's operator model doesn't handle as cleanly."

## ⚡ Cheat Sheet

**Teradata architecture**
```
AMPs (Access Module Processors): parallel processing units; each owns a data slice
PE (Parsing Engine):             parses SQL, optimizes, dispatches to AMPs
BYNET:                           high-speed interconnect between PEs and AMPs
Vproc (Virtual Processor):       logical unit (AMP or PE) within a node
```

**Primary Index (PI) — critical concept**
```sql
-- Unique Primary Index (UPI): distribute rows by hashing PI column(s)
CREATE TABLE orders (
    order_id INTEGER NOT NULL,
    amount DECIMAL(15,2),
    PRIMARY INDEX (order_id)  -- UPI by default if unique
);

-- Non-Unique Primary Index (NUPI): all rows with same PI hash to same AMP
-- Good for join performance; bad if low cardinality → hot AMP (data skew)

-- NOPI (No Primary Index): load-balanced by row number — best for staging
CREATE SET TABLE staging_orders ... NO PRIMARY INDEX;
```

**Skew and performance**
```sql
-- Check for data skew
SELECT hashamp(hashbucket(hashrow(order_id))) AS amp_no, COUNT(*) AS row_count
FROM orders GROUP BY 1 ORDER BY 2 DESC;
-- Even distribution = good PI; skewed = bad PI choice

-- Skew factor: (max_amp_rows / avg_amp_rows - 1) * 100
-- >10% skew = investigate PI selection
```

**BTEQ essentials**
```bteq
.LOGON server/username,password;
.SET SESSIONS 4;
.SET SEPARATOR '|';

SELECT TOP 10 * FROM orders;

.EXPORT REPORT FILE = /data/output.txt
SELECT * FROM orders WHERE order_date = '2024-01-15';
.EXPORT RESET

.QUIT;
```

**FastLoad / MultiLoad**
```
FastLoad:   empty table only; bypass transient journal; fastest for initial loads
MultiLoad:  supports INSERT/UPDATE/DELETE on existing table; uses work tables
TPT (Teradata Parallel Transporter): modern replacement; stream-based; supports both
FastExport: parallel export to flat files; most efficient for large exports
```

**Statistics**
```sql
-- Collect statistics for optimizer (like ANALYZE TABLE in other DBs)
COLLECT STATISTICS ON orders COLUMN (order_date);
COLLECT STATISTICS ON orders INDEX (order_id);
-- View statistics
HELP STATISTICS orders;
-- Check if stale (>24h old or >10% row count change)
SELECT * FROM dbc.columnstatsv WHERE tablename='orders';
```

**Workload Management (TASM/TWM)**
```
TASM: Teradata Active System Management — rule-based WLM
Priority: use workload definitions to assign CPU priority per user/query type
Throttling: limit concurrent queries per user or workload class
AMP usage limits: cap AMPs for low-priority queries to protect prod workloads
```

**Temporal tables (ANSI SQL time-period)**
```sql
-- Bi-temporal: valid time (business period) + transaction time (DB period)
CREATE TABLE price_history (
    product_id INTEGER,
    price DECIMAL(10,2),
    valid_period PERIOD(DATE) NOT NULL AS VALIDTIME,
    trans_period PERIOD(TIMESTAMP(6) WITH TIME ZONE) NOT NULL AS TRANSACTIONTIME,
    PRIMARY INDEX (product_id)
);
-- Query as of a specific business date
VALIDTIME AS OF DATE '2024-01-15' SELECT * FROM price_history WHERE product_id = 100;
```

**Query optimization tips**
```sql
-- Use EXPLAIN to see query plan
EXPLAIN SELECT * FROM orders WHERE order_date = '2024-01-15';
-- Look for: full AMP scans, product joins (bad), merge joins (good)

-- Join hints
-- Prefer hash join (default for large tables); avoid nested join (one-row-at-a-time)
-- Force partition elimination with PARTITION BY RANGE + filter on partition column

-- PI match for joins: if join key = PI of both tables → row hash match → no redistribution
```

**Key interview points**
- PI = hash-based distribution; UPI vs NUPI vs NOPI vs PI with partitioning
- Data skew on NUPI = hot AMP = performance bottleneck
- Teradata's optimizer is cost-based; fresh statistics are critical
- TPT replaces FastLoad/MultiLoad/FastExport for modern pipelines
- Teradata still dominant in large financial/telco data warehouses (often alongside cloud DW)
