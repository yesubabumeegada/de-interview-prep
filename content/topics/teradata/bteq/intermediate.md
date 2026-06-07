---
title: "Teradata - BTEQ Intermediate"
topic: teradata
subtopic: bteq
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [teradata, bteq, etl, macros, stored-procedures, multistatement, scripting]
---

# BTEQ — Intermediate

## Multi-Statement Requests

BTEQ supports sending multiple SQL statements as a single **multi-statement request** — they're parsed and dispatched together, which can improve efficiency:

```bteq
-- Multi-statement request (semicolons separate each statement within)
BT;
DELETE FROM staging_orders ALL;
INSERT INTO staging_orders SELECT * FROM source_orders WHERE load_date = CURRENT_DATE;
ET;
```

**BT/ET** (Begin Transaction / End Transaction) wraps statements in an explicit transaction. Both statements commit atomically — if the INSERT fails, the DELETE rolls back.

---

## Calling Macros and Stored Procedures

```bteq
-- Execute a macro
EXEC macro_refresh_daily_summary;

-- Execute a macro with parameters
EXEC macro_load_region('WEST', CURRENT_DATE - 1);

-- Call a stored procedure
CALL sp_calculate_risk_scores(CURRENT_DATE);

-- Check if the procedure/macro succeeded
.IF ERRORCODE <> 0 THEN .GOTO ABORT_PIPELINE;
```

---

## BTEQ Session Settings

```bteq
-- Set output width for REPORT mode
.SET WIDTH 200

-- Set separator for DATA mode output
.SET SEPARATOR '|'

-- Set null character representation
.SET NULLCHARVAL 'NULL'

-- Set maximum number of rows to return (safety limit)
.SET MAXERROR 1

-- Set transaction mode (TERA vs ANSI)
.SET SESSION TRANSACTION ANSI;

-- Increase query response timeout
.SET RETLIMIT 99999999
```

---

## Parameterized BTEQ with Shell Variables

BTEQ itself doesn't support variables, but you can use shell variable substitution before invoking BTEQ:

```bash
#!/bin/bash
LOAD_DATE=$(date -d "yesterday" +%Y-%m-%d)
TABLE="sales_fact"
DB="PROD_DW"

bteq <<EOF
.LOGON ${TERADATA_HOST}/${TERADATA_USER},${TERADATA_PASS};

.REMARK "Loading data for date: ${LOAD_DATE}";

INSERT INTO ${DB}.${TABLE}
SELECT *
FROM ${DB}.stg_${TABLE}
WHERE load_date = '${LOAD_DATE}';

.IF ERRORCODE <> 0 THEN .QUIT 8;

COLLECT STATISTICS ON ${DB}.${TABLE} COLUMN (sale_date);

.LOGOFF;
.QUIT 0;
EOF

exit $?
```

---

## Complex Conditional Logic

```bteq
.LOGON server/user,pass;

-- Count rows in staging
SELECT COUNT(*) AS row_count INTO :v_count FROM staging_sales;

-- BTEQ can't directly use SQL result in .IF, but ERRORCODE works
-- Alternative: use CASE in SQL to set a flag
SEL CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS check_flag
FROM staging_sales;

.IF ACTIVITYCOUNT = 0 THEN .GOTO NO_DATA;
.IF ERRORCODE <> 0 THEN .GOTO HANDLE_ERROR;

-- Main processing
INSERT INTO sales_fact SELECT * FROM staging_sales;
.IF ERRORCODE <> 0 THEN .GOTO HANDLE_ERROR;

.GOTO END_SCRIPT;

.LABEL NO_DATA
.REMARK "No data in staging - skipping load"
.LOGOFF;
.QUIT 0;

.LABEL HANDLE_ERROR
.REMARK "Error encountered - aborting"
.LOGOFF;
.QUIT 8;

.LABEL END_SCRIPT
.LOGOFF;
.QUIT 0;
```

**ACTIVITYCOUNT** = number of rows affected/returned by the last SQL statement.

---

## BTEQ Import: Loading from Files

```bteq
.LOGON server/user,pass;

-- Import DATA mode (delimited)
.IMPORT DATA FILE = /data/orders_20240115.dat

-- Read and insert rows from file
USING (
    order_id     INTEGER,
    customer_id  INTEGER,
    order_date   DATE FORMAT 'YYYY-MM-DD',
    amount       DECIMAL(10,2)
)
INSERT INTO orders VALUES (:order_id, :customer_id, :order_date, :amount);

.IMPORT RESET

.LOGOFF;
```

**Limitations of BTEQ import vs FastLoad:**
- Single-threaded (one row at a time, row-by-row INSERT)
- Much slower than FastLoad for large files
- Triggers all table indexes/triggers on each row
- Only suitable for small files (< 100K rows)

---

## Logging and Auditing in BTEQ Scripts

```bteq
.LOGON server/etl_user,pass;

.REMARK "=== Starting daily load pipeline ===";
.REMARK "Timestamp: ${CURRENT_TIMESTAMP}";

-- Log start to audit table
INSERT INTO etl_audit_log VALUES (
    CURRENT_TIMESTAMP, 'daily_load', 'STARTED', USER, NULL
);

-- ... pipeline steps ...

-- Log completion
INSERT INTO etl_audit_log VALUES (
    CURRENT_TIMESTAMP, 'daily_load', 'COMPLETED', USER, CAST(:row_count AS VARCHAR(20))
);

.LOGOFF;
.QUIT 0;
```

---

## BTEQ vs FastExport for Data Export

| Feature | BTEQ Export | FastExport |
|---|---|---|
| Speed | Single-threaded | Parallel across AMPs |
| Volume | Small/medium files | Large files (GB-scale) |
| Format | Report, Data, Indicator | Binary, delimited |
| Setup complexity | Low (inline) | High (separate utility) |
| Secondary index usage | Yes | No (bypasses) |
| Best for | Reports, small extracts | Large data extracts |

---

## Interview Tips

> **Tip 1:** "How do you handle transactions in BTEQ?" — "Use BT/ET (Begin/End Transaction) to wrap multiple statements in an explicit transaction. Both commit atomically — if any statement fails, the entire transaction rolls back. Without BT/ET, BTEQ in TERA mode auto-commits each statement individually."

> **Tip 2:** "How do you pass parameters to a BTEQ script?" — "BTEQ doesn't have native variables, so use shell variable substitution. Set environment variables or shell variables, then use here-doc (<<EOF) syntax where the shell substitutes ${VAR} values before passing the script to BTEQ. This makes BTEQ scripts effectively parameterized."

> **Tip 3:** "When would you choose BTEQ over FastLoad for loading data?" — "BTEQ for small files (< 100K rows), complex data transformations, or when you need full SQL flexibility (joins, lookups during load). FastLoad for high-throughput bulk loading of large files into empty tables — it's 10-100× faster for large volumes."

> **Tip 4:** "What is ACTIVITYCOUNT in BTEQ?" — "ACTIVITYCOUNT is set after each SQL statement to the number of rows affected or returned. Use it to check if a DELETE removed any rows, whether a SELECT returned results, or how many rows an INSERT processed. It's distinct from ERRORCODE, which indicates success/failure."
