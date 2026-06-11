---
title: "Teradata - BTEQ Fundamentals"
topic: teradata
subtopic: bteq
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [teradata, bteq, scripting, batch, etl, logon]
---

# BTEQ — Fundamentals


## 🎯 Analogy

Think of BTEQ (Basic Teradata Query) like a command-line SQL client with scripting superpowers: you can write conditional logic, loop over result sets, and chain SQL statements — the standard tool for Teradata batch ETL scripts.

---
## What Is BTEQ?

**BTEQ (Basic Teradata Query)** is Teradata's interactive and batch SQL scripting tool. It is the oldest and most universal Teradata client utility, pre-dating all modern tools. BTEQ can:
- Execute interactive SQL queries
- Run batch scripts for ETL and reporting
- Export query results to files
- Import data from files
- Conditionally execute SQL based on error codes

BTEQ is not a graphical tool — it's a command-line interface that runs SQL and BTEQ commands (prefixed with `.`).

---

## Basic BTEQ Commands

```bteq
-- Connect to Teradata
.LOGON teradata-server/username,password

-- Execute SQL
SELECT CURRENT_DATE;

-- Disconnect
.LOGOFF

-- Exit BTEQ (with return code 0)
.QUIT 0
```

---

## A Simple BTEQ Script

```bteq
.LOGON myserver/etl_user,s3cr3t;

-- Run a query
SELECT COUNT(*) FROM orders WHERE order_date = CURRENT_DATE - 1;

-- Export result to file
.EXPORT REPORT FILE = /tmp/daily_count.txt
SELECT COUNT(*) FROM orders WHERE order_date = CURRENT_DATE - 1;
.EXPORT RESET

.LOGOFF;
.QUIT 0;
```

---

## Error Handling with ERRORCODE

BTEQ sets `ERRORCODE` after each SQL statement:
- `0` = Success
- Non-zero = Error (specific code identifies the error type)

```bteq
.LOGON myserver/etl_user,pass;

DELETE FROM staging_table ALL;

-- Check if DELETE succeeded
.IF ERRORCODE <> 0 THEN .GOTO HANDLE_ERROR;

INSERT INTO staging_table SELECT * FROM source_table;

.IF ERRORCODE <> 0 THEN .GOTO HANDLE_ERROR;

.GOTO END;

.LABEL HANDLE_ERROR
.OS echo "Error occurred: ERRORCODE = " ${ERRORCODE}
.QUIT 8;

.LABEL END
.LOGOFF;
.QUIT 0;
```

---

## BTEQ Command Reference

| Command | Purpose |
|---|---|
| `.LOGON host/user,pass` | Connect to Teradata |
| `.LOGOFF` | Disconnect |
| `.QUIT [n]` | Exit BTEQ with return code n |
| `.IF ERRORCODE condition THEN cmd` | Conditional execution |
| `.GOTO label` | Jump to a label |
| `.LABEL name` | Define a jump target |
| `.EXPORT ...` | Start exporting results to a file |
| `.EXPORT RESET` | Stop exporting |
| `.IMPORT ...` | Import data from a file |
| `.OS command` | Execute an OS shell command |
| `.SET ...` | Set BTEQ session parameters |
| `.REMARK text` | Comment / log message |

---

## Export Modes

```bteq
-- REPORT mode: human-readable, column-aligned text
.EXPORT REPORT FILE = output.txt

-- DATA mode: raw delimited data (for downstream processing)
.EXPORT DATA FILE = output.dat

-- INDICATORMODE: binary format with null indicators
.EXPORT INDICATORMODE FILE = output.bin
```

**REPORT mode** is for human consumption (headers, alignment).  
**DATA mode** is for machine processing (CSV-like, field delimiters).

---

## Running BTEQ as a Batch Script

```bash
# Linux/Unix — pipe a script file to BTEQ
bteq < my_script.bteq

# Or with explicit file reference
bteq <<EOF
.LOGON server/user,pass;
SELECT CURRENT_DATE;
.LOGOFF;
EOF

# Capture return code
echo "BTEQ exit code: $?"
```

---

## BTEQ vs Other Teradata Tools

| Tool | Best For | Notes |
|---|---|---|
| **BTEQ** | SQL scripts, complex conditional logic | Universal, works everywhere |
| **FastLoad** | Bulk load into empty tables | High throughput, no DML |
| **MultiLoad** | DML (INSERT/UPDATE/DELETE) on existing tables | More complex, restartable |
| **FastExport** | Bulk export to flat files | Parallel, high throughput |
| **TPT** | Modern replacement for all of the above | Operator-based, parallel |

---


## ▶️ Try It Yourself

```bash
#!/usr/bin/env bteq
-- bteq_load_orders.btq

.LOGON my-teradata/etl_user,${TD_PASSWORD}
.SET ERROROUT STDERR
.SET MAXERROR 1

-- Run a SQL transformation
INSERT INTO silver.orders_cleaned
SELECT order_id, CAST(amount AS DECIMAL(12,2)), UPPER(region), CAST(order_date AS DATE FORMAT 'YYYY-MM-DD')
FROM raw.orders_staging
WHERE amount > 0;

-- Check row count
SELECT 'Rows loaded: ' || CAST(COUNT(*) AS VARCHAR(20)) FROM silver.orders_cleaned;

.IF ERRORCODE <> 0 THEN .QUIT ERRORCODE

.LOGOFF
.QUIT 0

# Run it:
# bteq < bteq_load_orders.btq 2>&1 | tee bteq_$(date +%Y%m%d).log
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What is BTEQ?" — "BTEQ is Teradata's command-line batch scripting tool for executing SQL and data operations. It supports conditional logic with ERRORCODE checking, file import/export, and can be automated as shell scripts. It's the most universal Teradata client — available wherever Teradata client libraries are installed."

> **Tip 2:** "How does error handling work in BTEQ?" — "After each statement, BTEQ sets the ERRORCODE variable. You use .IF ERRORCODE <> 0 THEN .GOTO HANDLE_ERROR to branch on failures. You exit with .QUIT n where n is the return code — non-zero return codes signal failures to the calling shell script or scheduler."

> **Tip 3:** "What are the different BTEQ export modes?" — "REPORT mode produces human-readable column-aligned output. DATA mode produces delimited raw data suitable for downstream processing. INDICATORMODE produces binary output with null-indicator bytes — used for high-performance data exchange with other Teradata utilities."
