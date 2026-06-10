---
title: "Teradata - FastLoad and MultiLoad Scenarios"
topic: teradata
subtopic: fastload-multiload
content_type: scenario_question
difficulty_level: senior
layer: scenarios
tags: [teradata, fastload, multiload, tpt, scenarios, bulk-load]
---

# FastLoad and MultiLoad — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Choose the Right Load Tool

You need to load 50 million rows of new customer data into Teradata. The target table `dim_customer` currently has 5 million existing rows. The file is a pipe-delimited CSV. You need the data loaded within 2 hours.

Which Teradata load tool should you use: BTEQ INSERT, FastLoad, or MultiLoad? Justify your choice.

<details>
<summary>💡 Hint</summary>

Consider: Is the table empty or does it have data? What DML type is needed? What are the throughput requirements?

</details>

<details>
<summary>✅ Solution</summary>

**Answer: MultiLoad (or TPT UPDATE operator)**

**Why not BTEQ INSERT:**
- 50 million single-row INSERTs would take 10-50 hours — far exceeds the 2-hour window
- BTEQ is single-threaded and processes each row individually

**Why not FastLoad:**
- `dim_customer` already has 5 million existing rows — FastLoad requires an **empty table**
- You cannot use FastLoad on a table with existing data

**Why MultiLoad:**
- Table has existing data ✓
- Need to INSERT new rows ✓
- 50M rows is well within MultiLoad's throughput capability (5-20M rows/hour)
- MultiLoad is designed exactly for this use case

**Practical implementation:**

```multiload
LOGON server/etl_user,pass;

BEGIN MLOAD INTO dim_customer
    WORKTABLES dim_customer_wt1
    ERRORTABLES dim_customer_et1, dim_customer_et2;

LAYOUT new_customers;
FIELD customer_id   * INTEGER;
FIELD customer_name * VARCHAR(100);
FIELD region        * VARCHAR(50);
FIELD signup_date   * DATE FORMAT 'YYYY-MM-DD';

TABLE dim_customer;

DML LABEL insert_customer;
INSERT INTO dim_customer VALUES (:customer_id, :customer_name, :region, :signup_date);

IMPORT INFILE /data/new_customers.dat
LAYOUT new_customers
APPLY insert_customer;

END MLOAD;
LOGOFF;
```

**Estimated time:**
- Phase 1 (Acquisition): ~40-60 minutes for 50M rows
- Phase 2 (Application): ~20-40 minutes with write lock
- Total: ~60-100 minutes — fits the 2-hour window ✓

**Alternative if you want maximum speed:** Load into a TEMPORARY empty table via FastLoad, then `INSERT INTO dim_customer SELECT * FROM temp_table` — this combines FastLoad speed with INSERT/SELECT for the final step. But this requires more steps and temporary storage.

</details>

</article>

---

<article data-difficulty="mid-level">

## Scenario 2: FastLoad Error Recovery

A FastLoad job loading 100 million rows into `fact_events` fails after processing 40 million rows. The error log shows:

```
Error 2644: No Unique Primary Index value.
...
Acquisition phase terminated. 40,000,000 rows loaded before error.
Error tables: fact_events_et (12 rows), fact_events_uv (847,293 rows)
```

The UV table has 847,293 rows. The ET table has only 12 rows. Describe your recovery plan and what the UV table content tells you.

<details>
<summary>✅ Solution</summary>

**Diagnosis:**

The UV table has 847,293 rows = **uniqueness violations**. The target table `fact_events` has a UPI (Unique Primary Index), and 847,293 rows in the input file had duplicate PI values. FastLoad correctly identified these and moved them to the UV table instead of loading them.

The fact that FastLoad *terminated* means `ERRLIMIT` was reached (the job was configured with an ERRLIMIT less than 847,293), or there was a different error triggering the abort.

**Step 1: Understand what's in the UV table**
```sql
-- Check what the duplicates look like
SELECT TOP 10 * FROM fact_events_uv ORDER BY 1;

-- How many distinct event_ids are duplicated?
SELECT COUNT(DISTINCT event_id) FROM fact_events_uv;
```

**Step 2: Investigate the data quality issue**
847K uniqueness violations in 100M rows = ~0.85% of data. This is significant — likely a data quality issue upstream:
- Duplicate rows in the source file?
- PI column (event_id) not actually unique?
- File concatenated twice accidentally?

```bash
# Check for duplicate event_ids in the source file
sort -t'|' -k1,1 /data/events.dat | awk -F'|' '{if ($1==prev) print; prev=$1}' | head -20
```

**Step 3: Recovery decision**

**Option A: Accept the UV rows as data quality issues, complete the load**
```sql
-- Check if the 40M rows already loaded are complete and usable
SELECT COUNT(*) FROM fact_events;  -- Should show 40M minus any et/uv rows

-- Drop error tables to allow restart
DROP TABLE fact_events_et;
DROP TABLE fact_events_uv;
```

Then fix the source file (deduplicate), and restart FastLoad. But first: should you truncate the 40M already-loaded rows?

- **If table was empty before:** Yes, the 40M rows are partial. Truncate (`DELETE FROM fact_events ALL`) and reload the full fixed file.
- **If partial load is acceptable for now:** You can load the remaining 60M rows (minus duplicates) separately, but this is risky for data integrity.

**Recommended recovery:**
```sql
-- 1. Drop error tables
DROP TABLE fact_events_et;
DROP TABLE fact_events_uv;

-- 2. Truncate the partially loaded table (clean start)
DELETE FROM fact_events ALL;
```

```bash
# 3. Fix the source file: deduplicate by event_id
sort -t'|' -k1,1n /data/events.dat | awk -F'|' '!seen[$1]++' > /data/events_deduped.dat

# Verify: original vs deduped count
wc -l /data/events.dat /data/events_deduped.dat
```

```fastload
-- 4. Restart FastLoad with deduped file and higher ERRLIMIT
SESSIONS 16;
ERRLIMIT 10;   -- Now we expect near-zero duplicates
...
FILE = /data/events_deduped.dat;
...
```

**Step 4: Fix the upstream process**
847K duplicates isn't a random anomaly — it points to a systematic issue:
- Add deduplication to the source extract query
- Add a data quality check: `SELECT event_id, COUNT(*) FROM source GROUP BY event_id HAVING COUNT(*) > 1` before generating the FastLoad file

</details>

</article>

---

<article data-difficulty="senior">

## Scenario 3: Designing a High-Throughput Load Pipeline

You are designing a data pipeline for a financial services firm that needs to:
- Load 5 billion trade records daily (arriving from 3 source systems)
- Each source delivers a flat file between midnight and 3 AM (staggered)
- Target table `fact_trades` has 50 billion existing records
- All trades must be in the table by 6 AM (market open)
- Failed loads must be retartable without human intervention where possible
- 5 million of the 5 billion records are updates to existing trades (amended trades)

Design the full load architecture, tool choices, parallelism strategy, and error handling.

<details>
<summary>💡 Hint</summary>

Think about: separating new inserts from updates, using the right tool for each, parallelism across source files, staging vs direct load, and automatic restart design.

</details>

<details>
<summary>✅ Solution</summary>

**Architecture Overview:**

```
Source System A (2B records) → /data/trades_src_a.dat (arrives 00:30)
Source System B (1.5B records) → /data/trades_src_b.dat (arrives 01:00)  
Source System C (1.5B records) → /data/trades_src_c.dat (arrives 01:30)
                ↓
    Separate: 4.995B new | 5M amended
                ↓
New Trades: FastLoad into NoPI staging tables (parallel)
Amendments: Collect for MultiLoad batch
                ↓
INSERT/SELECT: staging → fact_trades (redistributes by trade_id PI)
MultiLoad: Apply 5M amendments to fact_trades
```

**Phase 1: Parallel Staging Loads (00:30–03:00)**

Run FastLoad for each source file as it arrives (don't wait for all three):

```bash
# Shell: trigger FastLoad as each file arrives
inotifywait -m -e close_write /data/ | while read dir event file; do
    case $file in
        trades_src_a.dat) fastload < load_src_a.fl & ;;
        trades_src_b.dat) fastload < load_src_b.fl & ;;
        trades_src_c.dat) fastload < load_src_c.fl & ;;
    esac
done
```

Each FastLoad loads into a separate empty staging table with NoPI:

```fastload
-- load_src_a.fl
SESSIONS 32;
PACK 2000;
CHECKPOINT 10000000;   -- Checkpoint every 10M rows for restart

BEGIN LOADING stg_trades_src_a
    ERRORFILES stg_trades_a_et, stg_trades_a_uv;
...
```

**Phase 2: Validate Staging (03:00–03:15)**

```sql
-- Validate all three staging tables before proceeding
SELECT 'SRC_A' AS source, COUNT(*) AS rows FROM stg_trades_src_a
UNION ALL
SELECT 'SRC_B', COUNT(*) FROM stg_trades_src_b
UNION ALL
SELECT 'SRC_C', COUNT(*) FROM stg_trades_src_c;
-- Compare against expected row counts from source systems
```

```sql
-- Check error tables
SELECT 'SRC_A' AS source, COUNT(*) AS errors FROM stg_trades_a_et
UNION ALL SELECT 'SRC_B', COUNT(*) FROM stg_trades_b_et
UNION ALL SELECT 'SRC_C', COUNT(*) FROM stg_trades_c_et;
-- Fail pipeline if any errors > threshold
```

**Phase 3: Separate New from Amended (03:15–03:30)**

```sql
-- Separate new trades (not yet in fact_trades)
CREATE VOLATILE TABLE vt_new_trades AS (
    SELECT s.*
    FROM (
        SELECT * FROM stg_trades_src_a
        UNION ALL SELECT * FROM stg_trades_src_b
        UNION ALL SELECT * FROM stg_trades_src_c
    ) s
    LEFT JOIN fact_trades f ON s.trade_id = f.trade_id
    WHERE f.trade_id IS NULL   -- Not in fact table = new trade
) WITH DATA PRIMARY INDEX (trade_id) ON COMMIT PRESERVE ROWS;

-- Amended trades
CREATE VOLATILE TABLE vt_amended_trades AS (
    SELECT s.*
    FROM (SELECT * FROM stg_trades_src_a UNION ALL ...) s
    JOIN fact_trades f ON s.trade_id = f.trade_id
) WITH DATA PRIMARY INDEX (trade_id) ON COMMIT PRESERVE ROWS;
```

**Phase 4: Insert New Trades (03:30–04:30)**

```sql
-- Set-based INSERT/SELECT: Teradata redistributes by PI internally
INSERT INTO fact_trades
SELECT * FROM vt_new_trades;
-- ~5 billion rows, all-AMP parallel operation
-- Estimated: 60-90 minutes at 50-80M rows/min
```

**Why INSERT/SELECT instead of FastLoad here:**
- `fact_trades` has 50 billion existing rows — not empty, can't use FastLoad
- INSERT/SELECT is set-based and parallel across all AMPs
- Using volatile table with PI = trade_id ensures AMP-local joins for deduplication

**Phase 5: Apply Amendments (04:30–05:30)**

```bash
# Export vt_amended_trades to file for MultiLoad
fastexport < export_amended.fl  # ~5M rows, fast

# Run MultiLoad for amendments
multiload < apply_amendments.ml
```

```multiload
-- apply_amendments.ml
BEGIN MLOAD INTO fact_trades
    WORKTABLES fact_trades_wt1
    ERRORTABLES fact_trades_et1, fact_trades_et2;

DML LABEL update_trade TYPE UPDATE;
UPDATE fact_trades
SET status = :new_status, amended_ts = :amended_ts, ...
WHERE trade_id = :trade_id;

IMPORT INFILE /data/amended_trades.dat
LAYOUT amendment_layout
APPLY update_trade;

END MLOAD;
```

**Phase 6: Collect Statistics (05:30–06:00)**

```sql
COLLECT STATISTICS ON fact_trades COLUMN (trade_date);
COLLECT STATISTICS ON fact_trades COLUMN (PARTITION);
```

**Automatic Restart Design:**

- Staging FastLoad: `CHECKPOINT` ensures restart from last 10M-row checkpoint
- Phase 3 volatile tables: re-creatable from staging tables if lost
- INSERT/SELECT (Phase 4): preceded by `DELETE FROM fact_trades WHERE load_date = :today` for idempotency
- MultiLoad (Phase 5): work tables preserved for restart
- Pipeline checkpoints stored in `etl.pipeline_log` — restart logic re-runs only failed phases

**Total timeline:**
- 00:30–03:00: Parallel FastLoad staging (2.5 hours)
- 03:00–03:30: Validation and separation (30 min)
- 03:30–04:30: INSERT new trades (60 min)
- 04:30–05:30: MultiLoad amendments (60 min)
- 05:30–06:00: Statistics + validation (30 min)
- **Ready by 06:00 ✓**

**Risk mitigations:**
- Source file arrives late: monitor for files, alert if not present by 00:45
- FastLoad error: immediate alert, restart automatically from checkpoint
- INSERT/SELECT takes longer: validate row counts at 05:00, escalate if behind
- 30-minute buffer before market open: catches minor delays

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is FastLoad and what is it designed for?**
A: FastLoad is a Teradata utility for bulk-loading data from flat files into empty tables at high speed. It bypasses transient journaling and uses multiple parallel sessions to maximize throughput. Because it requires an empty target table, it's best suited for initial loads and staging table refreshes, not incremental updates.

**Q: What are the two phases of a FastLoad operation?**
A: Phase 1 (Acquisition): FastLoad sessions read the input file, hash each row's Primary Index, and send rows to the appropriate AMP. AMPs hold rows in a work table. Phase 2 (Application): AMPs move rows from the work table to the target table. Checkpointing occurs between phases to enable restart after failure.

**Q: What is MultiLoad and how does it differ from FastLoad?**
A: MultiLoad supports high-throughput INSERT, UPDATE, DELETE, and UPSERT operations on populated tables using multiple parallel sessions. Unlike FastLoad (which requires an empty table and only does inserts), MultiLoad can perform DML on existing data. It uses a "work table" and "error tables" to stage and validate changes before applying them.

**Q: What are the error tables in MultiLoad and what do they capture?**
A: MultiLoad creates two error tables: ET (error table) capturing rows that violated Unique Primary Index constraints, and UV (unique violation table) capturing duplicate primary index rows. After a load, you must check and drop these error tables before re-running or continuing. Rows in error tables are not loaded into the target.

**Q: What is the maximum number of sessions in FastLoad/MultiLoad and why does it matter?**
A: The session count affects parallelism—more sessions mean more AMPs receiving data simultaneously, up to one session per AMP. Diminishing returns occur beyond the number of AMPs. For very large loads, maximizing sessions (up to the AMP count) minimizes load time.

**Q: What is Teradata Parallel Transporter (TPT) and how does it supersede FastLoad/MultiLoad?**
A: TPT is a unified, scriptable framework that combines the functionality of FastLoad, MultiLoad, FastExport, and BTEQ into a single, parallelizable pipeline with a consistent scripting language. TPT supports streaming (reading from source and loading simultaneously), is more flexible, and is the modern recommended approach for production data movement on Teradata.

**Q: What are the table locking implications of FastLoad and MultiLoad?**
A: FastLoad places an exclusive lock on the target table for the duration of the load, preventing any other reads or writes. MultiLoad holds a write lock during the Apply phase. This means no concurrent access to the table during loading—plan loads during maintenance windows or use a staging table pattern to minimize impact.

**Q: How do you handle FastLoad restarts after a failure?**
A: FastLoad checkpoints progress between phases. If interrupted, restarting the FastLoad script detects the checkpoint and resumes from Phase 2 (Application) without re-reading the input file. The restart log must be intact—if it's deleted, the load cannot be restarted and must begin from scratch after dropping the work tables.

---

## 💼 Interview Tips

- Always explain the FastLoad constraint (empty target table) immediately—interviewers will probe whether you know the limitations. Jumping to "FastLoad for everything" without mentioning this signals junior experience.
- Distinguish the use cases clearly: FastLoad for initial bulk loads into staging tables, MultiLoad for incremental DML on populated tables, TPT for modern flexible pipelines. Interviewers want to see you match the tool to the job.
- Know the error table workflow in MultiLoad—checking error tables after every load is not optional. Many production data quality issues are silently dropped rows in error tables that teams never examined. Showing this awareness is a production readiness signal.
- Bring up table locking: loading a 1TB table with FastLoad blocks all readers for hours. Discuss strategies like loading into a staging table and swapping with RENAME TABLE to minimize production impact.
- Position TPT as the modern standard while showing you can maintain legacy FastLoad/MultiLoad scripts—this combination of legacy fluency and modern awareness is exactly what enterprises modernizing Teradata environments need.
