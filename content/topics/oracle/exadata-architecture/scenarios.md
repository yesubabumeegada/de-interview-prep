---
title: "Exadata Architecture — Scenarios"
topic: oracle
subtopic: exadata-architecture
content_type: scenario_question
tags: [oracle, exadata, interview, scenarios, performance, architecture]
---

# Exadata Architecture — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: Why Is Smart Scan Not Working?

**Scenario:** A query scanning 500GB of sales data on Exadata takes 20 minutes. A colleague says "Smart Scan should make this fast." But `io_cell_offload_eligible_bytes` in V$SQL is 0. What are the possible reasons?

<details>
<summary>💡 Hint</summary>

Smart Scan requires four conditions to be true simultaneously: (1) the table must physically live on Exadata storage cells (not a regular SAN), (2) the query must do a full or large range scan — small indexed lookups go through the buffer cache instead, (3) the table must NOT be cached in the buffer cache (cached objects use memory, not Smart Scan), and (4) the query must pass through Direct Path IO. Check `v$sql.io_cell_offload_eligible_bytes` — if it's 0, start with whether the data file is on ASM on the Exadata cells.

</details>

<details>
<summary>✅ Solution</summary>

**Check each reason systematically:**

```sql
-- 1. Is the table actually on Exadata storage?
SELECT segment_name, tablespace_name
FROM dba_segments WHERE segment_name = 'SALES';

SELECT name, type FROM v$tablespace t
JOIN v$datafile d ON t.ts# = d.ts#
WHERE t.name = <the tablespace name>;
-- Data file path should be on +DATA (ASM diskgroup on Exadata cells)
-- If path is like /u01/oradata/... it's NOT on Exadata cells
```

```sql
-- 2. Is the query doing a full scan?
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR('sql_id', 0, 'BASIC'));
-- Must show TABLE ACCESS FULL or PARTITION RANGE ALL
-- If it shows INDEX RANGE SCAN → Smart Scan won't activate (index scans are selective/random)
```

```sql
-- 3. Is offloading enabled?
SELECT value FROM v$parameter WHERE name = 'cell_offload_processing';
-- Must be TRUE

-- Check session level
SHOW PARAMETER cell_offload_processing
```

```sql
-- 4. Is the table too small?
SELECT blocks FROM dba_tables WHERE table_name = 'SALES';
-- Smart Scan minimum threshold: ~32-128MB
-- Small tables just use buffer cache
```

**Most common fix for the scenario:** The table exists in a non-Exadata tablespace (e.g., migrated from legacy storage but not placed on ASM). 

```sql
-- Fix: Move table to Exadata ASM-backed tablespace
ALTER TABLE sales MOVE TABLESPACE tbs_exadata_data;
ALTER INDEX idx_sales_date REBUILD TABLESPACE tbs_exadata_idx;
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design the Compression Strategy

**Scenario:** You have a 10TB sales table on Exadata with 5 years of data. Current disk usage is near capacity. No data older than 1 year is ever updated. How do you design the compression strategy?

<details>
<summary>💡 Hint</summary>

**Partitioning assumption:** Monthly range partitions (best practice for time-series on Exadata)

</details>

<details>
<summary>✅ Solution</summary>

**Partitioning assumption:** Monthly range partitions (best practice for time-series on Exadata)

**Compression tiers:**
```
Current month  → No compression (active DML)
1-3 months old → COMPRESS FOR OLTP (minimal overhead, 2-4× ratio)
3-12 months    → COMPRESS FOR QUERY HIGH (10-15×, read workloads ok, no updates)
> 1 year       → COMPRESS FOR ARCHIVE HIGH (15-50×, pure archival)
```

**Implementation:**
```sql
-- Apply compression by partition age
-- Run this monthly as a maintenance job

DECLARE
  PROCEDURE compress_partition(p_name IN VARCHAR2, p_for IN VARCHAR2) IS
  BEGIN
    EXECUTE IMMEDIATE
      'ALTER TABLE SALES_SCHEMA.SALES MOVE PARTITION ' || p_name ||
      ' COMPRESS FOR ' || p_for ||
      ' ONLINE UPDATE INDEXES PARALLEL 8';
    DBMS_OUTPUT.PUT_LINE('Compressed ' || p_name || ' FOR ' || p_for);
  END;
BEGIN
  -- Compress previous month to OLTP
  compress_partition('P' || TO_CHAR(ADD_MONTHS(SYSDATE,-1),'YYYY_MM'), 'OLTP');
  
  -- Compress 3 months ago to QUERY HIGH
  compress_partition('P' || TO_CHAR(ADD_MONTHS(SYSDATE,-3),'YYYY_MM'), 'QUERY HIGH');
  
  -- Compress 12 months ago to ARCHIVE HIGH
  compress_partition('P' || TO_CHAR(ADD_MONTHS(SYSDATE,-12),'YYYY_MM'), 'ARCHIVE HIGH');
END;
/
```

**Expected outcome:**
- 10TB current → ~2TB after compression (average 5× ratio with mixed tiers)
- HCC compressed partitions also benefit from better Smart Scan (less data to transfer even with 100% filter)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Exadata vs Cloud for a New DW

**Scenario:** Your company is building a new data warehouse with 50TB initial data, 100 analysts, mixed ad-hoc + nightly batch workloads, Oracle database preferred. Should you go with Exadata on-premises, Exadata Cloud Service (ExaCS), or move to Autonomous Data Warehouse (ADW)?

<details>
<summary>💡 Hint</summary>

Evaluate on three axes: *control vs operations*, *economics at your data scale*, and *workload fit*. On-premises Exadata gives maximum control but requires hardware refresh cycles and DBA headcount. ExaCS is Exadata in Oracle's cloud — same performance characteristics, elastic sizing, but you still manage the database. ADW is fully managed (Oracle runs everything) with auto-tuning but you lose deep control over SQL plans and resource management. At 50TB with mixed workloads, the key question is whether you have the DBA headcount to justify on-prem, and whether your workloads need raw Exadata capabilities (Smart Scan, HCC compression) that ADW abstracts away.

</details>

<details>
<summary>✅ Solution</summary>

**Evaluation framework:**

| Criterion | Exadata On-Prem | ExaCS (Exadata Cloud) | ADW |
|

</details>

</article>