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
---

## ⚡ Quick-fire Q&A

**Q: What is Oracle Exadata and what makes it different from a standard Oracle database server?**
A: Exadata is an engineered system combining database servers, Smart Storage Servers (cells), and an InfiniBand interconnect optimized for Oracle workloads. Its key differentiator is Smart Scan—offloading filtering, column projection, and decompression to storage cells, dramatically reducing data transferred to database nodes.

**Q: What is Smart Scan and when does it activate?**
A: Smart Scan is Exadata's storage-layer offloading feature that executes predicate filtering and column projection in the storage cell, returning only matching rows/columns to the database node. It activates for full segment scans (full table scans, fast full index scans) over direct-path reads—it does NOT fire for single-block (index range scan) access.

**Q: What is Hybrid Columnar Compression (HCC) and what are its compression tiers?**
A: HCC stores data in a columnar format within Oracle blocks, achieving far higher compression ratios than OLTP compression. Tiers: Query Low/High (balanced compression for analytics), Archive Low/High (maximum compression for infrequently accessed data). HCC requires Exadata or certain Oracle Cloud storage.

**Q: What is the InfiniBand network used for in Exadata?**
A: The internal InfiniBand fabric provides ultra-low-latency, high-bandwidth connectivity between database nodes and storage cells. It carries iDB (Intelligent Database) protocol traffic (Smart Scan results, RDMA I/O) and RAC Cache Fusion interconnect traffic, all on the same high-speed fabric.

**Q: What is Storage Index and how does it accelerate queries?**
A: A Storage Index is an in-memory data structure maintained in each storage cell that records the min/max values of frequently predicate-filtered columns per 1 MB storage region. If a query predicate falls outside a region's range, that region is skipped entirely—providing index-like benefit without actual index maintenance.

**Q: How does Exadata handle I/O Resource Management (IORM)?**
A: IORM allows administrators to allocate I/O bandwidth to different database services or consumer groups via DBRM plans. This prevents one workload (e.g., a runaway batch job) from saturating storage I/O and starving OLTP queries—critical in consolidated multi-tenant Exadata deployments.

**Q: What is the difference between Exadata X8 database servers and storage cells?**
A: Database servers (compute nodes) run Oracle Database instances, host the SGA, and execute SQL. Storage cells (Smart Storage Servers) run the Exadata Storage Server Software (CELLSRV), manage flash and disk, execute Smart Scan offloading, and serve I/O requests via iDB protocol. The two tiers communicate over InfiniBand.

**Q: How does Exadata improve RAC performance compared to commodity hardware?**
A: Exadata's InfiniBand interconnect provides ~40 Gb/s with microsecond latency for Cache Fusion traffic, vastly outperforming standard 10 GbE. Smart Scan reduces the volume of data nodes must transfer over the interconnect. Together, they make inter-node block shipping in RAC far more efficient.

---

## 💼 Interview Tips

- Interviewers test whether you know Smart Scan's activation conditions—not every query benefits. State clearly: Smart Scan requires full segment scan + direct-path read. Index-based access does not use it.
- When discussing HCC, always pair it with the DML caveat: HCC-compressed blocks must be decompressed before row-level DML, making it unsuitable for hot OLTP tables but ideal for historical/archival data.
- Senior Exadata questions often involve diagnosing why Smart Scan is not firing. Walk through the checklist: direct-path reads enabled? predicate pushdown supported? storage cell software version compatible? table in non-default storage?
- Demonstrate IORM knowledge for multi-workload scenarios—if a DW query is impacting OLTP latency, IORM is the lever, not just killing the query.
- Connect Exadata to cloud: Oracle Exadata Cloud Service (ExaCS) and Exadata Cloud@Customer are common paths. Showing awareness of the cloud deployment model is a differentiator for 2024+ roles.
