---
title: "Exadata Architecture — Intermediate"
topic: oracle
subtopic: exadata-architecture
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [oracle, exadata, iorm, smart-flash, asm, cell-offload, monitoring]
---

# Exadata Architecture — Intermediate

## I/O Resource Management (IORM)

Exadata allows controlling I/O at the storage cell level — prevents one workload from saturating storage and impacting others:

```sql
-- View current IORM plan
SELECT name, objective, status FROM v$iorm_plan;

-- View IORM consumer groups
SELECT name, allocation, limit FROM v$iorm_category;

-- Create an IORM plan (done via cellcli on storage cells, or via EM Cloud Control)
-- Example: prioritize OLTP over batch
-- In CellCLI on each storage cell:
-- ALTER IORMPLAN ACTIVE 
--   catplan=((name=oltp, level=1, allocation=70),
--             (name=batch, level=2, allocation=20),
--             (name=other, level=3, allocation=10));

-- Database-side: map consumer groups to IORM categories
BEGIN
  DBMS_RESOURCE_MANAGER.CREATE_PENDING_AREA;
  DBMS_RESOURCE_MANAGER.CREATE_PLAN_DIRECTIVE(
    plan             => 'EXADATA_PLAN',
    group_or_subplan => 'OLTP_GROUP',
    comment          => 'Online transaction workloads',
    mgmt_p1          => 70  -- 70% of CPU/IOPS
  );
  DBMS_RESOURCE_MANAGER.SUBMIT_PENDING_AREA;
END;
/
```

---

## Smart Flash Cache Configuration

```sql
-- Check if Smart Flash Cache is enabled and sized correctly
-- From the database (requires SYSDBA or SELECT privilege on v$cell)
SELECT cellname, fc_size_gb, fc_used_gb,
       ROUND(fc_used_gb * 100.0 / NULLIF(fc_size_gb, 0), 1) pct_used
FROM (
  SELECT cell_name AS cellname,
         SUM(CASE WHEN metric_name = 'FC_BYTES_USED' THEN metric_value ELSE 0 END) / 1e9 fc_used_gb,
         SUM(CASE WHEN metric_name = 'FC_BYTES_TOTAL' THEN metric_value ELSE 0 END) / 1e9 fc_size_gb
  FROM v$cell_global_history
  WHERE metric_name IN ('FC_BYTES_USED', 'FC_BYTES_TOTAL')
  GROUP BY cell_name
);

-- Control caching behavior for specific objects
-- Keep a frequently-accessed dimension table in flash cache
ALTER TABLE dim_products STORAGE (CELL_FLASH_CACHE KEEP);

-- Prevent large fact table scans from evicting valuable cached data  
ALTER TABLE sales_history STORAGE (CELL_FLASH_CACHE NONE);

-- Default behavior
ALTER TABLE orders STORAGE (CELL_FLASH_CACHE DEFAULT);
```

---

## ASM on Exadata

Exadata uses ASM (Automatic Storage Management) with specific disk groups optimized for the hardware:

```sql
-- Standard Exadata ASM disk group layout
-- DATA: primary data files (all cell disks)
-- RECO: FRA, archive logs, redo logs
-- DBFS_DG: optional - for database filesystem

-- Check ASM disk group utilization
SELECT group_number, name, state, type,
       ROUND(total_mb / 1024, 2) total_gb,
       ROUND(free_mb / 1024, 2) free_gb,
       ROUND((total_mb - free_mb) * 100.0 / NULLIF(total_mb, 0), 1) pct_used
FROM v$asm_diskgroup
ORDER BY name;

-- Check disk health
SELECT group_number, disk_number, name, path, mode_status,
       ROUND(total_mb / 1024, 2) total_gb,
       read_errs, write_errs
FROM v$asm_disk
WHERE group_number IN (SELECT group_number FROM v$asm_diskgroup)
ORDER BY group_number, disk_number;
```

---

## Monitoring Smart Scan Efficiency

```sql
-- Check if queries are benefiting from Smart Scan offloading
SELECT 
  s.sql_id,
  s.executions,
  -- I/O offloaded to cells (should be >> 0 for Smart Scan)
  ROUND(s.io_cell_offload_eligible_bytes / 1e9, 2) eligible_gb,
  ROUND(s.io_cell_offload_returned_bytes / 1e9, 2) returned_gb,
  -- Offload efficiency: how much data was filtered at cell vs sent to DB server
  CASE WHEN s.io_cell_offload_eligible_bytes > 0 THEN
    ROUND(100 * (1 - s.io_cell_offload_returned_bytes /
                     s.io_cell_offload_eligible_bytes), 1)
  ELSE 0 END AS offload_pct,
  SUBSTR(s.sql_text, 1, 80) sql_preview
FROM v$sql s
WHERE s.io_cell_offload_eligible_bytes > 0
ORDER BY s.io_cell_offload_eligible_bytes DESC
FETCH FIRST 20 ROWS ONLY;

-- Session-level Smart Scan statistics
SELECT name, value
FROM v$mystat m JOIN v$statname n ON m.statistic# = n.statistic#
WHERE name LIKE '%cell%'
  AND value > 0
ORDER BY value DESC;
```

---

## Exadata for ETL and DW Workloads

```sql
-- Pattern: parallel direct path load into HCC-compressed table
-- 1. Disable indexes (rebuild after load)
-- 2. APPEND hint: direct path write (bypasses buffer cache, goes straight to disk)
-- 3. PARALLEL: use all DB server CPUs

ALTER TABLE sales_dw NOLOGGING;  -- reduce redo generation

INSERT /*+ APPEND PARALLEL(16) */ INTO sales_dw
SELECT * FROM sales_staging_ext;  -- external table or staging table
COMMIT;

ALTER TABLE sales_dw LOGGING;

-- Rebuild indexes in parallel after bulk load
ALTER INDEX idx_sales_dw_date REBUILD PARALLEL 16 NOLOGGING;
ALTER INDEX idx_sales_dw_date LOGGING;  -- re-enable logging after rebuild

-- Compress the loaded data
ALTER TABLE sales_dw MOVE COMPRESS FOR QUERY HIGH PARALLEL 16;
-- If using partitioning: compress per partition in parallel
```

---

## Exadata-Specific V$ Views

```sql
-- Storage cell metrics
SELECT metric_name, metric_value, metric_unit
FROM v$cell_global
WHERE metric_name IN (
  'CELL_OFFL_ELIG_BYTES', 'CELL_OFFL_RET_BYTES',
  'CELL_SMART_TABLE_SCAN', 'CELL_SMART_INDEX_SCAN',
  'FC_IO_BYRDS', 'FC_IO_BYWTS'
)
ORDER BY metric_name;

-- Flash cache hit rate
SELECT 
  SUM(CASE WHEN metric_name = 'FC_IO_BYRDS' THEN metric_value ELSE 0 END) flash_reads,
  SUM(CASE WHEN metric_name = 'HD_IO_BYRDS' THEN metric_value ELSE 0 END) disk_reads,
  ROUND(
    SUM(CASE WHEN metric_name = 'FC_IO_BYRDS' THEN metric_value ELSE 0 END) * 100.0 /
    NULLIF(SUM(CASE WHEN metric_name IN ('FC_IO_BYRDS','HD_IO_BYRDS') THEN metric_value ELSE 0 END), 0),
    1
  ) flash_hit_pct
FROM v$cell_global;
```

---

## Interview Tips

> **Tip 1:** "How does IORM work on Exadata and why is it useful?" — I/O Resource Management (IORM) enforces I/O priorities at the storage cell level — not just at the database server. This is critical because even if you use Oracle Resource Manager to throttle CPU for a batch job, without IORM that batch job can still flood the storage cells with disk I/O, degrading OLTP response time. IORM ensures high-priority OLTP queries get their I/O bandwidth guaranteed even when batch workloads are running simultaneously.

> **Tip 2:** "What metrics tell you if Smart Scan is working well?" — Key metric: `IO_CELL_OFFLOAD_RETURNED_BYTES / IO_CELL_OFFLOAD_ELIGIBLE_BYTES`. A ratio of 5% means 95% of the data was filtered at the cell — excellent offload. A ratio of 90%+ means the predicates aren't filtering much (either the table is small, or the query returns most rows). Also check `CELL_SMART_TABLE_SCAN` in V$CELL_GLOBAL to confirm Smart Scans are executing.

> **Tip 3:** "When does Smart Scan NOT activate?" — Smart Scan requires a full table/partition scan. It won't activate for: index range scans (too random for Smart Scan), tables smaller than a few blocks, tables not on Exadata storage, queries with `ROWID` access, or when `CELL_OFFLOAD_PROCESSING = FALSE`. To verify, check `IO_CELL_OFFLOAD_ELIGIBLE_BYTES` in V$SQL — if 0, Smart Scan wasn't triggered.
