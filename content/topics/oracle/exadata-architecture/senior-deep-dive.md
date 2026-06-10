---
title: "Exadata Architecture — Senior Deep Dive"
topic: oracle
subtopic: exadata-architecture
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [oracle, exadata, in-memory, rac-on-exadata, smart-flash-log, consolidation]
---

# Exadata Architecture — Senior Deep Dive

## Exadata + In-Memory Database (IMCS)

Oracle In-Memory Column Store (IMCS) on Exadata combines the columnar in-memory format with Exadata's parallel processing:

```sql
-- Enable In-Memory on Exadata
-- Size In-Memory area (typically 10-30% of RAM per DB server)
ALTER SYSTEM SET inmemory_size = 100G SCOPE=SPFILE;
-- Restart required to allocate the IMCS pool

-- Populate frequently-accessed tables in memory
ALTER TABLE dim_products INMEMORY PRIORITY CRITICAL;  -- populate immediately
ALTER TABLE sales INMEMORY PRIORITY HIGH;              -- populate soon
ALTER TABLE sales_history INMEMORY PRIORITY LOW;       -- populate when space available
ALTER TABLE archive_data NO INMEMORY;                  -- never cache

-- Populate specific columns (exclude rarely-queried ones to save space)
ALTER TABLE sales INMEMORY MEMCOMPRESS FOR QUERY HIGH
  NO INMEMORY (order_text, notes);  -- exclude large LOB-like columns

-- Check IMCS population status
SELECT owner, segment_name, inmemory_size / 1e9 imcs_gb, bytes_not_populated,
       populate_status, con_id
FROM v$im_segments
ORDER BY inmemory_size DESC;

-- Query performance: IMCS vs buffer cache
SELECT name, value
FROM v$mystat m JOIN v$statname n ON m.statistic# = n.statistic#
WHERE name IN ('IM scan CUs columns accessed',
               'IM scan CUs pruned',
               'IM scan rows projected')
  AND value > 0;
```

---

## Exadata Database Consolidation

Running multiple databases on one Exadata (multi-tenancy pattern):

```sql
-- CDB (Container Database) + PDBs on Exadata
-- Each PDB is an isolated database; all share the Exadata hardware

-- Check PDBs in a CDB
SELECT con_id, name, open_mode, restricted
FROM v$pdbs
ORDER BY con_id;

-- Allocate resources per PDB using Resource Manager
BEGIN
  DBMS_RESOURCE_MANAGER.CREATE_CDB_PLAN(
    plan    => 'EXADATA_CDB_PLAN',
    comment => 'Resource plan for Exadata multi-tenant'
  );
  
  -- Give OLTP_PDB 60% of shares
  DBMS_RESOURCE_MANAGER.CREATE_CDB_PLAN_DIRECTIVE(
    plan                  => 'EXADATA_CDB_PLAN',
    pluggable_database    => 'OLTP_PDB',
    shares                => 6,
    utilization_limit     => 80   -- max 80% of CPU
  );
  
  -- Give REPORTING_PDB 30% of shares
  DBMS_RESOURCE_MANAGER.CREATE_CDB_PLAN_DIRECTIVE(
    plan               => 'EXADATA_CDB_PLAN',
    pluggable_database => 'REPORTING_PDB',
    shares             => 3,
    utilization_limit  => 60
  );
  
  DBMS_RESOURCE_MANAGER.VALIDATE_PENDING_AREA;
  DBMS_RESOURCE_MANAGER.SUBMIT_PENDING_AREA;
END;
/

-- Apply the plan
ALTER SYSTEM SET RESOURCE_MANAGER_PLAN = 'EXADATA_CDB_PLAN';
```

---

## Exadata Smart Flash Log

Smart Flash Log uses flash devices for redo log writes, reducing log write latency:

```sql
-- Smart Flash Log is automatic on Exadata — no configuration needed
-- Monitor redo log write performance
SELECT event, total_waits, time_waited/100 time_sec,
       average_wait/100 avg_wait_ms
FROM v$system_event
WHERE event IN ('log file sync', 'log file parallel write')
ORDER BY time_waited DESC;

-- 'log file sync': sessions waiting for commit to complete
-- On Exadata with Smart Flash Log: avg_wait_ms should be < 1ms
-- Non-Exadata spinning disk: typically 2-10ms
-- This is 2-10× improvement in commit latency

-- Flash log usage
SELECT name, value, unit
FROM v$cell_global
WHERE name LIKE '%REDO%' OR name LIKE '%LOG%FLASH%'
ORDER BY name;
```

---

## Exadata Elastic Configurations (X8M+)

Modern Exadata X8M uses RDMA over Converged Ethernet (RoCE) instead of InfiniBand:

```
Traditional Exadata:
  DB Server → InfiniBand switch → Storage Cell

X8M Exadata (RDMA):
  DB Server → Ethernet (RoCE) → Persistent Memory (PMem) in Storage Cell
  DB Server can read/write cell PMem with RDMA — bypasses CPU on storage cell
  → 1-microsecond I/O latency (vs ~100 microseconds for SSD)
```

```sql
-- Check if RDMA is in use (X8M+)
SELECT name, value
FROM v$cell_global
WHERE name LIKE '%RDMA%' OR name LIKE '%PMEM%'
ORDER BY name;

-- Monitor PMem (persistent memory) utilization
SELECT cell_name, 
       ROUND(pmem_used_gb, 2), 
       ROUND(pmem_total_gb, 2),
       ROUND(pmem_used_gb * 100.0 / NULLIF(pmem_total_gb, 0), 1) pct_used
FROM v$cell_pmem_usage;  -- actual view name varies by version
```

---

## Capacity Planning and Sizing

```sql
-- Exadata sizing analysis: estimate compression savings
SELECT 
  segment_name AS table_name,
  ROUND(SUM(bytes) / 1e9, 2) current_size_gb,
  -- Estimate HCC QUERY HIGH: ~10× compression
  ROUND(SUM(bytes) / 1e9 / 10, 2) estimated_hcc_gb,
  -- Estimated Flash Cache needed: ~20% of hot data in cache
  ROUND(SUM(bytes) / 1e9 * 0.20, 2) flash_cache_est_gb
FROM dba_segments
WHERE owner = 'DW_SCHEMA'
  AND segment_type IN ('TABLE', 'TABLE PARTITION')
GROUP BY segment_name
HAVING SUM(bytes) > 1e9  -- tables > 1GB
ORDER BY current_size_gb DESC;

-- Smart Flash Cache efficiency analysis
-- Compare I/O from Flash vs Disk for each table
SELECT object_name, 
       ROUND(flash_cache_reads / 1e6, 2) flash_reads_m,
       ROUND(disk_cache_reads / 1e6, 2) disk_reads_m,
       ROUND(flash_cache_reads * 100.0 / 
             NULLIF(flash_cache_reads + disk_cache_reads, 0), 1) flash_hit_pct
FROM v$segment_statistics
WHERE statistic_name IN ('physical reads cache', 'physical reads cache prefetch')
  AND owner = 'DW_SCHEMA'
  AND flash_cache_reads > 0
ORDER BY flash_hit_pct;
```

---

## Interview Tips

> **Tip 1:** "How does Oracle In-Memory Column Store interact with Exadata?" — IMCS stores data columnarly in DRAM on the database servers, bypassing I/O entirely for hot data. On Exadata, you get both: IMCS for the hottest data (zero I/O, sub-millisecond queries) and Exadata Smart Scan for data that doesn't fit in memory (still faster than traditional storage due to offloading). The optimizer automatically chooses between IMCS access and Smart Scan based on the statistics and whether the segment is in memory.

> **Tip 2:** "What is the key architectural difference between Exadata X8 and X8M?" — X8M introduced RDMA over Converged Ethernet (RoCE) and Persistent Memory (PMem) on the storage cells. With RDMA, the database server's CPU can directly read/write storage cell PMem without involving the storage cell's CPU — eliminating the inter-process communication overhead. This brings OLTP single-block I/O latency from ~100 microseconds (SSD) to ~1-2 microseconds (PMem via RDMA), a 50-100× improvement relevant for OLTP commit latency.

> **Tip 3:** "When would you recommend Exadata vs cloud alternatives like AWS RDS or Snowflake?" — Exadata excels when: (1) you need extreme Oracle-specific performance (Smart Scan, HCC, SPM stability), (2) migrating an existing large Oracle OLTP + DW workload where Oracle licensing is already sunk, (3) compliance requires on-premises data. Cloud alternatives win when: (1) you want managed services with auto-scaling, (2) workloads are not Oracle-specific, (3) cost per query matters more than raw performance. Exadata Cloud (ExaCS, ExaCC) bridges this gap — Exadata infrastructure in Oracle Cloud.

## ⚡ Cheat Sheet

**Key Exadata Offload Features**
- Smart Scan: WHERE/JOIN/GROUP predicates pushed to Storage Cells; only relevant rows returned via Exadata Smart Scan API
- Storage Index: in-memory min/max per 1 MB region; eliminates I/O for non-matching regions without consuming SGA
- Hybrid Columnar Compression (HCC): compress by column within CU (~1 MB); `QUERY HIGH` ~10-15x, `ARCHIVE HIGH` ~50x
- Smart Flash Log: log write mirrored to NAND flash; `log file sync` waits drop dramatically
- IORM (I/O Resource Manager): per-database/consumer-group I/O throttling on storage layer

**Hardware Tiers (Full Rack)**
| Component | Count | Role |
|---|---|---|
| DB Servers | 8 | Oracle instances, RAC nodes |
| Storage Cells | 14 | Smart Scan, HCC, Storage Index |
| InfiniBand switches | 3 | 40 Gb/s internal fabric |

**HCC Compression Decision**
- `QUERY LOW` / `QUERY HIGH`: good for DW/reporting (still allows DML but with overhead)
- `ARCHIVE LOW` / `ARCHIVE HIGH`: max compression; best for cold/historical partitions
- Never use HCC on OLTP hot rows — row-level locking requires decompression; use OLTP compression instead
- Check: `SELECT compression, compress_for FROM dba_tables WHERE table_name='...'`

**Smart Scan Eligibility Rules**
- Must be a Full Table Scan or Fast Full Index Scan (no single-block I/O)
- Table must be on Exadata storage (not OS file, NFS, etc.)
- `CELL_OFFLOAD_PROCESSING=TRUE` (session or system level)
- Inhibited by: `ROWID` access, object stored in SECUREFILE with encryption

**Monitoring Commands**
- `SELECT * FROM v$cell_state` — cell health
- `SELECT * FROM v$sql_plan WHERE operation='TABLE ACCESS' AND options='STORAGE FULL'` — Smart Scan plans
- `SELECT name,value FROM v$sysstat WHERE name LIKE 'cell%'` — offload statistics
- AWR's "Cell" section shows Smart Scan efficiency ratio; target > 95%
