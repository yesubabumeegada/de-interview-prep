---
title: "Exadata Architecture — Real World"
topic: oracle
subtopic: exadata-architecture
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [oracle, exadata, production, optimization, migration, monitoring]
---

# Exadata Architecture — Real World Patterns

## Pattern 1: Migrating a DW to Exadata — Compression Strategy

```sql
-- Phase 1: Assess current storage and identify compression candidates
SELECT 
  owner, segment_name AS table_name, 
  ROUND(SUM(bytes)/1e9, 2) size_gb,
  -- Check current compression
  NVL(t.compress_for, 'NONE') compression
FROM dba_segments s
LEFT JOIN dba_tables t ON s.owner = t.owner AND s.segment_name = t.table_name
WHERE s.segment_type IN ('TABLE', 'TABLE PARTITION')
  AND s.owner = 'DW_SCHEMA'
GROUP BY s.owner, s.segment_name, t.compress_for
HAVING SUM(bytes)/1e9 > 0.5  -- tables > 500MB
ORDER BY size_gb DESC;

-- Phase 2: Compress historical partitions (no longer updated)
-- For each historical partition (> 3 months old):
ALTER TABLE sales_dw MOVE PARTITION p2022_q4
  COMPRESS FOR QUERY HIGH 
  TABLESPACE tbs_compressed
  PARALLEL 16
  ONLINE                    -- keep table accessible
  UPDATE INDEXES;

-- Phase 3: Verify compression ratio achieved
SELECT table_name, partition_name, compression, compress_for,
       ROUND(blocks * 8192 / 1e9, 3) compressed_gb
FROM dba_tab_partitions
WHERE table_name = 'SALES_DW' AND owner = 'DW_SCHEMA'
ORDER BY partition_position;

-- Phase 4: Ongoing maintenance — compress new partitions after load
-- (Called from ETL procedure after each monthly load)
CREATE OR REPLACE PROCEDURE compress_completed_partition(
  p_partition IN VARCHAR2
) IS
BEGIN
  EXECUTE IMMEDIATE
    'ALTER TABLE DW_SCHEMA.SALES_DW MOVE PARTITION ' || p_partition ||
    ' COMPRESS FOR QUERY HIGH ONLINE UPDATE INDEXES PARALLEL 8';
  DBMS_OUTPUT.PUT_LINE('Compressed: ' || p_partition);
END;
/
```

---

## Pattern 2: Smart Scan Monitoring Dashboard

```sql
-- Daily Smart Scan efficiency report
-- (Run as part of daily health check)
CREATE OR REPLACE VIEW v_exadata_smartscan_report AS
SELECT
  TRUNC(s.last_active_time) report_date,
  s.parsing_schema_name schema_nm,
  COUNT(*) unique_sqls,
  SUM(s.executions) total_executions,
  ROUND(SUM(s.io_cell_offload_eligible_bytes)/1e12, 3) eligible_tb,
  ROUND(SUM(s.io_cell_offload_returned_bytes)/1e12, 3) returned_tb,
  ROUND(100 * (1 - SUM(s.io_cell_offload_returned_bytes) /
                   NULLIF(SUM(s.io_cell_offload_eligible_bytes), 0)), 1) offload_efficiency_pct,
  ROUND(SUM(s.elapsed_time)/1e9/NULLIF(SUM(s.executions),0), 2) avg_elapsed_sec
FROM v$sql s
WHERE s.io_cell_offload_eligible_bytes > 0
  AND s.parsing_schema_name NOT IN ('SYS', 'SYSTEM')
GROUP BY TRUNC(s.last_active_time), s.parsing_schema_name
ORDER BY report_date DESC, offload_efficiency_pct;

SELECT * FROM v_exadata_smartscan_report;
```

---

## Pattern 3: Exadata Health Checks

```sql
-- 1. Check cell disk availability
SELECT cell_name, disk_name, status, error_count, 
       ROUND(hard_disk_bytes/1e12, 2) disk_tb
FROM v$asm_disk d
JOIN v$asm_diskgroup g ON d.group_number = g.group_number
WHERE g.name = 'DATA'
ORDER BY cell_name, disk_name;

-- 2. Check InfiniBand throughput (should be near 100Gb/s per link)
SELECT metric_name, value, unit
FROM v$cell_global
WHERE metric_name IN ('IB_BYTES_READ', 'IB_BYTES_WRITTEN')
ORDER BY metric_name;

-- 3. Storage cell alert history
SELECT begin_time, metric_name, metric_value, alert_state
FROM v$cell_metric_history
WHERE alert_state != 'normal'
  AND begin_time > SYSDATE - 7
ORDER BY begin_time DESC;

-- 4. Flash cache exhaustion check
SELECT cell_name,
       ROUND(fc_bytes_used/1e12, 2) used_tb,
       ROUND(fc_bytes_total/1e12, 2) total_tb,
       ROUND(fc_bytes_used * 100.0 / NULLIF(fc_bytes_total, 0), 1) pct_full
FROM (
  SELECT cell_name,
         SUM(CASE WHEN metric_name='FC_BYTES_USED' THEN metric_value END) fc_bytes_used,
         SUM(CASE WHEN metric_name='FC_BYTES_TOTAL' THEN metric_value END) fc_bytes_total
  FROM v$cell_global_history
  WHERE metric_name IN ('FC_BYTES_USED','FC_BYTES_TOTAL')
    AND begin_time > SYSDATE - 1/24  -- last hour
  GROUP BY cell_name
);
-- Alert if pct_full > 95% — need to review CELL_FLASH_CACHE object settings
```

---

## Common Exadata Issues and Fixes

| Issue | Symptom | Fix |
|---|---|---|
| Smart Scan not triggering | Full table scan but `io_cell_offload_eligible_bytes` = 0 | Verify table is on Exadata storage, full scan is used, `CELL_OFFLOAD_PROCESSING=TRUE` |
| HCC update performance | Updates are very slow on compressed tables | Move hot partitions to OLTP or no compression; keep HCC for cold read-only partitions |
| Flash cache not helping | Flash hit rate < 50% | Review CELL_FLASH_CACHE settings; mark hot tables KEEP, large scan tables NONE |
| IORM not limiting batch | Batch jobs still hammering storage | Check IORM plan is ACTIVE; verify consumer groups are mapped correctly |
| Cell disk failure | ASM degraded warning | Replace disk; ASM rebalances automatically; monitor `v$asm_operation` |

---

## Interview Tips

> **Tip 1:** "How would you verify that a query is using Smart Scan?" — Check `io_cell_offload_eligible_bytes` and `io_cell_offload_returned_bytes` in V$SQL for the SQL_ID. If `eligible_bytes > 0`, Smart Scan was attempted. The offload ratio `(1 - returned/eligible) * 100%` shows how much filtering happened at the cell. Also look at `CELL_SMART_TABLE_SCAN` statistic in V$MYSTAT after running the query.

> **Tip 2:** "A critical ETL on Exadata runs slower after adding new data. What do you check?" — First: check if partition pruning still works (plan shows Pstart/Pstop). Check if new data compressed or uncompressed. Verify Smart Scan efficiency vs before. Check if statistics are stale (new partitions may not have been analyzed). Check if the Flash Cache is evicting hot data (cache too small for the larger dataset). Check IORM — is another workload consuming I/O?

> **Tip 3:** "What's the difference between running a DW workload on Exadata vs on Snowflake?" — Exadata: Oracle-specific, on-premises or Oracle Cloud, excellent Smart Scan for full scans, HCC compression for 10-50×, tight integration with Oracle RAC for HA. Snowflake: multi-cloud, truly elastic (separate compute/storage scaling), simpler operations, auto-clustering instead of manual partitioning, micro-partitioning instead of HCC. For Oracle-heavy shops with existing licenses, Exadata often wins on raw performance and cost. For greenfield, Snowflake offers simpler ops and flexible scaling.
