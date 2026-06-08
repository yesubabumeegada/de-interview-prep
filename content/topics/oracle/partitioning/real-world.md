---
title: "Partitioning — Real World"
topic: oracle
subtopic: partitioning
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [oracle, partitioning, production, lifecycle, bulk-load, monitoring]
---

# Partitioning — Real World Patterns

## Pattern 1: Monthly Partition Maintenance Script

```plsql
-- Monthly task: add next month's partition, archive old partitions
CREATE OR REPLACE PROCEDURE monthly_partition_maintenance(
  p_table_owner IN VARCHAR2 DEFAULT 'SALES_SCHEMA',
  p_table_name  IN VARCHAR2 DEFAULT 'SALES'
) IS
  v_next_month    DATE := ADD_MONTHS(TRUNC(SYSDATE, 'MM'), 1);
  v_next_part_nm  VARCHAR2(30) := 'P' || TO_CHAR(v_next_month, 'YYYY_MM');
  v_archive_cutoff DATE := ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -13);  -- 13 months ago
  v_exists        NUMBER;
BEGIN
  -- 1. Add next month's partition (if not using INTERVAL)
  SELECT COUNT(*) INTO v_exists
  FROM dba_tab_partitions
  WHERE table_owner = p_table_owner
    AND table_name  = p_table_name
    AND partition_name = v_next_part_nm;
  
  IF v_exists = 0 THEN
    EXECUTE IMMEDIATE
      'ALTER TABLE ' || p_table_owner || '.' || p_table_name ||
      ' ADD PARTITION ' || v_next_part_nm ||
      ' VALUES LESS THAN (DATE ''' || TO_CHAR(ADD_MONTHS(v_next_month, 1), 'YYYY-MM-DD') || ''')';
    DBMS_OUTPUT.PUT_LINE('Added partition: ' || v_next_part_nm);
  ELSE
    DBMS_OUTPUT.PUT_LINE('Partition already exists: ' || v_next_part_nm);
  END IF;
  
  -- 2. Compress partitions older than 3 months
  FOR p IN (
    SELECT partition_name
    FROM dba_tab_partitions
    WHERE table_owner = p_table_owner
      AND table_name  = p_table_name
      AND compression != 'ENABLED'  -- not yet compressed
      AND TO_DATE(
            REGEXP_SUBSTR(high_value, '\d{4}-\d{2}-\d{2}'), 'YYYY-MM-DD'
          ) < ADD_MONTHS(TRUNC(SYSDATE, 'MM'), -3)
  ) LOOP
    BEGIN
      EXECUTE IMMEDIATE
        'ALTER TABLE ' || p_table_owner || '.' || p_table_name ||
        ' MOVE PARTITION ' || p.partition_name ||
        ' COMPRESS FOR QUERY HIGH ONLINE UPDATE INDEXES';
      DBMS_OUTPUT.PUT_LINE('Compressed: ' || p.partition_name);
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Could not compress ' || p.partition_name || ': ' || SQLERRM);
    END;
  END LOOP;
  
  COMMIT;
END monthly_partition_maintenance;
/
```

---

## Pattern 2: Parallel Bulk Load via Partition Exchange

```plsql
-- Build one staging table per partition, load in parallel, exchange all at once
-- This is the pattern for terabyte-scale loads (e.g., migrating 5 years of history)

CREATE OR REPLACE PROCEDURE load_year_via_exchange(
  p_year IN NUMBER
) IS
  v_stage_table VARCHAR2(30) := 'SALES_STAGE_' || p_year;
BEGIN
  -- 1. Create staging table (matches partition structure exactly)
  EXECUTE IMMEDIATE 'CREATE TABLE ' || v_stage_table || '
    AS SELECT * FROM SALES WHERE 1=0';
  
  -- 2. Load data with APPEND + PARALLEL (direct path write, fastest)
  EXECUTE IMMEDIATE '
    INSERT /*+ APPEND PARALLEL(8) NOLOGGING */ INTO ' || v_stage_table || '
    SELECT * FROM SALES_SOURCE_' || p_year;
  COMMIT;
  
  -- 3. Gather stats on staging table before exchange
  DBMS_STATS.GATHER_TABLE_STATS(
    ownname => USER, tabname => v_stage_table,
    estimate_percent => 20, degree => 8
  );
  
  -- 4. Exchange partition (atomic, instant)
  EXECUTE IMMEDIATE 
    'ALTER TABLE SALES EXCHANGE PARTITION P' || p_year ||
    ' WITH TABLE ' || v_stage_table ||
    ' INCLUDING INDEXES WITHOUT VALIDATION';
  
  DBMS_OUTPUT.PUT_LINE('Exchanged partition P' || p_year);
  
  -- 5. Drop staging table
  EXECUTE IMMEDIATE 'DROP TABLE ' || v_stage_table || ' PURGE';
  
EXCEPTION
  WHEN OTHERS THEN
    -- Cleanup staging table on failure
    BEGIN
      EXECUTE IMMEDIATE 'DROP TABLE ' || v_stage_table || ' PURGE';
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
    RAISE;
END load_year_via_exchange;
/
```

---

## Pattern 3: Partitioning Health Check

```sql
-- Check for partitions that need attention
SELECT
  table_name,
  partition_name,
  num_rows,
  blocks,
  ROUND(blocks * 8192 / 1024 / 1024 / 1024, 2) size_gb,
  compression,
  compress_for,
  last_analyzed,
  CASE
    WHEN last_analyzed IS NULL THEN '⚠ NO STATS'
    WHEN last_analyzed < SYSDATE - 7 AND num_rows > 1000000 THEN '⚠ STALE STATS'
    WHEN compression = 'DISABLED' AND ROUND(blocks * 8192 / 1024 / 1024 / 1024, 2) > 10
      THEN '⚠ LARGE UNCOMPRESSED'
    ELSE '✓ OK'
  END AS health_status
FROM dba_tab_partitions
WHERE table_name IN ('SALES', 'ORDER_EVENTS', 'TRANSACTIONS')
  AND table_owner = 'SALES_SCHEMA'
ORDER BY table_name, partition_position;

-- Find tables without partitioning that should be partitioned (>10GB)
SELECT owner, segment_name AS table_name, 
       ROUND(SUM(bytes)/1024/1024/1024, 2) size_gb
FROM dba_segments
WHERE segment_type = 'TABLE'
  AND owner NOT IN ('SYS','SYSTEM','DBSNMP')
  AND segment_name NOT IN (SELECT DISTINCT table_name FROM dba_tab_partitions)
GROUP BY owner, segment_name
HAVING SUM(bytes)/1024/1024/1024 > 10
ORDER BY size_gb DESC;
```

---

## Partitioning Anti-Patterns

| Anti-Pattern | What Happens | Better Approach |
|---|---|---|
| Partitioning a 100MB table | No benefit; overhead for metadata | Partition only when table > 2-5GB or for lifecycle management |
| Daily partitioning when monthly is enough | 1000+ partitions; DDL is slow | Use monthly or quarterly for large tables |
| No partition pruning in application queries | Full scan despite partitioning | Ensure WHERE clause uses the partition key column directly |
| Global indexes on every column | UPDATE GLOBAL INDEXES on partition ops becomes very slow | Use local indexes where possible; minimize global indexes |
| No MAXVALUE or DEFAULT partition | New values outside defined range cause ORA-14400 | Always add a catch-all partition |
| Forgetting ROW MOVEMENT | ORA-14402 when updating partition key | Enable if app updates the partition column |

---

## Interview Tips

> **Tip 1:** "A DBA wants to add partitioning to an existing 500GB unpartitioned table in production. How do you approach it?" — You can't add partitioning to an existing table in place. Options: (1) `CREATE TABLE ... PARTITION BY ... AS SELECT * FROM old_table` + rename (requires downtime/lock for large table), (2) `DBMS_REDEFINITION.START_REDEF_TABLE` — online redefinition that keeps the table available while building the partitioned version, (3) Exchange partition approach: create the partitioned table empty, load historical data partition-by-partition via exchange, then do a final delta sync. Always test the plan on a non-production copy first.

> **Tip 2:** "How do you validate a partition exchange didn't lose or corrupt data?" — Always run row count comparison: `SELECT COUNT(*) FROM staging` vs `SELECT COUNT(*) FROM partitioned_table PARTITION (pname)` after exchange. Validate key ranges: min/max of the partition key column. For critical loads, also compare checksums or sample row comparisons. Use `WITHOUT VALIDATION` only after this pre-validation is done on the staging table.

> **Tip 3:** "When would you choose INTERVAL partitioning over manually adding partitions?" — INTERVAL is better for append-only time-series tables where data arrives continuously — no risk of ORA-14400 from missing future partitions. Manual is better when: (1) you need custom partition names from the start (INTERVAL creates SYS_P names), (2) partition structure is irregular (e.g., fiscal quarters instead of calendar months), (3) you want explicit control over when partitions exist before data arrives.
