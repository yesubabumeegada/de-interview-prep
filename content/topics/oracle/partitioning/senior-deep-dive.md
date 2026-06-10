---
title: "Partitioning — Senior Deep Dive"
topic: oracle
subtopic: partitioning
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [oracle, partitioning, iops, segment-management, oltp-partitioning, lifecycle]
---

# Partitioning — Senior Deep Dive

## Partitioning Strategy for OLTP vs DW

### OLTP Considerations
In OLTP, most transactions access a small number of recent rows. Partitioning must not hurt single-row lookup performance:

```sql
-- BAD for OLTP: hash partitioning on PK causes index scans to go to multiple partitions
-- if the partition key isn't in the WHERE clause

-- GOOD: Use range-interval on date + local index on PK for OLTP
CREATE TABLE transactions (
  txn_id       NUMBER GENERATED ALWAYS AS IDENTITY,
  txn_date     DATE DEFAULT SYSDATE NOT NULL,
  account_id   NUMBER,
  amount       NUMBER(15,2),
  status       VARCHAR2(20)
)
PARTITION BY RANGE (txn_date) INTERVAL (NUMTODSINTERVAL(1, 'DAY'))
(PARTITION p_init VALUES LESS THAN (DATE '2024-01-01'))
ROW MOVEMENT DISABLE  -- for OLTP: prevent row migration if update changes partition key
ENABLE ROW MOVEMENT   -- enable only if updates to partition key are expected
;

-- Create local unique index (supports unique txn_id per partition — not global unique)
CREATE UNIQUE INDEX idx_txn_pk ON transactions(txn_id, txn_date) LOCAL;
-- Note: global unique index on txn_id only → needs UPDATE GLOBAL INDEXES on partition ops

-- For truly unique PK constraint without the date: use sequence + check constraint
ALTER TABLE transactions ADD CONSTRAINT pk_transactions PRIMARY KEY (txn_id)
  USING INDEX GLOBAL;  -- global index enforces uniqueness across all partitions
```

---

## Partition Lifecycle Management

For time-series data: automate the full partition lifecycle (create → warm → cold → archive → purge):

```plsql
CREATE OR REPLACE PROCEDURE manage_partition_lifecycle IS
  v_cutoff_compress DATE := ADD_MONTHS(SYSDATE, -3);  -- compress if > 3 months old
  v_cutoff_archive  DATE := ADD_MONTHS(SYSDATE, -12); -- move to archive TBS if > 1 year
  v_cutoff_purge    DATE := ADD_MONTHS(SYSDATE, -36); -- drop if > 3 years
  
  CURSOR c_old_partitions IS
    SELECT partition_name, high_value_date
    FROM (
      SELECT tp.partition_name,
             TO_DATE(
               REGEXP_SUBSTR(tp.high_value, '\d{4}-\d{2}-\d{2}'),
               'YYYY-MM-DD'
             ) AS high_value_date
      FROM dba_tab_partitions tp
      WHERE tp.table_name = 'SALES'
        AND tp.table_owner = 'SALES_SCHEMA'
    )
    WHERE high_value_date IS NOT NULL
    ORDER BY high_value_date;
BEGIN
  FOR p IN c_old_partitions LOOP
    IF p.high_value_date < v_cutoff_purge THEN
      -- Drop partition (>3 years old)
      DBMS_OUTPUT.PUT_LINE('Dropping partition: ' || p.partition_name);
      EXECUTE IMMEDIATE 'ALTER TABLE SALES_SCHEMA.SALES DROP PARTITION ' || p.partition_name ||
                        ' UPDATE GLOBAL INDEXES';
    ELSIF p.high_value_date < v_cutoff_archive THEN
      -- Move to archive tablespace (>1 year old)
      DBMS_OUTPUT.PUT_LINE('Archiving partition: ' || p.partition_name);
      EXECUTE IMMEDIATE 'ALTER TABLE SALES_SCHEMA.SALES MOVE PARTITION ' || p.partition_name ||
                        ' TABLESPACE TBS_ARCHIVE COMPRESS FOR QUERY HIGH ONLINE UPDATE INDEXES';
    ELSIF p.high_value_date < v_cutoff_compress THEN
      -- Compress in place (>3 months old, data no longer hot)
      DBMS_OUTPUT.PUT_LINE('Compressing partition: ' || p.partition_name);
      EXECUTE IMMEDIATE 'ALTER TABLE SALES_SCHEMA.SALES MOVE PARTITION ' || p.partition_name ||
                        ' COMPRESS FOR QUERY HIGH ONLINE UPDATE INDEXES';
    END IF;
  END LOOP;
  COMMIT;
END manage_partition_lifecycle;
/

-- Schedule this as a monthly DBMS_SCHEDULER job
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'PARTITION_LIFECYCLE_JOB',
    job_type        => 'STORED_PROCEDURE',
    job_action      => 'SALES_SCHEMA.MANAGE_PARTITION_LIFECYCLE',
    repeat_interval => 'FREQ=MONTHLY; BYDAY=1; BYHOUR=1',  -- 1am on 1st of month
    enabled         => TRUE
  );
END;
/
```

---

## Advanced Compression with Partitioning

```sql
-- Hybrid Columnar Compression (HCC) — Exadata/ZFS only
-- QUERY HIGH: best compression, read-only workloads
ALTER TABLE sales MOVE PARTITION p2021
COMPRESS FOR QUERY HIGH TABLESPACE tbs_cold;

-- ARCHIVE HIGH: maximum compression (slower reads)
ALTER TABLE sales MOVE PARTITION p2019
COMPRESS FOR ARCHIVE HIGH TABLESPACE tbs_archive;

-- OLTP compression: in-block compression for hot data (minimal CPU overhead)
ALTER TABLE sales MOVE PARTITION p2024_jan
COMPRESS FOR OLTP;

-- Check compression per partition
SELECT partition_name, compression, compress_for, blocks, 
       ROUND(blocks * 8192 / 1024 / 1024) size_mb
FROM dba_tab_partitions
WHERE table_name = 'SALES' AND owner = 'SALES_SCHEMA'
ORDER BY partition_position;
```

---

## Partition Pruning with Dynamic Predicates

```sql
-- Static pruning: literal value → partition numbers shown in plan
EXPLAIN PLAN FOR
SELECT * FROM sales WHERE sale_date = DATE '2024-06-15';
-- Pstart=5, Pstop=5 (exact partition number)

-- Dynamic pruning: bind variable → partition resolved at runtime
EXPLAIN PLAN FOR
SELECT * FROM sales WHERE sale_date = :dt;
-- Pstart=KEY, Pstop=KEY — pruning happens at execute time

-- No pruning: function on column prevents pruning
EXPLAIN PLAN FOR
SELECT * FROM sales WHERE TRUNC(sale_date, 'MM') = DATE '2024-06-01';
-- Pstart=1, Pstop=1000000 (ALL partitions scanned!)
-- Fix: WHERE sale_date >= DATE '2024-06-01' AND sale_date < DATE '2024-07-01'

-- Subquery pruning (12c+): partition pruning works through IN (subquery)
EXPLAIN PLAN FOR
SELECT * FROM sales WHERE sale_date IN (SELECT max_date FROM config WHERE key='REPORTING_DATE');
-- Pstart=KEY(SQ), Pstop=KEY(SQ) — subquery-based pruning
```

---

## Interval Partitioning Internals

```sql
-- INTERVAL partitions are created with system-generated names (SYS_P1, SYS_P2...)
-- Give them meaningful names by renaming after creation
ALTER TABLE sales RENAME PARTITION SYS_P123456 TO p2024_jul;

-- Or: set a naming convention using a trigger
CREATE OR REPLACE TRIGGER trg_rename_new_partition
AFTER INSERT ON sales
FOR EACH ROW
DECLARE
  v_part_name VARCHAR2(30);
BEGIN
  -- Note: this approach is complex — better to rename via maintenance job
  NULL;  -- actual implementation is more involved
END;
/

-- Better: Find and rename interval partitions in a maintenance script
BEGIN
  FOR p IN (
    SELECT partition_name, high_value
    FROM dba_tab_partitions
    WHERE table_name = 'SALES'
      AND partition_name LIKE 'SYS_P%'
  ) LOOP
    DECLARE
      v_new_name VARCHAR2(30);
      v_hv_date  DATE;
    BEGIN
      EXECUTE IMMEDIATE 'SELECT ' || p.high_value || ' FROM DUAL' INTO v_hv_date;
      v_new_name := 'P' || TO_CHAR(v_hv_date - 1, 'YYYY_MM');
      EXECUTE IMMEDIATE 'ALTER TABLE SALES RENAME PARTITION ' || p.partition_name ||
                        ' TO ' || v_new_name;
      DBMS_OUTPUT.PUT_LINE('Renamed ' || p.partition_name || ' → ' || v_new_name);
    EXCEPTION
      WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Could not rename ' || p.partition_name || ': ' || SQLERRM);
    END;
  END LOOP;
END;
/
```

---

## Partitioning Pitfalls

| Pitfall | Symptom | Fix |
|---|---|---|
| Global index not maintained | Query fails with ORA-01502 (unusable index) | Use `UPDATE GLOBAL INDEXES` or rebuild after partition operation |
| Function on partition key | All partitions scanned (no pruning) | Rewrite WHERE to use the column directly |
| Too many partitions | High partition overhead, slow DDL | Use monthly/quarterly instead of daily if not needed |
| Unique constraint on non-partition key | Forces global unique index | Evaluate if global uniqueness is truly needed |
| Row movement disabled + UPDATE changes partition key | ORA-14402 error | Enable ROW MOVEMENT if update can change partition key |
| Partition exchange WITH VALIDATION | Extremely slow for large tables | Use WITHOUT VALIDATION + pre-validate data |

---

## Interview Tips

> **Tip 1:** "How do you handle unique constraints on partitioned tables?" — A PRIMARY KEY or UNIQUE constraint on a partitioned table requires a global index (spans all partitions) to enforce cross-partition uniqueness. This creates global index maintenance burden on partition operations. Options: (1) include the partition key in the unique constraint (enables local index), (2) use sequence-generated synthetic keys and accept global index overhead, (3) enforce uniqueness in the application layer if acceptable.

> **Tip 2:** "How would you design a data retention policy for a 3-year partitioned table?" — Use partition lifecycle management: (1) current quarter: uncompressed on fast SSD, (2) 3-12 months: COMPRESS FOR OLTP on slower tier, (3) 1-3 years: COMPRESS FOR QUERY HIGH on archive tablespace, (4) >3 years: drop partition. Automate with a monthly DBMS_SCHEDULER job. For compliance: exchange old partitions to an archive database before dropping.

> **Tip 3:** "What is ROW MOVEMENT and when do you need it?" — ROW MOVEMENT allows Oracle to physically move a row to a different partition when an UPDATE changes the partition key value. By default it's disabled — an UPDATE that would change the partition key raises ORA-14402. Enable with `ALTER TABLE ... ENABLE ROW MOVEMENT` when your application does update the partition key column. Note: enabling row movement can change ROWIDs, breaking any application that caches ROWIDs.

## ⚡ Cheat Sheet

**Partition Type Decision**
| Type | Best For | Gotcha |
|---|---|---|
| RANGE | Time-series, rolling windows | Hot last partition on inserts |
| LIST | Discrete values (region, status) | Use DEFAULT for unknowns |
| HASH | Even distribution, no natural key | No partition pruning on range queries |
| RANGE-HASH (composite) | Time + hash distribution | Partition count = range × hash; can be large |
| INTERVAL | Auto-create range partitions on insert | Cannot be used as sub-partition type |

**Index Strategy**
- Local index: each partition has its own index segment; auto-maintained on partition ops; preferred for DW
- Global index: spans all partitions; required for unique constraints on non-partition key; must be rebuilt after partition drop/truncate unless `UPDATE GLOBAL INDEXES`
- Global partitioned index: global but partitioned by range/hash; better scalability than unpartitioned global

**Partition Pruning Rules**
- Pruning only works when the partition key column appears in WHERE with a literal or bind variable
- `TO_DATE(col)` or `TRUNC(col)` applied to partition key prevents pruning — store dates as DATE type
- Verify pruning: `EXPLAIN PLAN` → `PARTITION RANGE SINGLE` or `PARTITION RANGE ITERATOR` (good); `PARTITION RANGE ALL` (no pruning)

**Partition Maintenance Efficiency**
- `ALTER TABLE ... DROP PARTITION` = O(1) — metadata only, no row-by-row delete
- `ALTER TABLE ... EXCHANGE PARTITION WITH TABLE` = O(1) — metadata swap; use for bulk loads (load into staging, then exchange)
- `WITHOUT VALIDATION` on exchange skips row-level check — fast but dangerous; pre-validate data
- `UPDATE GLOBAL INDEXES` keeps global indexes usable but adds cost; weigh vs rebuild

**Key Numbers**
- Max partitions per table: 1,048,575
- Interval partitioning: transition point = first `INSERT` beyond last manual partition
- `dba_tab_partitions`, `dba_ind_partitions` — monitor partition-level stats staleness
