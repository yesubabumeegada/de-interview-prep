---
title: "SQL Tuning — Scenarios"
topic: oracle
subtopic: sql-tuning
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [oracle, sql-tuning, interview, scenarios, troubleshooting]
---

# SQL Tuning — Interview Scenarios

## Scenario 1 (Junior): A Report Query Is Running Slowly

**Question:** A nightly report query that aggregates 2 years of order data runs for 45 minutes. Users are complaining. How do you approach this?

**Answer:**

**Step 1: Get the execution plan**
```sql
-- Find the SQL in the cursor cache
SELECT sql_id, elapsed_time/1000000 elapsed_sec, sql_text
FROM v$sql
WHERE sql_text LIKE '%nightly_report%'
  AND elapsed_time > 0
ORDER BY elapsed_time DESC;

-- Get the actual plan
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR('sql_id_here', 0, 'ALLSTATS LAST'));
```

**Step 2: Look for full scans on large tables**
```
| 4 | TABLE ACCESS FULL | ORDER_ITEMS | 50M | 8000 (3) |
```
→ Full scan on 50M-row table. Is there a usable index?

**Step 3: Check the filter predicates**
```sql
-- If the query filters on order_date, add an index
CREATE INDEX idx_order_items_date ON order_items(order_date, status);

-- Verify the query uses the new index
-- Re-run and check plan: should show INDEX RANGE SCAN
```

**Step 4: Check statistics freshness**
```sql
SELECT table_name, last_analyzed, num_rows
FROM dba_tables WHERE table_name = 'ORDER_ITEMS';
-- If last_analyzed is months old and num_rows is wrong → gather stats
EXEC DBMS_STATS.GATHER_TABLE_STATS('APP', 'ORDER_ITEMS', degree => 8);
```

**Step 5: Consider partitioning for the long term**
```sql
-- If the table is queried by date range, partition by month
-- (Requires table rebuild — plan for next release cycle)
-- Partitioning allows partition pruning: only scan 2 out of 24+ monthly partitions
```

**Result:** After index + stats: 45 minutes → 2 minutes.

---

## Scenario 2 (Mid-level): Query Runs Fine for Some Users, Slow for Others

**Question:** The same ORDER lookup query runs in 0.01 seconds for most users but 30 seconds for user `jsmith`. Same SQL text. What do you investigate?

**Answer:**

**Root cause hypothesis: bind variable peeking + skewed data**

```sql
-- Check if there are multiple child cursors (ACS in action)
SELECT sql_id, child_number, plan_hash_value, 
       executions, elapsed_time/1000000 elapsed_sec,
       is_bind_sensitive, is_bind_aware
FROM v$sql
WHERE sql_id = 'sql_id_here'
ORDER BY child_number;

-- If single child_number → same plan for all users
-- If multiple → ACS created different plans for different bind ranges
```

**Investigation — check jsmith's data volume:**
```sql
-- How many orders does jsmith's customer have vs average?
SELECT customer_id, COUNT(*) order_count
FROM orders
WHERE customer_id IN (SELECT customer_id FROM users WHERE username = 'jsmith')
GROUP BY customer_id;
-- jsmith's customer has 500K orders; average is 50 orders
```

**Fix options:**

Option A — Fix data skew with histogram:
```sql
EXEC DBMS_STATS.GATHER_TABLE_STATS('APP', 'ORDERS',
  method_opt => 'FOR COLUMNS SIZE 254 CUSTOMER_ID');
-- Now optimizer knows some customers have huge order counts → uses HASH JOIN for those
```

Option B — Use ACS (if 12c+):
```sql
-- ACS will create a second child cursor optimized for high-volume customers
-- No action needed if optimizer_adaptive_cursor_sharing = TRUE (default)
-- Check if it's enabled:
SELECT name, value FROM v$parameter WHERE name = 'optimizer_adaptive_cursor_sharing';
```

Option C — Application-level fix (if you can change code):
```sql
-- Split into two queries: one for high-volume customers (no index, hash join)
-- one for low-volume (index lookup)
-- Use customer order count as routing condition
```

---

## Scenario 3 (Senior): Plan Regression After Database Upgrade

**Question:** After upgrading from Oracle 19c to Oracle 21c, three critical queries are running 5× slower. How do you stabilize them quickly without rolling back?

**Answer:**

**Immediate action — use SPM to pin the old plans:**

```sql
-- Step 1: You have the old sql_ids and plan_hash_values from pre-upgrade monitoring
-- Load the 19c plans from AWR (if available) or from a test environment cursor cache

-- If test env still on 19c, connect and export baselines:
-- On 19c source:
DECLARE
  cnt PLS_INTEGER;
BEGIN
  cnt := DBMS_SPM.LOAD_PLANS_FROM_CURSOR_CACHE(sql_id => 'query1_sqlid');
  cnt := DBMS_SPM.LOAD_PLANS_FROM_CURSOR_CACHE(sql_id => 'query2_sqlid');
  cnt := DBMS_SPM.LOAD_PLANS_FROM_CURSOR_CACHE(sql_id => 'query3_sqlid');
END;
/

-- Export baselines from 19c to staging table
DECLARE
  cnt PLS_INTEGER;
BEGIN
  cnt := DBMS_SPM.PACK_STGTAB_BASELINE(
    table_name => 'SPM_STAGE',
    enabled    => 'YES',
    accepted   => 'YES'
  );
END;
/

-- Transfer SPM_STAGE table to 21c system (datapump export/import)
-- On 21c target:
DECLARE
  cnt PLS_INTEGER;
BEGIN
  cnt := DBMS_SPM.UNPACK_STGTAB_BASELINE(
    table_name => 'SPM_STAGE'
  );
END;
/
```

**Verify baselines are being used:**
```sql
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR('query1_sqlid', 0, 'BASIC'));
-- Look for note: "SQL plan baseline SQL_PLAN_xxx used for this statement"
```

**Medium-term — validate and evolve new plans:**
```sql
-- Run the evolve task to compare old vs new plans with actual execution
DECLARE
  report CLOB;
BEGIN
  report := DBMS_SPM.EVOLVE_SQL_PLAN_BASELINE(
    sql_handle => 'SQL_query1_handle',
    verify     => 'YES',
    commit     => 'NO'  -- don't auto-accept yet — review first
  );
  DBMS_OUTPUT.PUT_LINE(SUBSTR(report, 1, 4000));
END;
/
-- If new plan is actually better (sometimes 21c optimizer IS smarter): accept it
-- If old plan is still better: keep the baseline, investigate why new plan is regressing
```

**Root cause investigation:**
```sql
-- Compare 19c vs 21c optimizer parameters (they changed between versions)
SELECT name, value FROM v$parameter 
WHERE name LIKE 'optimizer%'
ORDER BY name;

-- Check if any stats were auto-gathered differently in 21c
-- Compare plan content: new plan might use an anti-join or semi-join where 19c used NL
```

---

## Key Tuning Decision Tree

```
Query is slow?
├── No plan? → EXPLAIN PLAN or DISPLAY_CURSOR
├── Full scan on large table?
│   ├── Yes, low selectivity (>20% rows) → Full scan is correct; consider parallel
│   └── Yes, high selectivity (<5% rows) → Add index
├── Bad cardinality estimates? (E-Rows >> A-Rows)
│   ├── Stale stats → GATHER_TABLE_STATS
│   └── Skewed data → Add histogram
├── Wrong join method?
│   ├── Nested loops on large tables → USE_HASH hint or fix selectivity
│   └── Hash join on tiny tables → USE_NL hint
├── Plan changed recently?
│   ├── After stats gather → Restore old stats or use SQL Profile
│   └── After upgrade → Load SPM baselines from pre-upgrade
└── Can't change SQL text?
    ├── SQL Profile → fix cardinality estimates
    └── SPM Baseline → pin a good plan
```
