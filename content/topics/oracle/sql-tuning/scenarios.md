---
title: "SQL Tuning — Scenarios"
topic: oracle
subtopic: sql-tuning
content_type: scenario_question
tags: [oracle, sql-tuning, interview, scenarios, troubleshooting]
---

# SQL Tuning — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: A Report Query Is Running Slowly

**Scenario:** A nightly report query that aggregates 2 years of order data runs for 45 minutes. Users are complaining. How do you approach this?

<details>
<summary>💡 Hint</summary>

Start with the execution plan — use `DBMS_XPLAN.DISPLAY_CURSOR` to see what Oracle is actually doing. Look for two red flags: full table scans on large tables (millions of rows) and stale statistics (check `last_analyzed` in `dba_tables`). For a query filtering by date on a large table, the fix is usually an index on the filter column plus fresh stats. Measure before and after.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Query Runs Fine for Some Users, Slow for Others

**Scenario:** The same ORDER lookup query runs in 0.01 seconds for most users but 30 seconds for user `jsmith`. Same SQL text. What do you investigate?

<details>
<summary>💡 Hint</summary>

When the same SQL runs drastically differently for different users, the culprit is almost always *data skew + bind variable peeking*: Oracle peeked at the first user's bind value to build the plan, and that plan is wrong for jsmith who has 10,000× more rows. Check `v$sql` child cursors to see if ACS (Adaptive Cursor Sharing) created different plans. Then check `customer_id` cardinality for jsmith vs average — if jsmith's customer has a huge order volume, Oracle needs histograms or a manual hint to pick the right join method.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Plan Regression After Database Upgrade

**Scenario:** After upgrading from Oracle 19c to Oracle 21c, three critical queries are running 5× slower. How do you stabilize them quickly without rolling back?

<details>
<summary>💡 Hint</summary>

The fastest path to stability (without rollback) is SQL Plan Management (SPM): load the known-good 19c plans from AWR or a test environment into SQL Plan Baselines, then evolve only the plans that have been verified to be better than the baseline. This pins the old plan immediately. In parallel, investigate *why* the new optimizer chose a worse plan — usually changed statistics defaults, new optimizer features like Adaptive Plans, or parameter differences between 19c and 21c. Use `OPTIMIZER_CAPTURE_SQL_PLAN_BASELINES` and `DBMS_SPM` to manage the baselines.

</details>

<details>
<summary>✅ Solution</summary>

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

</details>

</article>
---

## ⚡ Quick-fire Q&A

**Q: What is the first step when tuning a slow Oracle SQL query?**
A: Get the execution plan using `EXPLAIN PLAN FOR ... / SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY)` or `SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY_CURSOR)` for the plan from the shared pool. Identify the most expensive operation by looking at the `Cost`, `Rows` estimate, and actual statistics with `GATHER_PLAN_STATISTICS` hint.

**Q: What does a high "Rows" estimate discrepancy between estimated and actual rows indicate?**
A: It indicates stale or missing optimizer statistics, skewed data that histogram-less statistics cannot represent, or a complex join cardinality issue. Inaccurate cardinality causes the optimizer to choose wrong join methods (nested loop vs. hash join) and wrong access paths (index vs. full scan).

**Q: When should you use a nested loop join vs. a hash join?**
A: Nested loop joins are efficient when the outer set is small and the inner table has an index on the join column—ideal for OLTP point lookups. Hash joins are efficient when both sets are large and there is no usable index—the optimizer builds a hash table from the smaller set and probes with the larger. Hash joins require memory (PGA).

**Q: What is an index skip scan and when does it apply?**
A: A skip scan allows Oracle to use a composite index even when the leading column is not in the WHERE clause. The optimizer logically splits the index into sub-indexes for each distinct leading-column value and scans each sub-index. It is only beneficial when the leading column has very few distinct values (low cardinality).

**Q: What is the SQL Tuning Advisor and what does it produce?**
A: The SQL Tuning Advisor (`DBMS_SQLTUNE`) analyzes a SQL statement and produces recommendations: create a missing index, gather statistics on a table, accept an SQL Profile (saved optimizer hints), or restructure the query. It runs automatically during maintenance windows and stores findings in `DBA_ADVISOR_RECOMMENDATIONS`.

**Q: What is an SQL Baseline (SQL Plan Management) and how does it differ from an SQL Profile?**
A: An SQL Baseline pins a specific execution plan for a SQL statement, preventing plan changes even after statistics refresh or optimizer upgrades. An SQL Profile provides supplemental statistics (hint-like corrections) that guide the optimizer toward a better plan without pinning. Baselines guarantee stability; profiles guide without locking.

**Q: What is the significance of the `CARDINALITY` and `NDISTINCT` statistics and how are they collected?**
A: Cardinality (number of distinct values) determines join order and access path selection. Collect with `DBMS_STATS.GATHER_TABLE_STATS` with appropriate `METHOD_OPT => 'FOR ALL COLUMNS SIZE AUTO'` to let Oracle decide on histogram creation. Skewed columns need histograms; uniform columns do not.

**Q: What are bind variable peeking issues and how do you address them?**
A: At first execution with peeking, Oracle optimizes for the actual bind value seen—the plan is then reused for all subsequent executions, which may be suboptimal for different bind values (e.g., a rare vs. common value). Address with Adaptive Cursor Sharing (ACS), which allows multiple plans per SQL based on bind value histograms, or with SQL Profiles/Baselines for specific plans.

---

## 💼 Interview Tips

- Always start with the execution plan and actual vs. estimated rows—this is the correct methodology and interviewers reward structured diagnostic thinking over jumping to "add an index."
- Know when NOT to add an index: indexes hurt DML performance (INSERT/UPDATE/DELETE must maintain the index), waste space on low-selectivity columns, and can cause the optimizer to choose a worse plan. Selectivity analysis comes first.
- Senior interviewers often present a specific wait event (`db file sequential read` at high volume) and expect you to correlate it to the execution plan (index range scan on a large result set where a full scan + hash join would be faster).
- Demonstrate SQL Plan Management fluency: baselines for plan stability, profiles for one-time corrections, and the ability to evolve baselines when a better plan is confirmed—this is the mature production approach.
- Connect tuning to monitoring: know how to find top SQL from AWR (`DBA_HIST_SQLSTAT`), identify regression after a deployment by comparing AWR snapshots, and set up SQL Performance Analyzer (SPA) for change impact testing.
