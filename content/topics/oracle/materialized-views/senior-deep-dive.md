---
title: "Materialized Views — Senior Deep Dive"
topic: oracle
subtopic: materialized-views
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [oracle, materialized-views, real-time-mv, dbms-mview, mv-rewrite-advanced, in-memory]
---

# Materialized Views — Senior Deep Dive

## Real-Time Materialized Views (12c R2+)

Real-time MVs combine a stale MV with fresh base table data at query time — no explicit refresh needed:

```sql
-- Create MV for real-time query rewrite
CREATE MATERIALIZED VIEW mv_sales_realtime
BUILD IMMEDIATE
REFRESH FAST ON STATEMENT  -- OR: REFRESH FAST ON DEMAND
ENABLE ON QUERY COMPUTATION -- KEY: enables real-time MV capability
AS
SELECT customer_id, 
       SUM(amount_usd) total_amount,
       COUNT(*) cnt
FROM orders
GROUP BY customer_id;

-- At query time: Oracle combines stale MV data + recent MV log changes
-- Result is always current — without waiting for a refresh

-- Check real-time MV status
SELECT mview_name, refresh_mode, on_query_computation
FROM dba_mviews
WHERE mview_name = 'MV_SALES_REALTIME';

-- Query: uses real-time MV even when stale
EXPLAIN PLAN FOR
SELECT customer_id, SUM(amount_usd) FROM orders GROUP BY customer_id;
SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
-- Plan shows: MAT_VIEW REWRITE ACCESS FULL + UNION (delta from log applied at query time)
```

---

## MV in Data Warehouse — Advanced Patterns

### Pattern 1: Pre-Join MVs for Star Schema
```sql
-- Star schema: fact table + dimension tables
-- Pre-join MV: eliminates join at query time

CREATE MATERIALIZED VIEW LOG ON fact_sales 
WITH ROWID, SEQUENCE (sale_id, date_key, product_key, customer_key, amount)
INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW LOG ON dim_date
WITH ROWID, SEQUENCE (date_key, year, quarter, month, week)
INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW LOG ON dim_product  
WITH ROWID, SEQUENCE (product_key, category, subcategory, brand)
INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW mv_sales_prejoin
REFRESH FAST ON DEMAND
ENABLE QUERY REWRITE
AS
SELECT 
  f.sale_id,                  -- rowid requirement for fast refresh join MV
  d.year, d.quarter, d.month,
  p.category, p.brand,
  c.region, c.country,
  f.amount
FROM fact_sales f
JOIN dim_date d    ON f.date_key    = d.date_key
JOIN dim_product p ON f.product_key = p.product_key
JOIN dim_customer c ON f.customer_key = c.customer_key;

-- BI tool query against star schema → Oracle rewrites to use the pre-joined MV
-- Eliminates 3-way join at query time — massive performance improvement
```

### Pattern 2: Nested MVs
```sql
-- MV built on another MV (two levels of pre-aggregation)

-- Level 1: daily aggregates
CREATE MATERIALIZED VIEW mv_sales_daily
REFRESH COMPLETE ON DEMAND
AS
SELECT customer_id, region, TRUNC(order_date) sale_day, 
       SUM(amount_usd) daily_total, COUNT(*) daily_count
FROM orders o JOIN customers c ON o.customer_id = c.customer_id
GROUP BY customer_id, region, TRUNC(order_date);

-- Level 2: monthly aggregates from Level 1 MV
CREATE MATERIALIZED VIEW mv_sales_monthly_nested
REFRESH COMPLETE ON DEMAND
AS
SELECT region, TRUNC(sale_day, 'MM') sale_month,
       SUM(daily_total) monthly_total, SUM(daily_count) monthly_count
FROM mv_sales_daily  -- based on the Level 1 MV!
GROUP BY region, TRUNC(sale_day, 'MM');

-- Refresh order: always refresh Level 1 before Level 2
-- Use DBMS_MVIEW.REFRESH with REFRESH_ALL='Y' for dependent order
EXEC DBMS_MVIEW.REFRESH('MV_SALES_DAILY', 'C');
EXEC DBMS_MVIEW.REFRESH('MV_SALES_MONTHLY_NESTED', 'C');
```

---

## MV Advisor — Automatic MV Recommendations

```sql
-- Use SQL Access Advisor to recommend which MVs to create
-- based on a SQL workload

DECLARE
  task_name VARCHAR2(50) := 'MV_ADVISOR_TASK';
BEGIN
  -- Create task
  DBMS_ADVISOR.CREATE_TASK('SQL Access Advisor', task_name);
  
  -- Add the workload (from STS)
  DBMS_ADVISOR.ADD_STS_REF(task_name, 'PROD_WORKLOAD_WK52');
  
  -- Configure to recommend MVs + indexes
  DBMS_ADVISOR.SET_TASK_PARAMETER(task_name, 'ANALYSIS_SCOPE', 'MVIEW,INDEX');
  DBMS_ADVISOR.SET_TASK_PARAMETER(task_name, 'MODE', 'COMPREHENSIVE');
  
  -- Run
  DBMS_ADVISOR.EXECUTE_TASK(task_name);
END;
/

-- View MV recommendations
SELECT rec_id, benefit, rank, action_type,
       SUBSTR(attr1, 1, 200) AS mv_or_index_ddl
FROM dba_advisor_recommendations r
JOIN dba_advisor_actions a ON r.task_name = a.task_name AND r.rec_id = a.rec_id
WHERE r.task_name = 'MV_ADVISOR_TASK'
  AND r.benefit > 50  -- significant benefit
ORDER BY benefit DESC;
```

---

## MV Performance Diagnostics

```sql
-- Monitor MV refresh performance over time
SELECT r.mview_name,
       r.refresh_method,
       r.start_time,
       r.end_time,
       ROUND((r.end_time - r.start_time) * 24 * 60, 2) duration_min,
       r.initial_num_rows,
       r.final_num_rows,
       r.num_rows_changed
FROM dba_mview_refresh_times r  -- 12c+ view
ORDER BY r.start_time DESC
FETCH FIRST 20 ROWS ONLY;

-- Find MVs with growing refresh time (capacity issue)
SELECT mview_name,
       ROUND(AVG(duration_min), 1) avg_duration_min,
       ROUND(MAX(duration_min), 1) max_duration_min,
       COUNT(*) refresh_count
FROM (
  SELECT mview_name, 
         ROUND((end_time - start_time) * 24 * 60, 2) duration_min
  FROM dba_mview_refresh_times
  WHERE start_time > SYSDATE - 30
)
GROUP BY mview_name
ORDER BY avg_duration_min DESC;

-- Profile a slow MV refresh using DBMS_MVIEW with trace
ALTER SESSION SET EVENTS '10046 TRACE NAME CONTEXT FOREVER, LEVEL 12';
EXEC DBMS_MVIEW.REFRESH('MV_SALES_MONTHLY', 'C');
ALTER SESSION SET EVENTS '10046 TRACE NAME CONTEXT OFF';
-- Then analyze the trace file: tkprof tracefile.trc output.txt
```

---

## MV and In-Memory Integration

```sql
-- Populate a MV in the In-Memory Column Store for ultra-fast query rewrite
ALTER MATERIALIZED VIEW mv_sales_monthly INMEMORY PRIORITY HIGH;

-- Combine MV + IMCS: Oracle uses MV for query rewrite AND IMCS for column storage
-- Best of both worlds: pre-aggregated (MV) + columnar scan (IMCS)

-- Check MV in memory
SELECT segment_name, inmemory_size/1e6 imcs_mb, populate_status
FROM v$im_segments
WHERE segment_name = 'MV_SALES_MONTHLY';
```

---

## Interview Tips

> **Tip 1:** "What is a Real-Time Materialized View and how does it differ from a regular MV?" — A Real-Time MV (`ON QUERY COMPUTATION`) answers queries using stale MV data PLUS the delta changes tracked in the MV log — at query time. The MV doesn't need to be refreshed to return current results. The query plan shows a UNION between the stale MV scan and a delta reconciliation from the log. It trades slightly more query-time work for always-current results, eliminating the need for frequent refreshes.

> **Tip 2:** "Can you build an MV on top of another MV? What are the considerations?" — Yes, nested MVs (or multilevel MVs) are supported. Oracle allows an MV query to reference another MV. Considerations: (1) refresh order — the base MV must be refreshed before the dependent MV, (2) fast refresh support is limited for nested MVs (often must use COMPLETE), (3) DBMS_MVIEW.REFRESH with dependency resolution handles the order automatically when using refresh groups. Nested MVs are common in DW: daily aggregation MV → weekly → monthly.

> **Tip 3:** "When would you recommend building an MV vs a caching layer in the application?" — MV when: (1) the query can benefit from Oracle's query rewrite (existing applications unchanged), (2) aggregation complexity is high and data is in Oracle, (3) data must be transactionally consistent with the base tables. Application cache when: (1) the data is small and read extremely frequently (millions of requests/second), (2) the application already has a caching framework (Redis, Memcached), (3) you need sub-millisecond latency that even an MV can't provide. MVs are ideal for DW/reporting; app cache for OLTP hot paths.

## ⚡ Cheat Sheet

**Refresh Type Decision**
| Refresh | When to Use | Requirement |
|---|---|---|
| FAST (incremental) | Frequent small changes | MV log on base table; no complex constructs |
| COMPLETE | Large changes, complex SQL | No log needed; rewrites entire MV |
| FORCE | Default; tries FAST, falls back COMPLETE | Flexibility but unpredictable time |
| ON QUERY COMPUTATION (Real-Time) | Need current data without refresh overhead | MV log; extra query-time work |

**Fast Refresh Blockers (must use COMPLETE)**
- DISTINCT, GROUP BY with ROLLUP/CUBE, CONNECT BY
- Set operators (UNION ALL allowed with restrictions)
- Subqueries in SELECT list or non-join WHERE
- Outer joins without MV log on outer table

**Query Rewrite Rules**
- `QUERY_REWRITE_ENABLED = TRUE` (system) and `ENABLE QUERY REWRITE` on MV
- `QUERY_REWRITE_INTEGRITY`: ENFORCED (default) → only rewrite if constraints proven; TRUSTED → trust NOT ENFORCED constraints
- Check rewrite: `EXPLAIN PLAN` → look for `MAT_VIEW REWRITE ACCESS FULL` in plan
- Troubleshoot: `DBMS_MVIEW.EXPLAIN_MVIEW('MV_NAME')` shows why rewrite failed

**MV Log Gotchas**
- `CREATE MATERIALIZED VIEW LOG ON orders WITH ROWID, PRIMARY KEY, SEQUENCE (col1, col2) INCLUDING NEW VALUES`
- Log must include all columns referenced in the MV SELECT
- Log purged automatically after FAST refresh; if refresh fails, log grows unboundedly → monitor `dba_mview_logs`

**Nested MVs & Refresh Groups**
- Refresh group ensures consistent point-in-time across multiple MVs: `DBMS_REFRESH.MAKE`
- Refresh order in nested MVs: base MV first; `DBMS_MVIEW.REFRESH(list, method, atomic_refresh=>FALSE)` is faster (parallel)
- `atomic_refresh=>FALSE` uses TRUNCATE+INSERT instead of DELETE+INSERT — much faster for large MVs but non-atomic
