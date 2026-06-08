---
title: "Materialized Views — Fundamentals"
topic: oracle
subtopic: materialized-views
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [oracle, materialized-views, query-rewrite, fast-refresh, mv-log]
---

# Materialized Views — Fundamentals

## What Is a Materialized View?

A materialized view (MV) is a database object that stores the result of a query physically on disk — unlike a regular view which just stores the query definition. When queried, an MV returns pre-computed results (no re-execution of the underlying query).

**Key benefits:**
- Pre-aggregated results for reporting (1-second instead of 10-minute queries)
- Query rewrite: Oracle transparently rewrites queries against base tables to use MVs
- Remote data caching: materialize data from remote databases or external sources
- Data warehousing: summary tables for BI tools

---

## Creating Basic Materialized Views

```sql
-- Simple MV: aggregate sales by region and month
CREATE MATERIALIZED VIEW mv_sales_monthly
BUILD IMMEDIATE         -- populate immediately on creation (vs DEFERRED: populate on first refresh)
REFRESH COMPLETE        -- full refresh: delete all data and re-run query
ON DEMAND               -- refresh only when explicitly called (vs ON COMMIT)
ENABLE QUERY REWRITE    -- allow Oracle to use this MV when rewriting queries
AS
SELECT 
  c.region,
  TRUNC(o.order_date, 'MM') AS order_month,
  COUNT(*) AS order_count,
  SUM(o.amount_usd) AS total_revenue,
  COUNT(DISTINCT o.customer_id) AS unique_customers
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.region, TRUNC(o.order_date, 'MM');

-- Verify creation
SELECT mview_name, refresh_mode, refresh_method, 
       last_refresh_date, staleness
FROM dba_mviews
WHERE mview_name = 'MV_SALES_MONTHLY';
```

---

## Refresh Types

| Refresh Method | How It Works | When to Use |
|---|---|---|
| `COMPLETE` | Truncate + re-insert all rows | Always works; slow for large MVs; simple queries |
| `FAST` | Apply only changed rows using MV log | Best for large MVs; requires MV log on base tables |
| `FORCE` | Try FAST; fall back to COMPLETE if FAST fails | Safe default |
| `NEVER` | Never refresh automatically | Static reference data |

---

## MV Logs — Enabling Fast Refresh

Fast refresh requires an MV log on each base table — the log tracks changes (INSERTs, UPDATEs, DELETEs) since the last refresh:

```sql
-- Create MV log on base table (required for FAST refresh)
CREATE MATERIALIZED VIEW LOG ON orders
WITH ROWID, SEQUENCE (order_id, customer_id, order_date, amount_usd)
INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW LOG ON customers
WITH ROWID, SEQUENCE (customer_id, region)
INCLUDING NEW VALUES;

-- Create MV with FAST refresh
CREATE MATERIALIZED VIEW mv_sales_monthly_fast
BUILD IMMEDIATE
REFRESH FAST ON DEMAND
ENABLE QUERY REWRITE
AS
SELECT 
  c.region,
  TRUNC(o.order_date, 'MM') AS order_month,
  COUNT(*) AS order_count,
  SUM(o.amount_usd) AS total_revenue,
  COUNT(*) AS cnt  -- required for fast refresh on aggregates
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.region, TRUNC(o.order_date, 'MM');
-- Oracle uses the MV logs to determine which rows changed → applies deltas only
```

---

## Refresh Scheduling

```sql
-- Manual refresh
EXEC DBMS_MVIEW.REFRESH('MV_SALES_MONTHLY', method => 'C');  -- C=Complete
EXEC DBMS_MVIEW.REFRESH('MV_SALES_MONTHLY', method => 'F');  -- F=Fast
EXEC DBMS_MVIEW.REFRESH('MV_SALES_MONTHLY', method => '?');  -- ?=Force (try fast, fallback complete)

-- Refresh multiple MVs in one call
EXEC DBMS_MVIEW.REFRESH_ALL_MVIEWS(0);  -- refresh all MVs that are stale (0 = don't halt on error)

-- Refresh with Oracle Scheduler (nightly)
BEGIN
  DBMS_SCHEDULER.CREATE_JOB(
    job_name        => 'REFRESH_SALES_MV',
    job_type        => 'PLSQL_BLOCK',
    job_action      => 'BEGIN DBMS_MVIEW.REFRESH(''MV_SALES_MONTHLY'', ''?''); END;',
    repeat_interval => 'FREQ=DAILY; BYHOUR=3; BYMINUTE=0',
    enabled         => TRUE
  );
END;
/

-- ON COMMIT refresh (refreshes automatically when base table is committed)
CREATE MATERIALIZED VIEW mv_order_totals
REFRESH FAST ON COMMIT  -- refresh synchronously on every commit to base tables
-- WARNING: adds overhead to every INSERT/UPDATE/DELETE on orders
-- Only use for small, critical MVs where real-time freshness is required
```

---

## Query Rewrite

Oracle can transparently use MVs to answer queries against base tables — without changing application SQL:

```sql
-- Enable query rewrite at session/system level
ALTER SESSION SET QUERY_REWRITE_ENABLED = TRUE;
-- Default: TRUE in most Oracle versions

-- Test query rewrite: application runs this SQL
EXPLAIN PLAN FOR
SELECT c.region, TRUNC(o.order_date, 'MM'), SUM(o.amount_usd)
FROM orders o JOIN customers c ON o.customer_id = c.customer_id
GROUP BY c.region, TRUNC(o.order_date, 'MM');

SELECT * FROM TABLE(DBMS_XPLAN.DISPLAY);
-- If query rewrite works, the plan shows: MAT_VIEW REWRITE ACCESS
-- Oracle is reading from MV_SALES_MONTHLY instead of the base tables!

-- Check why query rewrite didn't work
SELECT * FROM TABLE(DBMS_MVIEW.EXPLAIN_REWRITE(
  q  => 'SELECT region, SUM(amount_usd) FROM orders o JOIN customers c ON o.customer_id=c.customer_id GROUP BY region',
  mv => 'MV_SALES_MONTHLY'
));
```

---

## Interview Tips

> **Tip 1:** "What is the difference between a view and a materialized view?" — A view is just a stored SQL query — every time you query it, Oracle re-executes the underlying SQL. A materialized view stores the actual result set on disk. Querying a materialized view reads the pre-computed result directly — no re-execution. MVs are significantly faster for complex aggregations, but require refresh to stay current with base data.

> **Tip 2:** "When would you use REFRESH FAST vs REFRESH COMPLETE?" — FAST refresh uses MV logs to apply only the changes since the last refresh — much faster for large MVs where only a small % of data changes. COMPLETE refresh re-runs the entire query — simple but slow for large MVs. Use FAST when: the base tables have MV logs, the query supports fast refresh (aggregates with COUNT, SUM, GROUP BY), and the MV is large. Use COMPLETE when: the query is too complex for fast refresh, or it's a small MV where COMPLETE is fast enough.

> **Tip 3:** "What is query rewrite and why is it powerful?" — Query rewrite is Oracle's ability to automatically redirect a query against base tables to use a materialized view instead — without changing the application SQL. The application runs `SELECT ... SUM(...) GROUP BY region FROM orders`; Oracle's optimizer detects that MV_SALES_MONTHLY already has this aggregation and reads from the MV instead. The application doesn't know or care — it still gets correct results, but 100× faster. This makes MVs transparent to legacy applications.
