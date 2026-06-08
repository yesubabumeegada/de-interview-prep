---
title: "Materialized Views — Intermediate"
topic: oracle
subtopic: materialized-views
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [oracle, materialized-views, fast-refresh, partition-change-tracking, mv-rewrite, staleness]
---

# Materialized Views — Intermediate

## Fast Refresh Requirements and Constraints

Fast refresh has strict requirements — if violated, Oracle falls back to COMPLETE or fails:

```sql
-- Constraints for FAST refresh on aggregate MVs (most common):
-- 1. MV log must exist on ALL base tables with ROWID, SEQUENCE, and all referenced columns
-- 2. SELECT list must include COUNT(*) if any aggregate is present
-- 3. Joins must be inner joins (OUTER JOINs limit fast refresh)
-- 4. DISTINCT and certain analytic functions not supported
-- 5. Rowid must be usable (not multi-level joins with complex subqueries)

-- Correct fast-refreshable aggregate MV:
CREATE MATERIALIZED VIEW LOG ON orders
WITH ROWID, SEQUENCE (order_id, customer_id, amount_usd, status)
INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW mv_order_summary_fast
REFRESH FAST ON DEMAND
AS
SELECT 
  customer_id,
  status,
  COUNT(*) AS cnt,          -- REQUIRED for fast refresh (Oracle needs row count delta)
  SUM(amount_usd) AS total, -- SUM is fast-refreshable
  MAX(amount_usd) AS max_amount  -- MAX/MIN are fast-refreshable (with restrictions)
FROM orders
GROUP BY customer_id, status;

-- Test if MV is fast-refreshable
SELECT * FROM TABLE(DBMS_MVIEW.EXPLAIN_MVIEW('MV_ORDER_SUMMARY_FAST'));
-- CAPABILITY_NAME='PCT' or 'REFRESH_FAST' with 'Y' = fast refresh supported
```

---

## Partition Change Tracking (PCT)

PCT allows fast refresh of MVs even when the query doesn't fully satisfy fast refresh rules — by refreshing only the changed partitions:

```sql
-- Base table must be partitioned for PCT to work
CREATE TABLE sales (
  sale_id   NUMBER,
  sale_date DATE NOT NULL,
  region    VARCHAR2(20),
  amount    NUMBER
)
PARTITION BY RANGE (sale_date) INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
(PARTITION p_init VALUES LESS THAN (DATE '2024-01-01'));

-- MV using PCT: no MV log needed (uses partition-level tracking)
CREATE MATERIALIZED VIEW mv_sales_pct
REFRESH FAST ON DEMAND
AS
SELECT 
  region,
  TRUNC(sale_date, 'MM') sale_month,
  SUM(amount) total_amount,
  COUNT(*) cnt
FROM sales
GROUP BY region, TRUNC(sale_date, 'MM');

-- Only partitions that had changes are refreshed
-- Much faster than COMPLETE refresh for time-series tables with new partitions each month
```

---

## Checking and Monitoring MV Staleness

```sql
-- Check all MVs and their freshness status
SELECT mview_name, refresh_mode, refresh_method, 
       last_refresh_type, last_refresh_date,
       ROUND(SYSDATE - last_refresh_date, 2) days_since_refresh,
       staleness,
       compile_state
FROM dba_mviews
ORDER BY last_refresh_date NULLS FIRST;

-- staleness values:
-- FRESH: MV is up-to-date
-- NEEDS_COMPILE: MV definition has errors
-- STALE: base data has changed since last refresh
-- UNKNOWN: Oracle can't determine staleness (often: base table doesn't have change tracking)
-- UNUSABLE: MV is broken (base table changed schema, etc.)

-- What's in the MV log (how many changes are pending)?
SELECT master, log_table, timestamp, 
       count_rows, log_owner
FROM dba_mview_logs
WHERE log_owner = 'APP_SCHEMA';

SELECT COUNT(*) pending_changes
FROM app_schema.mlog$_orders;  -- MV log table (auto-named mlog$_tablename)
```

---

## MV Refresh Groups

Coordinate refreshing multiple MVs atomically — all or nothing:

```sql
-- Create a refresh group containing all related MVs
BEGIN
  DBMS_REFRESH.MAKE(
    name       => 'SALES_REPORTING_GROUP',
    list       => 'MV_SALES_MONTHLY,MV_CUSTOMER_SUMMARY,MV_PRODUCT_SALES',
    next_date  => SYSDATE,
    interval   => 'TRUNC(SYSDATE + 1) + 3/24',  -- daily at 3am
    implicit_destroy => FALSE,
    rollback_seg => NULL,
    push_deferred_rpc => TRUE,
    refresh_after_errors => TRUE
  );
END;
/

-- The group ensures: if any MV fails to refresh, all are marked stale
-- They all show the same consistent point-in-time data

-- Manual group refresh
BEGIN
  DBMS_REFRESH.REFRESH('SALES_REPORTING_GROUP');
END;
/

-- View refresh groups
SELECT name, next_date, interval, broken
FROM dba_refresh
ORDER BY name;
```

---

## MV and Indexes

Add indexes to MVs to speed up queries that use them:

```sql
-- Create index on the MV (treated like a regular table index)
CREATE INDEX idx_mv_sales_region ON mv_sales_monthly(region);
CREATE INDEX idx_mv_sales_month  ON mv_sales_monthly(order_month);
CREATE INDEX idx_mv_sales_region_month ON mv_sales_monthly(region, order_month);

-- Create a bitmap index (great for low-cardinality columns in DW MVs)
CREATE BITMAP INDEX bidx_mv_sales_region ON mv_sales_monthly(region);

-- Indexes on MVs survive COMPLETE refresh (Oracle rebuilds them)
-- Fast refresh: indexes automatically maintained during delta apply
```

---

## Tuning Query Rewrite

```sql
-- Check why query rewrite is not happening for a specific query
SELECT statement_id, remarks
FROM rewrite_table
WHERE statement_id = 'test1';

-- Or use DBMS_MVIEW.EXPLAIN_REWRITE:
DELETE FROM rewrite_table;
EXECUTE DBMS_MVIEW.EXPLAIN_REWRITE(
  q    => 'SELECT region, SUM(amount_usd) FROM orders o JOIN customers c ON o.customer_id=c.customer_id WHERE o.order_date > SYSDATE-30 GROUP BY region',
  mv   => 'MV_SALES_MONTHLY',
  stmt => 'test_rewrite'
);
SELECT message FROM rewrite_table ORDER BY pass, seq;
-- Messages explain why rewrite was rejected (date range not covered, column missing, etc.)

-- Force query rewrite (bypass cost-based decision)
SELECT /*+ REWRITE(mv_sales_monthly) */
  region, SUM(amount_usd)
FROM orders o JOIN customers c ON o.customer_id = c.customer_id
WHERE order_date > SYSDATE - 30
GROUP BY region;
```

---

## Interview Tips

> **Tip 1:** "What are the requirements for fast refresh on an aggregate MV?" — Must have: (1) MV log on all base tables with ROWID, SEQUENCE, and all referenced columns, including NEW VALUES, (2) COUNT(*) in the SELECT list (Oracle tracks row count changes), (3) only supported aggregate functions (SUM, COUNT, MIN, MAX, AVG — each with restrictions), (4) inner joins only (outer joins are not fast-refreshable in most cases). Violation of any of these causes Oracle to fall back to COMPLETE refresh or reject the fast refresh request.

> **Tip 2:** "What is PCT refresh and when is it better than FAST or COMPLETE?" — Partition Change Tracking (PCT) allows MV refresh by partition rather than row. When new data is added to a partitioned base table (e.g., a new monthly partition), Oracle refreshes only the MV rows corresponding to that partition — without an MV log. PCT is better than COMPLETE when the base table is partitioned and most data is unchanged (monthly reporting pattern). PCT is better than FAST when the query doesn't satisfy fast refresh requirements but is on a partitioned table.

> **Tip 3:** "How do you ensure multiple MVs always show consistent data?" — Use Refresh Groups. Define all related MVs in a group; when the group refreshes, all MVs are refreshed atomically (either all succeed or all remain stale). This ensures your `mv_sales_monthly`, `mv_customer_summary`, and `mv_product_sales` always reflect the same point in time — critical for reports that join or compare data across multiple MVs.
