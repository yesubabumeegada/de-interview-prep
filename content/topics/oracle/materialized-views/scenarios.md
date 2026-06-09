---
title: "Materialized Views — Scenarios"
topic: oracle
subtopic: materialized-views
content_type: scenario_question
tags: [oracle, materialized-views, interview, scenarios, query-rewrite, refresh-design]
---

# Materialized Views — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: Design MVs for a Reporting Dashboard

**Scenario:** A financial dashboard shows: total revenue by region (current month), top 10 customers by revenue (current year), and daily transaction count trend (last 90 days). The underlying tables have 500M+ rows. How would you design MVs for this?

<details>
<summary>💡 Hint</summary>

-- MV 1: Monthly revenue by region (refreshed nightly)

</details>

<details>
<summary>✅ Solution</summary>

```sql
-- MV 1: Monthly revenue by region (refreshed nightly)
CREATE MATERIALIZED VIEW LOG ON orders
WITH ROWID, SEQUENCE (order_id, customer_id, region, amount_usd, order_date)
INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW mv_revenue_monthly_region
BUILD IMMEDIATE
REFRESH FAST ON DEMAND
ENABLE QUERY REWRITE
AS
SELECT 
  region,
  TRUNC(order_date, 'MM') order_month,
  COUNT(*) order_count,
  SUM(amount_usd) total_revenue
FROM orders
GROUP BY region, TRUNC(order_date, 'MM');

-- MV 2: Annual revenue by customer (refreshed nightly)
CREATE MATERIALIZED VIEW mv_customer_annual_revenue
BUILD IMMEDIATE
REFRESH FAST ON DEMAND
ENABLE QUERY REWRITE
AS
SELECT 
  customer_id,
  EXTRACT(YEAR FROM order_date) order_year,
  COUNT(*) order_count,
  SUM(amount_usd) total_revenue
FROM orders
GROUP BY customer_id, EXTRACT(YEAR FROM order_date);

-- Dashboard query for top 10 customers → Oracle rewrites to use MV:
-- SELECT customer_id, SUM(amount_usd) FROM orders WHERE EXTRACT(YEAR FROM order_date) = 2024
-- GROUP BY customer_id ORDER BY SUM(amount_usd) DESC FETCH FIRST 10 ROWS ONLY;
-- → REWRITTEN to use mv_customer_annual_revenue (< 1 second vs 5 minutes)

-- MV 3: Daily transaction count (last 90 days — rolling)
CREATE MATERIALIZED VIEW mv_daily_order_count
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND  -- complete refresh (filter by 90 days changes daily)
ENABLE QUERY REWRITE
AS
SELECT 
  TRUNC(order_date) order_day,
  COUNT(*) daily_count,
  SUM(amount_usd) daily_revenue
FROM orders
WHERE order_date >= TRUNC(SYSDATE) - 90
GROUP BY TRUNC(order_date);

-- Refresh schedule: all 3 MVs nightly at 1am
BEGIN
  DBMS_REFRESH.MAKE(
    name     => 'DASHBOARD_MVS',
    list     => 'MV_REVENUE_MONTHLY_REGION,MV_CUSTOMER_ANNUAL_REVENUE,MV_DAILY_ORDER_COUNT',
    next_date => TRUNC(SYSDATE + 1) + 1/24,
    interval  => 'TRUNC(SYSDATE + 1) + 1/24'
  );
END;
/
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Query Rewrite Isn't Working

**Scenario:** You created `mv_sales_monthly` with `ENABLE QUERY REWRITE`. The explain plan for `SELECT region, SUM(amount_usd) FROM orders o JOIN customers c ON o.customer_id = c.customer_id GROUP BY region` still shows a full join instead of the MV. Why?

<details>
<summary>💡 Hint</summary>

**Systematic diagnosis:**

</details>

<details>
<summary>✅ Solution</summary>

**Systematic diagnosis:**

```sql
-- Step 1: Check if query rewrite is enabled
SHOW PARAMETER query_rewrite_enabled;
-- Should show: TRUE

-- Step 2: Check if the MV is fresh
SELECT staleness FROM dba_mviews WHERE mview_name = 'MV_SALES_MONTHLY';
-- STALE → Oracle won't use a stale MV for query rewrite by default!
-- Fix: refresh the MV
EXEC DBMS_MVIEW.REFRESH('MV_SALES_MONTHLY', 'C');

-- Step 3: Check QUERY_REWRITE_INTEGRITY setting
SHOW PARAMETER query_rewrite_integrity;
-- ENFORCED (default): only rewrites if MV is FRESH
-- TRUSTED: rewrites even if stale (trusting DBA that data is consistent)
-- STALE_TOLERATED: always rewrites, even with stale data

-- Temporarily allow stale rewrite (for testing)
ALTER SESSION SET QUERY_REWRITE_INTEGRITY = STALE_TOLERATED;

-- Step 4: Use EXPLAIN_REWRITE to find the exact reason
BEGIN
  DELETE FROM rewrite_table;
  DBMS_MVIEW.EXPLAIN_REWRITE(
    q    => 'SELECT region, SUM(amount_usd) FROM orders o JOIN customers c ON o.customer_id=c.customer_id GROUP BY region',
    mv   => 'MV_SALES_MONTHLY',
    stmt => 'test1'
  );
  COMMIT;
END;
/
SELECT message FROM rewrite_table WHERE statement_id = 'test1' ORDER BY seq;
```

**Common EXPLAIN_REWRITE messages and fixes:**

| Message | Meaning | Fix |
|

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: MV Refresh Failing in Production

**Scenario:** A critical MV `mv_financial_summary` has been failing to fast-refresh for 3 nights with ORA-12034: "materialized view log on table TRANSACTIONS younger than last refresh." Investigate and fix.

<details>
<summary>💡 Hint</summary>

**ORA-12034 explanation:** The MV log was truncated or recreated AFTER the last MV refresh — Oracle can't determine what changed. Fast refresh is impossible; only COMPLETE refresh can recover.

</details>

<details>
<summary>✅ Solution</summary>

**ORA-12034 explanation:** The MV log was truncated or recreated AFTER the last MV refresh — Oracle can't determine what changed. Fast refresh is impossible; only COMPLETE refresh can recover.

**Step 1: Immediate recovery (tonight)**
```sql
-- Force a COMPLETE refresh to get back in sync
EXEC DBMS_MVIEW.REFRESH('MV_FINANCIAL_SUMMARY', 'C');  -- C = COMPLETE

-- Verify it refreshed successfully
SELECT last_refresh_date, staleness 
FROM dba_mviews WHERE mview_name = 'MV_FINANCIAL_SUMMARY';
```

**Step 2: Investigate root cause**
```sql
-- Was the MV log recreated?
SELECT log_table, snapshot_id, log_date, log_owner, mlog$_rowid
FROM dba_mview_logs
WHERE master = 'TRANSACTIONS';

-- Check DDL history (if Unified Auditing is enabled)
SELECT event_timestamp, dbusername, action_name, object_name
FROM unified_audit_trail
WHERE object_name IN ('MLOG$_TRANSACTIONS', 'TRANSACTIONS')
  AND event_timestamp > SYSDATE - 4
ORDER BY event_timestamp;

-- Common causes:
-- 1. DBA/developer recreated the MV log (DROP + CREATE)
-- 2. Data pump import truncated the log
-- 3. A purge job deleted log entries that were still needed
```

**Step 3: Fix the root cause**
```sql
-- If a job is deleting log entries prematurely:
-- Check: when is the MV log purged?
-- Oracle auto-purges log entries that all dependent MVs have already applied

-- Check if there's a manual purge job
SELECT job_name, job_action FROM dba_scheduler_jobs
WHERE job_action LIKE '%MLOG%' OR job_action LIKE '%PURGE%';

-- If a manual job is running before the MV refresh: adjust the schedule

-- If the MV log was DROP+CREATEd by a developer:
-- Prevent: lock down MV log DDL with object audit / privilege restriction
-- Process: create change management policy: no DDL on production MV logs without DBA approval

-- If the MV log is growing too large (forcing purge pressure):
-- Check size:
SELECT ROUND(bytes/1e6, 2) size_mb FROM dba_segments
WHERE segment_name = 'MLOG$_TRANSACTIONS';

-- If log is too large: refresh MV more frequently so log entries are purged faster
-- Change from nightly to every 6 hours if the table changes heavily
```

**Step 4: Long-term prevention**
```sql
-- Switch to FORCE refresh (tries FAST, falls back to COMPLETE on ORA-12034)
-- Ensures MV always refreshes even if log is stale, no more failures
EXECUTE DBMS_MVIEW.REFRESH('MV_FINANCIAL_SUMMARY', '?');  -- ? = FORCE

-- Or update the scheduler job:
BEGIN
  DBMS_SCHEDULER.SET_JOB_ARGUMENT_VALUE(
    'REFRESH_FINANCIAL_MV', 1, '?'  -- change method to FORCE
  );
END;
/

-- Add monitoring: alert if MV hasn't refreshed in > 25 hours
-- (from the MV health check view pattern)
```

</details>

</article>