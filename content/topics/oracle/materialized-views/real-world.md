---
title: "Materialized Views — Real World"
topic: oracle
subtopic: materialized-views
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [oracle, materialized-views, production, etl, reporting, query-rewrite]
---

# Materialized Views — Real World Patterns

## Pattern 1: BI Reporting Layer — Pre-Aggregated MVs

A company has a 500GB transaction table. BI dashboards query it for monthly summaries but take 10+ minutes. Solution: build an MV hierarchy.

```sql
-- Step 1: MV logs on base tables
CREATE MATERIALIZED VIEW LOG ON transactions
WITH ROWID, SEQUENCE (txn_id, account_id, txn_date, amount, txn_type)
INCLUDING NEW VALUES;

CREATE MATERIALIZED VIEW LOG ON accounts
WITH ROWID, SEQUENCE (account_id, branch_id, account_type, region)
INCLUDING NEW VALUES;

-- Step 2: Daily summary MV (fast refresh)
CREATE MATERIALIZED VIEW mv_txn_daily
BUILD IMMEDIATE
REFRESH FAST ON DEMAND
ENABLE QUERY REWRITE
AS
SELECT 
  a.region, a.account_type, a.branch_id,
  TRUNC(t.txn_date) txn_day,
  COUNT(*) cnt,
  SUM(t.amount) total_amount,
  SUM(CASE WHEN t.txn_type = 'CREDIT' THEN t.amount ELSE 0 END) credits,
  SUM(CASE WHEN t.txn_type = 'DEBIT' THEN t.amount ELSE 0 END) debits
FROM transactions t
JOIN accounts a ON t.account_id = a.account_id
GROUP BY a.region, a.account_type, a.branch_id, TRUNC(t.txn_date);

-- Step 3: Monthly rollup MV (from daily MV)
CREATE MATERIALIZED VIEW mv_txn_monthly
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND  -- complete from daily MV (fast enough since it's already aggregated)
ENABLE QUERY REWRITE
AS
SELECT 
  region, account_type, branch_id,
  TRUNC(txn_day, 'MM') txn_month,
  SUM(cnt) cnt,
  SUM(total_amount) total_amount,
  SUM(credits) credits,
  SUM(debits) debits
FROM mv_txn_daily
GROUP BY region, account_type, branch_id, TRUNC(txn_day, 'MM');

-- Step 4: Refresh schedule
BEGIN
  -- Create a refresh group: daily MV refreshes at 1am, monthly at 2am
  DBMS_REFRESH.MAKE(
    name     => 'REPORTING_MV_GROUP',
    list     => 'MV_TXN_DAILY,MV_TXN_MONTHLY',
    next_date => TRUNC(SYSDATE + 1) + 1/24,  -- 1am tomorrow
    interval  => 'TRUNC(SYSDATE + 1) + 1/24',
    rollback_seg => NULL,
    push_deferred_rpc => TRUE,
    refresh_after_errors => TRUE
  );
END;
/
```

**Result:** Dashboard queries against `transactions + accounts` are automatically rewritten to read from `mv_txn_monthly` — queries drop from 10 minutes to < 1 second.

---

## Pattern 2: MV for ETL (Materialized Joins)

```sql
-- ETL pattern: use MV as a pre-joined staging layer
-- Downstream ETL reads from MV instead of joining 5 tables each time

CREATE MATERIALIZED VIEW mv_enriched_orders
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND   -- nightly full refresh before ETL
AS
SELECT 
  o.order_id,
  o.order_date,
  o.amount_usd,
  o.status,
  c.customer_name,
  c.email,
  c.region,
  c.tier,
  p.product_name,
  p.category,
  p.cost_usd,
  s.store_name,
  s.country,
  s.timezone
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
JOIN stores s ON o.store_id = s.store_id
WHERE o.order_date >= DATE '2024-01-01';  -- last year only

-- Schedule refresh before ETL
-- ETL reads from mv_enriched_orders — single table scan instead of 4 joins
-- Works even for ETL tools that can't write complex SQL (Informatica, Talend)
```

---

## Pattern 3: Monitoring MV Freshness

```sql
-- Production MV health check — run every 15 minutes
CREATE OR REPLACE VIEW v_mv_health AS
SELECT 
  mview_name,
  owner,
  refresh_mode,
  refresh_method,
  ROUND(SYSDATE - last_refresh_date, 4) * 24 hours_since_refresh,
  staleness,
  CASE
    WHEN staleness = 'UNUSABLE' THEN 'CRITICAL'
    WHEN staleness = 'STALE' AND ROUND(SYSDATE - last_refresh_date, 4) * 24 > 25 THEN 'WARNING'
    WHEN staleness = 'NEEDS_COMPILE' THEN 'CRITICAL'
    ELSE 'OK'
  END AS health_status,
  last_refresh_date
FROM dba_mviews
WHERE owner NOT IN ('SYS', 'SYSTEM', 'DBSNMP')
ORDER BY 
  CASE WHEN health_status = 'CRITICAL' THEN 1
       WHEN health_status = 'WARNING' THEN 2
       ELSE 3 END;

-- Alert on stale/broken MVs
SELECT * FROM v_mv_health WHERE health_status != 'OK';
```

---

## Common MV Mistakes and Fixes

| Mistake | Problem | Fix |
|---|---|---|
| Fast refresh MV without COUNT(*) | Fast refresh fails → COMPLETE used instead | Always include `COUNT(*) AS cnt` in aggregate MVs |
| No indexes on MV | Query against MV is slow (full scan of MV) | Add indexes on MV columns used in WHERE/JOIN |
| Refreshing during business hours | Refresh locks table/contention with BI tools | Schedule refreshes in off-hours or use ON QUERY COMPUTATION |
| Very large MV log | Lots of DML piles up; fast refresh takes as long as complete | Refresh more frequently or switch to COMPLETE for small MVs |
| Rewrite not happening | Queries not using MV → plan shows base table scan | Debug with EXPLAIN_REWRITE; check QUERY_REWRITE_ENABLED, MV staleness, coverage |
| Nested MV refresh in wrong order | Monthly MV not reflecting daily MV's latest data | Use Refresh Group; refresh daily → monthly sequentially |

---

## Interview Tips

> **Tip 1:** "How would you use materialized views to speed up a slow BI dashboard?" — Identify the common queries (find via AWR `dba_hist_sqlstat`). Create pre-aggregated MVs for the most common groupings and time grains (daily, monthly). Enable QUERY REWRITE so existing BI tool queries are transparently rewritten to use the MVs — no application changes needed. Set up refresh schedules aligned with business needs (nightly for daily reports; hourly for near-real-time dashboards).

> **Tip 2:** "A fast refresh is taking 3 hours even though only 5% of rows changed. Why?" — Possible causes: (1) the MV log has accumulated a very large backlog (many changes not yet applied), (2) the MV has complex joins — fast refresh on joins requires scanning the entire MV log for both sides of the join, (3) stale statistics on the MV log table — Oracle estimates wrong number of log rows to process, (4) MV log is not indexed (no index on `SNAPTIME$$` column). Check: `SELECT COUNT(*) FROM mlog$_your_table` — if millions of rows, the log is bloated.

> **Tip 3:** "When would you choose ON COMMIT refresh over ON DEMAND?" — ON COMMIT: the MV is refreshed synchronously on every committed transaction to the base table. Use only for very small MVs (< 10K rows) where real-time freshness is critical and the commit overhead is acceptable. Examples: a small lookup MV used by every OLTP transaction. Avoid ON COMMIT on large MVs — it makes every INSERT/UPDATE/DELETE on the base table slower. Use ON DEMAND for anything else, with appropriate refresh schedules.
