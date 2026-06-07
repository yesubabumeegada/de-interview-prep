---
title: "Teradata - Query Optimization Real World"
topic: teradata
subtopic: query-optimization
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [teradata, query-optimization, production, dbql, tuning, case-study]
---

# Query Optimization — Real World

## Case Study: The $2M Report That Ran for 16 Hours

**Setting:** A telecom company's monthly revenue reconciliation report.

**The problem:** A SQL query joined 7 tables, aggregated 2 billion CDRs, and produced a 500-row result. It was scheduled to complete in 2 hours. One month it ran for 16 hours, causing the regulatory submission deadline to be missed.

**Root cause investigation:**

```sql
-- DBQL showed this query ran 16 hours on that date
SELECT QueryText, AMPCPUTime, ElapsedTime, NumResultRows
FROM DBC.QryLogV
WHERE LogDate = '2024-03-01'
  AND ElapsedTime > 3600
ORDER BY ElapsedTime DESC;

-- EXPLAIN revealed a PRODUCT JOIN in step 4
-- Statistics on one of the dimension tables had gone stale
-- (the table had 10M rows added since last stats collection)
```

**The timeline:**
1. A dimension table (`rate_plan`) had a bulk load the day before that tripled its row count
2. Statistics were last collected 3 weeks prior (50K rows recorded)
3. Optimizer thought `rate_plan` had 50K rows → used it as the "small" table in a hash join
4. Actually 150K rows → hash table didn't fit in memory → spilled to spool
5. The spill triggered a fallback to product join in the next step
6. 16 hours of CPU time for what should be 90 minutes

**Fix:**
```sql
COLLECT STATISTICS ON rate_plan COLUMN (rate_plan_id);
COLLECT STATISTICS ON rate_plan INDEX (rate_plan_id);
```
Re-ran the query: 1 hour 45 minutes.

**Permanent fix:** Added post-load statistics collection to the ETL pipeline for all dimension tables.

---

## Production Pattern: Automated Statistics Maintenance

A large retailer implemented a statistics maintenance framework:

```sql
-- Procedure to identify stale stats (simplified)
SELECT
    DatabaseName,
    TableName,
    ColumnName,
    LastCollectDate,
    (CURRENT_DATE - LastCollectDate) AS DaysSinceCollect,
    RowCount AS StatsRowCount,
    CAST(DBC.TableSizeV.CurrentPerm / AvgRowSize AS BIGINT) AS EstCurrentRows
FROM DBC.StatsV
JOIN DBC.TableSizeV USING (DatabaseName, TableName)
WHERE DaysSinceCollect > 7
   OR (EstCurrentRows > StatsRowCount * 1.2)  -- 20% data growth
ORDER BY DaysSinceCollect DESC;
```

**Automation rules:**
- Any table with > 10% row count change since last stats → automatic collect (next maintenance window)
- Weekly full stats refresh for all tables in production schema
- Post-ETL stats collect triggered by ETL pipeline for affected tables

---

## Real-World Join Optimization: The Fact-to-Fact Join

**Challenge:** Two large fact tables needed to be joined (both > 10 billion rows):
- `web_events` (PI = user_id, 15B rows)
- `purchase_events` (PI = user_id, 8B rows)
- Join condition: `user_id`

**Since both tables have the same PI (`user_id`), the join is AMP-local.** No redistribution needed.

**EXPLAIN confirmed:**
```
Step 3: We do an all-AMPs JOIN step from spool 1 (all AMPs) and spool 2
  (all AMPs) by way of a MERGE JOIN operator, matched by rowkey only.
  No rows are redistributed.
```

**Query ran in 12 minutes** — all 23 billion rows joined across 256 AMPs simultaneously.

**Key insight:** When both fact tables have the same PI, fact-to-fact joins are viable in Teradata without a denormalized bridge. This is a Teradata strength vs Redshift (where fact-to-fact joins almost always require redistribution).

---

## War Story: The Spool Explosion

**Setting:** An investment bank's overnight risk calculation batch.

**The symptom:** "No more spool space" error at 3 AM. Report not ready by 6 AM market open. Traders blind.

**The query (simplified):**
```sql
-- Risk calculation: cross every position with every scenario
SELECT p.position_id, s.scenario_id, 
       p.notional * s.shock_factor AS scenario_pnl
FROM positions p, scenarios s  -- implicit cross join!
WHERE p.desk = 'RATES';
```

**The problem:** `positions` had 50,000 rows (after filter), `scenarios` had 10,000 rows. The developer forgot the join condition — this is a Cartesian product.

`50,000 × 10,000 = 500 million rows in spool` — each row containing DECIMAL(20,8) values.

**Actual fix:**
```sql
-- Scenarios were supposed to be applied by scenario type
SELECT p.position_id, s.scenario_id, 
       p.notional * s.shock_factor AS scenario_pnl
FROM positions p
JOIN scenarios s ON p.asset_class = s.asset_class  -- missing join condition!
WHERE p.desk = 'RATES';
```
Result: 50,000 positions × avg 15 relevant scenarios = 750,000 rows. No spool issue.

**Process improvement:** Added EXPLAIN to the pre-flight checklist for all batch queries. Any `PRODUCT JOIN` in EXPLAIN requires sign-off.

---

## Workload-Specific Optimization Patterns

### Pattern 1: Tactical Queries (SLA < 2 seconds)
```sql
-- Ensure single-AMP access with PI filter
SELECT order_id, status, total_amount
FROM orders
WHERE customer_id = 12345    -- PI column → single-AMP
  AND order_date > '2024-01-01';  -- PPI → partition elimination
```
- Must use PI in WHERE for single-AMP routing
- Add NUSI on frequently filtered non-PI columns
- Collect full statistics (no sampling)

### Pattern 2: Strategic Queries (SLA = hours)
```sql
-- Use PPI elimination for date-range aggregations
SELECT sale_date, region, SUM(amount)
FROM sales_fact
WHERE sale_date BETWEEN '2024-01-01' AND '2024-03-31'  -- PPI eliminates
GROUP BY sale_date, region;
```
- PPI is critical for time-series aggregation
- Collect partition-level statistics: `COLLECT STATISTICS COLUMN (PARTITION) ON sales_fact`
- Use SAMPLE stats if full collection takes too long

---

## Interview Tips

> **Tip 1:** "How do you prevent production query failures due to stale statistics?" — "Implement a stats maintenance framework that detects tables where row count has grown > 10-20% since last collection, and schedules automatic COLLECT STATISTICS in maintenance windows. Also trigger stats collection in ETL pipelines after significant loads."

> **Tip 2:** "Tell me about a spool space issue you've seen or would troubleshoot." — "Describe the spool explosion pattern: missing join condition = Cartesian product = spool overflow. Prevention: add EXPLAIN to batch query pre-flight checks. Any PRODUCT JOIN in the plan requires investigation. Also limit spool per user/profile to contain blast radius."

> **Tip 3:** "How would you optimize a fact-to-fact join in Teradata?" — "Choose the same PI for both fact tables (the shared join key). This makes the join AMP-local — no redistribution needed. EXPLAIN will show 'MERGE JOIN, matched by rowkey only, no rows redistributed.' This is one of Teradata's key advantages over cloud MPP systems."

> **Tip 4:** "How do you use DBQL for query optimization?" — "DBQL captures AMPCPUTime, ElapsedTime, SpoolUsage, and NumResultRows for every query. I sort by AMPCPUTime to find expensive queries, then look for anomalies (high CPU for few result rows = product join suspect). I join with DBQL's step-level tables for more detail on individual execution steps."
