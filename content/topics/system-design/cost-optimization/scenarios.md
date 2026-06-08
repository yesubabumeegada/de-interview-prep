---
title: "Cost Optimization — Scenarios"
topic: system-design
subtopic: cost-optimization
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [system-design, cost-optimization, interview, scenarios]
---

# Cost Optimization — Interview Scenarios

## Scenario 1: $100K/Month Cloud Bill — Find the Waste

**Question:** A startup has a $100K/month data infrastructure bill. The CTO wants to cut it by 40% without impacting any SLAs. Where do you start?

**Answer:**

```
Step 1: Get cost breakdown by category (1 day)
  AWS Cost Explorer: costs by service, last 3 months
  Typical breakdown found:
    Snowflake (compute):  $35,000 (35%) ← biggest lever
    S3 storage:           $20,000 (20%)
    EMR/Spark compute:    $25,000 (25%)
    Data transfer:         $8,000 (8%)
    Other:                $12,000 (12%)

Step 2: Quick wins (Week 1) — target $20K savings
  Snowflake warehouses: check AUTO_SUSPEND setting
    Found: 5 warehouses set to 10-minute auto-suspend, several running idle nights/weekends
    Fix: set all to 60 seconds → 80% reduction in idle time
    Estimated savings: $12,000/month

  S3 storage: check lifecycle policies
    Found: no lifecycle policies; all data in Standard tier
    Fix: add lifecycle (IA after 30d, Glacier after 90d)
    Estimated savings: $8,000/month (60% of S3 bill over 6 months as data migrates)

  Total quick wins: ~$20,000/month (20%) ✓

Step 3: Compute optimization (Week 2-3) — target $20K more savings
  EMR batch jobs: all using on-demand instances
    Fix: switch task nodes to spot (80% of workers) → $15,000/month savings

  Snowflake warehouse sizing: check utilization
    Found: 2 warehouses running at XL size; Spark UI shows < 20% CPU utilization
    Fix: downsize to L → save 50% compute for those warehouses → $5,000/month

  Total compute: ~$20,000/month (20%) ✓

Step 4: Query optimization (Week 4) — target $5K more savings
  Run "full scan queries" report (no partition filter)
  Found: 3 recurring queries scanning full 5TB table
  Fix: add WHERE order_date >= DATEADD(day, -90, CURRENT_DATE)
  Savings: 98% query cost reduction for those queries → $5,000/month

Total 30-day savings: $45,000/month = 45% of $100K bill ✓
All achieved without changing any SLAs.
```

---

## Scenario 2: Snowflake Bill Spike Investigation

**Question:** Snowflake credits consumed jumped from 500/day to 2,000/day over one weekend. No new features were deployed. What happened?

**Answer:**

```sql
-- Step 1: Find the spike date and warehouse
SELECT DATE(start_time) AS day,
       warehouse_name,
       SUM(credits_used) AS daily_credits
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time > DATEADD(day, -14, CURRENT_TIMESTAMP)
GROUP BY day, warehouse_name
ORDER BY day DESC, daily_credits DESC;
-- Result: 2024-01-20 Saturday: bi_warehouse: 1,800 credits (normally 200)

-- Step 2: Find what ran on that warehouse Saturday
SELECT start_time, user_name, query_text,
       execution_time/1000 AS exec_sec,
       partitions_scanned, partitions_total,
       bytes_scanned/1e9 AS gb_scanned
FROM snowflake.account_usage.query_history
WHERE warehouse_name = 'BI_WAREHOUSE'
  AND DATE(start_time) = '2024-01-20'
ORDER BY execution_time DESC
LIMIT 20;
-- Result: 50,000 queries from user 'tableau_service_account'
-- Each query: scans 50GB, 3 seconds, no partition filter
-- tableau_service_account ran a new dashboard extract... 50,000 times!

-- Step 3: Root cause
-- New Tableau dashboard was published Friday with a scheduled extract
-- Extract was set to run every 5 minutes (!) instead of daily
-- 24 hours × 12 extracts/hour × 50GB scan × $0.003/GB = $1,440 in compute

-- Fix:
-- 1. Suspend bi_warehouse immediately to stop bleeding
-- 2. Change Tableau extract schedule to daily (6 AM)
-- 3. Add partition filter to the dashboard queries (date = TODAY)
-- 4. Add alert: bi_warehouse credits > 300/day → immediate notification
-- 5. Process: require DBA approval for new scheduled Tableau extracts
--    before publishing to production
```

---

## Scenario 3: Design a Cost-Efficient Architecture for 1PB Data Lake

**Question:** Design a cost-efficient 1 petabyte data lake architecture. Data: 500TB recent data (queried daily), 500TB cold data (queried monthly or less). Budget constraint: minimize storage and query costs.

**Answer:**

```
Storage strategy:

Tier 1 — Hot (last 90 days, ~150TB):
  S3 Standard: $0.023/GB = $3,450/month
  Stored as: Delta Lake Parquet + zstd (10:1 compression from raw)
  Original raw: 1.5PB of events compressed to 150TB
  Queried by: Snowflake external tables + Databricks SQL

Tier 2 — Warm (90 days - 2 years, ~350TB):
  S3 Standard-IA: $0.0125/GB = $4,375/month
  Lifecycle: automatically moved from Tier 1 after 90 days
  Query frequency: monthly reports, ad-hoc investigations
  Retrieval cost: $0.01/GB (added to query cost)
  Alternative: Snowflake to query via external tables (pay per byte scanned)

Tier 3 — Cold (2+ years, ~500TB):
  S3 Glacier Instant: $0.004/GB = $2,000/month
  Query frequency: compliance, annual reports, rare audit requests
  Retrieval: milliseconds (instant) but $0.03/GB retrieval cost
  Alternative: S3 Glacier Deep Archive ($0.00099/GB = $495/month) if 12-hr retrieval OK

Total storage cost:
  Tier 1: $3,450/month
  Tier 2: $4,375/month
  Tier 3: $2,000/month (Glacier Instant) or $495/month (Glacier Deep)
  Total: ~$10,000/month for 1PB (vs $23,000/month if all in S3 Standard)
  Savings: 56%

Query cost optimization:
  Hot data: Databricks + Delta data skipping → scan only relevant files
  Warm data: Snowflake external tables + partition filter (only pay for scanned data)
  Cold data: restore to S3 Standard before running large queries (cheaper than scanning Glacier repeatedly)
  
  Pre-aggregate: for Tier 2/3, pre-compute monthly summaries in Snowflake
    Monthly reports query summaries (GBs) not raw data (TBs)
    Summary tables stay in Tier 1 forever (small, frequently queried)
```
