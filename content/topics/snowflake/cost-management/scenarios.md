---
title: "Snowflake Cost Management - Scenario Questions"
topic: snowflake
subtopic: cost-management
content_type: scenario_question
tags: [snowflake, cost, optimization, scenarios, interview]
---

# Scenario Questions — Snowflake Cost Management

<article data-difficulty="junior">

## 🟢 Junior: Configure a Warehouse for Cost Efficiency

**Scenario:** Your team has one warehouse used by 5 analysts for ad-hoc queries during business hours (9am–6pm). It's currently `LARGE` with no auto-suspend. Analysts complain queries sometimes queue. What changes would you make to reduce cost while improving experience?

<details>
<summary>✅ Solution</summary>

```sql
-- Current state: LARGE warehouse, always on, queries queueing

-- Problem 1: "Always on" wastes credits overnight/weekends
-- Problem 2: Queries queue = concurrency issue, not size issue

-- Solution: Right-size + multi-cluster + auto-suspend

ALTER WAREHOUSE analytics_wh
    SET WAREHOUSE_SIZE = 'MEDIUM'           -- downsize (ad-hoc queries rarely need LARGE)
    AUTO_SUSPEND = 60                        -- suspend after 1 min idle
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3                    -- scale out for concurrent queries (fixes queueing)
    SCALING_POLICY = 'ECONOMY';             -- prefer filling clusters before adding new ones

-- Additionally: schedule a suspend/resume for business hours only
-- (Manual workaround — Snowflake doesn't have native schedules for this)
-- Use a Snowflake Task to enforce:
CREATE TASK suspend_after_hours
    WAREHOUSE = system_wh
    SCHEDULE = 'USING CRON 0 18 * * MON-FRI America/New_York'
AS ALTER WAREHOUSE analytics_wh SUSPEND;

CREATE TASK resume_morning
    WAREHOUSE = system_wh
    SCHEDULE = 'USING CRON 0 9 * * MON-FRI America/New_York'
AS ALTER WAREHOUSE analytics_wh RESUME;

ALTER TASK suspend_after_hours RESUME;
ALTER TASK resume_morning RESUME;
```

**Expected result:**
- Cost: LARGE always-on (~8 credits/hr × 24h × 30 days = 5,760 credits) → MEDIUM + auto-suspend (~4 credits/hr × 8h business × 22 days = 704 credits). ~88% cost reduction.
- Experience: Multi-cluster eliminates queue (adds cluster instead of making queries wait).

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Investigate a 3x Credit Spike

**Scenario:** Your Snowflake bill jumped from 2,000 credits/month to 6,000 credits last month. No new features were deployed. Identify the root cause using ACCOUNT_USAGE views.

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Narrow down WHEN the spike happened (day-level)
SELECT
    DATE_TRUNC('day', start_time) AS day,
    warehouse_name,
    SUM(credits_used) AS credits
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
WHERE start_time >= DATEADD('month', -2, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY 1, 3 DESC;
-- Look for the day where credits jump — e.g., "2024-03-08: ANALYTICS_WH went from 60 to 350 credits"

-- Step 2: Find the most expensive queries on that day
SELECT user_name, query_text, warehouse_name,
       TOTAL_ELAPSED_TIME / 1000 AS secs,
       PARTITIONS_SCANNED, PARTITIONS_TOTAL,
       BYTES_SCANNED / 1e9 AS gb_scanned
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE start_time BETWEEN '2024-03-08' AND '2024-03-09'
ORDER BY TOTAL_ELAPSED_TIME DESC
LIMIT 20;

-- Step 3: Check serverless costs (often missed)
SELECT 'AutoCluster' AS service, SUM(credits_used) AS credits, MIN(start_time) AS first_charge
FROM SNOWFLAKE.ACCOUNT_USAGE.AUTOMATIC_CLUSTERING_HISTORY
WHERE start_time >= DATEADD('month', -2, CURRENT_TIMESTAMP())
UNION ALL
SELECT 'MVRefresh', SUM(credits_used), MIN(start_time)
FROM SNOWFLAKE.ACCOUNT_USAGE.MATERIALIZED_VIEW_REFRESH_HISTORY
WHERE start_time >= DATEADD('month', -2, CURRENT_TIMESTAMP())
UNION ALL
SELECT 'Snowpipe', SUM(credits_used), MIN(start_time)
FROM SNOWFLAKE.ACCOUNT_USAGE.PIPE_USAGE_HISTORY
WHERE start_time >= DATEADD('month', -2, CURRENT_TIMESTAMP());

-- Step 4: Look for new warehouses created around the spike date
SELECT warehouse_name, created_on
FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSES
WHERE created_on >= DATEADD('month', -2, CURRENT_TIMESTAMP())
ORDER BY created_on DESC;
```

**Common findings:**
- A new BI tool connected with auto-suspend disabled (warehouse idle 24/7)
- Automatic clustering was added to a high-DML table
- A dbt model lost its partition filter (full scan instead of incremental)
- A new developer created a 4XL warehouse for "testing" and forgot to suspend it

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design a Cost Allocation System for 8 Teams

**Scenario:** Your company has 8 data teams sharing one Snowflake account. Finance wants monthly chargeback reports showing how many credits each team consumed, split by compute and storage. Design the attribution system.

<details>
<summary>✅ Solution</summary>

**Design: warehouse naming convention + query tags + monthly report**

```sql
-- Convention: TEAM_USECASE_SIZE_WH
-- e.g.: MARKETING_ANALYTICS_M_WH, DATAENG_ETL_XL_WH, FINANCE_REPORTS_S_WH

-- Query attribution: require query tags for all non-interactive sessions
-- Enforce via dbt profile, Airflow operator, Jupyter kernel config:
-- query_tag: '{"team":"marketing","env":"prod","pipeline":"weekly_cohort"}'

-- Monthly chargeback report
WITH warehouse_credits AS (
    SELECT
        warehouse_name,
        SPLIT_PART(warehouse_name, '_', 1) AS team,
        SUM(credits_used) AS compute_credits
    FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
    WHERE start_time >= DATE_TRUNC('month', DATEADD('month', -1, CURRENT_DATE()))
      AND start_time < DATE_TRUNC('month', CURRENT_DATE())
    GROUP BY 1, 2
),
storage_by_role AS (
    SELECT
        LOWER(SPLIT_PART(table_owner, '_', 1)) AS team,
        SUM(active_bytes + time_travel_bytes + failsafe_bytes) AS total_bytes
    FROM SNOWFLAKE.ACCOUNT_USAGE.TABLE_STORAGE_METRICS
    WHERE deleted = FALSE
    GROUP BY 1
),
credits_per_dollar AS (SELECT 3.5 AS rate)  -- adjust per your contract
SELECT
    w.team,
    ROUND(w.compute_credits, 2) AS compute_credits,
    ROUND(w.compute_credits * r.rate, 2) AS compute_cost_usd,
    ROUND(s.total_bytes / 1e12 * 23, 2) AS storage_cost_usd,
    ROUND(w.compute_credits * r.rate + s.total_bytes / 1e12 * 23, 2) AS total_cost_usd
FROM warehouse_credits w
LEFT JOIN storage_by_role s USING (team)
CROSS JOIN credits_per_dollar r
ORDER BY total_cost_usd DESC;
```

**Governance layer:**
```sql
-- Per-team resource monitors (prevent one team from blowing the budget)
CREATE RESOURCE MONITOR marketing_guard
    WITH CREDIT_QUOTA = 2000
    FREQUENCY = MONTHLY
    TRIGGERS
        ON 80 PERCENT DO NOTIFY
        ON 100 PERCENT DO SUSPEND;
ALTER WAREHOUSE marketing_analytics_m_wh SET RESOURCE_MONITOR = marketing_guard;
ALTER WAREHOUSE marketing_reports_s_wh   SET RESOURCE_MONITOR = marketing_guard;
```

**Escalation path:**
- Teams see their own spend via a Snowsight dashboard (shared ACCOUNT_USAGE view)
- Monthly report exported to finance by data platform team
- Teams over budget by 20%+ get a review meeting before next month

</details>
</article>
