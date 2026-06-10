---
title: "Dynamic Tables - Scenario Questions"
topic: snowflake
subtopic: dynamic-tables
content_type: scenario_question
tags: [snowflake, dynamic-tables, interview, scenarios]
---

# Scenario Questions — Dynamic Tables

<article data-difficulty="junior">

## 🟢 Junior: Creating a Basic Dynamic Table

**Scenario:** Raw orders land in `raw.orders` via Snowpipe. Create a Dynamic Table that cleans the data (type casting, null filtering, deduplication) and keeps it fresh within 10 minutes.

<details>
<summary>💡 Hint</summary>
Use CREATE DYNAMIC TABLE with TARGET_LAG = '10 minutes'. Cast types, filter nulls, and use QUALIFY ROW_NUMBER for dedup on order_id.
</details>

<details>
<summary>✅ Solution</summary>

```sql
CREATE OR REPLACE DYNAMIC TABLE silver.orders
    TARGET_LAG = '10 minutes'
    WAREHOUSE = 'ETL_WH'
AS
    SELECT 
        order_id::NUMBER AS order_id,
        customer_id::NUMBER AS customer_id,
        amount::DECIMAL(10,2) AS amount,
        TRY_TO_DATE(order_date) AS order_date,
        status::VARCHAR AS status,
        _loaded_at
    FROM raw.orders
    WHERE order_id IS NOT NULL 
      AND amount > 0
      AND TRY_TO_DATE(order_date) IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1;

-- Verify it's working:
SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES()) WHERE NAME = 'ORDERS';
-- Shows: TARGET_LAG, REFRESH_MODE (should be INCREMENTAL), SCHEDULING_STATE
```

**Key Points:**
- TARGET_LAG = '10 minutes': Snowflake guarantees data is never >10 min stale
- QUALIFY ROW_NUMBER: keeps only the latest version of each order_id (dedup)
- TRY_TO_DATE: returns NULL for invalid dates (filter catches them)
- No stream, no task, no MERGE — Snowflake handles incremental refresh automatically!
- This replaces ~25 lines of stream + task + MERGE code with 10 lines

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Dynamic Table vs Task

**Scenario:** Your colleague built an ETL pipeline using Streams + Tasks (30 lines of SQL). You suggest replacing it with a Dynamic Table (10 lines). What are the trade-offs? When should you NOT replace?

<details>
<summary>💡 Hint</summary>
DTs are simpler but less flexible. They can't handle: multi-step DML, stored procedure calls, conditional logic, or SCD Type 2. Tasks are more verbose but support arbitrary SQL logic.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- REPLACE WITH DT when the task does:
-- Simple INSERT...SELECT or MERGE with straightforward logic
-- Example (30 lines with stream + task → 10 lines with DT):

-- TASK VERSION (30 lines):
CREATE STREAM s ON TABLE raw.orders;
CREATE TASK t WAREHOUSE='WH' SCHEDULE='15 MINUTE' WHEN SYSTEM$STREAM_HAS_DATA('s')
AS MERGE INTO silver.orders t USING (SELECT * FROM s WHERE METADATA$ACTION='INSERT'
   QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1) s
   ON t.order_id = s.order_id WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT ...;
ALTER TASK t RESUME;

-- DT VERSION (10 lines):
CREATE DYNAMIC TABLE silver.orders TARGET_LAG='15 minutes' WAREHOUSE='WH' AS
SELECT order_id, amount, order_date FROM raw.orders WHERE order_id IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY _loaded_at DESC) = 1;

-- DO NOT REPLACE when the task does:
-- 1. SCD Type 2 (UPDATE old record + INSERT new record = multi-step DML)
-- 2. Calls stored procedures (DTs can only be a SELECT)
-- 3. Conditional logic (IF stream_A has data → do X, ELSE → do Y)
-- 4. Cross-database operations
-- 5. DELETE operations (DT can't do explicit DELETEs from target)
-- 6. Notifications/alerts after processing
```

| Replace with DT? | Scenario | Reason |
|---|---|---|
| ✅ Yes | Simple SELECT + WHERE + GROUP BY | DT handles this perfectly |
| ✅ Yes | JOIN fact with dimension | DT supports JOINs incrementally |
| ✅ Yes | Dedup with QUALIFY | DT handles this |
| ❌ No | SCD Type 2 (history tracking) | Requires multi-step DML |
| ❌ No | Conditional branching | DTs are just a SELECT |
| ❌ No | Send notifications on completion | DT has no "after refresh" hook |
| ❌ No | Complex error handling | DT retries automatically but can't custom-handle |

**Key Points:**
- 80% of ETL tasks can be replaced by Dynamic Tables (standard transforms)
- 20% need Tasks (complex logic, multi-step operations, side effects)
- DT advantages: less code, automatic incremental, automatic scheduling
- Task advantages: full control, arbitrary SQL/procedures, error handling

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Multi-Layer Pipeline

**Scenario:** Build a 3-layer pipeline with Dynamic Tables: raw.events → silver.events (parsed, deduped) → gold.event_metrics (hourly counts). Set appropriate TARGET_LAGs and explain the end-to-end latency.

<details>
<summary>💡 Hint</summary>
Silver: faster refresh (5-10 min) since it's the foundation. Gold: can be slower (30 min) since it aggregates. End-to-end = sum of both lags in worst case.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- LAYER 1: Silver (parse + dedup, 5-min freshness)
CREATE DYNAMIC TABLE silver.events
    TARGET_LAG = '5 minutes'
    WAREHOUSE = 'ETL_WH'
AS
    SELECT 
        event_id::VARCHAR AS event_id,
        user_id::NUMBER AS user_id,
        event_type::VARCHAR AS event_type,
        TRY_TO_TIMESTAMP(event_time) AS event_time,
        properties::VARIANT AS properties,
        _loaded_at
    FROM raw.events
    WHERE event_id IS NOT NULL AND event_type IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY _loaded_at DESC) = 1;

-- LAYER 2: Gold (hourly aggregation, 30-min freshness)
CREATE DYNAMIC TABLE gold.event_metrics
    TARGET_LAG = '30 minutes'
    WAREHOUSE = 'ETL_WH'
AS
    SELECT 
        DATE_TRUNC('hour', event_time) AS hour,
        event_type,
        COUNT(*) AS event_count,
        COUNT(DISTINCT user_id) AS unique_users
    FROM silver.events
    WHERE event_time >= DATEADD('day', -30, CURRENT_DATE())  -- Rolling 30 days
    GROUP BY DATE_TRUNC('hour', event_time), event_type;

-- END-TO-END LATENCY ANALYSIS:
-- Worst case: raw change happens right AFTER silver refreshed
-- Wait for silver: up to 5 minutes
-- Wait for gold: up to 30 minutes AFTER silver updated
-- Total worst case: 5 + 30 = 35 minutes

-- Average case: raw change happens mid-cycle
-- Silver refreshes within ~2.5 min (midpoint of 5 min window)
-- Gold refreshes within ~15 min of silver update
-- Total average: ~17.5 minutes

-- BEST case: raw changes while both are about to refresh
-- Total: seconds (both refresh immediately after detecting change)

-- For the dashboard team:
-- "Data in gold.event_metrics is never more than 35 minutes behind reality"
-- (sufficient for hourly operational dashboards)
```

**Key Points:**
- Silver TARGET_LAG: 5 min (fast, since it's the foundation for all downstream)
- Gold TARGET_LAG: 30 min (acceptable for hourly metrics dashboard)
- End-to-end worst case: sum of all layer lags (5 + 30 = 35 min)
- Snowflake auto-manages: silver refreshes first, then gold refreshes after
- No dependency declaration needed — Snowflake infers from the SQL references
- Incremental: both DTs process only changes (not full recompute)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Handling Full Refresh DTs

**Scenario:** Your gold DT uses `ROW_NUMBER() OVER (ORDER BY total_spend DESC)` to rank customers. This forces FULL refresh (not incremental). It has 10M rows and costs $5/refresh. With TARGET_LAG='30 minutes', that's 48 refreshes/day = $240/day. Optimize.

<details>
<summary>💡 Hint</summary>
For full-refresh DTs: increase TARGET_LAG (less frequent refresh = less cost). Or restructure to avoid window functions (move ranking to a view on top of an incremental DT).
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- CURRENT: Full refresh DT (expensive)
CREATE DYNAMIC TABLE gold.customer_rankings
    TARGET_LAG = '30 minutes'  -- 48 refreshes/day × $5 = $240/day!
    WAREHOUSE = 'ETL_WH'
AS
    SELECT customer_id, total_spend,
           ROW_NUMBER() OVER (ORDER BY total_spend DESC) AS spending_rank
    FROM silver.customer_metrics;
-- Window function → FULL REFRESH every 30 min!

-- OPTIMIZATION 1: Increase TARGET_LAG (if business allows)
ALTER DYNAMIC TABLE gold.customer_rankings SET TARGET_LAG = '4 hours';
-- Now: 6 refreshes/day × $5 = $30/day (92% savings!)
-- Acceptable if rankings update every 4 hours (most businesses: yes!)

-- OPTIMIZATION 2: Split into incremental DT + view
-- Step A: Incremental DT (no window function) — cheap, frequent
CREATE DYNAMIC TABLE gold.customer_metrics_base
    TARGET_LAG = '30 minutes'
    WAREHOUSE = 'ETL_WH'
AS
    SELECT customer_id, name, region,
           SUM(amount) AS total_spend,
           COUNT(*) AS order_count
    FROM silver.orders
    GROUP BY customer_id, name, region;
-- This is INCREMENTAL (GROUP BY supports it) → cheap refresh!

-- Step B: View with ranking (computed at query time, not stored)
CREATE VIEW gold.customer_rankings AS
    SELECT *, ROW_NUMBER() OVER (ORDER BY total_spend DESC) AS spending_rank
    FROM gold.customer_metrics_base;
-- No storage cost, no refresh cost
-- Rankings computed fresh on every query (milliseconds for 10M rows with caching)

-- COST COMPARISON:
-- Original: $240/day (full refresh DT every 30 min)
-- Optimized (Option 1): $30/day (full refresh every 4 hours)
-- Optimized (Option 2): ~$5/day (incremental DT + view) ← BEST!
```

**Key Points:**
- Window functions force FULL REFRESH (entire table recomputed each time)
- For full-refresh DTs: increase TARGET_LAG proportionally to acceptable freshness
- Better: restructure to avoid window functions in the DT itself
- Pattern: incremental DT (base metrics) + view (adds ranking on query)
- Views add zero cost (computed at query time, not stored/refreshed)
- The view's ROW_NUMBER executes in milliseconds on the pre-aggregated DT

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Enterprise Dynamic Table Architecture

**Scenario:** Design a Dynamic Table architecture for 20 source tables, 30 silver transformations, and 15 gold aggregations. Requirements: silver fresh within 10 min, gold within 1 hour, total cost under $2K/month. Include monitoring and error handling.

<details>
<summary>💡 Hint</summary>
Use DOWNSTREAM for intermediate silver tables that only feed other DTs. Right-size warehouses (XS for small DTs, S for large). Monitor via DYNAMIC_TABLE_REFRESH_HISTORY. Suspend overnight if freshness isn't needed 24/7.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- ARCHITECTURE: 20 sources → 30 silver DTs → 15 gold DTs

-- WAREHOUSE STRATEGY (right-size by refresh cost):
-- Small DTs (< 1M rows, simple transforms): XS warehouse
-- Medium DTs (1-100M rows, joins): S warehouse  
-- Large DTs (> 100M rows, complex aggregations): M warehouse

-- SILVER LAYER (30 DTs, 10 min freshness)
-- 20 "primary" silver DTs: clean and type source tables
CREATE DYNAMIC TABLE silver.orders TARGET_LAG = '10 minutes' WAREHOUSE = 'ETL_XS' AS ...;
CREATE DYNAMIC TABLE silver.customers TARGET_LAG = '10 minutes' WAREHOUSE = 'ETL_XS' AS ...;
-- ... (20 similar DTs)

-- 10 "enriched" silver DTs: join primary silver tables
CREATE DYNAMIC TABLE silver.enriched_orders 
    TARGET_LAG = DOWNSTREAM  -- Only refreshes when gold needs it!
    WAREHOUSE = 'ETL_S' 
AS
    SELECT o.*, c.region, c.segment, p.category
    FROM silver.orders o
    JOIN silver.customers c ON o.customer_id = c.customer_id
    JOIN silver.products p ON o.product_id = p.product_id;
-- TARGET_LAG = DOWNSTREAM: saves cost (doesn't refresh unless gold needs it)

-- GOLD LAYER (15 DTs, 1 hour freshness)
CREATE DYNAMIC TABLE gold.daily_revenue TARGET_LAG = '1 hour' WAREHOUSE = 'ETL_S' AS ...;
CREATE DYNAMIC TABLE gold.customer_ltv TARGET_LAG = '1 hour' WAREHOUSE = 'ETL_S' AS ...;
-- ... (15 gold DTs)

-- COST ESTIMATE:
-- Silver (20 primary × 10 min = ~6 refreshes/hour × $0.01/refresh): $0.06/hour × 720 = $43/month
-- Silver (10 enriched × DOWNSTREAM ≈ 4 refreshes/hour × $0.03/refresh): $0.12/hour × 720 = $86/month
-- Gold (15 × 1 hour = 1 refresh/hour × $0.05/refresh): $0.75/hour × 720 = $540/month
-- Warehouse idle time: minimal (serverless or auto-suspend)
-- TOTAL: ~$670/month ✓ (well under $2K budget!)

-- MONITORING:
CREATE DYNAMIC TABLE ops.dt_health_metrics
    TARGET_LAG = '5 minutes'
    WAREHOUSE = 'OPS_XS'
AS
    SELECT 
        NAME, SCHEMA_NAME, TARGET_LAG, SCHEDULING_STATE,
        DATA_TIMESTAMP AS last_refreshed,
        TIMESTAMPDIFF('minute', DATA_TIMESTAMP, CURRENT_TIMESTAMP()) AS actual_lag_min
    FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLES());

-- ALERTING (Task checks every 10 min):
CREATE TASK ops.dt_alert WAREHOUSE = 'OPS_XS' SCHEDULE = '10 MINUTE' AS
BEGIN
    LET behind_count := (
        SELECT COUNT(*) FROM ops.dt_health_metrics
        WHERE actual_lag_min > SPLIT_PART(TARGET_LAG, ' ', 1)::NUMBER * 3  -- 3x lag = problem
    );
    IF (behind_count > 0) THEN
        CALL system$send_email('data-team@company.com', 'DT Health Alert',
            behind_count || ' Dynamic Tables exceed 3x their TARGET_LAG!');
    END IF;
END;

-- OVERNIGHT COST SAVINGS (if freshness not needed 24/7):
-- Suspend all gold DTs from 10 PM to 6 AM:
-- (Scheduled task suspends and resumes based on time)
```

**Key Points:**
- 65 total Dynamic Tables: manageable with zero manual orchestration
- DOWNSTREAM for intermediate tables: only refresh when actually needed (cost savings)
- Right-sized warehouses: XS for simple, S for joins, M for heavy aggregations
- Total cost: ~$670/month for 65 DTs (very cost-effective vs Streams+Tasks)
- Monitoring: meta-DT watches all other DTs (uses INFORMATION_SCHEMA)
- Alert on 3× lag exceedance (indicates a problem, not just normal variation)
- Overnight suspension: reduces cost by ~30% if 24/7 freshness not required

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What are Snowflake Dynamic Tables and how do they differ from regular views?**
A: Dynamic Tables are materialized query results that Snowflake automatically refreshes on a defined lag schedule. Unlike regular views (which re-execute on every query), dynamic tables store results and are incrementally maintained, giving you materialized view semantics with automatic orchestration—no separate pipeline needed.

**Q: What is the "target lag" parameter in a Dynamic Table?**
A: Target lag defines the maximum acceptable staleness of a dynamic table's data relative to its source tables. Snowflake's automatic refresh engine schedules refreshes to meet this lag. A target lag of "1 minute" means the table should never be more than 1 minute behind its sources.

**Q: How do Dynamic Tables handle incremental refresh?**
A: Snowflake analyzes the dynamic table's query and, where possible, computes only the rows affected by upstream changes since the last refresh (incremental processing). For queries that don't support incremental computation (e.g., certain aggregations), it falls back to a full refresh automatically.

**Q: What is the difference between Dynamic Tables and Streams + Tasks?**
A: Streams + Tasks require you to manually write the incremental logic and orchestrate refresh scheduling. Dynamic Tables abstract away both—Snowflake determines what changed and when to refresh automatically. Dynamic Tables are simpler to maintain but offer less control; Streams + Tasks are more flexible for complex CDC patterns.

**Q: Can Dynamic Tables be chained, and what does that enable?**
A: Yes. A dynamic table can be defined on top of other dynamic tables, forming a dependency graph. Snowflake automatically orchestrates the refresh order, ensuring upstream tables are refreshed before downstream ones. This enables declarative definition of multi-hop transformation pipelines without explicit orchestration code.

**Q: What are the limitations of Dynamic Tables?**
A: Dynamic Tables don't support every SQL construct—certain window functions or non-deterministic functions may prevent incremental refresh and force full refresh. They also require Snowflake Enterprise or above for some features. You cannot update or delete rows manually; the content is fully controlled by the query definition.

**Q: How do you monitor Dynamic Table refresh status and lag?**
A: Use `INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY` or the `DYNAMIC_TABLES` view to inspect refresh timestamps, durations, and errors. Snowflake also surfaces lag metrics in the UI. You should alert when actual lag exceeds the target lag to detect processing backlogs.

**Q: When would you choose a Dynamic Table over a dbt model?**
A: Dynamic Tables are best for always-on, low-latency continuous refresh scenarios within Snowflake. dbt models are better when you need transformation logic across multiple warehouse targets, rich testing, documentation generation, or complex orchestration with Airflow/Dagster. dbt also has a broader open-source ecosystem for governance and lineage.

---

## 💼 Interview Tips

- Emphasize that Dynamic Tables represent a shift toward declarative pipelines—you define the query and the lag, Snowflake handles the rest. This simplicity reduces operational overhead and is a strong selling point for platform simplification.
- Always compare Dynamic Tables to the alternatives (Streams + Tasks, dbt, materialized views) and articulate why you'd choose each—showing you can match tool to use case is what senior interviewers look for.
- Mention monitoring and lag alerting proactively—in production, knowing when your dynamic tables are falling behind is as important as setting them up.
- Be honest about limitations: not all queries support incremental refresh, and full refresh can be expensive for large tables. Demonstrating awareness of the cost model shows maturity.
- Senior interviewers may ask about chained dynamic table pipelines—know that Snowflake resolves the dependency graph automatically, but also understand this can make debugging harder when an upstream table has a schema change.
