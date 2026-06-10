---
title: "Snowflake Architecture - Scenario Questions"
topic: snowflake
subtopic: architecture
content_type: scenario_question
tags: [snowflake, architecture, interview, scenarios]
---

# Scenario Questions — Snowflake Architecture

<article data-difficulty="junior">

## 🟢 Junior: Choose the Right Warehouse Size

**Scenario:** Your team runs three workloads: (1) A nightly ETL that processes 2 TB of data and must finish in under 1 hour. (2) A BI dashboard used by 5 analysts during business hours. (3) A data scientist running ad-hoc heavy queries. How would you configure warehouses?

<details>
<summary>✅ Solution</summary>

```sql
-- ETL: Large/XL — needs power to process 2TB in <1 hour
-- Runs nightly, suspends immediately after
CREATE WAREHOUSE etl_wh
    WAREHOUSE_SIZE = 'XLARGE'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MAX_CLUSTER_COUNT = 1;  -- No scaling needed (one batch job)

-- BI Dashboard: Medium with multi-cluster for 5 concurrent users
CREATE WAREHOUSE bi_wh
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300       -- Suspend after 5 min idle (users may come back)
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3;   -- Scale for concurrent dashboard refreshes

-- Data Science: Large for heavy queries, auto-suspend aggressive
CREATE WAREHOUSE ds_wh
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 120       -- 2 min (between notebook cells)
    AUTO_RESUME = TRUE
    MAX_CLUSTER_COUNT = 1;   -- Single user, doesn't need multi-cluster
```

**Reasoning:**
- ETL needs raw power (XL = 16 nodes) to crunch 2TB quickly, then stops
- BI needs concurrency (multi-cluster) more than raw power per query
- Data science needs large memory/compute for complex queries but only one user

**Cost optimization:** ETL runs ~1 hour/day at XL (16 credits). BI runs ~8 hours/day at Medium (32 credits). DS runs ~4 hours/day at Large (32 credits). Total: ~80 credits/day.

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Recover from Accidental DELETE

**Scenario:** A developer accidentally ran `DELETE FROM production.customers WHERE 1=1` (deleted all rows). The mistake was discovered 45 minutes later. How do you recover?

<details>
<summary>✅ Solution</summary>

**Option 1: UNDROP (if table was dropped)**
```sql
-- If they DROP'd the table:
UNDROP TABLE production.customers;
```

**Option 2: Time Travel (if rows were deleted but table still exists)**
```sql
-- Restore from 45 minutes ago
CREATE TABLE production.customers_restored CLONE production.customers
    AT (OFFSET => -2700);  -- 2700 seconds = 45 minutes ago

-- Verify the restored data
SELECT COUNT(*) FROM production.customers_restored;

-- Swap the tables
ALTER TABLE production.customers RENAME TO production.customers_damaged;
ALTER TABLE production.customers_restored RENAME TO production.customers;

-- Clean up after verification
DROP TABLE production.customers_damaged;
```

**Option 3: Restore specific point using timestamp**
```sql
-- If you know the exact time before the DELETE
CREATE TABLE production.customers AS
SELECT * FROM production.customers 
    AT (TIMESTAMP => '2024-01-15 10:30:00'::TIMESTAMP);
```

**Prevention for the future:**
- Set `DATA_RETENTION_TIME_IN_DAYS = 7` (or higher) on critical tables
- Use row-level access control to prevent broad DELETEs
- Require `WHERE` clause in production roles

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Diagnose a Slow Query

**Scenario:** A query that usually takes 15 seconds now takes 8 minutes. The table hasn't grown significantly. The query:

```sql
SELECT customer_id, SUM(amount) as total
FROM fact_sales
WHERE sale_date BETWEEN '2024-01-01' AND '2024-01-31'
  AND region = 'APAC'
GROUP BY customer_id;
```

What would you check and how would you fix it?

<details>
<summary>✅ Solution</summary>

**Diagnostic checklist:**

1. **Check Query Profile for partition pruning:**
```sql
-- If partitions_scanned / partitions_total > 10%, pruning is poor
-- Look for: "Partitions scanned: 150,000 of 200,000" ← BAD
```

2. **Check for warehouse queuing:**
```sql
SELECT query_id, queued_provisioning_time, queued_overload_time
FROM snowflake.account_usage.query_history
WHERE query_id = '<id>';
-- If queued_overload_time > 0: warehouse was busy (concurrency issue)
```

3. **Check for disk spilling:**
```sql
-- In Query Profile: look for "Bytes spilled to local/remote storage"
-- If spilling: warehouse is undersized for this query
```

4. **Check if caching was lost:**
```sql
-- Was there a recent data change that invalidated the result cache?
-- Check: "Percentage scanned from cache: 0%" (usually it's high)
```

**Most likely causes and fixes:**

| Cause | Evidence | Fix |
|-------|----------|-----|
| Data recluster needed | High partition scan % | `ALTER TABLE fact_sales CLUSTER BY (sale_date, region)` |
| Warehouse contention | High queued_overload_time | Enable multi-cluster or separate warehouse |
| Warehouse suspended cold start | First query after suspend | Pre-warm or reduce auto-suspend |
| Memory spill | Bytes spilled > 0 | Increase warehouse size |
| Missing filter pushdown | Full table scan | Check for UDFs preventing pushdown |

**The fix (most likely partition pruning issue):**
```sql
-- Check current clustering
SELECT SYSTEM$CLUSTERING_INFORMATION('fact_sales', '(sale_date, region)');
-- If depth > 5 or overlap > 0.5: needs reclustering

-- Add clustering key
ALTER TABLE fact_sales CLUSTER BY (sale_date, region);
-- Snowflake will recluster in background over next few hours
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Multi-Tenant Data Platform

**Scenario:** You're building a data platform for 50 internal teams. Requirements:
- Each team sees only their own data
- Shared dimension tables (company-wide customer/product data)
- Teams can't accidentally impact each other's query performance
- Central data team manages shared infrastructure
- Monthly cost allocation per team

Design the Snowflake architecture.

<details>
<summary>✅ Solution</summary>

**Architecture:**

```sql
-- 1. DATABASE STRUCTURE
CREATE DATABASE shared_dimensions;     -- Company-wide dimensions
CREATE DATABASE team_sales;            -- Team-specific databases
CREATE DATABASE team_marketing;
CREATE DATABASE team_finance;
-- ... (one per team, or use schemas within fewer databases)

-- 2. ROLE HIERARCHY (RBAC)
-- Central admin
CREATE ROLE platform_admin;
-- Team-specific roles
CREATE ROLE team_sales_role;
CREATE ROLE team_marketing_role;
CREATE ROLE team_finance_role;

-- Grant shared read access to all teams
GRANT USAGE ON DATABASE shared_dimensions TO ROLE team_sales_role;
GRANT SELECT ON ALL TABLES IN SCHEMA shared_dimensions.curated TO ROLE team_sales_role;
-- Repeat for each team role...

-- Grant team-specific full access
GRANT ALL ON DATABASE team_sales TO ROLE team_sales_role;

-- 3. WAREHOUSE PER TEAM (performance isolation + cost tracking)
CREATE WAREHOUSE sales_wh
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 300
    RESOURCE_MONITOR = sales_monitor;

CREATE WAREHOUSE marketing_wh
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 300
    RESOURCE_MONITOR = marketing_monitor;

-- Resource monitor per team for cost allocation
CREATE RESOURCE MONITOR sales_monitor
    WITH CREDIT_QUOTA = 200  -- Monthly cap
    TRIGGERS ON 75 PERCENT DO NOTIFY
             ON 100 PERCENT DO NOTIFY;  -- Alert, don't suspend

-- 4. SHARED ETL WAREHOUSE (central team)
CREATE WAREHOUSE central_etl_wh
    WAREHOUSE_SIZE = 'XLARGE'
    AUTO_SUSPEND = 60
    COMMENT = 'Central team: shared dimension loads';
```

**Cost allocation query:**
```sql
-- Monthly credits by team (warehouse = team proxy)
SELECT 
    warehouse_name,
    DATE_TRUNC('month', start_time) AS month,
    SUM(credits_used) AS credits,
    SUM(credits_used) * 3.0 AS cost_usd
FROM snowflake.account_usage.warehouse_metering_history
WHERE start_time >= DATEADD(month, -1, CURRENT_DATE)
GROUP BY warehouse_name, month
ORDER BY cost_usd DESC;
```

**Data isolation via row access policies (fine-grained):**
```sql
-- Row access policy: users only see their team's data
CREATE ROW ACCESS POLICY team_filter AS (team_column VARCHAR)
RETURNS BOOLEAN ->
    team_column = CURRENT_ROLE()  -- Role name matches team column value
    OR IS_ROLE_IN_SESSION('PLATFORM_ADMIN');  -- Admin sees all

ALTER TABLE fact_events ADD ROW ACCESS POLICY team_filter ON (team_name);
```

**Key design decisions:**
- Separate warehouses per team = performance isolation + natural cost allocation
- Shared dimensions via cross-database grants = single source of truth
- Resource monitors = cost control without hard blocking (notify, don't suspend)
- Row access policies = fine-grained security for shared tables
- Central ETL warehouse = dedicated resources for critical shared loads

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What are the three layers of Snowflake's architecture?**
A: Snowflake has a storage layer (S3/GCS/Azure Blob for columnar micro-partitions), a compute layer (virtual warehouses—independent MPP clusters), and a cloud services layer (query parsing, optimization, metadata, access control). The separation means compute and storage scale independently.

**Q: What is a virtual warehouse in Snowflake and how does billing work?**
A: A virtual warehouse is an on-demand MPP compute cluster that executes queries. Billing is by the second when the warehouse is running (minimum 60 seconds), based on warehouse size (X-Small through 6X-Large). Warehouses auto-suspend when idle and auto-resume on query, enabling pay-per-use economics.

**Q: What are micro-partitions and how do they enable pruning?**
A: Snowflake automatically divides tables into compressed columnar micro-partitions of 50-500MB of uncompressed data. Each micro-partition stores metadata about the min/max values of each column. The query optimizer uses this metadata to skip micro-partitions that cannot contain relevant rows—called partition pruning—without reading any actual data.

**Q: What is the Snowflake query result cache?**
A: Snowflake caches the results of every query for 24 hours. If an identical query is re-submitted and the underlying data hasn't changed, the result is returned instantly from cache with no warehouse compute consumed. This dramatically reduces cost for repeated dashboard queries.

**Q: What is multi-cluster warehouses and when do you use them?**
A: Multi-cluster warehouses automatically spin up additional warehouse clusters when concurrency exceeds capacity, then scale back down. They're designed for high-concurrency workloads (e.g., many BI users querying simultaneously) where a single cluster would queue requests.

**Q: How does Snowflake handle semi-structured data?**
A: Snowflake stores JSON, Avro, Parquet, and XML in a VARIANT column type. The data is stored in columnar format with auto-detected schema. You query it using dot notation and bracket syntax (e.g., `col:field.nested`), and the optimizer can extract and cache frequently accessed paths for performance.

**Q: What is the difference between a Snowflake database, schema, and stage?**
A: A database is the top-level namespace. A schema groups related tables, views, and other objects within a database. A stage is a storage location (internal to Snowflake or external like S3) used as an intermediate landing zone for loading and unloading data.

**Q: What is Snowflake's separation of storage and compute and what operational benefits does it provide?**
A: Because storage and compute are fully separated, multiple virtual warehouses can query the same data concurrently without resource contention. You can size compute independently of data volume—run a small warehouse for light queries and a large one for heavy transforms—and pay for each independently.

---

## 💼 Interview Tips

- Lead with the three-layer architecture when asked any broad Snowflake question—it provides a framework for answering follow-up questions about performance, cost, and concurrency.
- Be specific about billing mechanics: per-second billing, auto-suspend/resume, and result cache are the key levers for cost optimization and interviewers at cost-conscious companies will probe these.
- Mention micro-partition pruning when discussing query performance—it shows you understand how Snowflake achieves performance without user-managed indexes, which is a key differentiator from traditional MPP databases.
- Avoid treating Snowflake as a black box. Senior interviewers appreciate candidates who know what happens under the hood—cloud services layer coordination, columnar storage format, and how metadata drives optimization.
- Bring up the shared-nothing compute model: each warehouse has its own compute resources, so workloads don't compete with each other—a key architectural benefit for mixed-workload environments.
