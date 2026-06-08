---
title: "Azure Synapse Analytics — Intermediate"
topic: azure
subtopic: azure-synapse
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [azure, synapse, dedicated-sql-pool, performance-tuning, workload-management, cetas]
---

# Azure Synapse Analytics — Intermediate

## Loading Data with COPY INTO (PolyBase Successor)

```sql
-- COPY INTO: fastest way to load data into Dedicated SQL Pool
-- Replaces PolyBase for most use cases (simpler syntax, better error handling)

-- Load Parquet from ADLS Gen2:
COPY INTO dbo.sales_orders (
    order_id, customer_id, product_id, amount, order_date, region
)
FROM 'https://account.dfs.core.windows.net/silver/orders/*.parquet'
WITH (
    FILE_TYPE = 'PARQUET',
    CREDENTIAL = (IDENTITY = 'Managed Identity'),
    AUTO_CREATE_TABLE = 'OFF'
);

-- Load CSV with options:
COPY INTO dbo.staging_customers
FROM 'https://account.blob.core.windows.net/raw/customers/2024/01/*.csv'
WITH (
    FILE_TYPE = 'CSV',
    FIRSTROW = 2,                -- skip header
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    CREDENTIAL = (IDENTITY = 'Managed Identity'),
    ERRORFILE = 'https://account.blob.core.windows.net/errors/',
    MAXERRORS = 100
);

-- Why COPY INTO over INSERT:
--   INSERT: row-by-row, uses transaction log heavily
--   COPY INTO: bulk parallel load across all 60 distributions
--   COPY INTO throughput: up to 200 MB/s per distribution = 12 GB/s total
--   Typical: load 1TB table in ~10 minutes

-- Best practice: load to HEAP staging table, then INSERT INTO CCI production table
CREATE TABLE dbo.staging_orders
WITH (DISTRIBUTION = ROUND_ROBIN, HEAP)  -- HEAP for fast bulk load
AS SELECT * FROM dbo.orders WHERE 1=0;   -- empty copy for structure

COPY INTO dbo.staging_orders FROM 'https://.../*.parquet' WITH (FILE_TYPE = 'PARQUET', ...);

-- Build statistics after load:
CREATE STATISTICS stat_cust_id ON dbo.staging_orders (customer_id);
CREATE STATISTICS stat_order_date ON dbo.staging_orders (order_date);
```

---

## CETAS: Create External Table As Select

```sql
-- CETAS: use Serverless SQL Pool to transform ADLS data and write results back
-- This is ELT on the data lake using T-SQL — no Spark needed

-- Pattern: Bronze → Silver transformation via CETAS

-- Step 1: Create external data source
CREATE EXTERNAL DATA SOURCE bronze_adls
WITH (
    LOCATION = 'https://account.dfs.core.windows.net/bronze',
    CREDENTIAL = adls_credential
);

CREATE EXTERNAL DATA SOURCE silver_adls
WITH (
    LOCATION = 'https://account.dfs.core.windows.net/silver',
    CREDENTIAL = adls_credential
);

-- Step 2: CETAS — transform and write to Silver
CREATE EXTERNAL TABLE silver.orders
WITH (
    LOCATION = 'orders/year=2024/month=01/',
    DATA_SOURCE = silver_adls,
    FILE_FORMAT = parquet_snappy_format
)
AS
SELECT
    order_id,
    customer_id,
    CAST(amount AS DECIMAL(18,2))        AS amount,
    CAST(order_date AS DATE)             AS order_date,
    UPPER(TRIM(region))                  AS region,
    CASE
        WHEN amount >= 1000 THEN 'high'
        WHEN amount >= 100  THEN 'medium'
        ELSE 'low'
    END                                  AS revenue_tier,
    GETUTCDATE()                         AS processed_at
FROM OPENROWSET(
    BULK 'orders/year=2024/month=01/*.parquet',
    DATA_SOURCE = 'bronze_adls',
    FORMAT = 'PARQUET'
) AS raw_orders
WHERE amount IS NOT NULL AND order_id IS NOT NULL;

-- CETAS writes Parquet files to ADLS and creates an external table pointer
-- Downstream: Dedicated SQL Pool reads from the external table for analytics
-- Or: Power BI queries Serverless SQL Pool directly via the external table
```

---

## Workload Management

```sql
-- Workload Management: control resource allocation per user/workload class
-- Prevents one heavy query from consuming all DWUs and starving others

-- Resource classes (legacy, still common):
--   smallrc: 1-3% of memory per query (default)
--   mediumrc: 6-12%
--   largerc: 22-26%
--   xlargerc: 70% (extreme queries — use carefully)

-- Assign user to resource class:
EXEC sp_addrolemember 'largerc', 'ETL_Service_Account';
EXEC sp_addrolemember 'smallrc', 'PowerBI_Reader';

-- Workload groups (modern approach):
CREATE WORKLOAD GROUP ETL_Heavy
WITH (
    MIN_PERCENTAGE_RESOURCE = 50,   -- guaranteed 50% DWUs
    CAP_PERCENTAGE_RESOURCE = 80,   -- cap at 80% DWUs
    REQUEST_MIN_RESOURCE_GRANT_PERCENT = 25,  -- each query gets 25%
    REQUEST_MAX_RESOURCE_GRANT_PERCENT = 50   -- max 50% per query
);

CREATE WORKLOAD GROUP BI_Interactive
WITH (
    MIN_PERCENTAGE_RESOURCE = 10,
    CAP_PERCENTAGE_RESOURCE = 50,
    REQUEST_MIN_RESOURCE_GRANT_PERCENT = 3,
    REQUEST_MAX_RESOURCE_GRANT_PERCENT = 10,
    IMPORTANCE = HIGH               -- BI queries get priority over default
);

-- Classifier: route user/app queries to workload group
CREATE WORKLOAD CLASSIFIER ETL_Classifier
WITH (
    WORKLOAD_GROUP = 'ETL_Heavy',
    MEMBERNAME = 'etl_service_account',
    IMPORTANCE = NORMAL
);

CREATE WORKLOAD CLASSIFIER BI_Classifier
WITH (
    WORKLOAD_GROUP = 'BI_Interactive',
    MEMBERNAME = 'powerbi_user',
    IMPORTANCE = HIGH
);

-- Monitor queue/execution:
SELECT * FROM sys.dm_pdw_exec_requests WHERE status = 'Running' ORDER BY submit_time;
SELECT * FROM sys.dm_pdw_waits WHERE type = 'Concurrency';
```

---

## Query Performance Tuning

```sql
-- Step 1: Check query plan
EXPLAIN
SELECT c.region, SUM(o.amount) AS total_revenue
FROM dbo.orders o
JOIN dbo.customers c ON o.customer_id = c.customer_id
WHERE o.order_date >= '2024-01-01'
GROUP BY c.region;

-- Look for: BroadcastMoveOperation, ShuffleMoveOperation
--   BroadcastMove: one table sent to all nodes (good for small tables)
--   ShuffleMove:   data redistributed by join key (expensive — minimize)
-- If customers table is ROUND_ROBIN → ShuffleMove before join
-- Fix: change customers to REPLICATE distribution (it's a dimension table)

-- Step 2: Update statistics (stale stats → bad plans)
UPDATE STATISTICS dbo.orders;
UPDATE STATISTICS dbo.customers;
-- Or create targeted stats:
CREATE STATISTICS stat_order_date ON dbo.orders (order_date);
CREATE STATISTICS stat_customer_id ON dbo.orders (customer_id);

-- Step 3: Check for data skew
DBCC PDW_SHOWSPACEUSED('dbo.orders');
-- If one distribution has 10× more rows → hash key is poorly chosen
-- Check: SELECT customer_id % 60 AS dist, COUNT(*) FROM orders GROUP BY customer_id % 60 ORDER BY 2 DESC

-- Step 4: Materialized views for complex repeated queries
CREATE MATERIALIZED VIEW mv_daily_revenue
WITH (DISTRIBUTION = HASH(region))
AS
SELECT order_date, region, SUM(amount) AS total, COUNT(*) AS cnt
FROM dbo.orders
GROUP BY order_date, region;
-- Synapse automatically uses MV when query includes these aggregations
-- MV updates automatically on DML (incremental maintenance)

-- Step 5: Result set caching
-- Identical queries return cached results instantly (no re-execution)
ALTER DATABASE [SynapseDW] SET RESULT_SET_CACHING ON;
-- Works for: same query text, same user data access, same parameters
-- Cache TTL: 48 hours or until table data changes
```

---

## Interview Tips

> **Tip 1:** "How do you handle slow joins in Synapse Dedicated SQL Pool?" — Diagnose with EXPLAIN: look for ShuffleMoveOperation on large tables — this means the join key doesn't match the distribution key, requiring a shuffle (redistribution of data across nodes). Fix: (a) change one table to HASH-distribute on the join key (if the table is large enough to benefit), (b) change a small table to REPLICATE (eliminates shuffle entirely for that join), (c) add a materialized view pre-joining the tables. For reporting-style queries, result set caching eliminates re-execution for identical queries.

> **Tip 2:** "What's the difference between pausing and scaling a Dedicated SQL Pool?" — Pause: compute stopped, data retained in Azure storage (columnar storage persists). Billed: storage only (much cheaper). Restart takes 5-15 minutes. Scale: change DWU level, remaps 60 distributions across different number of nodes. Scale takes 1-5 minutes, data is not moved. Best practice: auto-pause non-production pools on weekends/overnight (90% cost reduction), scale production pools based on workload type (more DWUs for large ETL runs, scale down for normal BI queries).

> **Tip 3:** "How does COPY INTO compare to PolyBase for loading data?" — Both perform bulk parallel loads across all 60 distributions. COPY INTO is the modern recommended approach: simpler syntax (no external file format/data source objects needed unless reusing), better error handling (ERRORFILE option), supports more authentication types (Managed Identity, SAS token, Storage Account Key). PolyBase: slightly faster for very large loads but requires pre-created external objects. Microsoft recommends COPY INTO for new pipelines.
