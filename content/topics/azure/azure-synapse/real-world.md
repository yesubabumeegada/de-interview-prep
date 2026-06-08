---
title: "Azure Synapse Analytics — Real World"
topic: azure
subtopic: azure-synapse
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [azure, synapse, production, loading, monitoring, performance]
---

# Azure Synapse Analytics — Real World

## Pattern 1: Production Star Schema Build

```sql
-- Production pattern: load fact table daily with partition management

-- Step 1: Create dimension tables (REPLICATE — used in joins)
CREATE TABLE dbo.dim_customer (
    customer_sk     INT          NOT NULL,
    customer_id     INT          NOT NULL,
    name            VARCHAR(200) NOT NULL,
    region          VARCHAR(50)  NOT NULL,
    tier            VARCHAR(20)  NOT NULL,
    valid_from      DATE         NOT NULL,
    valid_to        DATE,
    is_current      BIT          NOT NULL DEFAULT 1
)
WITH (
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);

CREATE TABLE dbo.dim_product (
    product_sk      INT          NOT NULL,
    product_id      INT          NOT NULL,
    product_name    VARCHAR(200),
    category        VARCHAR(100),
    subcategory     VARCHAR(100),
    list_price      DECIMAL(18,2)
)
WITH (DISTRIBUTION = REPLICATE, CLUSTERED COLUMNSTORE INDEX);

-- Step 2: Create fact table (HASH on customer_sk for join performance)
CREATE TABLE dbo.fact_sales (
    sale_sk         BIGINT       NOT NULL,
    order_date_sk   INT          NOT NULL,
    customer_sk     INT          NOT NULL,
    product_sk      INT          NOT NULL,
    quantity        INT,
    unit_price      DECIMAL(18,2),
    gross_amount    DECIMAL(18,2),
    discount_amount DECIMAL(18,2),
    net_amount      DECIMAL(18,2)
)
WITH (
    DISTRIBUTION = HASH(customer_sk),
    CLUSTERED COLUMNSTORE INDEX,
    PARTITION (order_date_sk RANGE RIGHT FOR VALUES (
        20240101, 20240201, 20240301, 20240401, 20240501, 20240601,
        20240701, 20240801, 20240901, 20241001, 20241101, 20241201
    ))
);

-- Step 3: Daily load via CTAS + partition switch
-- Load today's data to staging
CREATE TABLE dbo.stg_fact_sales_20240115
WITH (
    DISTRIBUTION = HASH(customer_sk),
    CLUSTERED COLUMNSTORE INDEX,
    HEAP  -- load as HEAP, then convert to CCI
)
AS
SELECT
    NEXT VALUE FOR sale_sk_seq     AS sale_sk,
    FORMAT(o.order_date, 'yyyyMMdd') AS order_date_sk,
    dc.customer_sk,
    dp.product_sk,
    o.quantity, o.unit_price,
    o.quantity * o.unit_price      AS gross_amount,
    o.discount                     AS discount_amount,
    (o.quantity * o.unit_price) - o.discount AS net_amount
FROM OPENROWSET(BULK 'https://account.dfs.core.windows.net/silver/orders/order_date=2024-01-15/*.parquet',
    FORMAT='PARQUET') AS o
JOIN dbo.dim_customer dc ON o.customer_id = dc.customer_id AND dc.is_current = 1
JOIN dbo.dim_product  dp ON o.product_id  = dp.product_id;

-- Build statistics
CREATE STATISTICS stg_stat_cust ON dbo.stg_fact_sales_20240115 (customer_sk);

-- Switch partition (instant, metadata-only)
ALTER TABLE dbo.stg_fact_sales_20240115
SWITCH TO dbo.fact_sales PARTITION 2;  -- January 2024 partition

-- Cleanup staging
DROP TABLE dbo.stg_fact_sales_20240115;
```

---

## Pattern 2: Serverless SQL for ELT

```sql
-- Use Serverless SQL Pool to clean Bronze → write to Silver (no Spark needed)

-- Read messy Bronze CSV, clean, and write clean Parquet to Silver via CETAS

CREATE EXTERNAL TABLE silver.cleaned_orders
WITH (
    LOCATION = 'silver/orders/processed_date=2024-01-15/',
    DATA_SOURCE = adls_silver,
    FILE_FORMAT = parquet_snappy_format
)
AS
WITH raw AS (
    SELECT *
    FROM OPENROWSET(
        BULK 'bronze/orders/ingest_date=2024-01-15/*.csv',
        DATA_SOURCE = 'adls_bronze',
        FORMAT = 'CSV',
        PARSER_VERSION = '2.0',
        HEADER_ROW = TRUE,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '\n'
    )
    WITH (
        order_id        VARCHAR(50),
        customer_id     VARCHAR(50),
        amount          VARCHAR(50),  -- intentionally VARCHAR (messy data)
        order_date      VARCHAR(50),
        region          VARCHAR(100),
        status          VARCHAR(50)
    ) AS raw_data
),
cleaned AS (
    SELECT
        CAST(order_id AS BIGINT)             AS order_id,
        CAST(customer_id AS INT)             AS customer_id,
        TRY_CAST(amount AS DECIMAL(18,2))    AS amount,
        TRY_CAST(order_date AS DATE)         AS order_date,
        UPPER(TRIM(region))                  AS region,
        LOWER(TRIM(status))                  AS status,
        GETUTCDATE()                         AS processed_at
    FROM raw
    WHERE order_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND TRY_CAST(amount AS DECIMAL(18,2)) IS NOT NULL     -- reject bad amounts
      AND TRY_CAST(order_date AS DATE) IS NOT NULL          -- reject bad dates
)
SELECT * FROM cleaned;

-- Serverless SQL processes this via distributed query across ADLS files
-- Cost: $5/TB scanned (the Bronze input, typically small per day)
-- No cluster spin-up time: first byte returned in seconds
```

---

## Pattern 3: Synapse Monitoring and Alerting

```sql
-- Monitor active queries and waits
SELECT
    r.request_id,
    r.session_id,
    r.status,
    r.start_time,
    DATEDIFF(MINUTE, r.start_time, GETUTCDATE()) AS duration_min,
    r.resource_class,
    r.label,
    LEFT(r.command, 200) AS query_text
FROM sys.dm_pdw_exec_requests r
WHERE r.status = 'Running'
ORDER BY r.start_time;

-- Check concurrency slots usage
SELECT
    rp.name AS resource_pool,
    rp.min_percentage_resource,
    rp.cap_percentage_resource,
    COUNT(r.request_id) AS active_requests
FROM sys.dm_workload_management_workload_groups rp
LEFT JOIN sys.dm_pdw_exec_requests r ON r.resource_class = rp.name AND r.status = 'Running'
GROUP BY rp.name, rp.min_percentage_resource, rp.cap_percentage_resource;

-- Data skew check for distribution
DBCC PDW_SHOWSPACEUSED('dbo.fact_sales');
-- Look for: distributions with 5× more rows than average → hash key is poorly distributed

-- Azure Monitor alert for failed loads:
-- Enable Synapse diagnostic settings → Log Analytics
-- KQL alert:
-- SynapseRbacOperations
-- | where OperationName == "Microsoft.Synapse/workspaces/sqlPools/write"
-- | where Status == "Failed"
-- | summarize count() by bin(TimeGenerated, 5m)
-- | where count_ > 0
```

---

## Interview Tips

> **Tip 1:** "A Synapse daily ETL is taking 3 hours, but the table is only 50GB. What's wrong?" — Common causes: (a) data skew — check `DBCC PDW_SHOWSPACEUSED` for uneven distribution, one node doing 90% of work; (b) insufficient statistics — run `UPDATE STATISTICS` on join and filter columns, stale stats lead to full table scans; (c) wrong distribution — fact table joined on a column that's not its distribution key, causing ShuffleMove; (d) CCI row groups are OPEN (not compressed) — rebuild index to force delta stores to compress; (e) too few DWUs — scale up during ETL window, scale down after.

> **Tip 2:** "What is a materialized view in Synapse and how is it different from a regular view?" — A regular view is a saved query that re-executes on every access. A materialized view pre-computes and physically stores the result as a CCI table. Synapse automatically maintains the materialized view when the underlying data changes (incremental maintenance via delta rows). The query optimizer automatically rewrites queries to use the MV when it can answer the query using the pre-computed data — the user doesn't need to reference the MV directly. Use MVs for expensive repeated aggregations (SUM, COUNT, AVG over large tables).

> **Tip 3:** "How do you design a Synapse DW to support 50 concurrent BI users without query interference?" — Use workload groups: create a `BI_Interactive` workload group with min 10% resource guarantee and importance = HIGH, plus a `ETL_Batch` group with 50% resources. ETL runs at off-hours. During business hours: 50 concurrent BI queries share 50% resources, each getting ~1% of memory (enough for simple aggregations). Enable result set caching — identical dashboard queries return instantly after first run. Scale to DW2000c during business hours, DW500c overnight.
