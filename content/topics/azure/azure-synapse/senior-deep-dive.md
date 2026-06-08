---
title: "Azure Synapse Analytics — Senior Deep Dive"
topic: azure
subtopic: azure-synapse
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [azure, synapse, architecture, synapse-link, lakehouse, security, performance]
---

# Azure Synapse Analytics — Senior Deep Dive

## Synapse Link: Zero-ETL Operational Analytics

```
Synapse Link = analytical replica of operational store, updated in near-real-time
No ETL pipeline, no performance impact on source

Supported sources:
  Azure Cosmos DB (HTAP — Hybrid Transactional/Analytical Processing)
  Azure SQL Database (Synapse Link for SQL)
  Microsoft Dataverse (for Power Platform data)

Cosmos DB + Synapse Link:
  Cosmos DB analytical store = column-oriented store auto-synced from transactional store
  Updates propagate in 2-5 minutes (not real-time, near-real-time)
  Queries run against analytical store — zero impact on Cosmos DB RUs
  
  Setup:
  1. Enable analytical store on Cosmos DB container:
     az cosmosdb sql container create --name orders --analytical-ttl -1
  2. Create Synapse Link connection in Synapse workspace
  3. Query from Synapse Spark or Serverless SQL:
  
  -- Serverless SQL query on Cosmos DB analytical store:
  SELECT TOP 100 *
  FROM OPENROWSET(
      PROVIDER = 'CosmosDB',
      CONNECTION = 'Account=account;Database=ecommerce',
      OBJECT = 'orders',
      SERVER_CREDENTIAL = 'cosmos_key'
  )
  WITH (
      order_id    VARCHAR(100) '$.id',
      customer_id VARCHAR(100) '$.customerId',
      amount      FLOAT        '$.amount',
      status      VARCHAR(50)  '$.status'
  ) AS orders

SQL Database + Synapse Link:
  Change feed on Azure SQL DB → replicated to Synapse workspace automatically
  Maintains columnar replica in Synapse storage
  Sub-minute latency for analytical queries
  Use case: report on live transactional data without impacting OLTP
```

---

## Dedicated SQL Pool: Deep Performance Architecture

```sql
-- Columnar Segment Elimination (row group pruning)
-- CCI stores data in row groups of ~1M rows with min/max metadata
-- WHERE order_date BETWEEN '2024-01-01' AND '2024-01-31' 
--   → skip row groups where max(order_date) < '2024-01-01' OR min(order_date) > '2024-01-31'

-- Check CCI health:
SELECT
    t.name AS table_name,
    i.name AS index_name,
    p.rows AS row_count,
    rg.state_desc,
    rg.deleted_rows,
    rg.total_rows
FROM sys.tables t
JOIN sys.indexes i ON t.object_id = i.object_id AND i.type = 5  -- CCI
JOIN sys.partitions p ON i.object_id = p.object_id
JOIN sys.column_store_row_groups rg ON p.object_id = rg.object_id
WHERE rg.state_desc IN ('OPEN', 'TOMBSTONE')  -- OPEN = not compressed, TOMBSTONE = deleted
ORDER BY rg.deleted_rows DESC;

-- Row groups with high deleted_rows → rebuild index to recover space
ALTER INDEX [ClusteredColumnstoreIndex_orders] ON dbo.orders REBUILD PARTITION = ALL;

-- Partition switching for fast data management (zero-copy):
-- Move a partition from staging to production instantly
ALTER TABLE dbo.staging_orders
SWITCH PARTITION 1 TO dbo.orders PARTITION 6;  -- moves Jan data instantly
-- Requirements: same columns, same distribution, same index type
-- Use case: monthly partition refresh (delete old, add new) in seconds

-- Ordered CCI (Synapse-specific):
-- Normal CCI: rows inserted in any order within segments
-- Ordered CCI: sorts rows by specified columns before creating segments → better pruning
CREATE TABLE dbo.orders (
    order_id    BIGINT,
    order_date  DATE,
    region      VARCHAR(50),
    amount      DECIMAL(18,2)
)
WITH (
    DISTRIBUTION = HASH(customer_id),
    CLUSTERED COLUMNSTORE INDEX ORDER (order_date, region)
);
-- Queries filtering on order_date + region skip 90%+ of segments
-- Trade-off: slower loads (requires sort), faster reads
```

---

## Synapse Security Architecture

```sql
-- 1. Column-level security (column masking)
CREATE MASKING POLICY PARTIAL ON dbo.customers (email) USING (
    CASE WHEN IS_MEMBER('DataAnalysts') = 1 THEN email
         ELSE CONCAT(LEFT(email, 2), '***', RIGHT(email, CHARINDEX('@', REVERSE(email))-1 + 1))
    END
);

-- 2. Row-level security
CREATE FUNCTION dbo.fn_region_filter(@region NVARCHAR(50))
RETURNS TABLE
WITH SCHEMABINDING
AS
    RETURN SELECT 1 AS fn_result
    WHERE @region = USER_NAME() OR USER_NAME() = 'admin';

CREATE SECURITY POLICY region_access_policy
ADD FILTER PREDICATE dbo.fn_region_filter(region)
ON dbo.orders
WITH (STATE = ON);

-- 3. Dynamic Data Masking
ALTER TABLE dbo.customers
ALTER COLUMN ssn ADD MASKED WITH (FUNCTION = 'default()');
ALTER TABLE dbo.customers
ALTER COLUMN email ADD MASKED WITH (FUNCTION = 'email()');
-- email() masking: a***@example.com

-- 4. Private Link for Synapse workspace
-- Block public internet access to Synapse workspace
-- Use private endpoints for:
--   Synapse SQL (port 1433) → private IP in your VNet
--   Synapse Dev (ADF/notebooks) → private IP in your VNet
-- Combined with Managed VNet for Spark: all data stays within Azure backbone

-- 5. Microsoft Purview integration
-- Scan Synapse assets for classification (PII, financial data)
-- Lineage: ADF pipeline → Synapse table → Power BI report tracked automatically
```

---

## Synapse vs Azure Databricks: Positioning

```
When to choose Azure Synapse Dedicated SQL Pool:
  ✓ Team is SQL-first (DBA background, no Spark experience)
  ✓ Standard BI/reporting workloads with structured data
  ✓ Heavy use of Power BI (native connector, DirectQuery optimization)
  ✓ Need T-SQL features: stored procedures, views, workload management
  ✓ Regulatory: data must stay in Azure SQL-equivalent environment
  ✗ Complex Python/ML workloads
  ✗ Data volumes > 100TB with frequent updates (compaction overhead)

When to choose Azure Databricks:
  ✓ Data science / ML / feature engineering
  ✓ Delta Lake / open table format requirements
  ✓ Complex transformations requiring Python, Scala, R
  ✓ Streaming pipelines (Structured Streaming)
  ✓ Multi-cloud or vendor-agnostic requirement
  ✗ Team lacks Spark skills
  ✗ Simple BI with SQL-only users

Common enterprise pattern (both):
  ADLS Gen2 (storage)
    ↓ Databricks (transformation: Bronze → Silver → Gold in Delta)
    ↓ Synapse Dedicated SQL Pool (serving layer: Gold data loaded here for BI)
    ↓ Power BI (reporting)

  Databricks handles the hard transformation work
  Synapse handles the SQL serving layer for BI tools
  ADLS is the shared storage layer

Synapse Spark vs Databricks Spark:
  Same core: both run Apache Spark
  Databricks advantages: Delta Live Tables, MLflow, Unity Catalog, cluster optimizations (Photon)
  Synapse advantages: integrated with other Synapse services, no extra cost for SQL users
```

---

## Interview Tips

> **Tip 1:** "How does partition switching work in Synapse and why is it important?" — Partition switching is a metadata-only operation that moves an entire partition from one table to another in milliseconds (no data copy). Use case: monthly data load pattern — load new month's data into a staging table, then switch it into the production table for the corresponding partition. Also used for fast deletes: switch the partition to an empty staging table and truncate it. Requirements: source and target must have identical schema, distribution strategy, and index type. This is how large Synapse tables handle time-based data management without long-running DELETE operations.

> **Tip 2:** "What is an Ordered Clustered Columnstore Index and when would you use it?" — Normal CCI inserts rows in any order within each row group (1M rows). With Ordered CCI, Synapse sorts data by the specified columns before compressing into row groups, so all rows for 'January 2024' are in the same row groups. Queries with `WHERE order_date = '2024-01-15'` can skip entire row groups vs scanning them. The trade-off: 20-40% slower INSERT/CTAS because of sorting. Use it when the table is loaded in bulk (infrequent large loads) and queries consistently filter on the same columns.

> **Tip 3:** "How does Synapse Link compare to traditional ETL from Cosmos DB?" — Traditional ETL: Cosmos DB → ADF pipeline → Synapse (15-minute to hourly latency, uses Cosmos DB RUs for reads, requires pipeline maintenance). Synapse Link: Cosmos DB transactional store → automatic sync to analytical store → Synapse queries run against analytical store (2-5 min latency, zero RU consumption for analytics, no pipeline needed). Synapse Link enables HTAP: the same data supports both millisecond OLTP reads and multi-second analytical queries simultaneously.
