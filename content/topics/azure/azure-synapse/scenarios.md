---
title: "Azure Synapse Analytics — Scenarios"
topic: azure
subtopic: azure-synapse
content_type: scenario_question
tags: [azure, synapse, scenarios, interview, performance, design]
---

# Azure Synapse Analytics — Interview Scenarios

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design a Synapse DW for a Retail Company

**Scenario:** A retail company has 5 years of historical sales data (2TB), 50M customers, 1M products, 500M transactions/year. They have 30 BI analysts querying daily. Design the Synapse Dedicated SQL Pool schema and architecture.

<details>
<summary>💡 Hint</summary>
Scale the DWU to the analyst concurrency (30 users → DW2000c). For the schema: REPLICATE small dimensions, HASH large ones. The fact table distribution key should co-locate with the most common join. Use Ordered CCI for date-range queries.
</details>

<details>
<summary>✅ Solution</summary>

```
Scale sizing:
  Data volume: 2TB historical + ~400GB/year new
  Query concurrency: 30 analysts
  DWU recommendation: DW2000c
    → 4 compute nodes, handles 32 concurrent queries
    → Can scale to DW6000c for month-end reporting spikes

Schema design:

Dimension tables (REPLICATE — all < 2GB):
  dim_customer  (50M rows × 200 bytes = 10GB → too large, use HASH)
    ↑ Exception: 50M rows is too big for REPLICATE
    → HASH(customer_sk) instead

  dim_product   (1M rows × 300 bytes = 300MB → REPLICATE ✓)
  dim_date      (365×10=3650 rows → REPLICATE ✓)
  dim_store     (10K rows → REPLICATE ✓)
  dim_promotion (100K rows → REPLICATE ✓)

Fact table:
  fact_sales: 500M rows/year × 5 years = 2.5B rows
  Distribution: HASH(customer_sk)
    → joins to dim_customer (also HASH(customer_sk)) = co-located, no shuffle
  Index: CLUSTERED COLUMNSTORE INDEX
  Partition: by order_date_sk (monthly) = 60 partitions × 5 years = 60 partitions
    → each partition: ~42M rows (good size for CCI row groups)

Ordered CCI: ORDER(order_date_sk, store_sk)
  → Most queries filter by date + store → excellent segment pruning

Common query: all joins hit co-located data (customer) or replicated data (product, store)
Result: analytical queries on 2.5B rows in 2-5 seconds

Performance extras:
  Materialized views for top 10 pre-computed aggregations
  Result set caching enabled (identical dashboard queries = instant)
  Workload group: BI_Interactive (30 users, importance=HIGH, min=25% resources)

Architecture:
  ADLS Gen2 (Bronze/Silver/Gold) 
    ↓ ADF COPY INTO (nightly at 2 AM)
    ↓ Synapse Dedicated SQL Pool (serving layer)
    ↓ Power BI (reports — Import mode for best performance)
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Slow Query Investigation

**Scenario:** A Power BI report that queries the fact_sales table runs in 45 seconds. Business users complain it should be faster. How do you investigate and fix?

<details>
<summary>💡 Hint</summary>
Run EXPLAIN on the slow query to see ShuffleMoveOperation. Check table distributions: a small dimension table with HASH distribution (instead of REPLICATE) causes unnecessary data movement. Update statistics and consider materialized views.
</details>

<details>
<summary>✅ Solution</summary>

```
Step 1: Capture the query
  Enable query store or use sys.dm_pdw_exec_requests to capture the query text
  Run manually in Synapse Studio with SET STATISTICS IO ON

Step 2: EXPLAIN the query
  EXPLAIN
  SELECT s.store_name, p.category, SUM(f.net_amount) AS revenue
  FROM dbo.fact_sales f
  JOIN dbo.dim_store s ON f.store_sk = s.store_sk
  JOIN dbo.dim_product p ON f.product_sk = p.product_sk
  WHERE f.order_date_sk BETWEEN 20240101 AND 20240131
  GROUP BY s.store_name, p.category;

  EXPLAIN output shows: ShuffleMoveOperation on dim_store
  → dim_store is HASH-distributed (should be REPLICATE for 10K rows)

Step 3: Check distributions
  DBCC PDW_SHOWSPACEUSED('dbo.dim_store');
  → dim_store is HASH(store_sk) — wrong choice for a 10K-row table

Fix 1: Change dim_store to REPLICATE
  CREATE TABLE dbo.dim_store_new (...) WITH (DISTRIBUTION = REPLICATE, CCI);
  INSERT INTO dbo.dim_store_new SELECT * FROM dbo.dim_store;
  RENAME OBJECT dbo.dim_store TO dim_store_old;
  RENAME OBJECT dbo.dim_store_new TO dim_store;

Step 4: Update statistics
  UPDATE STATISTICS dbo.fact_sales;
  UPDATE STATISTICS dbo.dim_store;
  UPDATE STATISTICS dbo.dim_product;

Step 5: Create materialized view for common aggregation
  CREATE MATERIALIZED VIEW mv_monthly_sales_by_store_category
  WITH (DISTRIBUTION = HASH(store_name))
  AS
  SELECT
    s.store_name, p.category,
    f.order_date_sk / 100 AS order_month,  -- YYYYMM
    SUM(f.net_amount) AS revenue,
    COUNT(*) AS transaction_count
  FROM dbo.fact_sales f
  JOIN dbo.dim_store s ON f.store_sk = s.store_sk
  JOIN dbo.dim_product p ON f.product_sk = p.product_sk
  GROUP BY s.store_name, p.category, f.order_date_sk / 100;

Step 6: Enable result set caching
  ALTER DATABASE [SynapseDW] SET RESULT_SET_CACHING ON;
  -- First run: 45s → 8s (after fixes), cached → <1s

Result: 45s → 8s from distribution fix + 8s → <1s for cached dashboard runs
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Synapse vs Databricks Decision

**Scenario:** Your company is deciding between Azure Synapse Dedicated SQL Pool and Azure Databricks as the primary data platform. Make the case for each.

<details>
<summary>💡 Hint</summary>
Frame this as: Synapse Dedicated = SQL-centric teams needing concurrency management and T-SQL features. Databricks = engineering teams needing ML, Python, streaming, Delta Lake. The hybrid (Databricks ETL + Synapse serving) is usually the right answer for large enterprises.
</details>

<details>
<summary>✅ Solution</summary>

```
Case for Synapse Dedicated SQL Pool:
  Team profile: 15 SQL developers, 3 data engineers, no Python/Scala experience
  Workloads: standard BI (Power BI reports, SSRS migration), star schema DW
  
  Advantages:
  - Familiar T-SQL for the entire team (no learning curve)
  - Power BI DirectQuery mode optimized for Synapse (query folding, result cache)
  - Built-in workload management for 30+ concurrent BI users
  - SQL compliance: stored procedures, views, UDFs — existing SQL code reusable
  - Lower cost for pure BI workloads ($4.50/DWU-hour at DW1000c)
  - Pause/resume: pay only during business hours (~$1,620/month at DW1000c 8h/day)

Case for Azure Databricks:
  Team profile: 5 data engineers (Python/Spark), 2 data scientists, 3 SQL analysts
  Workloads: ML models, complex Python transformations, Delta Lake, streaming CDC

  Advantages:
  - Delta Lake: ACID transactions, schema evolution, time travel on data lake
  - ML integration: MLflow, Feature Store, AutoML in same platform
  - Python/Scala for complex transformations (regex, custom logic, NLP)
  - Photon engine: 3-4× faster than open-source Spark for SQL workloads
  - Structured Streaming: real-time pipelines with Kafka/Event Hubs
  - Unity Catalog: column-level security across all Databricks workspaces
  - Cost for heavy compute: spot instances via Databricks pools (70% savings)

Recommended hybrid:
  ADLS Gen2 (storage for everything)
    ↓ Databricks (Bronze → Silver → Gold transformations + ML)
    ↓ Synapse Dedicated SQL Pool (Gold → serving layer for BI)
    ↓ Power BI (reporting)

  Databricks owns: ETL complexity, ML, streaming
  Synapse owns: SQL serving, BI concurrency, T-SQL analytics
  ADF orchestrates: Databricks notebooks → COPY INTO Synapse

Decision factors:
  If team is SQL-only → Synapse
  If team needs ML + streaming → Databricks
  If both → hybrid architecture above
```

</details>

</article>
---

## Interview Tips

> **Tip 1:** "How do you handle schema changes in Synapse Dedicated SQL Pool?" — Synapse CCI tables don't support ALTER TABLE for all changes. For adding a nullable column: `ALTER TABLE dbo.fact_sales ADD new_column INT NULL` (safe, sets NULL for existing rows). For changing a column type or adding NOT NULL: must CTAS — create new table with CTAS from old table, rename, drop old. For partition structure changes: same CTAS approach. This is why schema-on-read (Serverless SQL) is attractive for exploration — no schema to maintain.

> **Tip 2:** "What's the cost of a Synapse Dedicated SQL Pool at DW2000c?" — DW2000c: $14.40/DWU-hour × 2000/100 = $14.40/hour. If running 24/7: $14.40 × 24 × 30 = $10,368/month. With pause/resume (8 hours/day, 5 days/week): $14.40 × 8 × 22 = $2,534/month. With auto-scaling for month-end (scale to DW4000c for 2 days): extra ~$700. Total: ~$3,200/month for a production BI DW — similar to a mid-tier Snowflake subscription.

> **Tip 3:** "How does Synapse Serverless SQL price compare to Synapse Dedicated SQL Pool?" — Serverless: $5/TB scanned, no idle cost, no provisioning. Dedicated: $4.50-$14.40/hour regardless of query volume. Break-even: if you query more than ~1-2 TB/hour consistently, Dedicated is cheaper. Serverless is ideal for: (a) development/exploration (unpredictable query patterns), (b) infrequent reporting (daily/weekly queries on large data), (c) ELT jobs that run once daily. Dedicated is ideal for: (a) 30+ concurrent BI users querying the same tables hourly, (b) teams that need T-SQL procedural logic, stored procedures, workload management.

