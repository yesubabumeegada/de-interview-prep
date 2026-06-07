---
title: "Lakehouse Architecture - Scenario Questions"
topic: databricks
subtopic: lakehouse-architecture
content_type: scenario_question
tags: [databricks, lakehouse, interview, scenarios, architecture]
---

# Scenario Questions — Lakehouse Architecture

<article data-difficulty="junior">

## 🟢 Junior: Medallion Layer Design

**Scenario:** Your company receives raw order data as JSON files every hour. Design the medallion architecture (bronze/silver/gold) for this data. What goes in each layer?

<details>
<summary>💡 Hint</summary>
Bronze = raw as-is (append-only), Silver = cleaned/typed/deduped (business entity), Gold = aggregated for consumption (business metrics).
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- BRONZE: Raw orders exactly as received
CREATE TABLE production.bronze.orders (
    raw_json STRING,            -- Original payload (preserve everything)
    order_id STRING,            -- Might be malformed
    customer_id STRING,
    amount STRING,              -- Not typed yet (could be "N/A")
    order_date STRING,
    _ingested_at TIMESTAMP,     -- When we loaded it
    _source_file STRING         -- Which file it came from
) USING DELTA
PARTITIONED BY (date(_ingested_at));
-- Rules: no transforms, append-only, accept everything

-- SILVER: Clean order entity
CREATE TABLE production.silver.orders (
    order_id BIGINT NOT NULL,          -- Typed, validated
    customer_id BIGINT NOT NULL,
    amount DECIMAL(10,2) NOT NULL,     -- Proper type
    order_date DATE NOT NULL,
    status STRING,
    _loaded_at TIMESTAMP
) USING DELTA;
-- Rules: deduplicated on order_id, type-cast, nulls filtered out

-- GOLD: Business metrics
CREATE TABLE production.gold.daily_revenue (
    order_date DATE,
    total_orders INT,
    total_revenue DECIMAL(12,2),
    avg_order_value DECIMAL(10,2)
) USING DELTA;
-- Rules: pre-aggregated, ready for dashboards, fast to query
```

**Key Points:**
- Bronze: raw data preserved for auditability and reprocessing
- Silver: single source of truth for the business entity (one row per order)
- Gold: business-specific aggregations optimized for queries
- Data flows one direction: source → bronze → silver → gold
- If gold has issues: fix silver logic and rebuild (bronze is the safety net)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Lakehouse vs Data Warehouse

**Scenario:** Your CTO asks "Why should we use a lakehouse instead of just putting everything in Snowflake?" Give 3 concrete reasons for your use case (you have 5TB of data, need ML, and have streaming data).

<details>
<summary>💡 Hint</summary>
Consider: ML support, streaming capability, cost at 5TB scale, and vendor lock-in. What can a lakehouse do that a traditional DW can't?
</details>

<details>
<summary>✅ Solution</summary>

```
REASON 1: ML Support
- Lakehouse: Data scientists access Delta tables DIRECTLY in PySpark/pandas
  No export needed. Train models on the same data that feeds dashboards.
- Snowflake: Must export data to S3 → load into SageMaker → train → import results back
  Extra ETL, latency, and data copies.

REASON 2: Streaming (Real-Time Data)
- Lakehouse: Structured Streaming writes to Delta tables in near-real-time (seconds)
  Same table serves both streaming writes AND batch reads — one system.
- Snowflake: Snowpipe handles basic ingestion but Structured Streaming isn't possible.
  Real-time analytics requires a separate streaming system (Kafka → separate processor).

REASON 3: Cost at 5TB Scale
- Lakehouse: 5 TB × $23/TB/month (S3) = $115/month storage
  Compute: only pay when processing (Jobs compute at $0.15/DBU)
  Total: ~$2,000-3,000/month
- Snowflake: 5 TB × $40/TB/month (compressed) = $200/month storage
  Compute: warehouse credits running 10 hrs/day = ~$3,000-5,000/month
  Total: ~$3,500-5,500/month (40-80% more expensive)

BONUS: Open Format
- Lakehouse: Delta/Parquet files on S3. If you leave Databricks, your data is still accessible.
- Snowflake: Proprietary format. Leaving means exporting everything (expensive, slow).
```

**Key Points:**
- Lakehouse wins for: ML, streaming, cost-sensitive large data, open format
- Snowflake wins for: pure SQL analytics teams, simpler operations, excellent BI performance
- Many companies use both: lakehouse for engineering + ML, Snowflake for analyst self-serve
- The "right answer" depends on your workload mix — not a universal truth

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Choosing Partitioning Strategy

**Scenario:** Your `silver.events` table has 10B rows, queried primarily by `event_date` and `user_id`. Should you partition by date, user_id, or both? The table has 1M unique users.

<details>
<summary>💡 Hint</summary>
Partition by low-cardinality columns only. 1M user_ids = 1M partitions = disaster (tiny files). Date has 365 values/year = good partition candidate.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- CORRECT: Partition by date (low cardinality, ~365 values/year)
CREATE TABLE production.silver.events (
    event_id BIGINT,
    user_id BIGINT,
    event_type STRING,
    event_timestamp TIMESTAMP,
    event_date DATE,
    properties MAP<STRING, STRING>
) USING DELTA
PARTITIONED BY (event_date);

-- For user_id (high cardinality): use Z-ORDER instead
OPTIMIZE production.silver.events
ZORDER BY (user_id);

-- Or use Liquid Clustering (best of both):
CREATE TABLE production.silver.events (...)
CLUSTER BY (event_date, user_id);  -- Handles both automatically

-- WHY NOT partition by user_id:
-- 1M users = 1M directories in S3
-- Each partition might have only 10 KB of data (tiny files!)
-- S3 listing 1M directories is extremely slow
-- Delta metadata becomes bloated

-- Query performance after ZORDER:
SELECT * FROM production.silver.events
WHERE event_date = '2024-03-15'   -- Partition pruning: skips other dates
  AND user_id = 12345;             -- Z-ORDER: skips ~99% of files within the partition

-- Without Z-ORDER: scans all files for that date (maybe 100 files)
-- With Z-ORDER: scans 1-3 files (user_id data is clustered)
```

**Key Points:**
- Partition by: low-cardinality columns (date, region, status) — target <1000 values
- Z-ORDER for: high-cardinality columns (user_id, product_id) — any cardinality
- Never partition by high-cardinality: creates millions of tiny files
- Liquid Clustering (new): handles both cases automatically, no manual choice needed
- Rule: each partition should contain >1 GB of data (below this = too many partitions)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Streaming and Batch on Same Table

**Scenario:** Your team has a Delta table `production.silver.orders` that: (A) receives new orders via streaming (every 30 seconds), and (B) is read by a nightly batch job for daily reporting. Can both happen on the same table simultaneously?

<details>
<summary>💡 Hint</summary>
Yes! Delta Lake supports concurrent readers and writers. The streaming writer and batch reader don't conflict.
</details>

<details>
<summary>✅ Solution</summary>

```python
# WRITER: Streaming job adds new orders continuously
streaming_write = (
    spark.readStream.table("production.bronze.orders")
    .writeStream
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", "/checkpoints/silver_orders/")
    .toTable("production.silver.orders")  # Writes every 30 seconds
)

# READER (concurrent): Nightly batch reads yesterday's data
# Runs at the same time as the streaming writer — no conflict!
daily_report = spark.sql("""
    SELECT order_date, COUNT(*) as orders, SUM(amount) as revenue
    FROM production.silver.orders
    WHERE order_date = current_date() - 1
    GROUP BY order_date
""")
daily_report.write.mode("overwrite").saveAsTable("production.gold.daily_revenue")

# READER (another stream): Reads from the SAME table as a stream!
purchase_stream = (
    spark.readStream.table("production.silver.orders")  # Read Delta table as stream
    .filter(col("status") == "completed")
    .writeStream
    .toTable("production.silver.completed_orders")
)

# All three run simultaneously on the SAME Delta table:
# 1. Streaming writer (appends new rows every 30s)
# 2. Batch reader (reads historical data for reporting)
# 3. Streaming reader (processes new rows as they appear)
```

**Key Points:**
- Delta Lake uses MVCC (Multi-Version Concurrency Control) — readers see a consistent snapshot
- Writers don't block readers, readers don't block writers
- A batch reader sees data as of the version when it started (consistent, not live)
- A streaming reader sees new data as it's committed (low latency, ~30s)
- This eliminates the need for separate streaming and batch pipelines (Lambda architecture)
- Key advantage of lakehouse: one table, multiple access patterns, no duplication

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: When to Use OPTIMIZE and VACUUM

**Scenario:** Your Delta table has 10,000 small files (from frequent streaming writes). Queries are slow. You also have 180 days of old file versions from time travel. What should you do?

<details>
<summary>💡 Hint</summary>
OPTIMIZE compacts small files into larger ones (faster reads). VACUUM removes old file versions (saves storage). They solve different problems.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- PROBLEM 1: Too many small files → slow reads
-- 10,000 files × overhead per file = slow scans
-- Target: 128 MB - 1 GB per file

-- FIX: OPTIMIZE compacts small files into ~1 GB files
OPTIMIZE production.silver.orders;
-- Result: 10,000 small files → ~100 large files
-- Query speedup: 5-50x (fewer files to open/read)

-- With Z-ORDER (optimize AND cluster data):
OPTIMIZE production.silver.orders
ZORDER BY (customer_id, order_date);
-- Compacts AND clusters → even faster queries filtering on these columns

-- PROBLEM 2: Old file versions consuming storage
-- Delta keeps old versions for time travel (default: 30 days)
-- After 180 days: lots of wasted storage

-- FIX: VACUUM removes files older than retention period
VACUUM production.silver.orders RETAIN 7 DAYS;
-- Deletes old file versions older than 7 days
-- Saves storage cost
-- WARNING: time travel no longer works beyond 7 days after this!

-- Schedule:
-- OPTIMIZE: daily (or after heavy write batches)
-- VACUUM: weekly (with appropriate retention for your SLA)

-- IMPORTANT: VACUUM does NOT compact files (that's OPTIMIZE)
-- OPTIMIZE does NOT delete old versions (that's VACUUM)
-- You need BOTH for a healthy table
```

**Key Points:**
- OPTIMIZE = compact small files → faster queries (addresses file count problem)
- VACUUM = delete old versions → save storage (addresses storage bloat)
- They solve DIFFERENT problems — do both regularly
- OPTIMIZE is safe (doesn't delete any data, just reorganizes)
- VACUUM is destructive (deletes old versions, breaks time travel beyond retention)
- Default retention: 7 days. For compliance: may need 30-90 days before VACUUM
- Schedule OPTIMIZE after heavy writes, VACUUM weekly during low-traffic periods

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Incremental Processing Design

**Scenario:** Your gold table `daily_customer_metrics` takes 2 hours to rebuild because it processes the entire silver.orders table (500M rows) every day. Only ~500K new rows arrive daily. Design an incremental approach that processes only new data.

<details>
<summary>💡 Hint</summary>
Use Delta's Change Data Feed (CDF) to read only changed rows from the silver table, then MERGE them into the gold table. Alternatively, use watermark-based filtering.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Enable Change Data Feed on the source table
ALTER TABLE production.silver.orders SET TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true'
);

-- Step 2: Read only changes since last processing
-- Method A: CDF (reads only new/updated rows)
CREATE OR REPLACE TEMPORARY VIEW new_order_changes AS
SELECT customer_id, amount, order_date, _change_type
FROM table_changes('production.silver.orders', @last_processed_version)
WHERE _change_type IN ('insert', 'update_postimage');

-- Step 3: MERGE incremental changes into gold
MERGE INTO production.gold.daily_customer_metrics t
USING (
    SELECT 
        customer_id,
        order_date,
        COUNT(*) AS new_orders,
        SUM(amount) AS new_revenue
    FROM new_order_changes
    GROUP BY customer_id, order_date
) s
ON t.customer_id = s.customer_id AND t.metric_date = s.order_date
WHEN MATCHED THEN UPDATE SET
    t.order_count = t.order_count + s.new_orders,
    t.total_revenue = t.total_revenue + s.new_revenue,
    t._updated_at = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
    customer_id, metric_date, order_count, total_revenue, _updated_at
) VALUES (
    s.customer_id, s.order_date, s.new_orders, s.new_revenue, current_timestamp()
);

-- Step 4: Record the processed version for next run
SET VARIABLE last_processed_version = (SELECT MAX(version) FROM (DESCRIBE HISTORY production.silver.orders));

-- RESULT:
-- Before: Full rebuild of 500M rows → 2 hours
-- After: Process 500K changed rows → 2 minutes
-- Speedup: 60x faster, 95% less compute cost
```

**Key Points:**
- CDF provides a stream of changes (insert, update, delete) since a version
- Only process NEW data — don't re-scan the entire table
- MERGE handles both new customers (INSERT) and existing customers (UPDATE)
- Track last_processed_version to know where to resume next run
- Alternative (simpler): filter by `_loaded_at > @last_run_timestamp`
- CDF is more reliable (catches updates/deletes, not just inserts)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Data Quality Gates

**Scenario:** Bad data in bronze (null customer_ids, negative amounts) is propagating to silver and gold layers, causing dashboard errors. Design quality gates that prevent bad data from reaching silver.

<details>
<summary>💡 Hint</summary>
Add quality checks at the bronze→silver boundary. Good data flows to silver; bad data goes to a quarantine table. Alert if bad data rate exceeds threshold.
</details>

<details>
<summary>✅ Solution</summary>

```python
from pyspark.sql.functions import col, when, current_timestamp, lit

def bronze_to_silver_with_quality(source_table: str, target_table: str, quarantine_table: str):
    """Transform bronze to silver with data quality gates."""
    
    bronze_df = spark.table(source_table).filter(
        col("_ingested_at") >= lit(last_processed_timestamp)
    )
    
    # Define quality rules
    quality_rules = [
        (col("order_id").isNotNull(), "order_id_not_null"),
        (col("customer_id").isNotNull(), "customer_id_not_null"),
        (col("amount").cast("decimal(10,2)").isNotNull(), "amount_is_numeric"),
        (col("amount").cast("decimal(10,2)") > 0, "amount_positive"),
        (col("order_date").cast("date").isNotNull(), "date_is_valid"),
    ]
    
    # Apply all rules — add a boolean column for each
    checked_df = bronze_df
    for rule_expr, rule_name in quality_rules:
        checked_df = checked_df.withColumn(f"_qc_{rule_name}", rule_expr)
    
    # Split: all rules pass → silver, any rule fails → quarantine
    all_rules_pass = None
    for _, rule_name in quality_rules:
        condition = col(f"_qc_{rule_name}")
        all_rules_pass = condition if all_rules_pass is None else (all_rules_pass & condition)
    
    good_df = checked_df.filter(all_rules_pass)
    bad_df = checked_df.filter(~all_rules_pass)
    
    # Write good data to silver (with transformations)
    silver_df = (good_df
        .select(
            col("order_id").cast("bigint").alias("order_id"),
            col("customer_id").cast("bigint").alias("customer_id"),
            col("amount").cast("decimal(10,2)").alias("amount"),
            col("order_date").cast("date").alias("order_date"),
            current_timestamp().alias("_loaded_at"),
        )
    )
    silver_df.write.mode("append").saveAsTable(target_table)
    
    # Write bad data to quarantine (with failure reasons)
    if bad_df.count() > 0:
        quarantine_df = bad_df.select(
            col("*"),
            current_timestamp().alias("_quarantined_at"),
        )
        quarantine_df.write.mode("append").saveAsTable(quarantine_table)
    
    # Alert if bad data rate is high
    total = bronze_df.count()
    bad_count = bad_df.count()
    bad_rate = bad_count / max(total, 1)
    
    if bad_rate > 0.05:
        alert(f"Data quality alert: {bad_rate:.1%} of records failed quality checks!")
    
    return {"total": total, "passed": total - bad_count, "quarantined": bad_count, "bad_rate": bad_rate}
```

**Key Points:**
- Quality gate at bronze→silver boundary (bad data never reaches silver/gold)
- Quarantine table: preserves bad records for investigation (not lost, just isolated)
- Alert threshold: 5% failure rate triggers investigation of upstream source
- Rules are explicit and auditable (anyone can see what "quality" means)
- Pattern: check → split → route (good to target, bad to quarantine)
- Run quarantine analysis weekly: identify systematic issues, feed back to source team

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Multi-Tenant Lakehouse

**Scenario:** Your SaaS platform serves 50 enterprise customers. Each customer's data must be isolated (Customer A can never see Customer B's data). Design the multi-tenancy model using Unity Catalog.

<details>
<summary>💡 Hint</summary>
Options: catalog-per-tenant (strongest isolation), schema-per-tenant (moderate), or row-level filtering (weakest). Choose based on compliance requirements and operational overhead.
</details>

<details>
<summary>✅ Solution</summary>

```sql
-- OPTION A: Catalog per tenant (strongest isolation, highest ops cost)
-- Best for: strict compliance (healthcare, finance), large tenants

CREATE CATALOG tenant_acme;
CREATE CATALOG tenant_globex;
-- Each tenant has completely separate namespace
-- Grant: ONLY tenant's team can access their catalog
GRANT ALL PRIVILEGES ON CATALOG tenant_acme TO `acme-admins`;
-- 50 tenants = 50 catalogs to manage (automation via Terraform required)

-- OPTION B: Schema per tenant (moderate isolation, manageable)
-- Best for: most SaaS scenarios, 10-100 tenants

CREATE CATALOG platform;
CREATE SCHEMA platform.tenant_acme;
CREATE SCHEMA platform.tenant_globex;
-- Shared catalog, separate schemas
GRANT ALL PRIVILEGES ON SCHEMA platform.tenant_acme TO `acme-team`;
-- Simpler management: one catalog, N schemas

-- OPTION C: Row-level filtering (weakest isolation, most efficient)
-- Best for: 100+ tenants, less strict compliance needs

CREATE TABLE platform.shared.events (
    tenant_id STRING,
    event_data STRING,
    ...
);
-- All tenants' data in ONE table, filtered at query time
CREATE FUNCTION platform.security.tenant_filter(tid STRING)
RETURN tid = current_user_tenant_id();
ALTER TABLE platform.shared.events
SET ROW FILTER platform.security.tenant_filter ON (tenant_id);

-- RECOMMENDATION for 50 enterprise customers:
-- Schema per tenant (Option B) with:
-- - Automated provisioning (Terraform creates schema + grants on signup)
-- - Standard medallion per tenant (bronze/silver/gold schemas)
-- - Shared reference data catalog (read-only for all)
-- - Row-level security for shared analytics tables
```

**Key Points:**
- Catalog isolation: strongest, but 50 catalogs = significant ops overhead (use Terraform)
- Schema isolation: good balance of security and manageability for most SaaS
- Row filtering: most efficient but relies on application-level filter (higher risk)
- Automate provisioning: new tenant = Terraform script creates catalog/schema/grants
- Shared resources: reference data, platform metrics → separate catalog with read-only access
- Testing: regularly verify isolation (query as tenant A, confirm can't see tenant B)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Cost Optimization

**Scenario:** Your lakehouse costs $12K/month. Breakdown: $4K compute (all-purpose clusters running 24/7), $3K SQL warehouse, $2K storage, $3K other. The CFO wants it under $6K. Find $6K in savings.

<details>
<summary>💡 Hint</summary>
All-purpose clusters 24/7 is the biggest waste. Switch to jobs compute + scheduling. SQL warehouse: auto-stop and serverless. Storage: VACUUM + tiered storage.
</details>

<details>
<summary>✅ Solution</summary>

```python
# CURRENT: $12K/month
# TARGET: $6K/month (50% reduction)

OPTIMIZATIONS = {
    "1_all_purpose_to_jobs": {
        "current": "$4,000 (all-purpose clusters 24/7)",
        "fix": "Switch to Jobs compute (60% cheaper) + scheduled runs (not 24/7)",
        "after": "$800 (jobs compute × 6 hrs/day × 30 days)",
        "savings": "$3,200",
    },
    "2_sql_warehouse_optimization": {
        "current": "$3,000 (SQL warehouse always on)",
        "fix": "Auto-stop after 10 min idle + Serverless (pay-per-query)",
        "after": "$1,200 (active only during business hours + queries)",
        "savings": "$1,800",
    },
    "3_storage_cleanup": {
        "current": "$2,000 (50TB all active storage)",
        "fix": "VACUUM old versions + archive cold data to S3 Glacier",
        "after": "$1,000 (30TB active + 20TB archived at Glacier rates)",
        "savings": "$1,000",
    },
    "4_spot_instances": {
        "current": "$800 (estimated remaining compute)",
        "fix": "Use spot instances for ETL (70% cheaper, Auto Loader is fault-tolerant)",
        "after": "$300",
        "savings": "$500",
    },
    "total_savings": "$6,500",
    "new_monthly": "$5,500 ✓ (under $6K target)",
}

# Implementation priority:
# Week 1: Switch all-purpose → jobs compute (biggest impact, low effort)
# Week 2: Configure SQL warehouse auto-stop + serverless
# Week 3: Run VACUUM across all tables, archive old data
# Week 4: Enable spot instances on ETL clusters

# Key changes:
# - Jobs compute: 60% cheaper than all-purpose (same performance)
# - Auto-stop: warehouse shuts down after 10 min idle (saves overnight)
# - Serverless SQL: pay per query, not per hour (ideal for bursty BI)
# - Spot instances: Auto Loader handles failures gracefully (checkpointed)
```

**Key Points:**
- #1 waste: all-purpose clusters running 24/7 (switch to jobs compute + scheduling)
- #2 waste: SQL warehouse always on (auto-stop + serverless for bursty workloads)
- #3 waste: storing years of old Delta versions (VACUUM with appropriate retention)
- Spot instances for ETL: 70% savings, safe with checkpoint-based pipelines
- These optimizations have minimal quality/performance impact (same work, less cost)
- Monitor after changes: ensure SLAs are still met at lower cost

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Enterprise Lakehouse Design

**Scenario:** Design a lakehouse architecture for a 1000-employee company: 50 data engineers, 100 analysts, 30 data scientists, data from 20+ sources, 100TB total data, strict governance (SOC 2), and a $50K/month budget. Provide the full architecture.

<details>
<summary>💡 Hint</summary>
Consider: workspace topology, compute strategy (ETL/BI/ML), governance model (Unity Catalog), medallion design, monitoring, DR, and cost allocation. Think about each persona's needs.
</details>

<details>
<summary>✅ Solution</summary>

```python
ENTERPRISE_ARCHITECTURE = {
    "workspaces": {
        "etl_workspace": "For data engineers — ETL pipelines, Auto Loader",
        "analytics_workspace": "For analysts — SQL queries, dashboards",
        "ml_workspace": "For data scientists — model training, experiments",
        # All connected to ONE Unity Catalog metastore
    },
    
    "governance": {
        "metastore": "Single regional metastore (Unity Catalog)",
        "catalogs": ["production", "staging", "development", "sandbox"],
        "schemas_per_catalog": "By domain (sales, marketing, finance, product, ops)",
        "permission_model": "RBAC via groups synced from Okta/Azure AD",
        "audit": "System tables → weekly compliance reports",
        "row_security": "PII tables have row filters by region/role",
        "column_masking": "PII columns (email, phone) masked for non-authorized users",
    },
    
    "compute": {
        "etl": {
            "type": "Jobs compute clusters",
            "instance": "r5.2xlarge (spot, 70% savings)",
            "scaling": "Auto-scale 4-16 workers",
            "schedule": "Hourly Auto Loader + daily batch transforms",
            "monthly_cost": "$8,000",
        },
        "sql_analytics": {
            "type": "Serverless SQL Warehouses",
            "sizes": ["Small (analysts)", "Medium (dashboards)", "Large (heavy reports)"],
            "auto_stop": "10 minutes",
            "monthly_cost": "$15,000",
        },
        "ml_training": {
            "type": "GPU clusters (g5.xlarge)",
            "scaling": "On-demand, terminate after training",
            "spot": True,
            "monthly_cost": "$5,000",
        },
        "interactive": {
            "type": "All-purpose clusters (for exploration only)",
            "policy": "Auto-terminate after 2 hours idle",
            "monthly_cost": "$3,000",
        },
    },
    
    "data_architecture": {
        "medallion": "Bronze → Silver → Gold (per domain)",
        "ingestion": "Auto Loader for all 20+ sources",
        "transformation": "DLT for streaming, Workflows for batch",
        "serving": "SQL Warehouses for BI, Feature Store for ML",
        "storage": "100 TB on S3 = $2,300/month",
    },
    
    "monitoring": {
        "cost": "System billing tables → Grafana dashboard → per-team chargeback",
        "quality": "DQ framework: freshness, null rates, row counts, expectations",
        "pipeline": "Workflow alerts on failure, SLA monitoring",
        "security": "Audit log analysis, privilege escalation alerts",
    },
    
    "dr_and_compliance": {
        "backup": "S3 cross-region replication for critical tables",
        "retention": "Bronze: 90 days, Silver: 1 year, Gold: 3 years",
        "soc2": "Audit logging, access reviews, encryption at rest (default)",
        "gdpr": "UC lineage → find PII → DELETE + VACUUM",
    },
    
    "budget_breakdown": {
        "etl_compute": "$8,000",
        "sql_warehouses": "$15,000",
        "ml_compute": "$5,000",
        "interactive": "$3,000",
        "storage_100tb": "$2,300",
        "platform_overhead": "$3,000 (networking, metadata, logs)",
        "buffer": "$13,700",
        "total": "$50,000 ✓",
    },
}
```

**Key Points:**
- Three workspaces (ETL/Analytics/ML) connected to one metastore: clear separation of concerns
- Jobs compute for ETL (60% cheaper), Serverless SQL for BI (pay-per-query), GPU for ML
- Unity Catalog: single governance layer across all workspaces and all users
- Per-team cost tracking via billing system tables + custom tags
- Spot instances for fault-tolerant workloads (ETL, ML training) = 70% savings
- SOC 2 addressed by: audit logging (default), access reviews (system tables), encryption (S3 default)
- Budget of $50K/month supports 1000 users with full governance and DR

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Lakehouse Migration from Legacy

**Scenario:** Your company has a fragmented data platform: Hadoop on-prem (30TB, HDFS), Redshift (10TB, operational analytics), and MongoDB (event data, 5TB). Consolidate everything into a Databricks lakehouse. Design the migration plan.

<details>
<summary>💡 Hint</summary>
Phased migration: one source at a time. Start with the easiest (Redshift → S3 → Delta), then Hadoop (HDFS → S3 via DistCp), then MongoDB (connector → streaming). Validate at each phase.
</details>

<details>
<summary>✅ Solution</summary>

```python
MIGRATION_PLAN = {
    "phase_0_foundation": {
        "duration": "2 weeks",
        "tasks": [
            "Set up Databricks workspace + Unity Catalog",
            "Create catalog/schema structure (medallion architecture)",
            "Set up networking (VPC peering to on-prem, VPN)",
            "Configure IAM roles, storage credentials, external locations",
            "Set up CI/CD (Repos, Terraform for infrastructure)",
        ],
    },
    
    "phase_1_redshift": {
        "duration": "4 weeks",
        "source": "Redshift (10TB, 200 tables)",
        "approach": "UNLOAD to S3 Parquet → Register as Delta External Tables",
        "steps": [
            "UNLOAD Redshift tables to S3 as Parquet (schema-preserving)",
            "Register tables in Unity Catalog as external Delta tables",
            "Validate: row counts, checksums, sample query results match",
            "Migrate BI dashboards from Redshift → Databricks SQL",
            "Switch ETL outputs from Redshift → Delta tables",
            "Run parallel for 2 weeks, then decommission Redshift",
        ],
        "risk": "Low (Parquet/SQL-based, well-understood)",
    },
    
    "phase_2_hadoop": {
        "duration": "6 weeks",
        "source": "Hadoop HDFS (30TB, Hive tables + raw files)",
        "approach": "DistCp to S3, convert Hive tables to Delta, move jobs to Spark on Databricks",
        "steps": [
            "DistCp: copy HDFS data to S3 (30TB, ~3-5 days on dedicated bandwidth)",
            "Convert Hive tables to Delta: CTAS for each table",
            "Migrate Hive metastore entries to Unity Catalog",
            "Port MapReduce/Hive jobs to PySpark on Databricks",
            "Validate: compare outputs between Hadoop and Databricks",
            "Decommission Hadoop cluster (save $20K+/month in hardware)",
        ],
        "risk": "Medium (complex jobs may need rewriting)",
    },
    
    "phase_3_mongodb": {
        "duration": "3 weeks",
        "source": "MongoDB (5TB, event data, semi-structured)",
        "approach": "MongoDB Spark Connector → streaming → Delta",
        "steps": [
            "Initial bulk load: MongoDB Spark Connector full read → Delta",
            "Ongoing sync: Change Streams → Kafka → Auto Loader → Delta",
            "Flatten nested documents into silver tables",
            "Validate: compare document counts and sample queries",
        ],
        "risk": "Medium (schema variability in MongoDB docs)",
    },
    
    "timeline": "Total: ~15 weeks (parallel where possible → 10 weeks actual)",
    "cost_savings_after_migration": {
        "hadoop_decommission": "$20,000/month (hardware, ops)",
        "redshift_decommission": "$15,000/month",
        "consolidated_lakehouse": "$8,000/month",
        "net_savings": "$27,000/month",
    },
}
```

**Key Points:**
- Migrate one system at a time (reduce risk, validate at each step)
- Start with easiest (Redshift: SQL→SQL, well-understood transformation)
- Hadoop: biggest but most savings (on-prem hardware costs eliminated)
- MongoDB: use Change Streams for ongoing sync (don't just one-time dump)
- Validate at EVERY step: row counts, checksums, sample query comparison
- Run parallel during cutover period: both systems active, catch discrepancies
- 15-week plan is realistic for a 5-person platform team
- Net savings: $27K/month = $324K/year (migration pays for itself in 2-3 months)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Real-Time Lakehouse

**Scenario:** Build a near-real-time lakehouse where data is queryable within 60 seconds of event occurrence. Sources: Kafka (1M events/min), API webhooks (10K/min), and database CDC (5K changes/min). Current batch pipeline has 6-hour latency.

<details>
<summary>💡 Hint</summary>
Replace batch with streaming at every layer: Auto Loader/Kafka → bronze (streaming), bronze → silver (streaming), silver → gold (streaming or micro-batch). Delta tables support streaming reads and writes.
</details>

<details>
<summary>✅ Solution</summary>

```python
# ARCHITECTURE: Streaming at every layer for sub-60s latency

# LAYER 1: Bronze ingestion (streaming)

# Source A: Kafka events → bronze
kafka_to_bronze = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "user_events")
    .option("startingOffsets", "latest")
    .load()
    .selectExpr("CAST(value AS STRING) as raw_json", "timestamp as kafka_timestamp")
    .writeStream
    .trigger(processingTime="10 seconds")  # Micro-batch every 10s
    .option("checkpointLocation", "/checkpoints/bronze_kafka/")
    .toTable("production.bronze.user_events")
)

# Source B: API webhooks (files via Auto Loader)
webhooks_to_bronze = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")
    .load("s3://lake/landing/webhooks/")
    .writeStream
    .trigger(processingTime="30 seconds")
    .option("checkpointLocation", "/checkpoints/bronze_webhooks/")
    .toTable("production.bronze.webhook_events")
)

# Source C: Database CDC (Debezium → Kafka → Delta)
cdc_to_bronze = (
    spark.readStream
    .format("kafka")
    .option("subscribe", "cdc.public.orders")
    .load()
    .writeStream
    .trigger(processingTime="10 seconds")
    .option("checkpointLocation", "/checkpoints/bronze_cdc/")
    .toTable("production.bronze.orders_cdc")
)

# LAYER 2: Silver transformation (streaming from bronze)
bronze_to_silver = (
    spark.readStream.table("production.bronze.user_events")
    .selectExpr("from_json(raw_json, schema) as data", "kafka_timestamp")
    .select("data.*", "kafka_timestamp")
    # Type casting, validation, dedup
    .withWatermark("event_timestamp", "1 minute")
    .dropDuplicatesWithinWatermark(["event_id"])
    .writeStream
    .trigger(processingTime="15 seconds")
    .option("checkpointLocation", "/checkpoints/silver_events/")
    .toTable("production.silver.user_events")
)

# LAYER 3: Gold aggregation (streaming micro-batch)
silver_to_gold = (
    spark.readStream.table("production.silver.user_events")
    .withWatermark("event_timestamp", "2 minutes")
    .groupBy(window("event_timestamp", "1 minute"), "event_type")
    .count()
    .writeStream
    .trigger(processingTime="30 seconds")
    .outputMode("update")
    .option("checkpointLocation", "/checkpoints/gold_metrics/")
    .toTable("production.gold.event_counts_1min")
)

# END-TO-END LATENCY:
# Event occurs → Kafka (1s) → Bronze (10s trigger) → Silver (15s trigger) → Gold (30s trigger)
# Total: ~56 seconds worst case → meets 60-second SLA!

# Compared to batch: 6 hours → 56 seconds = 400x improvement
```

**Key Points:**
- Streaming at every layer: bronze, silver, AND gold all use streaming
- Each layer's trigger is additive: 10s + 15s + 30s = ~55s end-to-end
- Watermarks handle late data (events arriving slightly out of order)
- Deduplication within watermark prevents duplicates from Kafka redelivery
- Same Delta tables used for both streaming writes and batch/BI reads
- Cost: always-on clusters for each stream (~$5-8K/month for 3 streams)
- Trade-off: 60s latency costs more than 6-hour batch (continuous compute)
- Optimization: combine streams where possible (one cluster, multiple queries)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Disaster Recovery Design

**Scenario:** Your lakehouse processes $5M/day in e-commerce transactions. An S3 regional outage would stop all processing and reporting. Design a DR strategy with RPO < 1 hour and RTO < 30 minutes.

<details>
<summary>💡 Hint</summary>
S3 Cross-Region Replication for data, secondary workspace in DR region, Terraform for infrastructure recreation, and regular DR testing.
</details>

<details>
<summary>✅ Solution</summary>

```python
DR_ARCHITECTURE = {
    "primary": {
        "region": "us-east-1",
        "workspace": "production-workspace",
        "storage": "s3://lakehouse-primary/",
        "metastore": "uc-us-east-1",
    },
    "secondary": {
        "region": "us-west-2",
        "workspace": "dr-workspace (warm standby)",
        "storage": "s3://lakehouse-dr/ (replicated)",
        "metastore": "uc-us-west-2",
    },
    
    "data_replication": {
        "method": "S3 Cross-Region Replication (CRR)",
        "scope": "All Delta table files (Parquet + _delta_log)",
        "rpo": "< 15 minutes (S3 CRR latency)",
        "cost": "~$500/month for 100 TB of replication traffic",
    },
    
    "metadata_replication": {
        "method": "Terraform state (infrastructure-as-code)",
        "scope": "Unity Catalog: catalogs, schemas, grants, external locations",
        "rpo": "24 hours (last Terraform apply)",
        "rto": "10 minutes (terraform apply against DR metastore)",
    },
    
    "failover_runbook": [
        "1. Confirm primary region is down (not just a brief blip)",
        "2. Verify S3 CRR is caught up (check replication metrics)",
        "3. Run: terraform apply -target=module.dr_workspace",
        "4. Update external locations to point to DR bucket",
        "5. Start critical pipelines in DR workspace",
        "6. Switch DNS for SQL warehouse endpoints",
        "7. Notify users: use DR workspace until further notice",
    ],
    
    "testing": {
        "frequency": "Quarterly DR drill",
        "test": "Fail over, run critical ETL + BI queries, validate results, fail back",
        "last_test_rto": "22 minutes (met <30 min target)",
    },
    
    "cost": {
        "crr_replication": "$500/month",
        "dr_workspace": "$200/month (warm standby, minimal compute)",
        "quarterly_testing": "$500/quarter (compute during test)",
        "total_dr_overhead": "$900/month (cheap insurance for $5M/day business)",
    },
}
```

**Key Points:**
- RPO < 1 hour: S3 CRR provides ~15 minute lag (exceeds requirement)
- RTO < 30 minutes: Terraform recreates infrastructure in 10 min, pipelines start in 10 min
- DR workspace: warm standby (exists but minimal compute until failover)
- Test quarterly: a DR plan that's never tested is not a plan
- Cost: $900/month to protect $5M/day in transactions = trivial insurance
- Key risk: Delta log consistency during failover (CRR is eventually consistent)
- Mitigation: run validation queries on DR after failover to confirm data integrity
- Post-failover: reverse-replicate any data created during outage back to primary

</details>

</article>
