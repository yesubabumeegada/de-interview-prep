---
title: "Azure Databricks — Scenarios"
topic: azure
subtopic: azure-databricks
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [azure, databricks, scenarios, interview, performance, architecture]
---

# Azure Databricks — Interview Scenarios

## Scenario 1: Migrate Legacy Hadoop/Hive to Azure Databricks

**Question:** Your company has a 500TB Hadoop/Hive-based data lake on-premises. You need to migrate to Azure Databricks + ADLS Gen2. Design the migration plan.

**Answer:**

```
Migration phases:

Phase 1: Assessment (2-4 weeks)
  Inventory:
    hive -e "SHOW DATABASES; SHOW TABLES IN db_name;" > table_inventory.txt
    Count: tables, views, UDFs, stored procedures
    Identify: file formats (ORC/Parquet/CSV), compression, partitioning
    Dependencies: which jobs read which tables (lineage scan of Spark/Hive scripts)
    Users: teams, access patterns, SLAs
  
  Decision: migrate as-is (Hive → Delta) or refactor (add proper Bronze/Silver/Gold)
  Recommendation: refactor (once-in-a-decade migration opportunity)

Phase 2: Infrastructure Setup (2-3 weeks)
  Azure resources:
    ADLS Gen2 accounts (bronze, silver, gold, checkpoints)
    Databricks workspace (Premium tier — needed for Unity Catalog)
    Unity Catalog metastore
    Azure Key Vault for secrets
    Azure DevOps for CI/CD
    VNet injection (if compliance requires private network)
  
  Network:
    ExpressRoute or Site-to-Site VPN from on-prem to Azure
    Required for initial data transfer and parallel running

Phase 3: Data Migration (4-8 weeks)
  Method 1: ADF Copy Activity (managed, monitored, incremental)
    ADF Self-Hosted IR on on-prem → copy tables to ADLS Bronze
    500TB estimate: 500TB / (200 MB/s SHIR throughput) = ~700 hours
    With 5 parallel copies: ~140 hours (6 days)
  
  Method 2: Azure Data Box (for >1PB or limited bandwidth)
    Ship physical appliance, load data offline, ship to Azure datacenter
    Not needed at 500TB with adequate network
  
  Incremental sync during migration:
    Keep on-prem as source of truth until cutover
    Nightly delta sync: Hive → ADLS Bronze (changed records only, by updated_at)

Phase 4: Pipeline Recreation (6-12 weeks)
  Convert HiveQL to PySpark/Delta:
    SELECT / JOIN / GROUP BY: translate directly
    Dynamic partition INSERT: replace with df.write.partitionBy().mode("overwrite")
    Hive UDFs: rewrite as PySpark UDFs or built-in functions
    
  Validate output:
    Row counts: old_count == new_count
    Aggregate checks: SUM(amount) matches between Hive and Delta
    Schema checks: column names, types, nullability

Phase 5: Cutover (2 weeks)
  Final sync: complete last incremental load from on-prem
  Redirect all consumers (BI tools, downstream apps) to Azure endpoints
  Parallel running: 2 weeks with both systems live (validate agreement)
  Decommission: stop on-prem jobs, decommission Hadoop cluster

Timeline: 4-6 months total
Cost: $0 for ADLS storage + Databricks clusters during migration period (minimize cost)
Risk mitigation: keep on-prem as fallback until parallel validation complete
```

---

## Scenario 2: Optimize a Slow Databricks Job

**Question:** A nightly Databricks job transforms 500GB of Silver orders data to Gold. It runs in 4 hours but should finish in 30 minutes. How do you diagnose and fix?

**Answer:**

```
Step 1: Profile the job
  Open Spark UI: Databricks UI → Job Run → Spark UI → Stages

  Look for:
  a) Stage with highest "Input" size → data reading bottleneck
  b) Stage with highest "Shuffle Read/Write" → join or aggregation bottleneck  
  c) Stage with few tasks taking very long → data skew
  d) GC Time > 10% of task time → memory pressure

Step 2: Diagnose specific issue

Issue A: Data skew in join
  Symptom: Spark UI → Tasks → one task running 100× longer than others
  Find skewed key: 
    df.groupBy("customer_id").count().orderBy(F.desc("count")).show(10)
    # customer_id "unknown" has 200M rows (30% of all rows)
  Fix: salt the skewed key
    df = df.withColumn("salted_key", 
        F.when(F.col("customer_id") == "unknown", 
               F.concat("customer_id", (F.rand() * 100).cast("int")))
         .otherwise(F.col("customer_id")))
    # Salt dimension side too: explode "unknown" 100 times with salt 0-99

Issue B: Too many shuffle partitions
  Symptom: 200 tasks, each only 2.5MB (200 × 2.5MB = 500MB total)
  200 default shuffle partitions × 2.5MB = lots of tiny tasks
  Fix: reduce shuffle partitions
    spark.conf.set("spark.sql.shuffle.partitions", "50")
    # Or use AQE (already enabled): AQE will auto-coalesce from 200 → 50

Issue C: Small files in Silver (source is fragmented)
  Symptom: Stage 1 "Input" = 500GB but 50,000 tasks (500GB / 50K = 10MB each — small)
  Fix: cache/repartition after initial read
    df = spark.table("prod.silver.orders").repartition(400).cache()
    # 400 partitions × 1.25GB = still large tasks (Spark handles this well)
    # Or: OPTIMIZE the silver.orders table (run as pre-step or separate job)

Issue D: Join with large table without broadcast
  Symptom: Stage with large Shuffle Write (10GB+) → broadcast join not used
  Fix: force broadcast for the smaller table
    from pyspark.sql.functions import broadcast
    result = large_fact_df.join(broadcast(dim_region_df), "region_code")

Step 3: Apply and measure
  Implement fixes in dev cluster with sample data
  Benchmark: time the job with representative 50GB sample
  Apply to production: commit to Git, deploy via CI/CD
  
Expected result: 4 hours → 25-35 minutes (8-10× improvement)
```

---

## Scenario 3: Design a Multi-Tenant Data Platform

**Question:** Build a Databricks-based data platform for 5 business units (Finance, Marketing, HR, Operations, Legal). Each unit has its own data and analysts. They should not see each other's data. One central data engineering team manages pipelines.

**Answer:**

```
Architecture: Unity Catalog with domain catalogs + shared Silver

Unity Catalog structure:
  shared_silver.          (shared schema, ETL team manages, no direct analyst access)
    orders              → masked/filtered before domain consumption
    customers           → PII masked
    products
  
  finance_catalog.        (Finance team domain)
    silver.orders         → view of shared_silver.orders filtered to Finance regions
    gold.revenue          → Finance-specific aggregations
    gold.budget_vs_actual

  marketing_catalog.      (Marketing team domain)
    silver.customers      → PII masked for Marketing analysts
    gold.campaign_metrics

  hr_catalog.             (HR domain — most sensitive)
    silver.employees      → strict access control
    gold.headcount

  legal_catalog.          (Legal domain)
    gold.audit_trail
    gold.compliance_reports

Access model:
  ETL Service Principal:
    Contributor on shared_silver, all domain catalogs (write ETL outputs)
  
  Finance analysts (group: finance-analysts):
    USAGE on finance_catalog
    SELECT on finance_catalog.silver.*, finance_catalog.gold.*
    DENIED on finance_catalog.hr.*  (explicitly denied at catalog level)
  
  HR analysts:
    SELECT on hr_catalog only
    Column masking on employees: SSN fully masked, salary masked for non-managers

Row-level isolation (example: Finance sees only their region):
  CREATE FUNCTION finance_region_filter(region STRING)
  RETURNS BOOLEAN
  RETURN IS_MEMBER('finance-analysts') AND region IN ('US', 'EU')
         OR IS_MEMBER('etl-service')  -- ETL can see all

  ALTER TABLE shared_silver.orders
  SET ROW FILTER finance_region_filter ON (region)

Cluster isolation:
  Option A: shared cluster + Unity Catalog enforcement (most cost-efficient)
    All analysts share one SQL Warehouse; Unity Catalog enforces access per user
    
  Option B: separate SQL Warehouses per business unit (for strict compute isolation)
    Each BU has its own warehouse — billing attributed to BU
    Higher cost but clearer charge-back

Cost attribution:
  Databricks: Account Console → Usage → tag clusters by business unit
  Each job cluster tagged: {"businessUnit": "finance", "env": "prod"}
  Monthly report: cost per BU per job

Governance:
  Purview scan on ADLS: classify PII data → tag in Unity Catalog
  Lineage: ETL notebook → table → Power BI dashboard (automated via Unity Catalog)
  Audit: all Unity Catalog access logged to audit log table (Databricks system table)
    SELECT * FROM system.access.audit WHERE user_identity LIKE '%finance%'
```

---

## Interview Tips

> **Tip 1:** "How do you enforce that analysts cannot run expensive queries on large clusters?" — Cluster policies in Databricks: create a policy that limits cluster size (max workers = 4), forces auto-termination (30 min idle), and prevents modifying certain cluster properties. Assign the policy to the `analysts` group — they can only create clusters within the policy limits. For SQL users: use SQL Warehouse with query size limits (`warehouse_size = 'Medium'`, scaling limits). Also use Unity Catalog row filters to prevent full-table scans on billion-row tables by returning only relevant rows.

> **Tip 2:** "What is `dbutils.secrets.get` and why is it used instead of environment variables?" — `dbutils.secrets.get(scope, key)` retrieves secrets from Azure Key Vault-backed secret scopes. The returned value is redacted in notebook output (shown as `[REDACTED]`) to prevent accidental logging. Environment variables in notebooks can be accidentally printed or logged. Databricks secret scopes integrate directly with Azure Key Vault: create a secret scope backed by your Key Vault, then `dbutils.secrets.get("my-scope", "storage-key")` retrieves the Key Vault secret at runtime. Secrets are never stored in the notebook.

> **Tip 3:** "What's the Databricks lakehouse architecture decision: one large cluster vs many small clusters?" — One large cluster for ETL + one SQL warehouse for analytics is simpler and cheaper for small-medium teams. For large enterprises: separate job clusters per pipeline (isolation, right-sizing), one SQL warehouse per business unit (cost attribution, performance isolation). Clusters should be sized for the workload: a 10GB daily ETL doesn't need a 20-node cluster. Profile data volumes: `df.count()` and expected shuffle size determine correct cluster size. The wrong size (over-provisioned) is a common Databricks cost issue.
