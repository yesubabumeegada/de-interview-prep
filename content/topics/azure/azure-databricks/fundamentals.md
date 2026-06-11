---
title: "Azure Databricks — Fundamentals"
topic: azure
subtopic: azure-databricks
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [azure, databricks, spark, delta-lake, unity-catalog, notebooks]
---

# Azure Databricks — Fundamentals


## 🎯 Analogy

Think of Azure Databricks like Spark as a managed cloud service inside Azure: your notebooks, jobs, and Delta Lake tables live in one workspace, integrated with Azure AD for auth, ADLS for storage, and ADF for orchestration.

---
## What Is Azure Databricks?

Azure Databricks is a **managed Apache Spark platform** built on Azure, jointly developed by Microsoft and Databricks. It provides a collaborative workspace with notebooks, automated cluster management, Delta Lake, MLflow, and Unity Catalog — all in one managed service.

```
Azure Databricks = Apache Spark + Delta Lake + MLflow + Unity Catalog + 
                   Photon (C++ vectorized engine) + managed notebooks + Azure integration

Positioning:
  vs Raw Spark (HDInsight/EMR):  Databricks adds Delta Lake, MLflow, auto-scaling,
                                  Photon, Unity Catalog — enterprise-ready out of box
  vs Azure Synapse Spark:        Databricks is faster (Photon), richer ML ecosystem,
                                  better Delta Lake support
  vs Azure Synapse SQL Pool:     Databricks for transformation; Synapse SQL for BI serving

Azure-specific integrations:
  Identity:   Azure Active Directory / Microsoft Entra ID SSO
  Storage:    ADLS Gen2 (native ABFS support)
  Secrets:    Azure Key Vault-backed secret scopes
  Monitoring: Azure Monitor, Log Analytics
  Networking: Azure VNet injection (clusters run in your VNet)
  Catalog:    Unity Catalog (metadata + governance layer)
  CI/CD:      Azure DevOps integration
```

---

## Cluster Types

```
1. All-Purpose Cluster (interactive)
   Created manually or via API
   Used for: notebook development, exploratory analysis, ad-hoc jobs
   Billing: per DBU (Databricks Unit) while running
   Recommendation: auto-terminate after 30 min idle (cost control)

2. Job Cluster (automated)
   Created fresh for each job run, terminated when done
   Used for: scheduled ETL pipelines, production jobs
   Billing: cheaper than All-Purpose (different DBU rate)
   Benefit: isolated environment per run (no cluster state contamination)

3. SQL Warehouse (formerly SQL Endpoint)
   Optimized for SQL queries and BI tools
   Used for: Databricks SQL, Power BI, Tableau connections
   Scaling: auto-scales based on concurrent queries
   Types: Serverless (fastest start), Pro, Classic

Cluster configuration:
  Driver node:   coordinates job (8 GB RAM minimum for production)
  Worker nodes:  execute tasks (2-200+ workers, auto-scaling)
  
  Node types (common):
    Standard_DS3_v2:  4 vCPU, 14 GB RAM (development)
    Standard_DS4_v2:  8 vCPU, 28 GB RAM (light production)
    Standard_DS5_v2: 16 vCPU, 56 GB RAM (heavy production)
    Standard_L8s_v3: 8 vCPU, 64 GB RAM, NVMe SSD (shuffle-heavy jobs)

Cluster pools:
  Pre-initialized VMs ready to use
  New cluster creation: 30 sec (vs 3 min from scratch)
  Recommended for: pipelines with frequent job cluster creation
```

---

## Delta Lake on Databricks

```python
# Delta Lake is the default table format in Databricks
# All tables created without FORMAT = ... default to Delta

# Create Delta table
spark.sql("""
  CREATE TABLE IF NOT EXISTS silver.orders (
    order_id    BIGINT,
    customer_id INT,
    amount      DECIMAL(18,2),
    order_date  DATE,
    region      VARCHAR(50)
  )
  USING DELTA
  PARTITIONED BY (order_date)
  LOCATION 'abfss://silver@myaccount.dfs.core.windows.net/orders'
""")

# Write DataFrame to Delta
df.write.format("delta").mode("overwrite").partitionBy("order_date") \
    .save("abfss://silver@myaccount.dfs.core.windows.net/orders")

# Merge (upsert)
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "abfss://silver@myaccount.dfs.core.windows.net/orders")
delta_table.alias("target").merge(
    source=df.alias("source"),
    condition="target.order_id = source.order_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()

# Time travel
df_yesterday = spark.read.format("delta") \
    .option("versionAsOf", 5) \
    .load("abfss://silver@myaccount.dfs.core.windows.net/orders")

# Maintenance
spark.sql("OPTIMIZE silver.orders ZORDER BY (customer_id)")
spark.sql("VACUUM silver.orders RETAIN 168 HOURS")
```

---

## Unity Catalog

```
Unity Catalog: centralized governance layer for all Databricks workspaces

Three-level namespace: catalog.schema.table
  Examples:
    prod.silver.orders       (production catalog)
    dev.silver.orders        (dev catalog — same table name, different catalog)
    prod.gold.daily_revenue

Key features:
  Centralized metastore: one catalog across all workspaces in Azure region
  Fine-grained access: column masking, row filters, table-level ACLs
  Automated lineage: tracks which notebook/job read/wrote which table
  Audit logging: all data access logged (who, when, what)
  External locations: register ADLS paths securely (no more hardcoded credentials)
  Delta Sharing: share data with external orgs securely

Setup:
  Unity Catalog metastore: one per Azure region (created once)
  Workspace assignment: attach workspace to the metastore
  Catalog: top-level container per environment (prod, dev, test)
  External location: registered ADLS Gen2 path with managed identity

Access control:
  GRANT SELECT ON TABLE prod.silver.orders TO GROUP `data-analysts`;
  GRANT USAGE ON CATALOG prod TO GROUP `data-engineers`;
  GRANT CREATE TABLE ON SCHEMA prod.silver TO GROUP `data-engineers`;
```

---


## ▶️ Try It Yourself

```python
# Submit a Databricks job via REST API
import requests
import os

DATABRICKS_HOST = os.environ["DATABRICKS_HOST"]  # https://adb-xxx.azuredatabricks.net
TOKEN = os.environ["DATABRICKS_TOKEN"]

headers = {"Authorization": f"Bearer {TOKEN}"}

# Run a notebook as a job
resp = requests.post(
    f"{DATABRICKS_HOST}/api/2.1/jobs/run-now",
    headers=headers,
    json={
        "job_id": 12345,
        "notebook_params": {"process_date": "2024-01-15"},
    },
)
print("Run ID:", resp.json()["run_id"])

# Check run status
run_id = resp.json()["run_id"]
status = requests.get(
    f"{DATABRICKS_HOST}/api/2.1/jobs/runs/get?run_id={run_id}",
    headers=headers,
).json()
print("State:", status["state"]["life_cycle_state"])
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "When would you use an All-Purpose cluster vs a Job cluster?" — All-Purpose cluster: development and interactive exploration where you need persistent notebook state and cluster between runs. Job cluster: production ETL and scheduled jobs — creates a fresh cluster for each run (isolated, reproducible), uses cheaper Job DBU pricing (~50% less than All-Purpose), auto-terminates after the job. Rule: never run production jobs on All-Purpose clusters (expensive, no isolation). All-Purpose for dev/test, Job clusters for production.

> **Tip 2:** "What is a DBU (Databricks Unit) and how does it relate to cost?" — DBU is Databricks' unit of processing power. 1 DBU ≈ 1 compute unit of processing. The cost per DBU depends on: workload type (All-Purpose vs Jobs vs SQL), tier (Standard vs Premium), and Azure region. Example: Standard_DS4_v2 worker = 0.75 DBU/hour on Jobs tier at ~$0.07/DBU = ~$0.05/worker/hour. Total cluster cost = EC2/VM cost + DBU license cost. For cost optimization: use Spot/Spot (preemptible) VMs for workers (70% cheaper) with fallback to on-demand if spot capacity unavailable.

> **Tip 3:** "What's the difference between an external table and a managed table in Databricks Unity Catalog?" — Managed table: Databricks controls the data location (stores in Unity Catalog's managed storage). When you DROP the table, the data is deleted. External table: data lives in a registered external location (e.g., ADLS Gen2 path you own). When you DROP the table, only the metadata is removed — the data stays. For production lakehouses: always use external tables with data in your own ADLS Gen2 (you own the data, vendor lock-in protection).
