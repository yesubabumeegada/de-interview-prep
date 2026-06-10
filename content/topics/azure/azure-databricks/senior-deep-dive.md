---
title: "Azure Databricks — Senior Deep Dive"
topic: azure
subtopic: azure-databricks
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [azure, databricks, photon, unity-catalog, vnet-injection, cost-optimization, performance]
---

# Azure Databricks — Senior Deep Dive

## Photon: C++ Vectorized Execution Engine

```
Photon = Databricks' native C++ query execution engine (replacement for JVM Spark SQL)

How it works:
  Traditional Spark: JVM Scala/Python → JVM bytecode → JVM execution
  Photon: Spark SQL → C++ vectorized execution → operates on SIMD (AVX-512)
  
  Vectorized execution: process 256-512 values simultaneously using CPU SIMD instructions
  vs row-at-a-time: process 1 value per instruction
  
  Speedup: 2-10× faster for SQL/analytics workloads
  Specific wins:
    Hash joins: ~5× faster (cache-friendly hash table in C++ vs JVM)
    Aggregations: ~4× faster (SIMD min/max/sum/count)
    Wide table scans: ~3× faster (columnar processing in C++)
    Parquet reads: ~2× faster (native Parquet reader, no JVM deserialization)
  
  Limited benefit (Photon doesn't accelerate):
    Complex UDFs (Python UDFs still go through JVM/Python boundary)
    Arbitrary Scala/Python transformations (only SQL-equivalent operations)
    ML training (uses standard Spark ML, not Photon)

Enable Photon:
  Cluster runtime: "Databricks Runtime" (not ML, not legacy)
  Cluster mode: Standard or High Concurrency
  Photon: check "Enable Photon acceleration" (enabled by default on recent runtimes)
  
  Verify Photon usage:
  spark.sql("EXPLAIN EXTENDED SELECT ...")
  -- Look for: "Photon" node types in the plan
  
  Monitor: Databricks UI → Cluster → Spark UI → SQL tab → check execution nodes
```

---

## VNet Injection and Network Security

```
Default Databricks: clusters run in Databricks-managed VNet (not your VNet)
VNet injection: clusters run in YOUR Azure VNet

Benefits of VNet injection:
  1. Access private resources: on-prem via ExpressRoute, private Azure SQL, private ADLS endpoint
  2. Custom NSG rules: block outbound internet from worker nodes
  3. Network monitoring: route traffic through Azure Firewall / NVA for logging
  4. Compliance: cluster IPs are in your RFC 1918 space, auditable

Architecture:
  Your VNet: 10.0.0.0/16
    Subnet: databricks-public  (10.0.1.0/24) — cluster public NIC
    Subnet: databricks-private (10.0.2.0/24) — cluster private NIC
  
  Each cluster node gets both a public and private NIC
  NSG: Databricks-required rules (platform traffic) + your custom rules
  
  Private Endpoint:
    ADLS Gen2: 10.0.3.5 (private IP in your VNet)
    Azure SQL: 10.0.3.6
    Key Vault: 10.0.3.7
  
  Data path: Databricks worker → private IP → ADLS (never leaves Azure backbone)

Subnet sizing:
  Each cluster node requires 2 IPs (public + private)
  Max nodes per cluster × 2 = minimum subnet size
  50-node cluster: 100 IPs minimum → /25 subnet (128 IPs)
  With multiple clusters: size generously (/22 = 1024 IPs)
  Important: subnet delegation to Databricks is permanent per subnet

Secure Cluster Connectivity (SCC) / No Public IP:
  Worker nodes have NO public IP (private subnet only)
  Traffic: Databricks control plane → private relay → worker (via Azure private link)
  Recommended for production: eliminates one attack surface
  Limitation: VNet injection required, slightly higher setup complexity
```

---

## Unity Catalog Deep Dive

```python
# Unity Catalog: enterprise data governance for Databricks

# --- External Locations (secure ADLS access without connection strings) ---

# Setup (one-time, by admin):
# 1. Create Storage Credential (managed identity for storage account):
spark.sql("""
  CREATE STORAGE CREDENTIAL adls_silver_cred
  WITH AZURE_MANAGED_IDENTITY = (DIRECTORY '/subscriptions/.../.../storageAccounts/mysilver')
""")
# Grants Databricks MI the Storage Blob Data Contributor role on the account

# 2. Register External Location:
spark.sql("""
  CREATE EXTERNAL LOCATION silver_location
  URL 'abfss://silver@mysilver.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL adls_silver_cred)
""")

# 3. Grant access to team:
spark.sql("GRANT READ FILES ON EXTERNAL LOCATION silver_location TO GROUP `data-engineers`")

# Now data engineers can read abfss://silver@... without knowing account keys

# --- Column masking for PII ---
spark.sql("""
  CREATE FUNCTION prod.security.mask_email(email STRING, user STRING)
  RETURNS STRING
  RETURN CASE WHEN IS_MEMBER('data-analysts') THEN email
              ELSE CONCAT(LEFT(email, 2), '***@***.com')
         END
""")

spark.sql("""
  ALTER TABLE prod.silver.customers
  ALTER COLUMN email SET MASK prod.security.mask_email USING COLUMNS (email, current_user())
""")

# --- Row filters for multi-tenant ---
spark.sql("""
  CREATE FUNCTION prod.security.region_filter(region STRING)
  RETURNS BOOLEAN
  RETURN IS_MEMBER('admin') OR region = current_user_region()
""")

spark.sql("""
  ALTER TABLE prod.silver.orders
  SET ROW FILTER prod.security.region_filter ON (region)
""")

# --- Lineage ---
# Automatically tracked: notebook A read table X → wrote table Y
# View lineage in UI: Catalog → table → Lineage tab
# Query lineage programmatically via Unity Catalog REST API:
# GET /api/2.0/lineage-tracking/table-lineage?table_name=prod.silver.orders
```

---

## Cluster Performance Tuning

```python
# AQE (Adaptive Query Execution) — enabled by default in Databricks Runtime 7+
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")  # merge small partitions
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")            # split skewed partitions

# Shuffle partition tuning:
# Default: 200 (Spark default, bad for large data)
# Rule of thumb: total shuffle data / 128MB = target partitions
# 100GB shuffle → 100GB / 128MB = 800 partitions
spark.conf.set("spark.sql.shuffle.partitions", "800")

# Dynamic resource allocation:
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "2")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "20")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")

# Broadcast join threshold (default 10MB — often too low):
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50mb")  # broadcast tables < 50MB

# Memory management:
# JVM heap per executor: configured at cluster level
# Fraction for execution vs storage:
spark.conf.set("spark.memory.fraction", "0.8")           # 80% for execution+storage
spark.conf.set("spark.memory.storageFraction", "0.5")    # 50% of above for cached data

# Caching strategies:
# CACHE TABLE: persistent cache, survives between cells
spark.sql("CACHE TABLE silver.orders")                   # cache in memory

# df.cache(): cache at DataFrame level (cleared when no reference)
orders_df = spark.table("silver.orders").cache()

# Databricks IO Cache (SSD cache):
# Automatically caches Parquet/Delta reads to NVMe SSD on worker nodes
# No configuration — Databricks manages it
# Benefit: 10× faster repeated reads vs remote ADLS reads
# Check: Cluster → Storage → IO Cache size used
```

---

## Interview Tips

> **Tip 1:** "How does Photon improve performance and what are its limitations?" — Photon replaces the JVM Spark SQL execution engine with a C++ vectorized engine using SIMD instructions to process 256-512 values simultaneously. This gives 3-10× improvement for SQL aggregations, hash joins, and wide table scans. Limitations: Python UDFs bypass Photon (go through Python serialization boundary), complex custom Scala transformations, and ML training operations. Best use case: SQL analytics, Delta table operations, Databricks SQL warehouses. Not effective for: heavy UDF-based transformations — use `pandas_udf` (Arrow-based) to stay partially in vectorized path.

> **Tip 2:** "What is Secure Cluster Connectivity and when is it required?" — SCC (also called "No Public IP") configures Databricks workers with no public IP. All traffic between the Databricks control plane and worker nodes goes through a secure relay (Azure Private Link). Why needed: without SCC, each worker has a public IP, which means if an attacker gets onto the worker node, they can reach the public internet. With SCC: workers are fully isolated — no inbound/outbound public internet. Required for: financial services, government, healthcare environments where network isolation is mandatory. Also simplifies NSG configuration (no inbound rules needed for worker traffic).

> **Tip 3:** "How do you estimate and control Databricks costs in production?" — Cost = VM cost (Azure) + DBU license (Databricks). Monitor via: Databricks Account Console → Usage → filter by workspace/cluster/user. Key controls: (1) Spot VMs for workers (70% VM cost reduction, use SPOT_WITH_FALLBACK_AZURE to use on-demand if spot unavailable), (2) cluster auto-termination on All-Purpose clusters (30 min idle = off), (3) cluster pools to reduce cold start (avoid cluster bloat), (4) Databricks budget alerts in Account Console, (5) Unity Catalog usage monitoring to identify high-consumption notebooks. Cost benchmark: $2–5/hour for a 5-node DS4_v2 production job cluster on Jobs tier.

## ⚡ Cheat Sheet

**Azure Databricks workspace tiers**
| Tier | Features |
|---|---|
| Standard | Notebooks, jobs, basic clusters |
| Premium | Unity Catalog, RBAC, SSO, audit logs |
| Trial | 14-day premium features |

**Unity Catalog on Azure**
- One metastore per region per tenant (Entra ID tenant)
- External location: backed by ADLS Gen2 + Azure Managed Identity
- Access connector: `dbxAccessConnector` resource → assigns managed identity to workspace
- RBAC: Entra ID groups synced via SCIM; mapped to UC groups

**Networking modes**
| Mode | Description |
|---|---|
| No isolation | Clusters on Databricks-managed VNet; simple but less secure |
| VNet injection | Clusters in customer VNet; enables private endpoints |
| Private Link | Control plane + data plane traffic through private endpoints |

**ADLS Gen2 access patterns**
```python
# Service principal (legacy, avoid)
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth2.client.id", client_id)
# Managed identity (recommended with Unity Catalog)
# No code needed — Unity Catalog handles credential via Access Connector
spark.read.parquet("abfss://container@account.dfs.core.windows.net/path")
```

**Cost optimization**
- Spot VMs (preemptible): 60–80% cheaper; enable `spot` in cluster policy for batch
- Cluster policies: enforce instance types, autoscale limits, spot usage
- Instance pools: reduce cluster start time; reduce DBU billing for frequently-created clusters
- Serverless SQL warehouses: auto-pause; per-second billing; no idle cost

**Azure Monitor integration**
- Diagnostic settings: send workspace logs to Log Analytics
- Key log tables: `DatabricksJobs`, `DatabricksClusters`, `DatabricksNotebook`
- Alert: `DatabricksJobs | where ActionName == "runFailed"` → Logic App action
