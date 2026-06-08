---
title: "Azure Data Factory — Senior Deep Dive"
topic: azure
subtopic: azure-data-factory
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [azure, adf, performance-tuning, ir, managed-vnet, dataflows, cdc]
---

# Azure Data Factory — Senior Deep Dive

## Integration Runtime Architecture Deep Dive

```
Azure IR vs Self-Hosted IR vs Azure-SSIS IR

Azure Managed IR:
  Runs in Microsoft-managed infrastructure
  Autoscales DIUs from 2 to 256
  Region selection: auto (nearest) or specify (data residency compliance)
  Managed VNet option: IR runs inside an isolated VNet
    → private endpoint to Azure SQL, ADLS, etc.
    → no public internet exposure
    → required for regulated industries (healthcare, finance)
  Limitations: cannot reach on-prem or private IP resources

Self-Hosted IR (SHIR):
  Windows/.NET application installed on customer-managed VM
  Communicates outbound to Azure Service Bus (port 443) — no inbound firewall rules needed
  High availability: install SHIR on 2+ nodes (auto-failover)
  Credential encryption: secrets encrypted with node's machine key
  Max nodes: 4 per SHIR (scale-out for concurrent copy jobs)
  Proxy for cloud activities: SHIR can proxy to Azure IR for cloud resources
  
  SHIR node sizing:
    Minimum: 4 CPU, 8GB RAM
    Recommended: 8+ CPU, 16GB RAM for parallel copy activities
    Heavy workloads: 32 CPU, 128GB RAM (large on-prem SQL export)

Azure-SSIS IR:
  Full SQL Server Integration Services runtime in Azure
  Run existing SSIS packages (.dtsx) without code changes
  Node tiers: Standard (D-series), Enterprise (higher memory), Azure SQL SSIS Store
  Pricing: per node-hour (expensive — typically 2–8 nodes running 24/7)
  Optimization: use "Start/Stop SSIS IR" activity in pipeline (only run when needed)
```

---

## Copy Activity: DIU Tuning and Parallelism

```python
# Copy Activity performance optimization

# DIU (Data Integration Unit) = unit of Azure IR compute power
# 1 DIU = 1 vCPU + proportional memory/network
# Min: 2, Max: 256, Default: auto (ADF decides based on data size)

# Throughput estimates:
# Azure Blob → Azure Blob:  2 DIUs = ~50 MB/s, 32 DIUs = ~500 MB/s
# On-prem SQL → ADLS:      SHIR-limited (SHIR is the bottleneck, not DIUs)
# Azure SQL → ADLS:        32 DIUs → ~600 MB/s

# Setting DIU in pipeline JSON:
{
  "name": "CopyOrdersToADLS",
  "type": "Copy",
  "typeProperties": {
    "source": { "type": "AzureSqlSource" },
    "sink": {
      "type": "AzureBlobFSSink",
      "copyBehavior": "MergeFiles"
    },
    "dataIntegrationUnits": 32,    # explicit DIU
    "parallelCopies": 8,           # parallel read partitions
    "enableStaging": true,
    "stagingSettings": {
      "linkedServiceName": { "referenceName": "AzureBlob_Staging" },
      "path": "staging-container"
    }
  }
}

# Parallel copy partitioning:
# For SQL sources — ADF can split by:
#   Physical partitions: reads each DB partition in parallel
#   Dynamic range: splits a column range into N buckets
#   Example: order_id 1–1M, parallelCopies=8 → 8 ranges of 125K

# Staging (PolyBase / COPY INTO path):
# Copy → Blob staging → Synapse COPY INTO
# 10-100× faster than row-by-row insert for Synapse
# enableStaging: true → ADF automatically uses PolyBase path

# Copy throughput monitoring:
# Pipeline run → Copy activity → Details → Data read, Data written, Throughput
# Target: throughput should approach network/storage limits (not DIU ceiling)
```

---

## Change Data Capture (CDC) in ADF

```
ADF Native CDC (preview → GA):
  Supported sources: Azure SQL DB, SQL Managed Instance, SQL Server (via SHIR)
  Uses SQL Server Change Tracking (lightweight) or CDC tables
  
  CDC pipeline design:
    1. Enable CDC on source table:
       EXEC sys.sp_cdc_enable_table @source_schema='dbo', @source_name='orders', @role_name=NULL
    
    2. ADF Copy Activity with CDC source:
       Source type: AzureSqlSource
       Additional columns: _sys_change_version, _sys_change_operation (I/U/D)
       "enablePartitionDiscovery": true
       "additionalColumns": [{"name": "_op", "value": "$$OPERATION"}]  -- I/U/D
    
    3. Sink: Delta table (upsert on primary key)
       Write mode: Upsert — merge on order_id, apply I/U/D operations

  Comparison: ADF CDC vs Debezium
    ADF CDC:   Managed, no Kafka needed, Azure-native, good for moderate volume
    Debezium:  Open source, needs Kafka cluster, high throughput, full log-based CDC
    Decision:  ADF CDC for Azure-first shops; Debezium for multi-cloud, high-volume
```

---

## Managed Virtual Network and Private Endpoints

```
Problem: default ADF IR routes data over public internet → security concern

Solution: ADF Managed Virtual Network
  ADF creates an isolated Azure VNet for the Integration Runtime
  Private Endpoints provisioned inside Managed VNet:
    → ADLS Gen2 private endpoint
    → Azure SQL private endpoint
    → Azure Key Vault private endpoint
  All data flows: ADF IR → private endpoint → resource (never leaves Azure backbone)

Setup steps:
  1. Enable Managed VNet on ADF instance (one-time, cannot disable)
  2. Create Managed Private Endpoints for each resource (portal/ARM)
  3. Resource owner approves private endpoint connection
  4. Test: pipeline run should succeed; verify via Network Watcher

Cost impact:
  Managed VNet IR is more expensive (always-on managed cluster)
  Extra ~$0.10/hour per IR even when idle
  Use interactive authoring cluster (4 vCores, 30 min TTL) during development

Private endpoint approval automation:
  Use Azure Policy to auto-approve PE connections from trusted ADF factories
  Or: ARM template with privateEndpointConnection approval in pipeline
```

---

## ADF with Databricks for Heavy Transformation

```python
# ADF as orchestrator, Databricks as compute — best of both worlds

# Pipeline design:
# 1. ADF Copy Activity: raw data → ADLS Bronze
# 2. ADF Databricks Notebook Activity: transform Bronze → Silver
# 3. ADF Databricks Notebook Activity: aggregate Silver → Gold
# 4. ADF Copy Activity: Gold → Azure SQL (serving layer)

# Databricks Notebook Activity configuration:
{
  "name": "TransformSilver",
  "type": "DatabricksNotebook",
  "linkedServiceName": { "referenceName": "AzureDatabricks_LS" },
  "typeProperties": {
    "notebookPath": "/Shared/etl/transform_silver",
    "baseParameters": {
      "run_date": "@formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd')",
      "source_path": "@concat('abfss://bronze@', variables('storageAccount'), '.dfs.core.windows.net/orders/')",
      "sink_path": "@concat('abfss://silver@', variables('storageAccount'), '.dfs.core.windows.net/orders/')"
    },
    "libraries": [
      {"pypi": {"package": "delta-spark==2.4.0"}}
    ]
  }
}

# Databricks linked service options:
#   New job cluster:    ADF creates cluster per run (cold start ~3 min, auto-terminates)
#   Existing cluster:   ADF uses pre-running cluster (no cold start, always billing)
#   Instance pool:      Pre-warmed VMs, cluster creation ~30 sec (balance)

# Best practice: instance pool for production (fast start, cost-controlled)

# Passing pipeline output to notebook:
# ADF Web Activity → call Databricks Jobs API → get run results
# Or: notebook writes status to Azure SQL → ADF Lookup reads it
```

---

## Interview Tips

> **Tip 1:** "ADF pipeline runs are taking 8 hours daily — how do you optimize?" — Diagnose first: check Copy Activity Details for throughput numbers. If throughput is low: (a) increase DIUs for cloud-to-cloud copies (try 32 DIUs); (b) check SHIR node CPU/memory for on-prem sources (add SHIR nodes); (c) enable staging (PolyBase path) for Synapse sinks; (d) split large table copies with partition range parallelism. If Data Flow is slow: check cluster size (increase to Memory Optimized for wide transformations), enable broadcast joins for small reference tables, check shuffle partitions (`set spark.sql.shuffle.partitions = 200`).

> **Tip 2:** "How do you handle secrets in ADF at enterprise scale?" — All credentials (connection strings, passwords, keys) go in Azure Key Vault, never hardcoded in Linked Service definitions. ADF Linked Services reference Key Vault secrets via `@Microsoft.KeyVault(SecretUri=...)` syntax. ADF managed identity (System Assigned) is granted "Key Vault Secrets User" RBAC role — no client secret needed. For multiple environments (dev/staging/prod), use separate Key Vaults per environment and parameterize the Key Vault URL in the Linked Service configuration using ADF global parameters.

> **Tip 3:** "When would you use ADF Mapping Data Flows vs Azure Databricks?" — Use Mapping Data Flows when: (a) team has limited Spark/Python skills (visual code-free development), (b) transformations are straightforward (join, filter, aggregate), (c) you need built-in connectors without writing code. Use Databricks when: (a) complex transformations requiring custom Python/Scala logic, (b) ML feature engineering, (c) data volumes > 100GB/run (Databricks has better cluster tuning), (d) already have Databricks for other workloads. Many production architectures use both: ADF for orchestration + Databricks for heavy compute.
