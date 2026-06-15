---
title: "Microsoft Fabric - Senior Deep Dive"
topic: azure
subtopic: microsoft-fabric
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [azure, microsoft-fabric, onelake, architecture, governance, capacity-planning, migration, delta-lake]
---

# Microsoft Fabric — Senior Deep Dive

## Strategic Architecture Decisions: When to Choose Fabric

At senior level, the key skill is knowing *when* Fabric is the right platform choice and *when* the existing Azure stack (Databricks + Synapse + ADF + Power BI Premium) is better.

### Fabric Strengths

1. **Microsoft-centric organisations**: Teams with strong Power BI, Azure SQL, and Excel culture will adapt faster to Fabric's T-SQL warehouse and Direct Lake than to Databricks.
2. **Simplified operations**: No cluster management, no infra provisioning, single billing SKU. Good for organisations without dedicated DevOps/MLOps teams.
3. **Direct Lake**: Eliminates the Power BI import/DirectQuery choice — significant advantage for BI-heavy organisations with large datasets.
4. **Shortcuts for data sharing**: Eliminates data copies between teams; enables data mesh patterns without complex replication pipelines.

### Fabric Weaknesses (vs. Databricks)

| Dimension | Fabric | Databricks |
|-----------|--------|-----------|
| Spark maturity | Managed Spark, limited config | Full Spark control, Photon engine |
| ML/MLOps | MLflow integration (early) | MLflow native, Unity Catalog for models |
| CI/CD | Git integration (improving) | Databricks Asset Bundles, mature DBX tooling |
| Multi-cloud | Azure only | AWS, Azure, GCP |
| Open ecosystem | Delta Lake (open), but Fabric vendor lock-in | Full open source stack |
| Streaming | Eventstream (no-code) + KQL | Structured Streaming (code-first, flexible) |

---

## OneLake Architecture Deep Dive

### OneLake Storage Internals

OneLake is built on Azure Data Lake Storage Gen2. It is not a new storage system — it's a managed layer on top of ADLS Gen2, with:
- **Hierarchical namespace**: Workspace → Item → Files/Tables directory structure
- **Delta Lake as default**: All lakehouse tables are Delta Parquet
- **ABFS access**: `abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<item-id>/Tables/`

This means external tools (Databricks, Azure Synapse, custom Python) can read Fabric OneLake data via ABFS if granted OneLake workspace reader access.

### Reading OneLake from External Spark

```python
# Access Fabric OneLake data from an external Databricks workspace
# Requires: OneLake workspace Member or Contributor access for the service principal

storage_options = {
    "accountName": "onelake",
    "tenantId": "<tenant_id>",
    "clientId": "<service_principal_client_id>",
    "clientSecret": "<service_principal_secret>",
}

# OneLake ABFS path format:
# abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com/<lakehouse-id>/Tables/gold_orders
onelake_path = (
    "abfss://<workspace-id>@onelake.dfs.fabric.microsoft.com"
    "/<lakehouse-id>/Tables/gold_orders"
)

df = spark.read.format("delta").load(onelake_path)
```

This hybrid architecture — Fabric for BI and governance, Databricks for ML and complex Spark — is a common enterprise pattern in 2024/2025.

---

## Capacity Planning for Fabric

### Understanding Capacity Unit (CU) Consumption

Different Fabric workloads consume CUs differently:

```
CU Consumption Rates (approximate):
┌────────────────────────────┬────────────────────────────────────────┐
│ Workload                   │ CU Consumption Model                   │
├────────────────────────────┼────────────────────────────────────────┤
│ Fabric Notebook (Spark)    │ Burst during computation; 0 idle       │
│ Fabric Pipeline (Copy)     │ Per-activity execution                 │
│ Fabric Warehouse (queries) │ Per-query CU-seconds                   │
│ KQL Database (ingestion)   │ Continuous ingestion CUs               │
│ Power BI (Direct Lake)     │ Per-query (low; reads OneLake directly)│
│ Power BI (Scheduled refresh│ Burst during refresh window            │
└────────────────────────────┴────────────────────────────────────────┘
```

### Smoothing CU Consumption: Smoothing Algorithm

Fabric uses a **smoothing algorithm** for bursty workloads (especially notebooks and pipelines). Burst CU consumption is smoothed over a 24-hour window — if you burst to 100 CUs for 10 minutes but your average over 24 hours is within your F16 (16 CU) limit, you are not throttled.

This means an F16 capacity can support a nightly Spark batch that temporarily uses 100+ CUs, as long as it doesn't run all day.

### Throttling vs. Rejection

| CU Usage | Behavior |
|----------|----------|
| < 100% capacity | Normal operations |
| 100–200% capacity | **Interactive throttling**: 20-second delays on interactive queries |
| > 200% capacity | **Background throttling**: Background jobs delayed 5–24 hours |
| Sustained > 100% | **Rejection**: Future job submissions rejected |

**Monitoring CU consumption:**

```
Fabric Admin Portal → Capacity Metrics App
- CU consumption per workspace and item
- Throttling events
- Burst usage patterns
```

---

## Governance with Microsoft Purview + Fabric

Fabric integrates natively with **Microsoft Purview** for data governance:

### Data Lineage in Fabric

Fabric automatically captures lineage between:
- Data sources (ADLS, SQL, REST APIs)
- Fabric items (Lakehouses, Pipelines, Notebooks, Warehouses)
- Power BI datasets and reports

In Purview, you can trace: "This Power BI report reads from this Fabric Lakehouse gold table, which was created by this Notebook, which reads from this bronze table ingested by this Pipeline from this SQL Database."

### OneLake Data Hub

The **OneLake Data Hub** (accessible from any Fabric workspace) shows all discoverable data across the tenant:

```
OneLake Data Hub
├── Browse datasets endorsed by the data team
├── Search tables and reports by business term
├── View data owner, sensitivity label, and quality score
└── Create shortcuts to shared datasets in one click
```

### Sensitivity Labels and Data Classification

Fabric inherits Microsoft Information Protection (MIP) sensitivity labels:

```
Label Hierarchy:
Public → Internal → Confidential → Highly Confidential

Applied to:
- Lakehouses (labels cascade to tables)
- Warehouses
- Reports
- Dataflows
```

When a sensitivity label is applied to a lakehouse table, Power BI reports consuming that table inherit the label automatically.

---

## Fabric Git Integration and CI/CD

Fabric provides **native Git integration** (GitHub and Azure DevOps). Each workspace can be connected to a branch, and item definitions are serialised to JSON/YAML in the repository.

### What Is Serialised to Git

```
fabric-workspace/ (Git repository)
├── .platform/
├── bronze_lakehouse.Lakehouse/
│   └── item.metadata.json     # Lakehouse metadata
├── transform_orders.Notebook/
│   └── notebook-content.py    # Notebook code (Python)
├── ingest_pipeline.DataPipeline/
│   └── pipeline-content.json  # Pipeline definition JSON
└── sales_warehouse.Warehouse/
    └── item.metadata.json     # Warehouse metadata
```

> **Current limitation (as of 2024)**: Lakehouse table data is NOT serialised to Git — only metadata and code. Delta table data lives in OneLake and is environment-specific. CI/CD focuses on code and configuration, not data.

### Fabric Deployment Pipelines

Fabric has **native Deployment Pipelines** (different from Azure DevOps Pipelines) — a built-in promotion mechanism:

```
Development Workspace  →  Test Workspace  →  Production Workspace
        │                       │                      │
   Publish changes          Review + approve      Auto-deploy
   (Git push to dev)        (manual gate)         (or auto-approve)
```

Fabric Deployment Pipelines diff items between stages and can selectively deploy changed items. No Azure DevOps required for basic promotion flows.

For complex scenarios (dbt + Fabric + ADF), combine:
- Fabric Deployment Pipelines for Fabric items
- Azure DevOps Pipelines for dbt and external tooling

---

## Advanced: Fabric for Data Mesh

Fabric's architecture naturally supports **data mesh** principles:

### Domain Ownership via Workspaces

```
Data Mesh in Fabric:
│
├── Finance Domain Workspace
│   ├── Owns: finance_gold_lakehouse
│   └── Publishes: Shortcuts + OneLake Data Hub endorsement
│
├── Marketing Domain Workspace
│   ├── Owns: marketing_gold_lakehouse
│   └── Consumes: shortcut to finance.customer_lifetime_value
│
└── Platform Team Workspace
    ├── Owns: shared_dim_date, shared_dim_geography
    ├── Manages: Capacity allocation (F SKUs per domain)
    └── Enforces: Purview governance policies
```

### Self-Serve Data Access via Shortcuts

```python
# Finance team creates a shortcut to expose their gold data
# Marketing team creates a shortcut TO that data — no ADF pipeline needed

# In Marketing Lakehouse, Shortcut creation (via Fabric REST API):
import requests

response = requests.post(
    "https://api.fabric.microsoft.com/v1/workspaces/{marketing_workspace_id}"
    "/lakehouses/{marketing_lakehouse_id}/tables",
    headers={"Authorization": f"Bearer {token}"},
    json={
        "path": "Tables/customer_lifetime_value",
        "shortcut": {
            "path": "Tables/customer_lifetime_value",
            "target": {
                "type": "OneLake",
                "oneLake": {
                    "workspaceId": "{finance_workspace_id}",
                    "itemId": "{finance_gold_lakehouse_id}"
                }
            }
        }
    }
)
```

---

## Key Senior Interview Questions

**"How does Direct Lake differ from Import and DirectQuery, and when would you choose each?"**

Import: copies data into VertiPaq memory — fastest queries but stale data (refresh lag) and size limits. DirectQuery: queries source on every report interaction — always fresh but slow. Direct Lake: reads Delta Parquet from OneLake into VertiPaq columns on-demand — fresh + fast. Choose Direct Lake when data is in Fabric OneLake; it's almost always superior to DirectQuery and has no Import size limits.

**"What happens when Fabric capacity is exceeded? How do you design for it?"**

Fabric uses CU smoothing over 24 hours. Exceeding 100% triggers interactive throttling (20s delays), sustained excess triggers background throttling (5-24h delays) and rejections. Design: monitor Capacity Metrics App, schedule heavy Spark jobs during off-peak hours to leverage smoothing, and right-size F SKU based on observed peak CU patterns. Use workspace-level CU limits to prevent one team's runaway job from starving others.

**"How would you architect a migration from Azure Synapse Analytics + Power BI Premium to Microsoft Fabric?"**

Phase 1: Enable Fabric on existing Power BI Premium capacity (P SKUs map to F SKUs). Existing Power BI reports continue working. Phase 2: Create Fabric Workspaces and Lakehouses. Set up Shortcuts from existing ADLS Gen2 data. Validate Direct Lake for Power BI datasets. Phase 3: Migrate Synapse Spark jobs to Fabric Notebooks. Migrate Synapse SQL DW to Fabric Warehouse. Phase 4: Migrate ADF pipelines to Fabric Pipelines. Phase 5: Decommission Synapse and legacy ADF. Timeline: typically 3–9 months depending on complexity.
