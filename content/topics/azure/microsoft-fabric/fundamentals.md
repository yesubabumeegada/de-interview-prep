---
title: "Microsoft Fabric - Fundamentals"
topic: azure
subtopic: microsoft-fabric
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [azure, microsoft-fabric, onelake, lakehouse, data-engineering, power-bi, saas]
---

# Microsoft Fabric — Fundamentals

## 🎯 Analogy

Think of Microsoft Fabric as an all-inclusive data resort. Instead of booking separate hotels (Azure Synapse, ADF, Power BI Premium, Databricks), you get one wristband that gets you into every venue: the data lake, the transformation studio, the warehouse, the streaming lounge, the ML lab, and the BI suite — all connected by a single unified storage layer.

---

## What Is Microsoft Fabric?

**Microsoft Fabric** is a unified, SaaS-based analytics platform announced at Microsoft Build 2023 (GA in November 2023). It integrates multiple analytics workloads into a single product with shared storage, governance, and billing.

**Key differentiators from the previous Azure analytics stack:**
- **SaaS, not PaaS**: No infrastructure to provision. Fabric is managed end-to-end by Microsoft.
- **OneLake**: All workloads share a single logical data lake — no data copying between services.
- **Unified billing**: One capacity (F SKU) covers all Fabric workloads.
- **Power BI native**: Power BI is built into Fabric, not a separate product.

---

## OneLake: The Unified Storage Foundation

**OneLake** is the single, tenant-wide logical data lake that underlies all Fabric workloads. It is conceptually similar to Google's "one storage" model (each Google product doesn't have its own storage).

```
Microsoft Fabric Tenant
└── OneLake (tenant-wide)
    ├── Workspace A (Finance Team)
    │   ├── Lakehouse: finance_lakehouse
    │   │   ├── Tables/ (Delta Parquet)
    │   │   └── Files/ (raw files)
    │   └── Warehouse: finance_warehouse
    │       └── (also stored in OneLake as Delta)
    ├── Workspace B (Marketing Team)
    │   └── Lakehouse: marketing_lakehouse
    └── Workspace C (Platform Team)
        └── (shortcuts to external data)
```

### OneLake Key Properties

| Property | Detail |
|----------|--------|
| Storage format | Delta Parquet (open format) |
| Location | Per-region, per-tenant (cannot span regions on one workspace) |
| Access | ABFS (Azure Blob File System) compatible — existing tools work |
| Hierarchy | Tenant → Workspace → Item (Lakehouse/Warehouse) → Tables or Files |
| External access | Data can be accessed via ABFS endpoint from Spark, Power BI, etc. |
| Shortcuts | Reference external data (ADLS Gen2, S3, GCS) without copying |

---

## Fabric Workloads

Microsoft Fabric organises capabilities into **workloads** (formerly called "experiences"):

```
Microsoft Fabric Workloads:
┌──────────────────┬──────────────────────────────────────────────────┐
│ Data Engineering │ Spark notebooks, Spark jobs, lakehouses           │
├──────────────────┼──────────────────────────────────────────────────┤
│ Data Factory     │ Pipelines (ADF-compatible), Dataflows Gen2        │
├──────────────────┼──────────────────────────────────────────────────┤
│ Data Warehouse   │ Serverless SQL warehouse (T-SQL, MPP)             │
├──────────────────┼──────────────────────────────────────────────────┤
│ Real-Time Intel  │ Eventstream, KQL Database, Real-Time Dashboard    │
├──────────────────┼──────────────────────────────────────────────────┤
│ Data Science     │ Notebooks, ML models, experiments (MLflow)        │
├──────────────────┼──────────────────────────────────────────────────┤
│ Power BI         │ Reports, datasets, dashboards — native in Fabric  │
└──────────────────┴──────────────────────────────────────────────────┘
```

Each workload creates **items** within a workspace:
- Data Engineering item types: Lakehouse, Notebook, Spark Job Definition, Environment
- Data Factory item types: Pipeline, Dataflow Gen2
- Data Warehouse item types: Warehouse
- Real-Time Intelligence: Eventstream, KQL Database, KQL Queryset

---

## Lakehouse vs. Warehouse in Fabric

This is one of the most common Fabric interview questions.

| | Lakehouse | Warehouse |
|--|-----------|-----------|
| Storage | Delta Parquet on OneLake | Delta Parquet on OneLake |
| Query engine | Spark (notebooks) + Lakehouse SQL endpoint (serverless) | Dedicated T-SQL engine (MPP) |
| Language | PySpark, Scala, SparkSQL, T-SQL (via endpoint) | T-SQL only |
| Schema enforcement | Schema-on-read by default; Delta enforces schema | Schema-on-write (DDL required) |
| Best for | Data engineering, large-scale transformations, ML | BI reporting, structured analytics, SQL-native teams |
| Concurrency | Spark pools: limited concurrent users | Higher concurrency for BI workloads |
| Transactions | Delta ACID on Spark | Full ACID via T-SQL |

> **Key insight**: Both lakehouses and warehouses ultimately store data as Delta Parquet in OneLake. A lakehouse table IS a warehouse-queryable table (via cross-item shortcuts or unified SQL endpoint). Data doesn't need to be copied.

---

## Fabric Notebooks

**Fabric Notebooks** are Spark-based interactive notebooks (similar to Databricks notebooks) within the Fabric workspace.

### Key Features

- **Default Spark runtime**: Fabric-managed Spark (no cluster config needed for basic use)
- **Delta Lake first-class**: Read/write Delta tables in the lakehouse directly
- **Multi-language**: Python, Scala, Spark SQL, R
- **Native lakehouse attachment**: Attach a lakehouse to a notebook; its tables are directly accessible as `spark.table("TableName")`

### Basic Notebook Usage

```python
# In a Fabric Notebook attached to a Lakehouse

# Read a file from the lakehouse Files section
df = spark.read.csv(
    "Files/raw/orders/2024/*.csv",
    header=True,
    inferSchema=True
)

# Write as a Delta table (creates lakehouse table)
df.write.format("delta").saveAsTable("bronze_orders")

# Read the table back
orders = spark.table("bronze_orders")
print(f"Row count: {orders.count()}")

# Merge (upsert) pattern using Delta
from delta.tables import DeltaTable

delta_table = DeltaTable.forName(spark, "silver_orders")
delta_table.alias("target").merge(
    source=orders.alias("source"),
    condition="target.OrderId = source.OrderId"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

---

## Fabric Pipelines

**Fabric Pipelines** are orchestration pipelines based on the same engine as Azure Data Factory. If you know ADF, you know Fabric Pipelines — the activity types, triggers, and expressions are identical.

Key difference from ADF:
- Fabric Pipelines are **workspace items** — no separate ADF instance to manage.
- No separate billing for Fabric Pipelines — included in Fabric capacity.
- Fabric-native activities: "Notebook" activity runs a Fabric notebook; "Copy data" copies to/from OneLake.

```
ADF Pipeline knowledge transfers directly to Fabric Pipelines:
✅ Copy Data activity
✅ Notebook activity
✅ Web activity (call REST APIs)
✅ If Condition, For Each, Until loops
✅ Schedule and event-based triggers
✅ Parameters and expressions (@pipeline().parameters.x)
```

---

## Licensing: Fabric Capacities (F SKUs)

Fabric uses a **capacity-based model** with F SKUs (Fabric capacity units):

| SKU | Capacity Units (CUs) | Rough Monthly Cost | Suitable For |
|-----|---------------------|-------------------|--------------|
| F2  | 2 CU | ~$260/month | Development/testing |
| F4  | 4 CU | ~$520/month | Small teams |
| F8  | 8 CU | ~$1,040/month | Small-medium org |
| F16 | 16 CU | ~$2,080/month | Medium org |
| F64 | 64 CU | ~$8,320/month | Large org |
| F128| 128 CU | ~$16,640/month | Enterprise |

**Important billing notes:**
- F capacity is shared across ALL Fabric workloads (notebook Spark jobs, pipeline runs, warehouse queries, Power BI refreshes).
- Burst consumption is possible — workloads exceeding reserved CUs are throttled, not billed extra.
- **Power BI Premium P SKUs** can be upgraded to Fabric F SKUs (P1 → F8 equivalent).

---

## Key Fundamentals Interview Topics

1. **What is OneLake?** — Single tenant-wide data lake (Delta Parquet) shared by all Fabric workloads. No data copying between services.

2. **What is the difference between a Lakehouse and a Warehouse in Fabric?** — Same storage, different query engines. Lakehouse = Spark + serverless SQL endpoint. Warehouse = dedicated T-SQL MPP engine. Choose based on workload and team SQL fluency.

3. **How does Fabric relate to ADF and Synapse?** — Fabric absorbs ADF (as Fabric Pipelines), Synapse Spark (as Fabric Notebooks/Spark), and Synapse SQL (as Fabric Warehouse). It's the next generation unified platform.

4. **What are F SKUs?** — Capacity units that power all Fabric workloads on a shared billing model.

5. **What is a Shortcut in Fabric?** — A reference to external data (ADLS, S3, GCS) that appears as a lakehouse table without copying data.
