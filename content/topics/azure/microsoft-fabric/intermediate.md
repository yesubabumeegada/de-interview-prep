---
title: "Microsoft Fabric - Intermediate"
topic: azure
subtopic: microsoft-fabric
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [azure, microsoft-fabric, shortcuts, direct-lake, medallion, delta-lake, real-time, copilot]
---

# Microsoft Fabric — Intermediate

## Shortcuts: Cross-Workspace and External Data Access

**Shortcuts** are one of Fabric's most powerful features. A Shortcut is a pointer to data stored elsewhere — in another Fabric workspace, ADLS Gen2, AWS S3, or Google Cloud Storage — that appears as a native table in your lakehouse **without copying data**.

### Shortcut Types

| Shortcut Target | Use Case |
|----------------|----------|
| Another Fabric OneLake item | Cross-workspace data sharing (no data duplication) |
| Azure Data Lake Storage Gen2 | Reference existing ADLS data lake |
| Amazon S3 | Multi-cloud data lake access |
| Google Cloud Storage | GCS bucket access |
| OneLake external table (S3-compatible) | Access S3-compatible storage |

### Creating a Shortcut (ADLS Gen2 → Fabric Lakehouse)

In Fabric UI: Lakehouse → Files or Tables → New shortcut → Azure Data Lake Storage Gen2 → provide ADLS URL and credential.

Once created, the shortcut appears as a native folder/table in the lakehouse:

```python
# Access a shortcut in a Fabric Notebook — transparent to Spark
df = spark.read.format("delta").load("Files/shortcuts/customer_master/")

# Or if the shortcut was created as a Table shortcut (Delta-formatted external data):
df = spark.table("customer_master_shortcut")
```

### Cross-Workspace Shortcut Pattern

```
Platform Team Workspace          Analytics Team Workspace
├── gold_lakehouse               ├── analytics_lakehouse
│   └── Tables/                 │   └── Tables/
│       └── dim_customers        │       └── dim_customers_shortcut
│           (master copy)         │           (→ shortcut to gold_lakehouse)
│                                └── No data copied
```

This pattern enables a **data mesh architecture** in Fabric: each domain team owns their gold lakehouse, and consuming teams create shortcuts to reference canonical datasets without ownership confusion or data copying costs.

---

## Direct Lake Mode for Power BI

**Direct Lake** is Fabric's breakthrough Power BI connectivity mode. It is the key reason Fabric is strategically important for Microsoft.

### Power BI Connectivity Modes Comparison

| Mode | How Data is Accessed | Latency | Scale |
|------|---------------------|---------|-------|
| Import | Data copied into Power BI dataset | Fast (in-memory) | Limited by dataset size |
| DirectQuery | Query runs against source on demand | Slow (network + DB) | Unlimited scale |
| **Direct Lake** | Reads Delta Parquet from OneLake directly into VertiPaq | **Fast + large scale** | V-Order optimised |

### How Direct Lake Works

```
Power BI Report Request
        │
        ▼
Power BI / Fabric VertiPaq engine
        │
        ├── Column statistics cached (metadata only)
        │
        └── Data read directly from OneLake Delta Parquet files
            (no import copy, no DirectQuery round-trip)
```

Direct Lake achieves **Import-like performance at DirectQuery-like freshness** by reading Delta Parquet files directly from OneLake using VertiPaq's columnar engine. When data changes in the lakehouse, Power BI queries the updated files immediately — no scheduled refresh needed.

### V-Order Optimisation

Microsoft adds **V-Order** — a proprietary write-time compression optimisation applied on top of Parquet — when Delta tables are written by Fabric workloads (notebooks, pipelines, warehouse). V-Order dramatically improves Direct Lake read performance.

```python
# Fabric notebooks apply V-Order automatically
# Explicitly enable when writing from Spark:
spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.microsoft.delta.optimizeWrite.vorder.enabled", "true")

df.write.format("delta").saveAsTable("sales_fact")
# This table is V-Order optimised for Direct Lake
```

### Fallback to DirectQuery

Direct Lake automatically falls back to DirectQuery mode when:
- A DAX query cannot be served from cached VertiPaq data
- The lakehouse table exceeds the capacity guardrail limits
- Dynamic row-level security is applied

Monitor fallback rate in Power BI Performance Analyzer — high fallback rates indicate performance issues.

---

## Medallion Architecture in Fabric

Fabric's lakehouse is ideal for implementing the **Bronze → Silver → Gold** medallion pattern:

```
Bronze Layer (Raw)          Silver Layer (Cleaned)        Gold Layer (Business)
├── Lakehouse: bronze       ├── Lakehouse: silver         ├── Lakehouse: gold
│   └── Tables/             │   └── Tables/               │   └── Tables/
│       ├── raw_orders      │       ├── orders (cleansed) │       ├── fact_sales
│       ├── raw_customers   │       ├── customers (deduped)│       ├── dim_customers
│       └── raw_products    │       └── products (enriched)│       └── agg_revenue_daily
│                           │                             │
│   Source: ADF pipeline    │   Source: Fabric Notebooks  │   Source: Fabric Notebooks
│   (Copy activity → Files) │   (Spark transformations)   │   (Business logic + joins)
│                           │                             │
│   All raw data preserved  │   DQ checks applied         │   Power BI Direct Lake
│   (no transformations)    │   (nulls removed, types cast)│   reports here
```

### Implementing Bronze Ingestion with Fabric Pipeline

```
Fabric Pipeline: ingest_orders
├── Activity: Copy Data
│   Source: REST API / SFTP / Azure SQL
│   Destination: Bronze Lakehouse → Files/raw_orders/YYYY/MM/DD/
│   Format: Parquet (preserve source schema)
│
└── Activity: Notebook
    Notebook: bronze_append_to_delta
    Parameters: { "date": "@pipeline().parameters.runDate" }
```

```python
# bronze_append_to_delta notebook
from pyspark.sql.functions import current_timestamp, lit
import sys

date = getArgument("date", "2024-01-01")   # Fabric notebook parameter

# Read raw parquet files landed by Copy activity
raw_df = spark.read.parquet(f"Files/raw_orders/{date.replace('-', '/')}/*.parquet")

# Add metadata columns
bronze_df = raw_df.withColumn("_ingested_at", current_timestamp()) \
                  .withColumn("_source_date", lit(date))

# Append to bronze Delta table
bronze_df.write.format("delta").mode("append").saveAsTable("bronze_orders")

print(f"Ingested {bronze_df.count()} rows for date {date}")
```

---

## Fabric Copilot

**Copilot in Fabric** is AI assistance integrated across all Fabric workloads (generally available as of 2024, requires F64+ capacity or trial).

### Copilot Capabilities by Workload

| Workload | Copilot Feature |
|----------|----------------|
| Data Engineering (Notebook) | Generate PySpark code from natural language; explain existing code |
| Data Factory (Pipeline) | Natural language pipeline creation; generate dataflow transforms |
| Data Warehouse | T-SQL generation from natural language; query optimisation suggestions |
| Power BI | Generate reports from natural language; explain visuals; suggest measures |
| Data Science | Generate ML code; explain model outputs |

### Notebook Copilot Example

In a Fabric Notebook, you can open the Copilot pane and type:

```
"Read the bronze_orders table, remove rows where Amount is null or negative,
cast OrderDate to date type, and deduplicate by OrderId keeping the latest row."
```

Copilot generates:

```python
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read bronze table
df = spark.table("bronze_orders")

# Remove null/negative amounts
df_clean = df.filter(
    F.col("Amount").isNotNull() & (F.col("Amount") > 0)
)

# Cast OrderDate to DateType
df_typed = df_clean.withColumn("OrderDate", F.to_date(F.col("OrderDate")))

# Deduplicate by OrderId, keeping latest row
window = Window.partitionBy("OrderId").orderBy(F.desc("_ingested_at"))
df_deduped = (
    df_typed
    .withColumn("rn", F.row_number().over(window))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

display(df_deduped)
```

> **Interview note**: Copilot requires F64+ capacity because it calls Azure OpenAI APIs. Smaller F SKUs (F2–F32) do not include Copilot.

---

## Real-Time Intelligence (KQL Database + Eventstream)

Fabric's **Real-Time Intelligence** workload handles streaming data:

### Eventstream

An **Eventstream** is a managed, no-code streaming pipeline:

```
External Sources → Eventstream → Destinations
├── Azure Event Hubs              ├── KQL Database (for analytics)
├── Azure IoT Hub                 ├── Lakehouse (Delta table)
├── Kafka                         ├── Custom App (via Event Hub)
└── Custom App                    └── Derived Eventstream (branching)
```

### KQL Database

A **KQL Database** (formerly Azure Data Explorer) stores and queries streaming/time-series data using KQL:

```kql
// Query last hour of IoT sensor data in Fabric KQL Database
SensorReadings
| where Timestamp > ago(1h)
| where SensorType == "temperature"
| summarize AvgTemp = avg(Value), MaxTemp = max(Value) by bin(Timestamp, 5m), DeviceId
| order by Timestamp desc
| render timechart
```

### When to Use KQL Database vs. Lakehouse

| | KQL Database | Lakehouse (Delta) |
|--|-------------|-------------------|
| Ingestion | Streaming (sub-second) | Micro-batch (minutes) |
| Query | Sub-second on time-series | Minutes for large scans |
| Retention | Auto-tiering (hot/cold) | Manual management |
| Best for | Real-time dashboards, IoT, log analytics | Historical batch analytics |

---

## Fabric Environments

A **Fabric Environment** is a configurable Spark execution environment attached to notebooks and Spark jobs:

```python
# Create Environment in Fabric UI or via REST API
# Set:
# - Spark version: 3.4 / 3.5
# - Compute: Starter Pool (auto) or Custom Pool (specific node types)
# - Libraries: pip packages (requirements.txt) or custom wheels
# - Spark config: custom spark.conf entries

# Example custom library in Environment:
# requirements.txt:
# great-expectations==0.18.0
# dbt-spark==1.7.0
```

Environments enable reproducible, versioned Spark configurations across notebooks — similar to Docker images for notebook runtimes.
