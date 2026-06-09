---
title: "Dataflows — Fundamentals"
topic: power-bi
subtopic: dataflows
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [power-bi, dataflows, interview, fundamentals]
---

# Dataflows — Fundamentals

## What Are Dataflows?

**Dataflows** are a self-service ETL (Extract, Transform, Load) capability in Power BI that runs transformation logic in the Power BI Service (cloud), separate from any individual report or dataset. They use Power Query Online — the same M language as Power Query in Desktop, but executed in the cloud.

**Key idea**: Dataflows prepare and store cleaned, transformed data that multiple datasets can connect to, instead of each dataset doing its own transformation.

```
Without Dataflows:
  Report A → Dataset A → Power Query transforms → Source
  Report B → Dataset B → Power Query transforms → Source (same transforms!)
  Report C → Dataset C → Power Query transforms → Source (duplicated again)

With Dataflows:
  Dataflow → Power Query transforms → Source (shared transformation once)
  Dataset A → Connects to Dataflow
  Dataset B → Connects to Dataflow
  Dataset C → Connects to Dataflow
```

---

## Why Use Dataflows?

| Benefit | Description |
|---|---|
| **Reusability** | One set of transformation logic, used by many datasets |
| **Consistency** | All reports use the same cleaned/transformed data |
| **Separation of concerns** | Data prep is separate from data modeling |
| **Reduced source load** | Source is queried once (by dataflow); datasets read from dataflow storage |
| **Self-service** | Business users can create dataflows without IT involvement |
| **Gateway sharing** | One gateway connection per source, shared across all datasets using the dataflow |

---

## Dataflow Gen1 vs Gen2

| Feature | Dataflow Gen1 | Dataflow Gen2 |
|---|---|---|
| Platform | Power BI Service | Microsoft Fabric |
| Storage | Azure Data Lake (CDM format) or Power BI internal | Fabric Lakehouse (Delta format) |
| Computed entities | ✅ Yes (Premium only) | ✅ Built-in |
| Linked entities | ✅ Yes (Premium only) | ✅ Built-in |
| Staging | Manual configuration | Automatic staging |
| AI features | Limited | Extended with Fabric AI |
| Direct Lake | ❌ No | ✅ Yes (Fabric) |

---

## Dataflow Entities

A **dataflow** consists of one or more **entities**. Each entity is a table produced by a Power Query transformation.

```
Dataflow: "Finance Dataflow"
├── Entity: SalesOrders       (loaded from SQL Server)
├── Entity: CustomerMaster    (loaded from SharePoint list)
├── Entity: ExchangeRates     (loaded from REST API)
└── Entity: CleanedSales      (computed from SalesOrders + ExchangeRates)
```

---

## Creating a Dataflow

### In Power BI Service (Gen1)

1. Open a Power BI workspace
2. **New** → **Dataflow**
3. Choose **Add new entities**
4. Select data source (SQL Server, SharePoint, REST API, etc.)
5. Transform data using Power Query Online
6. **Save** and then **Refresh Now**

### In Microsoft Fabric (Gen2)

1. Open a Fabric workspace
2. **New** → **Dataflow Gen2**
3. Same Power Query Online interface
4. Configure output destination (Lakehouse, Warehouse, etc.)

---

## Power Query Online

Power Query Online is the cloud version of Power Query. It uses the same M language and has the same transformations, with some differences:

| Aspect | Power BI Desktop (Power Query) | Dataflows (Power Query Online) |
|---|---|---|
| Runs on | Local machine | Cloud (Azure) |
| Data gateway | Optional (local files) | Required for on-premises sources |
| Computed entities | Not applicable | Available (Premium/Fabric) |
| Linked entities | Not applicable | Available (Premium/Fabric) |
| AI insights | Not available | Available |

---

## Computed Entities (Premium Feature)

A **computed entity** is an entity created by transforming another entity within the same dataflow. It avoids re-querying the source — the computation happens on already-loaded data.

```
Standard entities:    Query goes to source each time
                      SalesOrders ← SQL Server (queried once)

Computed entities:    Built from other entities, no additional source query
                      SalesOrdersAgg ← computed from SalesOrders (already in storage)
```

```
Dataflow with Computed Entity:
├── SalesOrders (standard)       → queries SQL Server
└── MonthlySalesSummary (computed) → transforms SalesOrders, NO extra SQL query
```

**Requirement**: Computed entities require **Power BI Premium** or **Microsoft Fabric** workspace.

---

## Linked Entities (Premium Feature)

A **linked entity** references an entity from a different dataflow in the same workspace (or the same dataflow). It allows sharing data between dataflows without reloading.

```
Foundation Dataflow:
└── DimCustomer (standard)

Analytical Dataflow:
└── DimCustomer_Linked (linked) → points to Foundation Dataflow's DimCustomer
└── FactSales_Cleaned (computed) → joins DimCustomer_Linked + raw sales
```

**Benefit**: DimCustomer is loaded once (in Foundation Dataflow) and reused by many analytical dataflows.

---

## Incremental Refresh for Dataflows

Dataflows support incremental refresh using the same `RangeStart`/`RangeEnd` parameter pattern as datasets.

```
Dataflow entity: SalesOrders
Incremental Refresh Policy:
  Archive: 3 years
  Refresh: Last 30 days
```

Dataflow incremental refresh also requires **Power BI Premium** or Fabric.

---

## Dataflow Storage Options

### Power BI Managed Storage (Default)

- Data stored in Microsoft-managed Azure Data Lake
- CDM (Common Data Model) format
- No direct access to the files
- Automatic management

### Bring Your Own Azure Data Lake (ADLS Gen2)

- Connect your own Azure Data Lake Storage Gen2 account
- Full control over the storage
- Access data files directly from Azure services
- Useful for integration with other Azure tools (Databricks, Synapse, etc.)

```
Configuration:
  Workspace Settings → Azure Connections → Azure Data Lake Storage Gen2
  → Enter storage account URL and authentication
```

---

## Dataflow vs Dataset vs Datamart

| Object | Purpose | Storage | Semantic Layer |
|---|---|---|---|
| **Dataflow** | ETL/data prep | Azure Data Lake (CDM/Delta) | No — raw/cleaned tables |
| **Dataset** | Semantic model with relationships, measures, RLS | VertiPaq (in-memory) | Yes — full Power BI model |
| **Datamart** | Self-service data warehouse | Azure SQL Database | Limited — auto-generated |

**Typical pipeline:**
```
Source → Dataflow (ETL) → Dataset (Model) → Reports
```

---

## Connecting a Dataset to a Dataflow

In Power BI Desktop:
1. **Get Data** → **Power BI dataflows**
2. Select the workspace and dataflow
3. Select the entity (table) to import
4. Apply additional transformations if needed
5. Load to the model

```powerquery
// In Power Query, the connection to a dataflow looks like:
Source = PowerBI.Dataflows(null),
Workspace = Source{[workspaceId="<guid>"]}[Data],
Dataflow = Workspace{[dataflowId="<guid>"]}[Data],
SalesOrders = Dataflow{[entity="SalesOrders"]}[Data]
```

---

## Orchestrating Dataflow Refreshes

Dataflows can be refreshed on a schedule (like datasets) or triggered via:

- **Power Automate**: Trigger dataflow refresh when a file lands in SharePoint
- **Azure Data Factory**: Trigger Power BI dataflow refresh as a pipeline activity
- **Power BI REST API**: `POST /groups/{workspaceId}/dataflows/{dataflowId}/refreshes`

**Dependency**: If Dataset A uses Dataflow A, set the dataset to refresh **after** the dataflow completes.

```
Power BI Service → Scheduled Refresh:
  Dataflow: 2:00 AM → 2:30 AM (estimated)
  Dataset: 3:00 AM (gives buffer after dataflow)
```

---

## Summary

- Dataflows centralize ETL/data prep logic, reducing duplication across datasets
- Uses **Power Query Online** (same M language as Desktop)
- **Computed entities** transform data already in storage (no re-querying source)
- **Linked entities** share entities across dataflows without reloading
- Incremental refresh and computed/linked entities require **Premium** or **Fabric**
- Gen2 dataflows store data in **Delta format** in Fabric Lakehouse
- Dataflows fit between source systems and datasets in the data pipeline
