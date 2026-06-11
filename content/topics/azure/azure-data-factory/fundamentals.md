---
title: "Azure Data Factory — Fundamentals"
topic: azure
subtopic: azure-data-factory
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [azure, adf, etl, pipelines, data-integration]
---

# Azure Data Factory — Fundamentals


## 🎯 Analogy

Think of Azure Data Factory like an orchestration hub for data movement: pipelines connect data stores (Blob, SQL, Cosmos DB, SAP), activities do the work (Copy, Databricks Notebook, Stored Procedure), and triggers schedule or event-start the whole thing.

---
## What Is Azure Data Factory?

Azure Data Factory (ADF) is Azure's **cloud-based ETL/ELT and data integration service**. It enables you to create data-driven workflows (pipelines) to orchestrate and automate data movement and data transformation.

```
Key roles ADF fills:
  Data Movement:    Copy data between 90+ source/sink connectors
  Orchestration:    Schedule and coordinate activities (like Airflow)
  Transformation:   Mapping Data Flows (Spark-based, code-free)
  Monitoring:       Built-in pipeline run history and alerts

ADF does NOT replace:
  Spark compute    → Use Azure Databricks for heavy transformations
  Stream processing → Use Azure Stream Analytics or Event Hubs
  OLAP queries     → Use Azure Synapse Analytics or dedicated SQL Pool
```

---

## Core ADF Concepts

```
1. Pipeline
   Container of activities (logical unit of work)
   Example: "Daily Sales ETL" pipeline with 4 activities

2. Activity
   A single step in a pipeline
   Types:
     Data movement:    Copy Activity (reads source → writes sink)
     Data flow:        Mapping Data Flow (transformations on Spark)
     Control flow:     If Condition, ForEach, Until, Wait, Execute Pipeline
     External:         Azure Function, Databricks Notebook, HDInsight Hive
     Stored procedure: SQL Stored Procedure on Azure SQL

3. Dataset
   Named pointer to data structure inside a Linked Service
   Example: "SalesOrdersTable" pointing to table in Azure SQL DB

4. Linked Service
   Connection string + credentials to an external system
   Example: Azure SQL DB connection, Blob Storage account, Snowflake

5. Trigger
   What starts a pipeline run
   Types:
     Schedule trigger:    Run on cron schedule (0 0 * * * = daily midnight)
     Tumbling window:     Fixed-size, sequential, non-overlapping windows
     Event trigger:       Fires on Blob created/deleted in Storage
     Manual trigger:      On-demand via portal or API
```

---

## Copy Activity (Core Feature)

```
Copy Activity = ADF's workhorse for data movement

Source → Staging (optional) → Sink

Supported connectors (90+):
  Azure:        Blob, ADLS Gen2, Azure SQL, Synapse, Cosmos DB, Event Hubs
  AWS:          S3, RDS, Redshift, DynamoDB
  GCP:          BigQuery, GCS, Cloud SQL
  Databases:    Oracle, SQL Server, MySQL, PostgreSQL, Teradata, SAP HANA
  Files:        CSV, Parquet, ORC, Avro, JSON, XML, Excel
  SaaS:         Salesforce, ServiceNow, SAP, Dynamics 365, REST APIs

Copy modes:
  Full load:        Read all records, overwrite destination
  Incremental:      Read only changed records (by watermark column)
  Change Data Capture (CDC): capture inserts/updates/deletes from source

Performance units:
  DIU (Data Integration Unit): parallelism control (2–256 DIUs)
  Higher DIUs = more parallel readers/writers = faster copy
  Cost: billed per DIU-hour
```

---

## Integration Runtime (IR)

```
IR = Compute infrastructure that ADF uses to execute activities

3 types:

1. Azure IR (default)
   Managed by Microsoft, no setup needed
   Runs in Azure region of your choosing
   Best for: Azure-to-Azure and cloud-to-cloud data movement

2. Self-Hosted IR (SHIR)
   Software installed on your on-premises or private network machine
   Bridges ADF (cloud) ↔ on-premises databases/file servers
   Best for: SQL Server on-prem, files on corporate network, private VNet

3. Azure-SSIS IR
   Lift-and-shift for existing SSIS packages
   Spins up SSIS runtime cluster in Azure
   Best for: migrating legacy SQL Server ETL (SSIS) to cloud

Setup flow for SHIR:
  1. Create Self-Hosted IR in ADF portal
  2. Download and install SHIR agent on on-prem machine
  3. Register agent with authentication key from ADF
  4. ADF can now reach on-prem systems through the SHIR
```

---

## Mapping Data Flows

```
Mapping Data Flow = visual, code-free transformation on Apache Spark

Available transformations:
  Source / Sink           Start/end of data flow
  Filter                  WHERE clause equivalent
  Select                  Column selection/renaming
  Derived Column          Add or modify columns with expressions
  Aggregate               GROUP BY + aggregate functions
  Join                    INNER/LEFT/RIGHT/FULL joins between streams
  Lookup                  Enrich with reference data (left outer join)
  Exists                  Semi-join / anti-join
  Union                   Stack multiple streams (UNION ALL)
  Pivot / Unpivot         Reshape wide↔long tables
  Sort                    ORDER BY
  Flatten                 Explode nested JSON arrays to rows
  Window                  Window functions (rank, lag, lead)
  Assert                  Data quality checks (fail on bad records)

Behind the scenes:
  Data Flow compiles to Spark code
  Runs on a Spark cluster provisioned by ADF (4-core default)
  Debug mode: spin up small cluster for iterative testing
  Time to spin up: ~2 minutes (cold start)
```

---


## ▶️ Try It Yourself

```python
# Trigger an ADF pipeline via the REST API / Python SDK
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient

cred = DefaultAzureCredential()
client = DataFactoryManagementClient(cred, subscription_id="sub-id")

# Trigger a pipeline run
run = client.pipelines.create_run(
    resource_group_name="rg-data",
    factory_name="my-adf",
    pipeline_name="orders-etl-pipeline",
    parameters={"processDate": "2024-01-15"},
)
print("Pipeline run ID:", run.run_id)

# Check run status
status = client.pipeline_runs.get(
    resource_group_name="rg-data",
    factory_name="my-adf",
    run_id=run.run_id,
)
print("Status:", status.status)
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What's the difference between a Linked Service and a Dataset?" — Linked Service = the *connection* (credentials + endpoint). Dataset = the *shape* of the data within that connection (which table, which file, which schema). One Linked Service can back many Datasets. Think of Linked Service as the database connection string, and Dataset as a specific table within that database.

> **Tip 2:** "When would you use a Self-Hosted Integration Runtime?" — Any time the data source cannot be reached from the Azure cloud network: on-premises SQL Server, private VNet databases, file servers behind a corporate firewall. SHIR acts as a secure bridge — ADF sends instructions through Azure Service Bus to the SHIR agent, and the SHIR agent executes locally and sends results back.

> **Tip 3:** "What is a Tumbling Window trigger and when do you use it?" — A Tumbling Window trigger fires at fixed-size, sequential, non-overlapping time windows with a defined start time. Unlike Schedule triggers, it guarantees no gap and no overlap in window coverage. Use it for hourly/daily incremental loads where you need exactly one pipeline run per window and automatic backfill if a run fails (it will retry that specific window).
