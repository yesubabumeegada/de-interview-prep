---
title: "Azure Data Factory — Real World"
topic: azure
subtopic: azure-data-factory
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [azure, adf, production, etl, incremental-load, monitoring]
---

# Azure Data Factory — Real World

## Pattern 1: Production Medallion Pipeline with ADF + ADLS

```
Architecture: On-Prem SQL → ADF → ADLS Bronze → Silver → Gold → Synapse

Pipeline 1: ingest_to_bronze (hourly, triggered by Tumbling Window)
  Activity 1: LookupWatermark
    - Query: SELECT last_loaded FROM control.watermark WHERE source = 'orders'
  Activity 2: CopyToBronze
    - Source: On-Prem SQL Server (via SHIR), query with watermark filter
    - Sink: ADLS Gen2 Bronze container
    - Format: Parquet + Snappy
    - Path: bronze/orders/year={year}/month={month}/day={day}/
    - DIUs: auto, parallelCopies: 4
  Activity 3: UpdateWatermark
    - Stored Procedure: usp_set_watermark('orders', @{pipeline().TriggerTime})
  Activity 4 (on failure): SendAlert
    - Web Activity → Logic App → Teams notification

Pipeline 2: transform_to_silver (daily, 2 AM schedule)
  Activity: DatabricksNotebook
    - Notebook: /etl/silver/orders_transform
    - Parameters: run_date = @{formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd')}
    - Cluster: instance pool (3-node Standard_DS3_v2)

Pipeline 3: aggregate_to_gold (daily, 4 AM — after silver completes)
  Activity: DatabricksNotebook
    - Notebook: /etl/gold/revenue_aggregates
    - Parameters: run_date = @{formatDateTime(pipeline().TriggerTime, 'yyyy-MM-dd')}

Pipeline 4: load_to_synapse (daily, 6 AM — after gold completes)
  Activity: CopyToSynapse
    - Source: ADLS Gold Parquet
    - Sink: Azure Synapse Dedicated SQL Pool
    - Copy method: PolyBase via staging (enableStaging: true)
    - Throughput: ~800 MB/s

Dependency chain: bronze_done → silver_done → gold_done → synapse_done
Implemented via: Execute Pipeline with "Wait on completion" checked
```

---

## Pattern 2: REST API Ingestion Pipeline

```python
# ADF Copy Activity supports REST API as source
# Use case: ingest data from SaaS APIs (Salesforce, HubSpot, custom APIs)

# Linked Service for REST API:
{
  "name": "RestApi_Salesforce",
  "type": "RestService",
  "typeProperties": {
    "url": "https://api.salesforce.com",
    "authenticationType": "OAuth2ClientCredential",
    "clientId": "@Microsoft.KeyVault(SecretUri=https://kv.vault.azure.net/secrets/sf-client-id/)",
    "clientSecret": "@Microsoft.KeyVault(SecretUri=https://kv.vault.azure.net/secrets/sf-client-secret/)",
    "tokenEndpoint": "https://login.salesforce.com/services/oauth2/token"
  }
}

# Dataset for REST source:
{
  "name": "Salesforce_Accounts",
  "type": "RestResource",
  "typeProperties": {
    "relativeUrl": "/services/data/v58.0/query",
    "requestMethod": "GET",
    "additionalHeaders": { "Content-Type": "application/json" },
    "paginationRules": {
      "AbsoluteUrl": "$.nextRecordsUrl"  # follow Salesforce pagination
    }
  }
}

# For large APIs: add ForEach over date ranges
# Day 1: GET /orders?start=2024-01-01&end=2024-01-01
# Day 2: GET /orders?start=2024-01-02&end=2024-01-02
# ...
# ForEach: iterate date list, each iteration calls child pipeline with date parameter

# Rate limiting handling:
# ADF retry policy: retry 3 times with 60-second interval
# For APIs with strict rate limits: add Wait activity between pages
```

---

## Pattern 3: ADF Monitoring with Azure Monitor + Log Analytics

```python
# Enable ADF diagnostic logging → send to Log Analytics Workspace

# In ADF: Diagnostic Settings → Send to Log Analytics Workspace
# Log categories:
#   PipelineRuns    → pipeline-level start/end/status
#   ActivityRuns    → activity-level start/end/status + metrics
#   TriggerRuns     → trigger fire events
#   SandboxActivityRuns → Data Flow debug sessions

# KQL queries for operational monitoring:

# Query 1: Failed pipelines in last 24 hours
ADFPipelineRun
| where TimeGenerated > ago(24h)
| where Status == "Failed"
| project PipelineName, Status, FailureType, ErrorMessage, Start, End
| order by Start desc

# Query 2: Copy activity throughput over time
ADFActivityRun
| where ActivityType == "Copy"
| where TimeGenerated > ago(7d)
| extend throughput = toreal(Output.throughput)  -- MB/s
| summarize avg_throughput = avg(throughput) by bin(TimeGenerated, 1h), ActivityName
| render timechart

# Query 3: Long-running pipelines alert
ADFPipelineRun
| where Status == "Succeeded"
| extend duration_min = (End - Start) / 1m
| where duration_min > 120  -- alert if >2 hours
| project PipelineName, duration_min, Start

# Azure Monitor Alert (ARM template):
{
  "criteria": {
    "metricName": "PipelineFailedRuns",
    "threshold": 0,
    "operator": "GreaterThan",
    "aggregation": "Total"
  },
  "actions": [{ "actionGroupId": "/subscriptions/.../actionGroups/DataTeamAlerts" }]
}
```

---

## Interview Tips

> **Tip 1:** "A Copy Activity that normally runs in 30 minutes is now taking 4 hours. What do you check?" — In order: (1) ADF Monitor → Activity Run Details → check throughput. If throughput dropped from 200 MB/s to 5 MB/s: source is the bottleneck. (2) Source database: check SQL Server wait stats or Query Activity Monitor for blocking queries. (3) SHIR: check SHIR node CPU/memory in Windows Task Manager (if on-prem). (4) Network: check link between SHIR and Azure (run Test Connectivity in SHIR Manager). (5) File count: if sink has millions of small files from previous runs, consider VACUUM/cleanup.

> **Tip 2:** "How do you orchestrate complex pipeline dependencies with ADF?" — ADF supports dependency via "Execute Pipeline" activity with "Wait on completion" toggle. For true DAG-style dependencies across many pipelines, pattern: use an orchestrator pipeline that calls child pipelines sequentially with Execute Pipeline activities, or use Azure Logic Apps / Azure Functions as an external orchestrator that triggers ADF pipelines via the ADF REST API. For very complex workflows, Azure Managed Airflow (preview) or Fabric Data Pipelines may be better.

> **Tip 3:** "What is the difference between schedule trigger and tumbling window trigger in production?" — Schedule trigger: fires at fixed cron time regardless of previous runs. If a run fails, the next schedule fires normally — no catch-up. Tumbling Window: fires for fixed-size non-overlapping windows from a start time, guarantees every window has exactly one run, retries failed windows, and supports backfill (set window start time in the past to process historical windows). For incremental ETL that must process every hour without gaps, always use Tumbling Window — it's the production-correct choice.
