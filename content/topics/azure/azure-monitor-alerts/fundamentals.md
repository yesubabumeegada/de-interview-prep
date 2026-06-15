---
title: "Azure Monitor & Alerts - Fundamentals"
topic: azure
subtopic: azure-monitor-alerts
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [azure, monitor, alerts, log-analytics, kql, observability, data-pipelines]
---

# Azure Monitor & Alerts — Fundamentals

## 🎯 Analogy

Think of Azure Monitor as the nervous system of your Azure infrastructure. Metrics are the heartbeat (numeric, fast, 30-day retention), Logs are the detailed medical records (structured events, configurable retention), and Alerts are the pager that wakes you up when something goes wrong. Log Analytics is the hospital records system where everything is stored and queried.

---

## What Is Azure Monitor?

**Azure Monitor** is Azure's unified observability platform for collecting, analysing, and acting on telemetry from Azure resources, on-premises systems, and applications. For data engineers, it is the primary tool for monitoring pipelines, detecting failures, and building operational dashboards.

### Core Components

```
Azure Monitor
├── Data Sources
│   ├── Azure Resources (ADF, Databricks, Synapse, Event Hubs, etc.)
│   ├── Azure Activity Log (subscription-level events)
│   ├── Guest OS Metrics (VMs, Kubernetes nodes)
│   └── Application Insights (application-level APM)
│
├── Data Stores
│   ├── Azure Monitor Metrics (numeric time-series, 93-day retention)
│   └── Log Analytics Workspace (logs, events, traces — KQL queryable)
│
├── Analysis & Visualisation
│   ├── Metrics Explorer (charts for numeric metrics)
│   ├── Log Analytics (KQL queries over log data)
│   └── Workbooks (dashboards combining metrics + logs)
│
└── Action
    ├── Alert Rules (trigger conditions)
    └── Action Groups (notification/automation)
```

---

## Azure Monitor Metrics

**Metrics** are numerical measurements collected at regular intervals (typically every 1 minute). They are ideal for:
- Current state monitoring (CPU %, pipeline run count, queue depth)
- Fast alerting (metric alerts evaluate every 1–5 minutes)
- Trend dashboards

### Key Metrics for Data Engineering

| Service | Key Metrics | What to Watch |
|---------|-------------|---------------|
| Azure Data Factory | `PipelineRunsEnded` (Failed), `ActivityRunsEnded` (Failed) | Failed pipeline/activity runs |
| Event Hubs | `IncomingMessages`, `ThrottledRequests`, `ActiveConnections` | Throughput and throttling |
| Azure Synapse | `SQLPoolActiveQueries`, `SQLPoolQueuedQueries` | Query concurrency |
| ADLS Gen2 | `Transactions` (Status=Error), `Latency` | Storage errors and performance |
| Azure Databricks | Available via Log Analytics (not native metrics) | Job runs, cluster state |

---

## Log Analytics Workspace

**Log Analytics Workspace** is the central store for log data in Azure Monitor. All diagnostic logs, activity logs, and custom application logs flow into a workspace and are queried using **KQL (Kusto Query Language)**.

### Workspace Architecture

```
Azure Resources
    │  Diagnostic Settings enabled
    ▼
Log Analytics Workspace
    ├── AzureActivity (subscription events)
    ├── AzureDiagnostics (resource diagnostic logs)
    ├── ADFActivityRun (ADF activity run details)
    ├── ADFPipelineRun (ADF pipeline run details)
    ├── ADFTriggerRun (ADF trigger run details)
    └── Custom tables (from code instrumentation)
```

### Enabling Diagnostic Settings

For each Azure resource, you must **enable Diagnostic Settings** to route logs to Log Analytics:

```bash
# Enable ADF diagnostic logs → Log Analytics
az monitor diagnostic-settings create \
  --name "adf-to-loganalytics" \
  --resource "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories/{adf_name}" \
  --workspace "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.OperationalInsights/workspaces/{workspace_name}" \
  --logs '[
    {"category": "PipelineRuns", "enabled": true},
    {"category": "ActivityRuns", "enabled": true},
    {"category": "TriggerRuns", "enabled": true}
  ]' \
  --metrics '[{"category": "AllMetrics", "enabled": true}]'
```

---

## KQL: Kusto Query Language Basics

KQL is the query language for Log Analytics. It reads left-to-right, pipe-delimited, similar to Unix shell pipes.

### KQL Syntax Pattern

```
TableName
| operator1 arguments
| operator2 arguments
| operator3 arguments
```

### Essential KQL Operators

```kql
// where — filter rows
AzureDiagnostics
| where TimeGenerated > ago(24h)
| where Category == "PipelineRuns"

// project — select columns
| project TimeGenerated, pipelineName_s, status_s, durationInMs_d

// summarize — aggregate
| summarize FailureCount = countif(status_s == "Failed") by pipelineName_s

// order by — sort
| order by FailureCount desc

// take / limit — limit results
| take 100

// render — visualise in Log Analytics UI
| render timechart
```

### Your First Useful KQL Query: ADF Pipeline Failures

```kql
// Failed ADF pipeline runs in last 24 hours
ADFPipelineRun
| where TimeGenerated > ago(24h)
| where Status == "Failed"
| project TimeGenerated, PipelineName, RunId, FailureType, ErrorMessage
| order by TimeGenerated desc
```

---

## Alert Rules

An **Alert Rule** monitors a condition and fires when the condition is met. The core components:

```
Alert Rule
├── Target Resource (what to monitor)
├── Signal (metric or log query)
├── Condition (threshold or query result)
│   ├── Threshold (e.g., > 0 failures)
│   ├── Evaluation frequency (every 5 min)
│   └── Lookback window (last 15 min)
├── Severity (0=Critical, 4=Verbose)
└── Action Group (what to do when fired)
```

### Types of Alert Rules

| Type | Best For | Latency |
|------|----------|---------|
| Metric alert | Numeric thresholds (CPU > 80%) | Near real-time (1–5 min) |
| Log search alert | KQL query results | 5–15 min delay |
| Activity Log alert | Azure control plane events (resource delete, policy change) | Near real-time |

---

## Action Groups

**Action Groups** define what happens when an alert fires. Multiple alerts can share one Action Group.

### Action Types

| Action Type | Use Case |
|-------------|----------|
| Email/SMS | On-call engineer notification |
| Azure Function | Custom remediation logic |
| Logic App | Complex workflows (create JIRA ticket) |
| Webhook | Slack, PagerDuty, Teams integration |
| Automation Runbook | Auto-remediation (restart resource) |
| ITSM | ServiceNow integration |

### Creating an Action Group (Azure CLI)

```bash
az monitor action-group create \
  --name "DataEngTeamAlerts" \
  --resource-group "rg-monitoring" \
  --short-name "DEAlerts" \
  --action email OnCallEngineer "oncall@company.com"
```

---

## Key Interview Concepts at Fundamentals Level

1. **What is the difference between Metrics and Logs in Azure Monitor?**
   - Metrics: numeric, fast, 93-day retention, good for dashboards and fast alerts.
   - Logs: structured events in Log Analytics, KQL queryable, configurable retention (default 30 days).

2. **What is a Log Analytics Workspace?**
   - The storage and query engine for Azure Monitor logs. Uses KQL. Must enable Diagnostic Settings on resources to route logs there.

3. **What are Diagnostic Settings?**
   - Per-resource configuration that routes logs and metrics to Log Analytics, Storage Account, or Event Hub.

4. **What is KQL?**
   - Kusto Query Language — pipe-based language for querying Log Analytics tables. `where`, `project`, `summarize`, `order by`, `render` are the essential operators.
