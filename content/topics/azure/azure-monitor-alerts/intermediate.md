---
title: "Azure Monitor & Alerts - Intermediate"
topic: azure
subtopic: azure-monitor-alerts
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [azure, monitor, kql, log-analytics, application-insights, alerts, workbooks, adf]
---

# Azure Monitor & Alerts — Intermediate

## KQL for Data Pipeline Monitoring

At the intermediate level, you need to write production-grade KQL queries that data engineering teams actually use to monitor their pipelines.

### ADF: Complete Pipeline Health Dashboard Query

```kql
// ADF pipeline health: last 7 days, grouped by pipeline and status
ADFPipelineRun
| where TimeGenerated > ago(7d)
| summarize
    TotalRuns      = count(),
    Succeeded      = countif(Status == "Succeeded"),
    Failed         = countif(Status == "Failed"),
    Cancelled      = countif(Status == "Cancelled"),
    AvgDurationMin = round(avg(DurationInMs) / 60000, 2),
    P95DurationMin = round(percentile(DurationInMs, 95) / 60000, 2)
    by PipelineName
| extend SuccessRate = round(todouble(Succeeded) / TotalRuns * 100, 1)
| order by Failed desc, TotalRuns desc
```

### ADF: Activity-Level Failure Analysis

```kql
// Drill into which activities within pipelines are failing
ADFActivityRun
| where TimeGenerated > ago(24h)
| where Status == "Failed"
| project
    TimeGenerated,
    PipelineName,
    ActivityName,
    ActivityType,
    Error = parse_json(Error),
    DurationInMs
| extend
    ErrorCode    = tostring(Error.errorCode),
    ErrorMessage = tostring(Error.message),
    FailureType  = tostring(Error.failureType)
| summarize FailureCount = count() by PipelineName, ActivityName, ErrorCode, ErrorMessage
| order by FailureCount desc
```

### ADF: Detecting Pipeline Duration Anomalies

```kql
// Identify pipelines running significantly longer than their 7-day average
let baseline = ADFPipelineRun
    | where TimeGenerated between (ago(8d) .. ago(1d))
    | where Status == "Succeeded"
    | summarize AvgDurationMs = avg(DurationInMs), StdDev = stdev(DurationInMs)
      by PipelineName;

ADFPipelineRun
| where TimeGenerated > ago(1d)
| where Status == "Succeeded"
| join kind=inner baseline on PipelineName
| extend
    ZScore = (DurationInMs - AvgDurationMs) / StdDev,
    DurationMin = DurationInMs / 60000
| where ZScore > 2.5   // more than 2.5 standard deviations above average
| project TimeGenerated, PipelineName, DurationMin, ZScore
| order by ZScore desc
```

### Event Hubs: Consumer Lag Monitoring

```kql
// Monitor Event Hub consumer group lag
AzureDiagnostics
| where ResourceType == "EVENTHUBS"
| where Category == "OperationalLogs"
| where TimeGenerated > ago(1h)
| where OperationName == "RetrieveConsumerGroup"
| project TimeGenerated, namespaceName_s, eventHubName_s, consumerGroup_s, message_s
```

### Custom Log Table: Data Quality Metrics

```kql
// Query custom data quality logs ingested via Log Analytics HTTP Data Collector API
DataQualityMetrics_CL
| where TimeGenerated > ago(24h)
| summarize
    AvgNullRate    = avg(NullRate_d),
    MaxNullRate    = max(NullRate_d),
    FailedChecks   = countif(Status_s == "FAIL")
    by TableName_s, ColumnName_s
| where MaxNullRate > 0.05  // alert on > 5% null rate
| order by MaxNullRate desc
```

---

## Application Insights for ADF Pipelines

**Application Insights** can be linked to ADF for richer telemetry, including custom events and dependency tracking.

### Enabling Application Insights in ADF

In ADF Studio: Manage → Diagnostic Settings → Add setting → Select Application Insights workspace.

Once enabled, ADF emits:
- Pipeline run success/failure events
- Activity duration and type
- Data movement throughput (copy activity)
- Custom logging from Azure Function activities

### Querying Application Insights with KQL

Application Insights uses the same KQL engine but different table names:

```kql
// ADF pipeline duration trends from App Insights
dependencies
| where timestamp > ago(7d)
| where cloud_RoleName == "DataFactory"
| where type == "PipelineRun"
| summarize
    AvgDuration  = avg(duration),
    P99Duration  = percentile(duration, 99),
    FailureRate  = countif(success == false) / count() * 100
    by name
| order by FailureRate desc
```

---

## Diagnostic Settings Deep Dive

### Routing Strategy for Different Use Cases

```
Resource Logs
    │
    ├──► Log Analytics Workspace
    │    - Interactive KQL queries
    │    - Alert rules
    │    - Workbooks dashboards
    │    - Retention: 30–730 days (billable after 30d)
    │
    ├──► Azure Storage Account
    │    - Long-term archival (years)
    │    - Compliance requirements
    │    - Low cost for rarely accessed logs
    │
    └──► Azure Event Hub
         - Real-time log streaming to SIEM (Splunk, QRadar)
         - Third-party monitoring tools
         - Custom stream processing
```

### Configuring Diagnostic Settings via ARM Template

```json
{
  "type": "Microsoft.Insights/diagnosticSettings",
  "apiVersion": "2021-05-01-preview",
  "name": "adf-diagnostics",
  "scope": "[resourceId('Microsoft.DataFactory/factories', parameters('adfName'))]",
  "properties": {
    "workspaceId": "[parameters('logAnalyticsWorkspaceId')]",
    "logs": [
      { "category": "PipelineRuns",  "enabled": true, "retentionPolicy": { "enabled": false } },
      { "category": "ActivityRuns",  "enabled": true, "retentionPolicy": { "enabled": false } },
      { "category": "TriggerRuns",   "enabled": true, "retentionPolicy": { "enabled": false } },
      { "category": "SandboxPipelineRuns",   "enabled": false },
      { "category": "SandboxActivityRuns",   "enabled": false }
    ],
    "metrics": [
      { "category": "AllMetrics", "enabled": true }
    ]
  }
}
```

---

## Alert Rules: Production Patterns

### Log Search Alert for ADF Pipeline Failures

```bash
az monitor scheduled-query create \
  --name "ADF-PipelineFailure-Alert" \
  --resource-group "rg-monitoring" \
  --scopes "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.OperationalInsights/workspaces/{workspace}" \
  --condition-query "ADFPipelineRun | where Status == 'Failed' | where TimeGenerated > ago(5m)" \
  --condition-threshold 0 \
  --condition-operator GreaterThan \
  --condition-time-aggregation Count \
  --evaluation-frequency 5 \
  --window-size 5 \
  --severity 2 \
  --action-groups "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Insights/actionGroups/DataEngTeamAlerts"
```

### Metric Alert for Event Hub Throttling

```bash
az monitor metrics alert create \
  --name "EventHub-ThrottledRequests" \
  --resource-group "rg-monitoring" \
  --scopes "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.EventHub/namespaces/{eh_namespace}" \
  --condition "avg ThrottledRequests > 10" \
  --window-size 5m \
  --evaluation-frequency 1m \
  --severity 2 \
  --description "Event Hub is throttling producers — check partition capacity" \
  --action "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.Insights/actionGroups/DataEngTeamAlerts"
```

### Activity Log Alert for Resource Deletion

```bash
# Alert if anyone deletes an ADF resource (critical for governance)
az monitor activity-log alert create \
  --name "ADF-ResourceDeletion" \
  --resource-group "rg-monitoring" \
  --scopes "/subscriptions/{sub}" \
  --condition 'category=Administrative and operationName=Microsoft.DataFactory/factories/delete' \
  --action-groups "DataEngTeamAlerts"
```

---

## Action Groups: Webhook to Slack

A common production pattern is routing Azure Monitor alerts to a Slack channel via webhook:

### Create Action Group with Slack Webhook

```bash
az monitor action-group create \
  --name "DataEngAlerting" \
  --resource-group "rg-monitoring" \
  --short-name "DEAlert" \
  --action webhook SlackNotification "https://hooks.slack.com/services/T.../B.../..." \
    useCommonAlertSchema true
```

### Custom Slack Payload via Logic App

For richer Slack messages (with pipeline name, error details), use an Azure Logic App as the webhook target:

```
Alert fires → Logic App HTTP trigger →
  Logic App parses alert payload →
    Formats Slack Block Kit message →
      POST to Slack webhook
```

Logic App expression to extract pipeline name from alert:
```
@{triggerBody()?['data']?['essentials']?['alertRule']}
@{triggerBody()?['data']?['alertContext']?['condition']?['allOf']?[0]?['searchQuery']}
```

---

## Azure Monitor Workbooks for Data Engineering

**Workbooks** combine KQL queries, metrics charts, and markdown into shareable interactive dashboards.

### Typical Data Engineering Workbook Structure

```
📊 Data Platform Operations Dashboard
├── Section 1: Pipeline Health (Last 24h)
│   ├── KQL: ADF pipeline success rate by pipeline name
│   └── KQL: Top 5 failed pipelines with error codes
│
├── Section 2: Throughput
│   ├── Metric chart: ADF data movement (bytes/sec)
│   └── Metric chart: Event Hub incoming messages/sec
│
├── Section 3: Data Quality
│   ├── KQL: Custom DataQualityMetrics_CL null rate trends
│   └── KQL: Schema validation failures
│
└── Section 4: Infrastructure Health
    ├── Metric chart: Synapse SQL pool queue depth
    └── KQL: Databricks cluster errors from OMS agent
```

Workbooks can be **parameterised** — a time range parameter passes to all KQL queries, letting operators drill into specific windows.
