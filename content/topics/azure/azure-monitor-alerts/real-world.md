---
title: "Azure Monitor & Alerts - Real-World Patterns"
topic: azure
subtopic: azure-monitor-alerts
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [azure, monitor, kql, observability, data-platform, incident-response, slo]
---

# Azure Monitor & Alerts — Real-World Patterns

## Production Monitoring Runbook Pattern

Real data engineering teams build **monitoring runbooks** — documented alert → investigation → remediation workflows that any on-call engineer can follow. Here's how they are structured alongside Azure Monitor.

---

## Production Pattern 1: End-to-End Pipeline SLO Monitoring

**Business requirement**: The "daily sales summary" pipeline must complete by 06:00 UTC. SLO: 99.5% of days meet this deadline (≤ 2 misses per year).

### KQL Alert Query

```kql
// Alert fires if daily_sales_summary hasn't succeeded by 05:45 UTC today
let targetPipeline = "daily_sales_summary";
let deadlineHour = 5;
let deadlineMinute = 45;

let todayDeadline = make_datetime(
    datetime_part("year", now()),
    datetime_part("month", now()),
    datetime_part("day", now()),
    deadlineHour,
    deadlineMinute,
    0
);

let hasCompletedToday = ADFPipelineRun
    | where TimeGenerated > startofday(now())
    | where PipelineName == targetPipeline
    | where Status == "Succeeded"
    | count;

hasCompletedToday
| where Count == 0  // no successful run today
| extend AlertMessage = strcat(targetPipeline, " has not succeeded before deadline at ", todayDeadline)
```

### SLO Tracking Dashboard Query

```kql
// Calculate SLO compliance for the last 30 days
let pipelineName = "daily_sales_summary";
let deadlineTime = 6h;  // 06:00 UTC

range d from ago(30d) to now() step 1d
| extend DayStart = startofday(d)
| extend Deadline = DayStart + deadlineTime
| join kind=leftouter (
    ADFPipelineRun
    | where PipelineName == pipelineName
    | where Status == "Succeeded"
    | summarize FirstSuccess = min(TimeGenerated) by bin(TimeGenerated, 1d)
    | project DayStart = startofday(TimeGenerated), FirstSuccess
) on DayStart
| extend
    MetSLO = iif(FirstSuccess < Deadline, true, false),
    MissedReason = case(
        isnull(FirstSuccess), "No successful run",
        FirstSuccess >= Deadline, "Completed late",
        "OK"
    )
| summarize
    TotalDays = count(),
    DaysMet = countif(MetSLO == true),
    DaysMissed = countif(MetSLO == false)
| extend SLOPercent = round(todouble(DaysMet) / TotalDays * 100, 2)
```

---

## Production Pattern 2: Incident Response Integration

### Alert → Slack → JIRA Automation

A typical production setup uses a **Logic App** to enrich alert payloads before routing to Slack and JIRA:

```
Azure Monitor Alert
        │
        ▼
Logic App HTTP Trigger
        │
        ├── Parse alert JSON (pipeline name, error, time)
        ├── Query Log Analytics for last 5 related failures
        │   (to distinguish new issue vs. recurring pattern)
        ├── Look up on-call engineer from PagerDuty API
        │
        ├──► POST to Slack #data-incidents with Block Kit message:
        │    ┌─────────────────────────────────┐
        │    │ 🔴 Pipeline Failure              │
        │    │ Pipeline: daily_sales_summary    │
        │    │ Error: Connection timeout        │
        │    │ On-call: @jane.smith             │
        │    │ Runbook: [link]                  │
        │    │ Last 5 failures: 2 (past 7d)     │
        │    └─────────────────────────────────┘
        │
        └──► Create JIRA ticket (if P1/P2 severity)
             Priority: set from alert severity
             Labels: data-pipeline, automated-alert
```

### Logic App JSON for Slack Notification

```json
{
  "body": {
    "blocks": [
      {
        "type": "header",
        "text": {
          "type": "plain_text",
          "text": "🔴 Data Pipeline Alert"
        }
      },
      {
        "type": "section",
        "fields": [
          { "type": "mrkdwn", "text": "*Pipeline:*\n@{body('Parse_Alert')?['pipelineName']}" },
          { "type": "mrkdwn", "text": "*Status:*\nFailed" },
          { "type": "mrkdwn", "text": "*Error:*\n@{body('Parse_Alert')?['errorMessage']}" },
          { "type": "mrkdwn", "text": "*Time:*\n@{utcNow()}" }
        ]
      },
      {
        "type": "actions",
        "elements": [
          {
            "type": "button",
            "text": { "type": "plain_text", "text": "View in ADF" },
            "url": "https://adf.azure.com/..."
          },
          {
            "type": "button",
            "text": { "type": "plain_text", "text": "Open Runbook" },
            "url": "https://confluence.company.com/..."
          }
        ]
      }
    ]
  }
}
```

---

## Production Pattern 3: Data Quality Observability Pipeline

A complete observability pipeline for data quality, integrated with Azure Monitor:

```
dbt test run
    │
    ▼
Python script parses dbt test_results.json
    │
    ├── Sends to Log Analytics HTTP API (DataQualityMetrics_CL table)
    │
    └── Publishes custom metric to Azure Monitor
        (via az monitor metrics not-exists, use Application Insights SDK)
```

### dbt Test Results → Log Analytics Ingestor

```python
import json
import subprocess
from pathlib import Path
from datetime import datetime, timezone
from log_analytics_client import LogAnalyticsClient  # custom wrapper

def parse_dbt_results_and_push(
    dbt_results_path: str,
    workspace_id: str,
    shared_key: str
):
    """After dbt test run, push quality metrics to Log Analytics."""
    with open(dbt_results_path) as f:
        results = json.load(f)

    client = LogAnalyticsClient(workspace_id, shared_key)
    records = []
    run_ts = datetime.now(timezone.utc).isoformat()

    for result in results["results"]:
        node = result["node"]
        records.append({
            "RunTimestamp": run_ts,
            "TestName": node["name"],
            "ModelName": node.get("attached_node", "unknown"),
            "Status": result["status"].upper(),
            "FailedRows": result.get("failures", 0),
            "ExecutionTimeSeconds": result.get("execution_time", 0),
            "Severity": node.get("config", {}).get("severity", "error").upper(),
            "ErrorMessage": result.get("message", "")
        })

    client.post_data(records, log_type="DbtTestResults")
    print(f"Pushed {len(records)} dbt test results to Log Analytics")

# In CI/CD after dbt test:
parse_dbt_results_and_push(
    "target/run_results.json",
    workspace_id=os.environ["LOG_ANALYTICS_WORKSPACE_ID"],
    shared_key=os.environ["LOG_ANALYTICS_KEY"]
)
```

### KQL Alert: dbt Test Failures in Production

```kql
// Alert on any dbt test failure with severity=error
DbtTestResults_CL
| where TimeGenerated > ago(30m)
| where Status_s == "FAIL"
| where Severity_s == "ERROR"
| project TimeGenerated, TestName_s, ModelName_s, FailedRows_d, ErrorMessage_s
| order by TimeGenerated desc
```

---

## Production Pattern 4: Workbook for Daily Data Platform Review

A real engineering team daily standup starts with checking this Azure Monitor Workbook:

```
📊 Data Platform Daily Health (parameterised: last 24h)
│
├── 🔴 Active Incidents
│   KQL: ADFPipelineRun | where Status=="Failed" | last 10
│
├── ✅ Pipeline Completion Status
│   Grid showing each scheduled pipeline:
│   - Last successful run time
│   - Hours since last success
│   - SLA status (GREEN/YELLOW/RED)
│
├── 📈 Throughput Trends
│   Metric: ADF data movement bytes (line chart)
│   Metric: Event Hub incoming messages/sec (line chart)
│
├── 🧪 Data Quality Summary
│   KQL: DbtTestResults_CL | last run | summarize by model
│
└── 💰 Cost Monitor
    Log Analytics: Ingestion volume by table (today vs. 7d avg)
    Metric: Event Hub throughput units consumed
```

---

## Key Interview Talking Points

**"How do you monitor data pipeline quality, not just pipeline availability?"**

Most monitoring covers infrastructure (pipeline succeeded/failed). The next level is monitoring **data quality inside pipelines**:

1. Instrument dbt tests to push results to Log Analytics after every run.
2. Create KQL alerts for quality degradation (null rate increase, row count deviation, schema drift).
3. Add data quality panels to the operational workbook next to pipeline health.
4. Wire quality failures to the same incident management workflow as infrastructure failures — a silent data quality issue can be more damaging than a visible pipeline failure.

**"How would you detect that an ADF pipeline is processing less data than expected without failing?"**

```kql
// Detect silent under-processing: pipeline succeeds but writes fewer rows than expected
ADFActivityRun
| where TimeGenerated > ago(25h)
| where ActivityName == "copy_orders_to_adls"
| where Status == "Succeeded"
| extend RowsCopied = tolong(Output.rowsCopied)
| summarize AvgRows = avg(RowsCopied) by bin(TimeGenerated, 1h)
| where AvgRows < 1000  // below expected minimum threshold
```

This pattern — alerting on a successful pipeline that produces surprisingly little output — catches upstream data feed issues that don't cause explicit errors.
