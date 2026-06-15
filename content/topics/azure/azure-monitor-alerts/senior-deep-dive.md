---
title: "Azure Monitor & Alerts - Senior Deep Dive"
topic: azure
subtopic: azure-monitor-alerts
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [azure, monitor, kql, observability, slo, alerting-strategy, cost, data-quality-monitoring]
---

# Azure Monitor & Alerts — Senior Deep Dive

## Observability Strategy for Data Platforms

At the senior level, monitoring is not about configuring individual alerts — it's about designing an **observability strategy** that covers the four pillars of data platform health:

```
1. FRESHNESS   — Is data arriving and processing on time?
2. COMPLETENESS — Is all expected data present?
3. ACCURACY    — Is data correct (schema, values, business rules)?
4. AVAILABILITY — Are the pipeline services healthy and reachable?
```

Each pillar maps to different Azure Monitor signals:
- **Freshness** → Log search alerts on time since last successful pipeline run
- **Completeness** → Custom log tables with row count checks vs. expected ranges
- **Accuracy** → Application Insights custom events from dbt test runs
- **Availability** → Metric alerts on service health + Activity Log alerts

---

## Advanced KQL Patterns

### Calculating Data Freshness SLA Violations

```kql
// Find pipelines that haven't succeeded in the expected interval
let pipelineSLAs = datatable(PipelineName: string, MaxAllowedDelayMinutes: int)
[
    "ingest_orders",        60,   // should run every 30 min, alert after 60
    "transform_customers",  240,  // should run every 2h, alert after 4h
    "load_sales_summary",   1440, // daily job, alert after 24h
];

let lastSuccessfulRuns = ADFPipelineRun
    | where TimeGenerated > ago(2d)
    | where Status == "Succeeded"
    | summarize LastSuccess = max(TimeGenerated) by PipelineName;

pipelineSLAs
| join kind=leftouter lastSuccessfulRuns on PipelineName
| extend
    MinutesSinceSuccess = datetime_diff('minute', now(), LastSuccess),
    SLAViolated = iif(
        isempty(LastSuccess) or MinutesSinceSuccess > MaxAllowedDelayMinutes,
        true,
        false
    )
| where SLAViolated == true
| project PipelineName, LastSuccess, MinutesSinceSuccess, MaxAllowedDelayMinutes
| order by MinutesSinceSuccess desc
```

### Root Cause Analysis: Cascading Pipeline Failures

```kql
// Identify whether a downstream failure was caused by an upstream failure
let failureWindow = 30min;

let upstreamFailures = ADFPipelineRun
    | where TimeGenerated > ago(24h)
    | where Status == "Failed"
    | where PipelineName in ("ingest_orders", "ingest_customers", "ingest_products")
    | project UpstreamPipeline = PipelineName, UpstreamFailTime = TimeGenerated;

let downstreamFailures = ADFPipelineRun
    | where TimeGenerated > ago(24h)
    | where Status == "Failed"
    | where PipelineName in ("transform_sales", "load_dashboard", "export_reporting")
    | project DownstreamPipeline = PipelineName, DownstreamFailTime = TimeGenerated;

downstreamFailures
| join kind=inner upstreamFailures on $left.DownstreamFailTime between (
    $right.UpstreamFailTime .. ($right.UpstreamFailTime + failureWindow)
)
| project
    DownstreamPipeline,
    DownstreamFailTime,
    LikelyCausedBy = UpstreamPipeline,
    UpstreamFailTime
| order by DownstreamFailTime desc
```

### Ingesting Custom Data Quality Metrics

Push custom metrics to Log Analytics via the **Log Analytics HTTP Data Collector API**:

```python
import requests
import json
import hashlib
import hmac
import base64
from datetime import datetime, timezone

class LogAnalyticsClient:
    def __init__(self, workspace_id: str, shared_key: str):
        self.workspace_id = workspace_id
        self.shared_key = shared_key
        self.log_type = "DataQualityMetrics"

    def _build_signature(self, date: str, content_length: int, method: str,
                         content_type: str, resource: str) -> str:
        x_headers = f"x-ms-date:{date}"
        string_to_hash = f"{method}\n{content_length}\n{content_type}\n{x_headers}\n{resource}"
        bytes_to_hash = string_to_hash.encode("utf-8")
        decoded_key = base64.b64decode(self.shared_key)
        encoded_hash = base64.b64encode(
            hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()
        ).decode("utf-8")
        return f"SharedKey {self.workspace_id}:{encoded_hash}"

    def post_data(self, records: list):
        body = json.dumps(records)
        method = "POST"
        content_type = "application/json"
        resource = "/api/logs"
        rfc1123date = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
        content_length = len(body)
        signature = self._build_signature(rfc1123date, content_length, method, content_type, resource)

        uri = (
            f"https://{self.workspace_id}.ods.opinsights.azure.com"
            f"{resource}?api-version=2016-04-01"
        )
        headers = {
            "Content-Type": content_type,
            "Authorization": signature,
            "Log-Type": self.log_type,
            "x-ms-date": rfc1123date,
        }
        response = requests.post(uri, data=body, headers=headers)
        response.raise_for_status()
        return response.status_code

# Usage: send data quality results after each dbt run
client = LogAnalyticsClient(workspace_id="xxx", shared_key="yyy")

quality_records = [
    {
        "TableName": "fact_orders",
        "CheckName": "null_order_id",
        "Status": "PASS",
        "FailedRows": 0,
        "TotalRows": 1_500_000,
        "FailureRate": 0.0,
        "RunTimestamp": datetime.now(timezone.utc).isoformat()
    },
    {
        "TableName": "dim_customers",
        "CheckName": "valid_email_format",
        "Status": "FAIL",
        "FailedRows": 342,
        "TotalRows": 85_000,
        "FailureRate": 0.00403,
        "RunTimestamp": datetime.now(timezone.utc).isoformat()
    }
]

client.post_data(quality_records)
```

---

## Alert Fatigue and Signal Quality

A senior concern is designing alert rules that are **actionable, not noisy**. Key principles:

### 1. Alert on Symptoms, Not Causes

```
❌ Bad:  Alert when Databricks cluster CPU > 80%
✅ Good: Alert when downstream pipeline hasn't completed in 2× expected duration

❌ Bad:  Alert on every ADF activity retry
✅ Good: Alert when a pipeline has failed 3 consecutive times (no self-healing)
```

### 2. Dynamic Thresholds

Instead of static thresholds, use Azure Monitor's **dynamic thresholds** (ML-based anomaly detection):

```bash
az monitor metrics alert create \
  --name "ADF-AnomalousRuntime" \
  --resource-group "rg-monitoring" \
  --scopes "{adf_resource_id}" \
  --condition "dynamic GreaterOrLessThan avg PipelineSucceededRuns with sensitivity High lookback-period P1D" \
  --severity 3
```

Dynamic thresholds automatically learn seasonal patterns (daily/weekly) and alert when observed values deviate significantly.

### 3. Alert Suppression for Maintenance Windows

```python
# Create maintenance window via Azure Monitor REST API
import requests

def create_maintenance_window(start_iso: str, end_iso: str, alert_rule_ids: list):
    """Suppress alerts during planned maintenance."""
    payload = {
        "location": "global",
        "properties": {
            "enabled": True,
            "scheduledQueryRules": alert_rule_ids,
            "startDateTime": start_iso,
            "endDateTime": end_iso,
            "timeZone": "UTC",
            "recurrenceType": "Once"
        }
    }
    # POST to /subscriptions/{sub}/providers/Microsoft.AlertsManagement/maintenanceConfigurations
    ...
```

---

## Cost Management for Azure Monitor

Log Analytics costs are primarily driven by **data ingestion volume** (GB/day). For data platforms with verbose diagnostic logs, this can become significant.

### Ingestion Cost Reduction Strategies

#### 1. Selective Log Categories

Don't enable all log categories — only what you actually query:

```json
// Don't enable these unless needed for debugging
{ "category": "SandboxPipelineRuns", "enabled": false },
{ "category": "SandboxActivityRuns", "enabled": false },
{ "category": "SSISPackageEventMessages", "enabled": false }
```

#### 2. Ingestion-Time Transformation

Filter or project columns before logs land in the workspace using **Data Collection Rules (DCR)**:

```json
{
  "transformKql": "source | where Status != 'Succeeded' | project TimeGenerated, PipelineName, Status, FailureType, ErrorMessage",
  "outputStream": "Microsoft-ADFPipelineRun"
}
```

This only stores failed runs — for a pipeline with 99% success rate, this reduces volume by 99%.

#### 3. Log Data Tiers

- **Analytics tier** (hot): Full KQL access, alert rules, 30-day default retention. Higher cost.
- **Basic tier** (cold): 8-day retention, limited query access. 80% cheaper — ideal for verbose debug logs.
- **Archive tier**: Cheap long-term storage for compliance; query via on-demand analytics.

```bash
# Set table to Basic tier (reduces ingestion cost)
az monitor log-analytics workspace table update \
  --resource-group "rg-monitoring" \
  --workspace-name "my-workspace" \
  --name "AzureDiagnostics" \
  --plan "Basic"
```

---

## Multi-Resource Monitoring Architecture

For an enterprise data platform with multiple ADF factories, Databricks workspaces, and Synapse instances:

```
Architecture: Centralised Monitoring Hub

Data Platform Resources                  Monitoring Hub
(Multiple subscriptions / regions)
                                         ┌──────────────────────────┐
ADF (prod-eastus)  ──Diagnostic──►       │  Log Analytics Workspace  │
ADF (prod-westeu)  ──Diagnostic──►       │  (centralised, one/env)   │
Databricks (prod)  ──OMS Agent──►        │                           │
Synapse (prod)     ──Diagnostic──►       │  Alert Rules              │
Event Hubs (prod)  ──Diagnostic──►       │  Workbooks                │
                                         │  Query Pack (shared KQL)  │
                                         └──────────────────────────┘
                                                     │
                                         ┌───────────┴────────────┐
                                         │    Action Groups         │
                                         ├── Slack #data-alerts    │
                                         ├── PagerDuty (P0/P1)    │
                                         └── JIRA ticket creation  │
                                         └──────────────────────────┘
```

**Key senior decisions:**
- **One workspace per environment** (dev/stage/prod): prevents dev noise from polluting prod alerts.
- **Cross-workspace queries**: Use `workspace("other-workspace").TableName` in KQL to correlate across workspaces.
- **RBAC on workspaces**: Data engineers get read access; SecOps get read access to Activity Logs only.
