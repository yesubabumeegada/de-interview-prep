---
title: "Azure Monitor & Alerts - Scenario Questions"
topic: azure
subtopic: azure-monitor-alerts
content_type: scenario_question
tags: [azure, monitor, kql, log-analytics, alerts, observability, data-pipelines]
---

# Azure Monitor & Alerts — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Setting Up Basic ADF Monitoring

You've just deployed a new Azure Data Factory with 5 pipelines that run daily. Your team lead asks you to set up monitoring so the team gets an email alert whenever any pipeline fails. You have a Log Analytics Workspace already provisioned. Walk through the steps to implement this.

<details>
<summary>✅ Solution</summary>

### Step 1: Enable Diagnostic Settings on ADF

First, route ADF logs to the Log Analytics Workspace:

```bash
az monitor diagnostic-settings create \
  --name "adf-pipeline-logs" \
  --resource "/subscriptions/{sub_id}/resourceGroups/{rg}/providers/Microsoft.DataFactory/factories/{adf_name}" \
  --workspace "/subscriptions/{sub_id}/resourceGroups/{rg}/providers/Microsoft.OperationalInsights/workspaces/{workspace_name}" \
  --logs '[
    {"category": "PipelineRuns", "enabled": true},
    {"category": "ActivityRuns", "enabled": true},
    {"category": "TriggerRuns",  "enabled": true}
  ]'
```

Or in the Azure Portal: ADF → Diagnostic settings → Add diagnostic setting → check PipelineRuns, ActivityRuns, TriggerRuns → select your Log Analytics workspace → Save.

### Step 2: Verify Logs Are Flowing

After the next pipeline run, validate in Log Analytics:

```kql
ADFPipelineRun
| where TimeGenerated > ago(2h)
| project TimeGenerated, PipelineName, Status, DurationInMs
| order by TimeGenerated desc
| take 20
```

If no results appear, wait 5–10 minutes (there is ingestion delay) and re-check the Diagnostic Settings configuration.

### Step 3: Create an Action Group for Email

```bash
az monitor action-group create \
  --name "DataTeamEmailAlert" \
  --resource-group "{rg}" \
  --short-name "DTEmail" \
  --action email DataTeamEmail "data-team@company.com"
```

### Step 4: Create a Log Search Alert Rule

```bash
az monitor scheduled-query create \
  --name "ADF-PipelineFailure" \
  --resource-group "{rg}" \
  --scopes "/subscriptions/{sub_id}/resourceGroups/{rg}/providers/Microsoft.OperationalInsights/workspaces/{workspace_name}" \
  --condition-query "ADFPipelineRun | where Status == 'Failed'" \
  --condition-threshold 0 \
  --condition-operator GreaterThan \
  --condition-time-aggregation Count \
  --evaluation-frequency 5 \
  --window-size 5 \
  --severity 2 \
  --action-groups "/subscriptions/{sub_id}/resourceGroups/{rg}/providers/Microsoft.Insights/actionGroups/DataTeamEmailAlert"
```

### Step 5: Test the Alert

Manually trigger a pipeline failure (e.g., point a source dataset to a non-existent file path) and confirm:
1. The pipeline appears in ADFPipelineRun with Status = "Failed"
2. The alert fires within 5–10 minutes
3. The email arrives at data-team@company.com

### Key Points

- Diagnostic Settings must be explicitly enabled — ADF logs don't flow to Log Analytics by default.
- Log search alerts have a minimum 1-minute evaluation frequency and a minimum 1-minute window.
- Alert severity levels: 0 (Critical) → 4 (Verbose). Use 2 (Error) for pipeline failures.
- The alert query evaluates every 5 minutes over the last 5 minutes of data.

</details>
</article>

---

<article data-difficulty="mid">

## Scenario 2: Diagnosing a Noisy Alert

Your team's "ADF Pipeline Failure" alert has been firing 50+ times per day, and engineers are ignoring it (alert fatigue). Investigation reveals that a retry-heavy pipeline is triggering the alert on each retry attempt, even though the pipeline ultimately succeeds. Redesign the alert to fire only on genuine failures.

<details>
<summary>✅ Solution</summary>

### Diagnosis

The problem: the KQL query `where Status == 'Failed'` fires on **Activity-level failures during retry**, not just final pipeline-level failures. ADF retries can show intermediate `Failed` activity statuses before the activity succeeds on retry.

### Fix 1: Query Only Pipeline-Level Final Status (Not Activity-Level)

```kql
// Only alert on FINAL pipeline failures (not activity retries)
ADFPipelineRun
| where TimeGenerated > ago(5m)
| where Status == "Failed"
// ADFPipelineRun records represent FINAL pipeline status
// (unlike ADFActivityRun which can show intermediate states)
| project TimeGenerated, PipelineName, RunId, FailureType
```

If using `ADFActivityRun` was the mistake, switching to `ADFPipelineRun` resolves it.

### Fix 2: Suppress Pipelines with Built-in Retry

For pipelines that are expected to succeed after 1–2 retries, add a cooldown:

```kql
// Alert only if the same pipeline fails twice in 30 minutes
ADFPipelineRun
| where TimeGenerated > ago(30m)
| where Status == "Failed"
| summarize FailureCount = count() by PipelineName
| where FailureCount >= 2
```

This suppresses single transient failures and only alerts on sustained failures.

### Fix 3: Exclude Known Flaky Pipelines

```kql
ADFPipelineRun
| where TimeGenerated > ago(5m)
| where Status == "Failed"
| where PipelineName !in (
    "external_api_pull",     // Known to be flaky, has own alert with higher threshold
    "optional_enrichment"    // Non-critical, allowed to fail
)
| project TimeGenerated, PipelineName, Status, FailureType
```

### Fix 4: Use Alert Deduplication

Configure the alert rule with a **suppress alerts** period (mute period) to prevent re-firing:

```bash
az monitor scheduled-query update \
  --name "ADF-PipelineFailure" \
  --resource-group "{rg}" \
  --auto-mitigate true \  # auto-resolve when condition clears
  --mute-actions-duration "PT30M"  # don't re-notify within 30 min of first alert
```

### Fix 5: Severity-Based Routing

Not all failures are equal. Redesign to route by pipeline criticality:

```kql
// Critical pipelines — Severity 1 (P1)
ADFPipelineRun
| where Status == "Failed"
| where PipelineName in ("daily_revenue_load", "customer_360_refresh", "compliance_export")

// Non-critical pipelines — Severity 3 (P3, email only, no page)
ADFPipelineRun
| where Status == "Failed"
| where PipelineName !in ("daily_revenue_load", "customer_360_refresh", "compliance_export")
```

Create separate alert rules per severity tier with different action groups (PagerDuty for P1, email for P3).

### Result

After applying these fixes:
- Alert volume drops from 50+/day to 2–3 actionable alerts/day
- Engineers re-engage with alerts because they are now meaningful
- Critical pipelines get immediate attention; non-critical ones get email summaries

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3: Designing an Observability Platform for a Multi-Team Data Platform

Your organisation has grown to 5 data engineering teams sharing an Azure data platform (ADF, Databricks, Synapse, Event Hubs). Each team owns different pipelines and tables. You're asked to design an Azure Monitor-based observability platform that supports: (a) team-level dashboards and alerts, (b) a centralised data platform SLO dashboard for the CTO, (c) data quality observability beyond pipeline availability, (d) cost governance per team.

<details>
<summary>✅ Solution</summary>

### Architecture Overview

```
Multi-team Data Platform Observability

Resources (tagged by team)          ──────►   Centralised Log Analytics Workspace (prod)
  ADF Pipelines (tagged: team=fintech)                │
  Databricks Jobs (tagged: team=marketing)            │
  Synapse Tables (tagged: team=data-science)          │
                                              ┌───────┴──────────────────────┐
                                              │  Tables:                      │
                                              │  - ADFPipelineRun             │
                                              │  - ADFActivityRun             │
                                              │  - DbtTestResults_CL          │
                                              │  - DataQualityMetrics_CL      │
                                              │  - PipelineSLOTracking_CL     │
                                              └───────────────────────────────┘
                                                          │
                         ┌────────────────────────────────┤
                         │                                │
                    Team Workbooks                   CTO Dashboard
                    (per-team scoped                 (platform-wide
                     by PipelineName                  SLO metrics)
                     tag or prefix)
```

### Component 1: Team-Level Alert Isolation

Each team gets its own Action Group so their alerts don't cross-pollinate:

```python
# IaC: Terraform to create per-team action groups and alert rules
teams = ["fintech", "marketing", "data-science", "platform", "growth"]

for team in teams:
    # Action group routes to team's Slack channel
    create_action_group(
        name=f"AlertGroup-{team}",
        slack_webhook=f"https://hooks.slack.com/.../{team}",
        email=f"data-eng-{team}@company.com"
    )

    # Alert rule scoped to this team's pipelines
    create_log_alert(
        name=f"PipelineFailure-{team}",
        kql=f"""
            ADFPipelineRun
            | where Status == 'Failed'
            | where PipelineName startswith '{team}_'
        """,
        action_group=f"AlertGroup-{team}",
        severity=2
    )
```

### Component 2: SLO Tracking Table

Push SLO status to a custom Log Analytics table after each pipeline run:

```python
# In each pipeline's ADF success/failure handler (Azure Function)
def record_slo_status(pipeline_name: str, completed_at: datetime, deadline: datetime):
    slo_record = {
        "PipelineName": pipeline_name,
        "Team": pipeline_name.split("_")[0],
        "CompletedAt": completed_at.isoformat(),
        "Deadline": deadline.isoformat(),
        "MetSLO": completed_at <= deadline,
        "MinutesBeforeDeadline": (deadline - completed_at).total_seconds() / 60,
        "RunTimestamp": datetime.now(timezone.utc).isoformat()
    }
    log_analytics_client.post_data([slo_record], log_type="PipelineSLOTracking")
```

### Component 3: CTO-Level SLO Dashboard KQL

```kql
// Platform-wide SLO scorecard (last 30 days)
PipelineSLOTracking_CL
| where TimeGenerated > ago(30d)
| summarize
    TotalRuns   = count(),
    SlaMet      = countif(MetSLO_b == true),
    SlaBreaches = countif(MetSLO_b == false),
    AvgMinutesBeforeDeadline = avg(MinutesBeforeDeadline_d)
    by Team_s
| extend SLOPercent = round(todouble(SlaMet) / TotalRuns * 100, 2)
| project
    Team = Team_s,
    ["SLO %"] = SLOPercent,
    ["Breaches (30d)"] = SlaBreaches,
    ["Avg Buffer (min)"] = round(AvgMinutesBeforeDeadline, 1)
| order by ["SLO %"] asc
```

### Component 4: Data Quality Observability

```kql
// Cross-team data quality summary: last 7 days
DbtTestResults_CL
| where TimeGenerated > ago(7d)
| summarize
    TotalTests  = count(),
    Passed      = countif(Status_s == "PASS"),
    Failed      = countif(Status_s == "FAIL"),
    Warned      = countif(Status_s == "WARN")
    by Team_s = split(TestName_s, ".")[0]
| extend QualityScore = round(todouble(Passed) / TotalTests * 100, 1)
| order by QualityScore asc
```

### Component 5: Cost Governance Per Team

Tag all Azure resources with `team` and use **Azure Cost Management** + **Log Analytics ingestion metrics**:

```kql
// Log Analytics ingestion volume per table (proxy for cost)
Usage
| where TimeGenerated > ago(30d)
| where DataType in ("ADFPipelineRun", "ADFActivityRun", "DbtTestResults_CL")
| summarize TotalGB = sum(Quantity) / 1024 by DataType, bin(TimeGenerated, 1d)
| render timechart
```

Combine with Azure Cost Management budget alerts tagged per team:
```bash
az consumption budget create \
  --budget-name "DataEngFintech-Monthly" \
  --amount 5000 \
  --time-grain Monthly \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --resource-group "rg-fintech-data" \
  --notifications '[{
    "enabled": true,
    "operator": "GreaterThan",
    "threshold": 80,
    "contact-emails": ["data-eng-fintech@company.com"]
  }]'
```

### Governance Model

- **Platform team owns**: Log Analytics workspace, centralised alert rules for infrastructure (service health, capacity), CTO workbook.
- **Domain teams own**: Their own alert rules (scoped to their pipeline prefixes), their team workbooks, their data quality alert thresholds.
- **Quarterly review**: Platform team presents SLO trends, cost trends, and top alert sources to CTO.

</details>
</article>
