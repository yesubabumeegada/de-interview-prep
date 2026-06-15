---
title: "Power BI Deployment Pipelines - Scenario Questions"
topic: power-bi
subtopic: deployment-pipelines
content_type: scenario_question
tags: [power-bi, deployment, ci-cd, devops]
---

# Power BI Deployment Pipelines — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Production Dataset Connecting to the Wrong Database

**Scenario:** Your team uses a Power BI Deployment Pipeline with three stages. The Finance dataset in Development connects to `finance-dev.database.windows.net`. After promoting the dataset to Production, business users report that their reports show development data instead of real production data. What went wrong and how do you fix it permanently?

<details>
<summary>💡 Hint</summary>

Think about how Power BI handles connection strings when content is promoted between pipeline stages. Is there a mechanism specifically designed to override connection settings per stage?

</details>

<details>
<summary>✅ Solution</summary>

**What went wrong:** No deployment rules were configured for the Production stage. When a dataset is deployed, it carries its connection settings from the source stage (Development). Without a rule to override the connection, the Production dataset still points to `finance-dev.database.windows.net`.

**Permanent fix — configure Deployment Rules:**

1. In the pipeline, click the **⚙ Deployment rules** icon on the Production stage.
2. Select the Finance dataset.
3. Under **Data source rules**, set:
   - Server: `finance-prod.database.windows.net`
   - Database: `finance-prod`
4. Alternatively, define M parameters in Power Query:

```m
// Power Query parameters
ServerName   = "finance-dev.database.windows.net"  // overridden by rule in Prod
DatabaseName = "finance-dev"

// Used in query source
Source = Sql.Database(ServerName, DatabaseName)
```

Then in the Production deployment rule, set `ServerName` → `finance-prod.database.windows.net` and `DatabaseName` → `finance-prod`.

**Going forward:** Deployment rules apply automatically every time you promote the dataset to Production — no manual reconfiguration needed. Validate by checking the dataset settings in the Production workspace after the next deploy to confirm it points to the production server.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Scheduled Refresh Stops Working After Every Production Deploy

**Scenario:** Your team deploys to Production every Friday evening. Every Saturday morning, the on-call analyst reports that the scheduled refresh on the Production dataset did not run. Investigation shows the dataset refresh schedule is gone after each deploy. Users are getting stale data on Monday morning. How do you solve this without requiring manual reconfiguration after every deploy?

<details>
<summary>💡 Hint</summary>

Power BI Deployment Pipelines do not copy scheduled refresh settings. Think about how you could automate the restoration of refresh settings as part of the deployment pipeline using the REST API.

</details>

<details>
<summary>✅ Solution</summary>

**Root cause:** Deployment Pipelines copy the dataset definition but **not** the scheduled refresh configuration or gateway bindings. Each deploy to Production overwrites the dataset, resetting its refresh settings to none.

**Solution — automate refresh schedule configuration as a post-deploy step in your CI/CD pipeline:**

```powershell
# Run after the deployment pipeline promote call succeeds

$headers = @{ Authorization = "Bearer $token"; "Content-Type" = "application/json" }

# 1. Re-bind the dataset to the on-premise data gateway
$gatewayBody = @{
    gatewayObjectId = $gatewayId
    datasourceObjectIds = @($datasourceId)
} | ConvertTo-Json

Invoke-RestMethod `
    -Uri "https://api.powerbi.com/v1.0/myorg/groups/$prodWorkspaceId/datasets/$datasetId/Default.BindToGateway" `
    -Method POST -Headers $headers -Body $gatewayBody

# 2. Update credentials for the datasource
$credBody = @{
    credentialDetails = @{
        credentialType       = "Basic"
        credentials          = '{"credentialData":[{"name":"username","value":"svc_pbi"},{"name":"password","value":"'"$dbPassword"'"}]}'
        encryptedConnection  = "Encrypted"
        encryptionAlgorithm  = "None"
        privacyLevel         = "Organizational"
    }
} | ConvertTo-Json -Depth 5

Invoke-RestMethod `
    -Uri "https://api.powerbi.com/v1.0/myorg/gateways/$gatewayId/datasources/$datasourceId" `
    -Method PATCH -Headers $headers -Body $credBody

# 3. Set the refresh schedule
$scheduleBody = @{
    value = @{
        enabled            = $true
        days               = @("Sunday","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday")
        times              = @("06:00", "12:00", "18:00")
        localTimeZoneId    = "Eastern Standard Time"
        notifyOption       = "MailOnFailure"
    }
} | ConvertTo-Json -Depth 5

Invoke-RestMethod `
    -Uri "https://api.powerbi.com/v1.0/myorg/groups/$prodWorkspaceId/datasets/$datasetId/refreshSchedule" `
    -Method PATCH -Headers $headers -Body $scheduleBody

Write-Host "Post-deploy configuration complete."
```

Store `$gatewayId`, `$datasourceId`, and credentials in Azure Key Vault, not in the pipeline script. This script becomes a reusable step in your Azure DevOps deployment pipeline that runs immediately after every Production promote — eliminating manual Saturday morning reconfiguration.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing a Zero-Downtime Power BI Deployment Strategy for 5,000 Users

**Scenario:** Your organization has 5,000 Power BI users consuming a central `SalesAnalytics` dataset that refreshes every 2 hours. The BI team wants to deploy a major schema change (adding 3 new tables, modifying 2 measures) without causing report errors or data unavailability during the 9am–5pm business window. Deployments currently require a full dataset refresh (45 minutes). How do you design a deployment strategy that meets these constraints?

<details>
<summary>💡 Hint</summary>

Consider how you can decouple the schema deployment from the user-facing content. Think about Blue/Green deployment patterns and whether Power BI supports any equivalent. Also consider the relationship between deployment pipeline promotion and XMLA endpoint capabilities.

</details>

<details>
<summary>✅ Solution</summary>

**Strategy: Blue/Green Deployment with a Report-Binding Switch**

The core insight is that you can maintain two Production-equivalent datasets simultaneously and switch reports over atomically once the new version is validated.

**Architecture:**

```
┌─────────────────────────────────────────────────────────────────────┐
│  Production Workspace                                                │
│                                                                     │
│  SalesAnalytics-Blue  (current live, serving 5,000 users)           │
│  SalesAnalytics-Green (new schema being deployed and refreshed)     │
│                                                                     │
│  All reports ──bind to──► SalesAnalytics-Blue (until switch)        │
└─────────────────────────────────────────────────────────────────────┘
```

**Step-by-step process:**

**Phase 1 — Deploy new schema to Green (off-hours or any time, no user impact):**

```powershell
# Deploy schema to Green dataset via XMLA (does not affect Blue)
TabularEditor.exe "SalesAnalytics-v2.bim" `
    -S "powerbi://api.powerbi.com/v1.0/myorg/Sales%20Prod" `
    -D "SalesAnalytics-Green" `
    -O -C
```

**Phase 2 — Refresh Green dataset and run regression tests:**

```powershell
# Trigger full refresh on Green
Invoke-RestMethod `
    -Uri "https://api.powerbi.com/v1.0/myorg/groups/$prodWorkspaceId/datasets/$greenDatasetId/refreshes" `
    -Method POST -Headers $headers -Body '{"type":"full"}'

# Wait for completion (poll), then run DAX regression tests against Green
# (same test suite as used in Test stage)
```

**Phase 3 — Atomic rebind of all reports from Blue to Green (2-minute window):**

```powershell
# Get all reports in the workspace
$reports = Invoke-RestMethod `
    -Uri "https://api.powerbi.com/v1.0/myorg/groups/$prodWorkspaceId/reports" `
    -Headers $headers

# Rebind each report that uses the Blue dataset to Green
$reportsToRebind = $reports.value | Where-Object { $_.datasetId -eq $blueDatasetId }
foreach ($report in $reportsToRebind) {
    $bindBody = @{ datasetId = $greenDatasetId } | ConvertTo-Json
    Invoke-RestMethod `
        -Uri "https://api.powerbi.com/v1.0/myorg/groups/$prodWorkspaceId/reports/$($report.id)/Rebind" `
        -Method POST -Headers $headers -Body $bindBody
}
Write-Host "Switched $($reportsToRebind.Count) reports from Blue to Green."
```

Users refreshing their reports after the rebind immediately see the new dataset. There is no full-refresh downtime because Green was already loaded.

**Phase 4 — Rename and retire:**

```powershell
# Rename Green → Blue (new canonical), rename old Blue → Archive
# Update dataset names via REST API, keep old Blue for 48h rollback window
```

**Rollback:** If issues are found post-switch, rebind all reports back to the original Blue dataset in the same way — takes under 2 minutes regardless of dataset size.

**Why not just use the standard pipeline deploy?**  
Standard deployment pipelines overwrite the dataset in place. During the 45-minute refresh that follows, users with auto-refreshing dashboards see stale or errored data. The Blue/Green approach eliminates this by keeping the live dataset untouched until the new one is fully ready.

</details>

</article>
