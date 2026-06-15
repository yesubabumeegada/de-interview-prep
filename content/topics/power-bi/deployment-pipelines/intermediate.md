---
title: "Power BI Deployment Pipelines - Intermediate"
topic: power-bi
subtopic: deployment-pipelines
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [power-bi, deployment, ci-cd, devops]
---

# Power BI Deployment Pipelines — Intermediate

## Deployment Rules

Deployment Rules let you override dataset connection strings and parameter values when content is promoted between stages. This is essential when your Development dataset points to a dev database and your Production dataset must point to a prod database.

### Types of Deployment Rules

| Rule Type            | What It Controls                                        |
|----------------------|---------------------------------------------------------|
| Data source rules    | Override the connection string / server / database      |
| Parameter rules      | Override Power Query / M parameter values               |

### Configuring Deployment Rules

1. In the pipeline, click the **⚙ Deployment rules** icon on the Test or Production stage.
2. Select a dataset.
3. Under **Data source rules**, map source endpoints to stage-specific endpoints.
4. Under **Parameter rules**, assign different values for named parameters.

```
Development Dataset             Production Deployment Rule
────────────────────            ────────────────────────────────────
Server: dev-sql.corp.com    →   Server: prod-sql.corp.com
Database: FinanceDev        →   Database: FinanceProd
```

### Parameter-Based Connection Switching (Best Practice)

Define M parameters in Power Query and reference them in the data source step:

```m
// In Power Query — Manage Parameters
ServerName    = "dev-sql.corp.com"     // type: Text
DatabaseName  = "FinanceDev"           // type: Text

// In the query source step
Source = Sql.Database(ServerName, DatabaseName)
```

Then in the deployment rule for Production, override `ServerName` → `prod-sql.corp.com` and `DatabaseName` → `FinanceProd`. This keeps the report logic identical across stages while switching connections automatically on deploy.

---

## Dataset Lineage and Dependency Awareness

When you deploy a **report** that depends on a **dataset**, the pipeline shows dependencies. If the dataset has not been deployed yet (or is out of sync), the report deployment may fail or connect to the wrong dataset.

**Best practice:** always deploy the **dataset first**, then the **report**.

---

## Partial Deployments

You do not have to promote all items in a stage at once. You can select individual items:

- Deploy only a single dataset (e.g., to push a schema change for testing)
- Deploy only specific reports (e.g., after a copy-writing update with no data model change)
- Exclude dashboards from a deploy if they are still in flux

---

## Access Management Per Stage

Each stage workspace has its own access control. Common setup:

| Role          | Development | Test     | Production |
|---------------|-------------|----------|------------|
| Data Engineers | Admin      | Member   | Viewer     |
| QA Analysts   | Viewer      | Member   | Viewer     |
| End Users     | No access   | Viewer   | Viewer     |

Restricting production workspace membership prevents accidental direct edits in production.

---

## Pipeline History and Audit

The Deployment Pipeline tracks deployment history:

- Who deployed
- When
- Which stage (Dev → Test, Test → Prod)
- Which items were included

This audit trail is accessible under **Deployment history** in the pipeline view and is also available via the Power BI Admin portal and Microsoft Purview.

---

## Comparing Datasets Across Stages

The pipeline compare feature highlights:

- Schema differences (new/removed tables, columns, measures)
- Modified measures or relationships
- Data source configuration changes

This comparison helps QA teams understand exactly what changed before approving a production deployment.

---

## Dataflow Deployment Considerations

Dataflows (Gen1) have stage-specific dataflow deployment rules similar to datasets:

- Override the output destination (e.g., different Azure Data Lake Storage account per stage)
- Override connection credentials per stage

Dataflow Gen2 (in Fabric) follows the same pipeline model but with Fabric-native workspace promotion instead.

---

## Common Mid-Level Interview Questions

**Q: What is the difference between a data source rule and a parameter rule in deployment pipelines?**  
A: A data source rule overrides the connection endpoint (server, database) at the pipeline level for a specific stage. A parameter rule overrides named Power Query parameters. Both achieve the same result, but parameter rules require the dataset to expose M parameters; data source rules work directly with the connection metadata. Parameter rules are more explicit and easier to audit.

**Q: How do you ensure a report always connects to the correct dataset after deployment?**  
A: The pipeline automatically rebinds a report to the dataset in the same stage. If the dataset is named `SalesModel` in Development, the pipeline creates/updates `SalesModel` in Test and the Test report binds to the Test copy — it does not point back to the Development workspace.

**Q: What happens to scheduled refresh after a dataset is deployed to Production?**  
A: Scheduled refresh configuration is **not** copied by deployment pipelines. You must configure the refresh schedule separately in each stage's workspace. Deployment rules handle connection credentials, but refresh schedules and gateway mappings must be set manually (or via REST API/PowerShell automation).
