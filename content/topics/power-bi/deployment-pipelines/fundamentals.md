---
title: "Power BI Deployment Pipelines - Fundamentals"
topic: power-bi
subtopic: deployment-pipelines
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [power-bi, deployment, ci-cd, devops]
---

# Power BI Deployment Pipelines — Fundamentals

## What Are Deployment Pipelines?

Power BI Deployment Pipelines is a built-in ALM (Application Lifecycle Management) tool in the Power BI service that lets you manage the lifecycle of Power BI content across **development**, **test**, and **production** stages. Each stage maps to a Power BI workspace, and you promote content between stages rather than manually republishing reports.

**Key requirement:** Deployment Pipelines require a **Power BI Premium** (per-capacity or per-user) license.

---

## The Three-Stage Model

```
┌──────────────┐      Promote      ┌──────────────┐      Promote      ┌──────────────┐
│ Development  │  ──────────────►  │     Test     │  ──────────────►  │  Production  │
│  Workspace   │                   │  Workspace   │                   │  Workspace   │
└──────────────┘                   └──────────────┘                   └──────────────┘
  Developers                         QA / UAT                          Business Users
  build here                         validate here                     consume here
```

| Stage       | Purpose                                          | Audience              |
|-------------|--------------------------------------------------|-----------------------|
| Development | Active development, experimental changes         | Developers            |
| Test        | UAT, stakeholder validation, performance testing | QA, business analysts |
| Production  | Stable, verified content for end users           | Business users        |

---

## Content Types That Can Be Promoted

- Reports
- Dashboards
- Datasets (semantic models)
- Dataflows (Gen1)
- Paginated reports
- Datamarts

---

## Creating a Deployment Pipeline

1. In the Power BI service, go to **Deployment Pipelines** in the left navigation.
2. Click **Create a pipeline** and give it a name.
3. Assign an existing workspace to each stage (Development, Test, Production).
   - Each workspace must already exist.
   - Each workspace must be in a Premium capacity.
4. Click **Deploy** to promote content from one stage to the next.

---

## The Promote Flow

When you click **Deploy** from Development → Test:

1. Power BI compares what is in Development vs. what is in Test.
2. Items that differ are flagged with a change indicator.
3. You choose which items to include in this deployment.
4. Power BI copies those items into the Test workspace, overwriting existing versions.

The source stage content is **never modified** — only the destination stage is updated.

---

## Comparing Content Across Stages

The pipeline view shows a side-by-side comparison:

- **New** — item exists in source but not in destination
- **Different** — item exists in both stages but has been modified
- **Same** — no changes detected since last deployment

---

## Key Interview Points (Junior Level)

- Deployment Pipelines are for **promoting**, not for version control. Git provides version control.
- Each stage is just a **regular Power BI workspace** — the pipeline is an organizational layer on top.
- Deploying does **not** move data — it moves the metadata (report definitions, dataset schemas). The actual data must be refreshed separately.
- Deployment Pipelines are only available in the **Power BI service** (not Desktop).
- You need at least **Admin** or **Member** role on both workspaces to deploy between them.

---

## Common Junior Interview Questions

**Q: What is the purpose of a deployment pipeline in Power BI?**  
A: To separate development, testing, and production environments so that changes can be validated before reaching business users. It prevents untested changes from breaking production reports.

**Q: Can you use deployment pipelines with a free Power BI license?**  
A: No. A Power BI Premium Per User (PPU) or Premium Per Capacity license is required.

**Q: Does deploying a dataset copy the data?**  
A: No. Deploying copies the dataset schema and configuration (tables, relationships, measures, connection settings). The data itself must be refreshed after deployment.

**Q: What happens if a report in Test was modified directly (not through the pipeline)?**  
A: The next time you deploy from Development to Test, those manual changes in Test will be **overwritten** by the Development version. Direct edits in downstream stages should be avoided.
