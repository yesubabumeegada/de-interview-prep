---
title: "Azure DevOps Pipelines - Fundamentals"
topic: azure
subtopic: azure-devops-pipelines
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [azure, devops, ci-cd, yaml-pipelines, data-engineering, dbt, adf]
---

# Azure DevOps Pipelines — Fundamentals

## 🎯 Analogy

Think of Azure DevOps Pipelines as an automated assembly line for your code. Just as a car factory has inspection stations (tests), assembly stages (builds), and quality gates before a car ships, a CI/CD pipeline runs tests, builds artifacts, and enforces approval gates before deploying to production — all triggered automatically when you push code.

---

## What Is Azure DevOps Pipelines?

**Azure DevOps Pipelines** is a cloud-hosted CI/CD platform that automates building, testing, and deploying applications. For data engineers, it's used to automate:
- **dbt CI**: run `dbt test` on every pull request to catch quality regressions
- **ADF deployments**: deploy ARM templates for Azure Data Factory changes
- **Databricks CI/CD**: validate and publish notebooks and jobs
- **Infrastructure changes**: deploy Terraform or Bicep for data platform infrastructure

### Pipeline Types

| Type | Description | Common Use in DE |
|------|-------------|-----------------|
| CI Pipeline | Triggered on PR/commit; runs tests, builds | dbt test on PR, lint Spark code |
| CD Pipeline | Triggered on merge/tag; deploys to environment | ADF ARM deploy, DBX job update |
| Scheduled Pipeline | Runs on cron schedule | Weekly dependency vulnerability scan |
| Multi-stage Pipeline | CI + CD in one YAML file with stages | Full deploy flow from test → staging → prod |

---

## YAML Pipeline Structure

Azure Pipelines are defined in YAML (`azure-pipelines.yml` in the repository root by default).

### Core Building Blocks

```yaml
# azure-pipelines.yml — minimum viable pipeline

trigger:
  branches:
    include:
      - main        # Run this pipeline when code is pushed to main

pool:
  vmImage: ubuntu-latest   # Hosted agent (Microsoft-managed VM)

variables:
  pythonVersion: '3.11'

stages:
  - stage: CI
    displayName: Continuous Integration
    jobs:
      - job: TestAndBuild
        displayName: Run Tests and Build
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '$(pythonVersion)'
            displayName: Set Python version

          - script: pip install -r requirements.txt
            displayName: Install dependencies

          - script: pytest tests/ --junitxml=test-results.xml
            displayName: Run unit tests

          - task: PublishTestResults@2
            inputs:
              testResultsFiles: 'test-results.xml'
              testRunTitle: 'Unit Tests'
            displayName: Publish test results
```

### Key YAML Sections

```
azure-pipelines.yml
├── trigger / pr          → What events start the pipeline
├── pool                  → Where the pipeline runs (hosted or self-hosted agent)
├── variables             → Key-value pairs referenced throughout
├── stages[]              → Top-level groupings (CI, CD)
│   └── jobs[]            → Work units within a stage
│       └── steps[]       → Individual commands and tasks
├── parameters            → Runtime inputs (environment, version)
└── resources             → External repos, containers, pipelines
```

---

## Triggers

```yaml
# CI trigger: run on push to main or feature branches
trigger:
  branches:
    include:
      - main
      - feature/*
  paths:
    include:
      - dbt/           # Only run if dbt files changed
      - src/

# PR trigger: run on pull requests targeting main
pr:
  branches:
    include:
      - main
  autoCancel: true    # Cancel old PR builds when new commits arrive

# Scheduled trigger (cron)
schedules:
  - cron: "0 2 * * *"   # 02:00 UTC daily
    displayName: Nightly dependency scan
    branches:
      include:
        - main
    always: true         # Run even if no code changes
```

---

## Service Connections

A **Service Connection** is an authenticated connection from Azure DevOps to an external service. For data engineering:

| Service Connection Type | Used For |
|------------------------|----------|
| Azure Resource Manager | Deploy to Azure (ADF, Databricks, Synapse) |
| Docker Registry | Push container images |
| GitHub | Access code in GitHub repos |
| SSH | Access on-premises servers |
| Generic (token) | dbt Cloud API, Databricks REST API |

Service connections are created in **Project Settings → Service Connections** and referenced in YAML:

```yaml
- task: AzureCLI@2
  inputs:
    azureSubscription: 'my-azure-service-connection'  # reference by name
    scriptType: bash
    scriptLocation: inlineScript
    inlineScript: |
      az group list
```

---

## Variable Groups and Key Vault Integration

**Variable Groups** store shared variables accessible across multiple pipelines. **Key Vault Variable Groups** link directly to Azure Key Vault, pulling secrets at pipeline runtime — no secrets in YAML files.

```yaml
# Link a variable group in YAML
variables:
  - group: DataPlatformSecrets   # includes DBT_CLOUD_API_KEY, DATABRICKS_TOKEN

# Key Vault-linked variable group setup (in Azure DevOps UI or via API):
# Library → Variable Groups → + Variable Group → Link secrets from Azure Key Vault
# Select Key Vault → Authorise → Add variables by secret name
```

Once linked, secrets are available as `$(SECRET_NAME)` in pipeline steps and are **masked in logs**.

---

## Agents: Hosted vs. Self-Hosted

| | Microsoft-Hosted Agent | Self-Hosted Agent |
|--|------------------------|-------------------|
| Management | Microsoft manages | You manage the VM/container |
| Cost | Included (1 free parallel job) | VM cost only |
| Performance | Fresh VM per run | Persistent, can cache |
| Network | Public internet only | Can access private VNets |
| Use case | Most CI jobs | Private resources (VNET-based Databricks, on-prem) |

For data engineering teams accessing Databricks in a private VNet or ADF integration runtimes in a VNET, a **self-hosted agent** running in that VNet is required.

---

## dbt CI Pipeline: The Fundamentals Version

The most common data engineering use of Azure Pipelines is running dbt tests on pull requests:

```yaml
# azure-pipelines.yml — basic dbt CI

trigger: none   # Only run on PRs (defined below)

pr:
  branches:
    include: [main]

pool:
  vmImage: ubuntu-latest

variables:
  - group: dbt-credentials   # Contains DBT_PROFILES_DIR, warehouse creds

steps:
  - task: UsePythonVersion@0
    inputs:
      versionSpec: '3.11'

  - script: pip install dbt-snowflake==1.7.0
    displayName: Install dbt

  - script: |
      cd dbt/
      dbt deps
      dbt compile --target ci
    displayName: Compile dbt project

  - script: |
      cd dbt/
      dbt test --target ci --select state:modified+
    displayName: Run dbt tests on changed models
    env:
      SNOWFLAKE_ACCOUNT: $(SNOWFLAKE_ACCOUNT)
      SNOWFLAKE_USER: $(SNOWFLAKE_USER)
      SNOWFLAKE_PASSWORD: $(SNOWFLAKE_PASSWORD)
```

> `--select state:modified+` runs tests only on changed models and their downstream dependents — not the entire project. This makes PRs fast even in large dbt projects.

---

## Key Concepts Glossary

| Term | Definition |
|------|-----------|
| Pipeline | Automated workflow defined in YAML |
| Stage | Logical group of jobs (e.g., CI, Staging, Prod) |
| Job | Unit of work that runs on a single agent |
| Step | Individual task or script within a job |
| Task | Pre-built action (e.g., `AzureCLI@2`, `UsePythonVersion@0`) |
| Artifact | File output from a pipeline (e.g., built ARM template, wheel file) |
| Environment | Named deployment target with approval gates |
| Agent | The machine that executes pipeline steps |
| Variable Group | Shared variable store, optionally linked to Key Vault |
| Service Connection | Authenticated connection to external service |
