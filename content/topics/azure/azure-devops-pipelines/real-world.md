---
title: "Azure DevOps Pipelines - Real-World Patterns"
topic: azure
subtopic: azure-devops-pipelines
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [azure, devops, ci-cd, data-engineering, production, adf, databricks, dbt]
---

# Azure DevOps Pipelines — Real-World Patterns

## The Monorepo Data Platform CI/CD Pattern

Large data engineering teams often use a **monorepo** — all data platform code in one repository — with Azure Pipelines path filters to trigger different pipelines based on what changed.

### Monorepo Structure

```
data-platform/
├── adf/                          # ADF ARM templates (from adf_publish)
│   └── ARMTemplateForFactory.json
├── dbt/                          # dbt project
│   ├── models/
│   ├── tests/
│   └── dbt_project.yml
├── databricks/                   # Databricks notebooks and jobs
│   ├── notebooks/
│   └── jobs/
├── terraform/                    # Infrastructure as code
│   ├── main.tf
│   └── environments/
├── azure-pipelines/              # Pipeline YAML files
│   ├── dbt-ci.yml
│   ├── adf-cd.yml
│   ├── databricks-cd.yml
│   └── terraform-cd.yml
└── azure-pipelines.yml           # Root pipeline router
```

### Root Pipeline Router

```yaml
# azure-pipelines.yml — root file that routes to specialised pipelines

trigger:
  branches:
    include: [main]

pr:
  branches:
    include: [main]

pool:
  vmImage: ubuntu-latest

# Path-based pipeline triggering
resources:
  pipelines:
    - pipeline: dbt-ci
      source: dbt-ci-pipeline
      trigger:
        branches:
          include: [main]
        paths:
          include: [dbt/]

    - pipeline: adf-cd
      source: adf-cd-pipeline
      trigger:
        branches:
          include: [adf_publish]

steps:
  - script: echo "Root pipeline — individual sub-pipelines handle specific components"
```

---

## Real Production Pipeline: dbt + ADF + Databricks Release Train

A common enterprise pattern is the **"release train"** — a weekly (or biweekly) coordinated deployment of all data platform components together.

### Release Pipeline YAML

```yaml
# azure-pipelines/release-train.yml

parameters:
  - name: targetEnvironment
    displayName: Target Environment
    type: string
    default: staging
    values:
      - staging
      - production

  - name: deployDbt
    displayName: Deploy dbt
    type: boolean
    default: true

  - name: deployADF
    displayName: Deploy ADF
    type: boolean
    default: true

  - name: deployDatabricks
    displayName: Deploy Databricks
    type: boolean
    default: true

stages:
  - stage: Pre_Deployment_Checks
    displayName: Pre-Deployment Checks
    jobs:
      - job: HealthCheck
        steps:
          # Check no active ADF pipeline runs before deploying
          - task: AzureCLI@2
            displayName: Check for active ADF runs
            inputs:
              azureSubscription: $(SERVICE_CONNECTION)
              scriptType: bash
              inlineScript: |
                ACTIVE=$(az datafactory pipeline-run query-by-factory \
                  --factory-name "$(ADF_NAME)-${{ parameters.targetEnvironment }}" \
                  --resource-group $(RESOURCE_GROUP) \
                  --filters "[{\"operand\":\"Status\",\"operator\":\"Equals\",\"values\":[\"InProgress\"]}]" \
                  --query "value | length(@)" -o tsv)

                if [ "$ACTIVE" -gt "0" ]; then
                  echo "##vso[task.logissue type=error]$ACTIVE active pipeline runs. Wait for completion before deploying."
                  exit 1
                fi
                echo "No active pipeline runs. Safe to deploy."

  - ${{ if eq(parameters.deployDbt, true) }}:
    - stage: Deploy_dbt
      displayName: Deploy dbt (${{ parameters.targetEnvironment }})
      dependsOn: Pre_Deployment_Checks
      jobs:
        - deployment: dbt_deploy
          environment: ${{ parameters.targetEnvironment }}
          strategy:
            runOnce:
              deploy:
                steps:
                  - script: |
                      pip install dbt-snowflake==1.7.2
                      cd dbt/
                      dbt deps
                      # Run full dbt build for deployment (not slim CI)
                      dbt build --target ${{ parameters.targetEnvironment }} \
                        --full-refresh \
                        --select models/staging/+
                    displayName: Deploy dbt staging models
                    env:
                      SNOWFLAKE_PASSWORD: $(SNOWFLAKE_PASSWORD_${{ upper(parameters.targetEnvironment) }})

  - ${{ if eq(parameters.deployADF, true) }}:
    - stage: Deploy_ADF
      displayName: Deploy ADF (${{ parameters.targetEnvironment }})
      dependsOn: Deploy_dbt
      condition: or(succeeded('Deploy_dbt'), eq('${{ parameters.deployDbt }}', 'false'))
      jobs:
        - deployment: adf_deploy
          environment: ${{ parameters.targetEnvironment }}
          strategy:
            runOnce:
              deploy:
                steps:
                  - task: AzureResourceManagerTemplateDeployment@3
                    inputs:
                      resourceGroupName: $(RESOURCE_GROUP)
                      csmFile: adf/ARMTemplateForFactory.json
                      overrideParameters: '-factoryName "$(ADF_NAME)-${{ parameters.targetEnvironment }}"'

  - ${{ if eq(parameters.deployDatabricks, true) }}:
    - stage: Deploy_Databricks
      displayName: Deploy Databricks (${{ parameters.targetEnvironment }})
      dependsOn: Pre_Deployment_Checks
      jobs:
        - deployment: databricks_deploy
          environment: ${{ parameters.targetEnvironment }}
          strategy:
            runOnce:
              deploy:
                steps:
                  - script: |
                      pip install databricks-cli
                      databricks workspace import_dir notebooks/ /Shared/pipelines/ \
                        --overwrite --format SOURCE
                      databricks jobs reset --job-id $(DBX_JOB_ID) \
                        --json-file databricks/jobs/main.json
                    displayName: Deploy Databricks notebooks and jobs
                    env:
                      DATABRICKS_HOST: $(DATABRICKS_HOST_${{ upper(parameters.targetEnvironment) }})
                      DATABRICKS_TOKEN: $(DATABRICKS_TOKEN_${{ upper(parameters.targetEnvironment) }})

  - stage: Post_Deployment_Smoke_Test
    displayName: Smoke Tests
    dependsOn:
      - Deploy_ADF
      - Deploy_Databricks
      - Deploy_dbt
    condition: always()
    jobs:
      - job: SmokeTests
        steps:
          - script: |
              python smoke_tests/run.py \
                --environment ${{ parameters.targetEnvironment }} \
                --timeout 300
            displayName: Run smoke tests
            env:
              SNOWFLAKE_PASSWORD: $(SNOWFLAKE_PASSWORD_${{ upper(parameters.targetEnvironment) }})
```

---

## Handling Secrets Rotation Without Pipeline Downtime

A real operational challenge: rotating database passwords without breaking CI/CD pipelines.

### Pattern: Key Vault Secret Versioning

```bash
# Step 1: Add new secret version (don't delete old yet)
az keyvault secret set \
  --vault-name "data-platform-kv" \
  --name "SNOWFLAKE-CI-PASSWORD" \
  --value "new-password-xyz"

# Step 2: Key Vault Variable Groups automatically pick up latest version
# (no pipeline change needed if variable group uses "Latest" version)

# Step 3: Verify pipelines work with new secret
# Trigger CI pipeline manually and confirm success

# Step 4: Rotate at source (update Snowflake password)
# Step 5: Optionally expire old secret version
az keyvault secret set-attributes \
  --vault-name "data-platform-kv" \
  --name "SNOWFLAKE-CI-PASSWORD" \
  --version "<old-version-id>" \
  --expires "$(date -u -d '+1 day' '+%Y-%m-%dT%H:%M:%SZ')"
```

### Pattern: Managed Identity for Pipelines (No Secrets Needed)

For self-hosted agents running in Azure, use **Managed Identity** to eliminate secrets entirely:

```yaml
# Service connections using managed identity (no client secret rotation needed)
- task: AzureCLI@2
  inputs:
    azureSubscription: 'managed-identity-connection'  # Uses agent VM's managed identity
    scriptType: bash
    inlineScript: |
      # Authenticates via managed identity — no token in YAML
      az storage blob download \
        --account-name $(STORAGE_ACCOUNT) \
        --container-name artifacts \
        --name manifest.json \
        --auth-mode login   # Uses managed identity
```

---

## Production Incident: Concurrent ADF Deployment Corruption

**What happened**: Two engineers both merged PRs within seconds of each other. Both triggered the ADF deployment pipeline simultaneously. The first deployment started, stopped triggers, and began writing ARM resources. The second deployment also stopped triggers and deployed, overwriting some resources mid-way through the first deployment. Result: ADF in corrupted state, triggers in inconsistent state.

**Root cause**: No concurrency control on the CD pipeline.

**Fix 1: Pipeline Concurrency Limit**

```yaml
# In azure-pipelines.yml
resources:
  pipelines:
    - pipeline: adf-cd

# Limit to 1 concurrent run — new run waits for previous to complete
concurrency:
  group: adf-production-deploy
  cancelPrevious: false   # false = queue, true = cancel previous
```

**Fix 2: Deployment Lock via Azure Resource Manager**

```bash
# Acquire a resource lock at start of deployment, release at end
az lock create \
  --name "adf-deploy-lock" \
  --resource-group $(RESOURCE_GROUP) \
  --lock-type CanNotDelete \
  --notes "Deployment in progress by pipeline $(Build.BuildId)"

# ... do deployment ...

az lock delete \
  --name "adf-deploy-lock" \
  --resource-group $(RESOURCE_GROUP)
```

---

## Interview Talking Points

**"Walk me through your ADF CI/CD process."**

Structure your answer as: Git integration → adf_publish branch → Azure DevOps pipeline triggers → ARM template deployment per environment → Key Vault parameter overrides → stop-deploy-start trigger pattern → environment gates for prod.

**"How do you test data engineering code before deploying to production?"**

Three layers:
1. **Unit tests** (Python/pytest): test transformation logic in isolation with mocked DataFrames.
2. **dbt slim CI**: `dbt build --select state:modified+` on a CI schema — tests changed models on real infrastructure.
3. **Integration tests / smoke tests**: after deployment to staging, run end-to-end pipeline and assert output row counts and key values.

**"What's the difference between a CI pipeline and a CD pipeline for data engineering?"**

- CI: fast feedback on PRs. Runs tests, linting, compilation. Must be fast (< 10 minutes). No writes to production.
- CD: deploys to environments on merge. Includes approval gates, ARM deployments, trigger management. Can take 20–30 minutes for a full deploy.
