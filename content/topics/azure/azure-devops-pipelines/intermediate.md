---
title: "Azure DevOps Pipelines - Intermediate"
topic: azure
subtopic: azure-devops-pipelines
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [azure, devops, ci-cd, adf, arm-templates, databricks, dbt, environment-gates]
---

# Azure DevOps Pipelines — Intermediate

## ADF CI/CD: ARM Template Deployment Pattern

Azure Data Factory **does not have native CI/CD**. The standard pattern is to manage ADF config in Git (ADF Git integration) and deploy via ARM templates exported from the `adf_publish` branch.

### ADF Git Branching Model

```
developer/feature-branch  →  PR  →  main (collaboration branch)
                                      │
                                      │ ADF Studio publishes
                                      ▼
                              adf_publish branch
                                (auto-generated ARM templates)
                                      │
                              Azure DevOps Pipeline picks up
                              ARMTemplateForFactory.json
                              and deploys to staging/prod
```

### ADF CD Pipeline YAML

```yaml
# azure-pipelines-adf-cd.yml

trigger:
  branches:
    include:
      - adf_publish   # Trigger when ADF publishes new ARM templates

pool:
  vmImage: ubuntu-latest

variables:
  - group: ADF-Deployment-Secrets
  # Contains: ARM_SUBSCRIPTION_ID, ARM_RESOURCE_GROUP, ADF_NAME

stages:
  - stage: Deploy_Staging
    displayName: Deploy to Staging
    jobs:
      - deployment: DeployADF
        displayName: Deploy ADF ARM Template
        environment: staging    # Named environment with approval gate
        strategy:
          runOnce:
            deploy:
              steps:
                # Stop ADF triggers before deployment (avoid in-flight run conflicts)
                - task: AzurePowerShell@5
                  displayName: Stop ADF Triggers
                  inputs:
                    azureSubscription: $(SERVICE_CONNECTION)
                    ScriptType: InlineScript
                    Inline: |
                      $triggers = Get-AzDataFactoryV2Trigger `
                        -ResourceGroupName "$(ARM_RESOURCE_GROUP)" `
                        -DataFactoryName "$(ADF_NAME)-staging"
                      $triggers | Stop-AzDataFactoryV2Trigger -Force

                # Deploy ARM template
                - task: AzureResourceManagerTemplateDeployment@3
                  displayName: Deploy ADF ARM Template
                  inputs:
                    deploymentScope: Resource Group
                    azureResourceManagerConnection: $(SERVICE_CONNECTION)
                    subscriptionId: $(ARM_SUBSCRIPTION_ID)
                    action: Create Or Update Resource Group
                    resourceGroupName: $(ARM_RESOURCE_GROUP)
                    location: East US
                    templateLocation: Linked artifact
                    csmFile: '$(Pipeline.Workspace)/adf_publish/ARMTemplateForFactory.json'
                    csmParametersFile: '$(Pipeline.Workspace)/adf_publish/ARMTemplateParametersForFactory.json'
                    overrideParameters: |
                      -factoryName "$(ADF_NAME)-staging"
                      -AzureKeyVault_properties_typeProperties_baseUrl "$(KV_URL_STAGING)"

                # Restart triggers after deployment
                - task: AzurePowerShell@5
                  displayName: Start ADF Triggers
                  inputs:
                    azureSubscription: $(SERVICE_CONNECTION)
                    ScriptType: InlineScript
                    Inline: |
                      Get-AzDataFactoryV2Trigger `
                        -ResourceGroupName "$(ARM_RESOURCE_GROUP)" `
                        -DataFactoryName "$(ADF_NAME)-staging" |
                      Start-AzDataFactoryV2Trigger -Force

  - stage: Deploy_Production
    displayName: Deploy to Production
    dependsOn: Deploy_Staging
    condition: succeeded()
    jobs:
      - deployment: DeployADFProd
        environment: production   # Requires manual approval
        strategy:
          runOnce:
            deploy:
              steps:
                - task: AzureResourceManagerTemplateDeployment@3
                  displayName: Deploy ADF to Production
                  inputs:
                    azureResourceManagerConnection: $(SERVICE_CONNECTION_PROD)
                    resourceGroupName: $(ARM_RESOURCE_GROUP_PROD)
                    csmFile: '$(Pipeline.Workspace)/adf_publish/ARMTemplateForFactory.json'
                    overrideParameters: '-factoryName "$(ADF_NAME)-prod"'
```

---

## Environment Gates and Approval Workflows

**Environments** in Azure DevOps define deployment targets with configurable checks and approvals.

### Creating Environments with Approvals (Azure CLI)

```bash
# Environments are created in the Azure DevOps UI or via REST API
# UI: Pipelines → Environments → New environment → Add approvers

# REST API: add approval check to environment
curl -X POST \
  "https://dev.azure.com/{org}/{project}/_apis/pipelines/checks/configurations?api-version=7.1-preview.1" \
  -H "Authorization: Basic $(echo -n :$PAT | base64)" \
  -H "Content-Type: application/json" \
  -d '{
    "type": { "id": "8c6f20a7-a545-4486-9777-f762fafe0d4e" },
    "settings": {
      "approvers": [{ "id": "<aad-user-id>" }],
      "instructions": "Verify staging test results before approving production deploy",
      "minRequiredApprovers": 1,
      "requesterCannotBeApprover": true
    },
    "resource": { "type": "environment", "id": "<environment-id>" }
  }'
```

### Gate Types

| Gate Type | Description |
|-----------|-------------|
| Manual approval | Named person(s) must click Approve |
| Business hours | Only deploy during business hours (avoid 2am deploys) |
| Branch policy | Require PR merge before deployment |
| Query Azure Monitor | Block deploy if active alert exists in the target environment |
| REST API gate | Call external API (run smoke tests, check SLA status) |

---

## Databricks CI/CD with dbx and Databricks CLI

### Pattern: Notebook + Job CI/CD

```yaml
# azure-pipelines-databricks.yml

variables:
  - group: DatabricksSecrets  # Contains DATABRICKS_HOST, DATABRICKS_TOKEN

stages:
  - stage: CI
    jobs:
      - job: ValidateNotebooks
        steps:
          - task: UsePythonVersion@0
            inputs:
              versionSpec: '3.11'

          - script: pip install databricks-cli nbconvert pytest
            displayName: Install tools

          # Lint notebooks using nbconvert → Python conversion
          - script: |
              for notebook in $(find notebooks/ -name "*.py" -o -name "*.ipynb"); do
                echo "Linting $notebook"
                python -m py_compile "$notebook" 2>&1
              done
            displayName: Syntax check notebooks

          # Run unit tests (for shared utility modules)
          - script: pytest src/tests/ -v --junitxml=test-results.xml
            displayName: Unit tests for shared modules

          - task: PublishTestResults@2
            inputs:
              testResultsFiles: 'test-results.xml'

  - stage: Deploy_Staging
    dependsOn: CI
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    jobs:
      - deployment: DeployToDatabricks
        environment: staging
        strategy:
          runOnce:
            deploy:
              steps:
                - script: pip install databricks-cli
                  displayName: Install Databricks CLI

                - script: |
                    # Configure Databricks CLI
                    databricks configure --token <<EOF
                    $(DATABRICKS_HOST_STAGING)
                    $(DATABRICKS_TOKEN_STAGING)
                    EOF
                  displayName: Configure Databricks CLI

                - script: |
                    # Upload notebooks to workspace
                    databricks workspace import_dir \
                      notebooks/ \
                      /Shared/pipelines/ \
                      --overwrite \
                      --format SOURCE
                  displayName: Deploy notebooks

                - script: |
                    # Update Databricks job definition
                    databricks jobs reset \
                      --job-id $(DBX_JOB_ID_STAGING) \
                      --json-file jobs/transform_orders.json
                  displayName: Update job configuration
```

### Using dbx for Multi-Environment Job Management

```yaml
# .dbx/project.json — project definition
{
  "environments": {
    "staging": {
      "profile": "staging",
      "workspace_dir": "/Shared/pipelines/staging"
    },
    "production": {
      "profile": "prod",
      "workspace_dir": "/Shared/pipelines/prod"
    }
  }
}
```

```yaml
# conf/deployment.yaml — job specification
environments:
  staging:
    workflows:
      - name: "transform_orders"
        job_clusters:
          - job_cluster_key: default
            new_cluster:
              spark_version: "13.3.x-scala2.12"
              node_type_id: Standard_DS3_v2
              num_workers: 4
        tasks:
          - task_key: transform
            job_cluster_key: default
            notebook_task:
              notebook_path: /Shared/pipelines/staging/transform_orders
              base_parameters:
                env: staging
```

```yaml
# In Azure DevOps pipeline:
- script: |
    pip install dbx
    dbx deploy --environment staging
    dbx launch --environment staging --job transform_orders --trace
  displayName: Deploy and run Databricks job
  env:
    DATABRICKS_HOST: $(DATABRICKS_HOST_STAGING)
    DATABRICKS_TOKEN: $(DATABRICKS_TOKEN_STAGING)
```

---

## dbt CI: Advanced Patterns

### Slim CI with State Comparison

The key performance optimisation for dbt CI: only test models that changed relative to the production manifest:

```yaml
# azure-pipelines-dbt-ci.yml — production-grade

steps:
  # Download the production manifest.json (from last prod dbt run)
  - task: AzureCLI@2
    displayName: Download prod manifest
    inputs:
      azureSubscription: $(SERVICE_CONNECTION)
      scriptType: bash
      inlineScript: |
        az storage blob download \
          --account-name $(STORAGE_ACCOUNT) \
          --container-name dbt-artifacts \
          --name prod/manifest.json \
          --file manifest.json \
          --auth-mode login

  # Run dbt test on only changed models + downstream
  - script: |
      cd dbt/
      dbt deps
      dbt seed --target ci --full-refresh
      dbt run --target ci --select state:modified+  --state ..
      dbt test --target ci --select state:modified+  --state ..
    displayName: Slim CI dbt run
    env:
      SNOWFLAKE_PASSWORD: $(SNOWFLAKE_CI_PASSWORD)

  # Upload new manifest as candidate for next comparison
  - task: AzureCLI@2
    displayName: Upload CI manifest
    condition: succeeded()
    inputs:
      azureSubscription: $(SERVICE_CONNECTION)
      scriptType: bash
      inlineScript: |
        az storage blob upload \
          --account-name $(STORAGE_ACCOUNT) \
          --container-name dbt-artifacts \
          --name ci/$(Build.BuildId)/manifest.json \
          --file dbt/target/manifest.json
```

---

## Key Vault Integration in Pipelines

Never hard-code secrets. The recommended pattern is **Key Vault-linked Variable Groups**:

```
Azure Key Vault
├── SNOWFLAKE-PASSWORD     → maps to variable SNOWFLAKE_PASSWORD
├── DATABRICKS-TOKEN       → maps to variable DATABRICKS_TOKEN
└── DBT-CLOUD-API-KEY      → maps to variable DBT_CLOUD_API_KEY

Azure DevOps Variable Group (Key Vault linked)
└── Links to above secrets → available as $(SNOWFLAKE_PASSWORD) in pipelines
```

For Key Vault access from pipelines, grant the Azure DevOps service principal **Key Vault Secrets User** RBAC role:

```bash
az role assignment create \
  --role "Key Vault Secrets User" \
  --assignee "<devops-service-principal-object-id>" \
  --scope "/subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.KeyVault/vaults/{kv_name}"
```
