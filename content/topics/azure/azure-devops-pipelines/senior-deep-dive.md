---
title: "Azure DevOps Pipelines - Senior Deep Dive"
topic: azure
subtopic: azure-devops-pipelines
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [azure, devops, ci-cd, platform-engineering, pipeline-templates, governance, security]
---

# Azure DevOps Pipelines — Senior Deep Dive

## Platform Engineering: Reusable Pipeline Templates

At senior level, you build CI/CD infrastructure that other teams consume — not one-off pipelines. The key pattern is **YAML pipeline templates** stored in a central repository.

### Template Repository Structure

```
azure-devops-templates/  (shared repo)
├── stages/
│   ├── dbt-ci.yml              # Reusable dbt CI stage
│   ├── adf-deploy.yml          # Reusable ADF deployment stage
│   └── databricks-deploy.yml   # Reusable Databricks deployment stage
├── jobs/
│   ├── python-lint.yml
│   └── terraform-plan.yml
├── steps/
│   ├── install-dbt.yml
│   ├── configure-databricks-cli.yml
│   └── download-prod-manifest.yml
└── README.md
```

### Parameterised Stage Template

```yaml
# stages/dbt-ci.yml — reusable template

parameters:
  - name: dbtVersion
    type: string
    default: '1.7.0'
  - name: dbtAdapter
    type: string
    default: 'dbt-snowflake'
  - name: dbtProjectDir
    type: string
    default: 'dbt/'
  - name: dbtTarget
    type: string
    default: 'ci'
  - name: serviceConnection
    type: string
  - name: storageAccount
    type: string
  - name: credentialsVariableGroup
    type: string

stages:
  - stage: dbt_CI
    displayName: dbt CI
    variables:
      - group: ${{ parameters.credentialsVariableGroup }}
    jobs:
      - job: dbt_test
        pool:
          vmImage: ubuntu-latest
        steps:
          - script: pip install ${{ parameters.dbtAdapter }}==${{ parameters.dbtVersion }}
            displayName: Install dbt

          - task: AzureCLI@2
            displayName: Download prod manifest
            inputs:
              azureSubscription: ${{ parameters.serviceConnection }}
              scriptType: bash
              inlineScript: |
                az storage blob download \
                  --account-name ${{ parameters.storageAccount }} \
                  --container-name dbt-artifacts \
                  --name prod/manifest.json \
                  --file manifest.json \
                  --output none 2>/dev/null || echo "No prod manifest found, running full test"

          - script: |
              cd ${{ parameters.dbtProjectDir }}
              dbt deps
              if [ -f ../manifest.json ]; then
                dbt build --target ${{ parameters.dbtTarget }} \
                  --select state:modified+ \
                  --state ..
              else
                dbt build --target ${{ parameters.dbtTarget }}
              fi
            displayName: dbt build (slim CI)
            env:
              SNOWFLAKE_PASSWORD: $(SNOWFLAKE_CI_PASSWORD)

          - script: |
              cd ${{ parameters.dbtProjectDir }}
              dbt docs generate --target ${{ parameters.dbtTarget }}
            displayName: Generate docs

          - task: PublishPipelineArtifact@1
            inputs:
              targetPath: '${{ parameters.dbtProjectDir }}target/'
              artifact: dbt-artifacts
```

### Consuming the Template from a Product Pipeline

```yaml
# In the data team's azure-pipelines.yml:
resources:
  repositories:
    - repository: templates
      type: git
      name: platform/azure-devops-templates
      ref: refs/tags/v2.1.0   # Pin to version, never use floating main

trigger:
  branches:
    include: [main]

extends:
  template: stages/dbt-ci.yml@templates
  parameters:
    dbtVersion: '1.7.2'
    dbtAdapter: 'dbt-snowflake'
    dbtProjectDir: 'analytics/'
    serviceConnection: 'azure-platform'
    storageAccount: 'companydataplatform'
    credentialsVariableGroup: 'SnowflakeCI'
```

---

## Security Hardening

### 1. Branch Protection + Required Pipelines

Enforce that PRs must pass the CI pipeline before merging:

```
Azure DevOps Branch Policies (main branch):
├── Require a minimum number of reviewers: 2
├── Check for linked work items: required
├── Build validation:
│   └── azure-pipelines-ci.yml must pass
├── Require approval from code owners
└── Limit merge types: Squash merge only
```

### 2. Protected Branches and Environment Deployment Policies

```yaml
# Only allow deployment to production from main branch
- stage: Deploy_Production
  condition: and(
    succeeded('Deploy_Staging'),
    eq(variables['Build.SourceBranch'], 'refs/heads/main')
  )
```

Combine with the **Environment protection rule**: production environment only accepts deployments from `main` branch (configured in Azure DevOps UI → Environments → Security).

### 3. Secret Scanning in Pipelines

```yaml
# Run Gitleaks on every PR to catch accidentally committed secrets
- script: |
    docker run --rm \
      -v $(Build.SourcesDirectory):/path \
      ghcr.io/gitleaks/gitleaks:latest detect \
      --source=/path \
      --report-format=json \
      --report-path=/path/gitleaks-report.json \
      --exit-code=1
  displayName: Scan for leaked secrets
  continueOnError: false
```

### 4. Pipeline Permissions Model

```
Azure DevOps Permissions Hierarchy:
Organization
└── Project
    ├── Pipeline (specific permissions)
    │   ├── Edit: Platform engineers only
    │   ├── Run: All data engineers
    │   └── Manage settings: Team leads only
    ├── Environments
    │   ├── production: Approvers = team lead + SRE
    │   └── staging: Approvers = pipeline auto-approves
    └── Service Connections
        ├── production-azure: Restricted to prod CD pipelines only
        └── staging-azure: Available to all pipelines
```

---

## Advanced Deployment Patterns

### Blue-Green ADF Deployment

```yaml
# azure-pipelines-adf-bluegreen.yml

variables:
  currentSlot: blue      # alternates between blue/green

stages:
  - stage: Deploy_Inactive_Slot
    jobs:
      - deployment: DeployInactiveADF
        steps:
          - script: |
              # Determine inactive slot
              ACTIVE=$(az datafactory show \
                --name "adf-prod-router" \
                --query "tags.activeSlot" -o tsv)

              if [ "$ACTIVE" == "blue" ]; then
                INACTIVE="green"
              else
                INACTIVE="blue"
              fi

              echo "Deploying to inactive slot: $INACTIVE"
              echo "##vso[task.setvariable variable=targetSlot;isOutput=true]$INACTIVE"
            name: DetermineSlot

          - task: AzureResourceManagerTemplateDeployment@3
            displayName: Deploy to inactive ADF
            inputs:
              resourceGroupName: $(ARM_RESOURCE_GROUP)
              csmFile: ARMTemplateForFactory.json
              overrideParameters: '-factoryName "adf-prod-$(DetermineSlot.targetSlot)"'

  - stage: Smoke_Test
    dependsOn: Deploy_Inactive_Slot
    jobs:
      - job: RunSmokeTests
        steps:
          - script: python smoke_tests/run.py --env $(DetermineSlot.targetSlot)

  - stage: Swap_Slots
    dependsOn: Smoke_Test
    condition: succeeded()
    jobs:
      - deployment: SwapTraffic
        environment: production
        steps:
          - script: |
              # Update router tag to point to new active slot
              az resource tag \
                --ids "/subscriptions/.../factories/adf-prod-router" \
                --tags activeSlot=$(DetermineSlot.targetSlot)
```

### Terraform-Managed Data Platform CI/CD

```yaml
# Terraform pipeline for infrastructure changes

stages:
  - stage: Terraform_Plan
    jobs:
      - job: Plan
        steps:
          - task: TerraformInstaller@1
            inputs:
              terraformVersion: '1.6.0'

          - task: TerraformTaskV4@4
            displayName: Terraform Init
            inputs:
              provider: azurerm
              command: init
              backendServiceArm: $(SERVICE_CONNECTION)
              backendAzureRmResourceGroupName: rg-terraform-state
              backendAzureRmStorageAccountName: tfstatecompany
              backendAzureRmContainerName: tfstate
              backendAzureRmKey: data-platform.tfstate

          - task: TerraformTaskV4@4
            displayName: Terraform Plan
            inputs:
              provider: azurerm
              command: plan
              commandOptions: '-out=tfplan -var-file=environments/staging.tfvars'
              environmentServiceNameAzureRM: $(SERVICE_CONNECTION)

          - task: PublishPipelineArtifact@1
            inputs:
              targetPath: tfplan
              artifact: terraform-plan

  - stage: Terraform_Apply
    dependsOn: Terraform_Plan
    jobs:
      - deployment: Apply
        environment: staging
        strategy:
          runOnce:
            deploy:
              steps:
                - task: TerraformTaskV4@4
                  displayName: Terraform Apply
                  inputs:
                    provider: azurerm
                    command: apply
                    commandOptions: '$(Pipeline.Workspace)/terraform-plan/tfplan'
                    environmentServiceNameAzureRM: $(SERVICE_CONNECTION)
```

---

## Pipeline Observability and Governance

### Tracking Deployment Frequency (DORA Metrics)

```python
# Script to extract DORA metrics from Azure DevOps REST API
import requests
from datetime import datetime, timedelta

ORG = "myorg"
PROJECT = "data-platform"
PAT = os.environ["AZURE_DEVOPS_PAT"]

def get_deployment_frequency(pipeline_id: int, days: int = 30):
    """Calculate deployment frequency for a CD pipeline."""
    url = (
        f"https://dev.azure.com/{ORG}/{PROJECT}/_apis/build/builds"
        f"?definitions={pipeline_id}"
        f"&resultFilter=succeeded"
        f"&minTime={(datetime.now() - timedelta(days=days)).isoformat()}"
        f"&api-version=7.1"
    )
    response = requests.get(url, auth=("", PAT))
    builds = response.json()["value"]

    deployments_per_day = len(builds) / days
    return {
        "total_deployments": len(builds),
        "days_measured": days,
        "deployments_per_day": round(deployments_per_day, 2),
        "dora_rating": (
            "Elite" if deployments_per_day >= 1 else
            "High" if deployments_per_day >= (1/7) else
            "Medium" if deployments_per_day >= (1/30) else
            "Low"
        )
    }
```

### Policy as Code: Required Pipeline Checks

Enforce security and quality standards at the pipeline template level:

```yaml
# steps/security-checks.yml — mandatory steps included in all pipelines

steps:
  - script: |
      pip install bandit safety
      bandit -r src/ -f json -o bandit-report.json || true
      safety check --json --output safety-report.json || true
    displayName: Security scan (bandit + safety)

  - task: PublishPipelineArtifact@1
    inputs:
      targetPath: '*-report.json'
      artifact: security-reports

  - script: |
      # Fail if HIGH severity vulnerabilities found
      python -c "
      import json
      with open('bandit-report.json') as f:
          report = json.load(f)
      high_issues = [i for i in report['results'] if i['issue_severity'] == 'HIGH']
      if high_issues:
          print(f'FAIL: {len(high_issues)} HIGH severity issues')
          exit(1)
      "
    displayName: Enforce zero high-severity vulnerabilities
```

---

## Interview Deep Dives

**"How do you handle ADF CI/CD across 3 environments (dev/staging/prod) with different configs?"**

Use **ARM template parameter files** per environment, overriding LinkedService connection strings and Key Vault URLs:

```bash
# Deploy with environment-specific parameter override
-factoryName "adf-$(environment)" \
-AzureKeyVault_url "$(KV_URL_$(ENVIRONMENT))" \
-AzureSQL_connectionString "$(SQL_CONN_$(ENVIRONMENT))"
```

Never commit connection strings to Git — always reference from Key Vault.

**"What's your strategy when a production pipeline deployment fails mid-way?"**

- **Stop ADF triggers before deploy** (included in the pipeline as shown above) to prevent triggers firing against a partially deployed ADF.
- **ARM deployments are idempotent**: re-run the deployment; it will converge to desired state.
- For Databricks: pin notebook workspace paths and use `--overwrite` — idempotent.
- For Terraform: `terraform apply` on the current state file recovers automatically.
- **Rollback**: keep the previous ARM template artifact and re-run with it; ADF deployments are fast (< 2 minutes).
