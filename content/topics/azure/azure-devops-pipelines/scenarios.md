---
title: "Azure DevOps Pipelines - Scenario Questions"
topic: azure
subtopic: azure-devops-pipelines
content_type: scenario_question
tags: [azure, devops, ci-cd, yaml-pipelines, dbt, adf, databricks, key-vault]
---

# Azure DevOps Pipelines — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Writing a dbt CI Pipeline

Your team has a dbt project in a Git repository. You need to write an Azure DevOps YAML pipeline that runs automatically on every pull request targeting `main`. The pipeline should install dbt (Snowflake adapter, version 1.7.2), run `dbt deps`, and then run `dbt test`. Snowflake credentials are stored in a Variable Group called `SnowflakeCI`. Write the complete YAML.

<details>
<summary>✅ Solution</summary>

```yaml
# azure-pipelines.yml

trigger: none   # Don't run on direct commits

pr:
  branches:
    include:
      - main
  autoCancel: true    # Cancel previous runs for the same PR when new commits arrive

pool:
  vmImage: ubuntu-latest

variables:
  - group: SnowflakeCI   # Contains SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD

steps:
  - task: UsePythonVersion@0
    displayName: Set Python version
    inputs:
      versionSpec: '3.11'

  - script: pip install dbt-snowflake==1.7.2
    displayName: Install dbt Snowflake adapter

  - script: |
      cd dbt/
      dbt deps
    displayName: Install dbt packages

  - script: |
      cd dbt/
      dbt compile --target ci
    displayName: Compile dbt project (validate syntax)

  - script: |
      cd dbt/
      dbt test --target ci
    displayName: Run dbt tests
    env:
      # Explicitly map variable group secrets to environment variables
      SNOWFLAKE_ACCOUNT:  $(SNOWFLAKE_ACCOUNT)
      SNOWFLAKE_USER:     $(SNOWFLAKE_USER)
      SNOWFLAKE_PASSWORD: $(SNOWFLAKE_PASSWORD)

  - task: PublishTestResults@2
    displayName: Publish test results
    condition: always()   # Publish even if tests fail (so we see failures in the UI)
    inputs:
      testResultsFormat: JUnit
      testResultsFiles: 'dbt/target/junit/*.xml'
      testRunTitle: 'dbt Tests - PR $(System.PullRequest.PullRequestNumber)'
```

### Key Design Decisions Explained

1. **`trigger: none` + `pr:` block**: The `trigger` block controls when the pipeline runs on branch pushes. Setting it to `none` and using `pr:` means it ONLY runs on pull requests, not when engineers push directly to feature branches.

2. **`autoCancel: true`**: When a developer pushes a new commit to an open PR, the old pipeline run is automatically cancelled. This saves agent minutes and avoids stale results confusing reviewers.

3. **`- group: SnowflakeCI`**: Variable Groups must be listed under `variables:` to be accessible. Secrets from the group are masked in logs.

4. **`env:` block on the dbt test step**: Variable group values are available as `$(VARIABLE_NAME)` in tasks, but some tools read environment variables directly. The `env:` block explicitly maps pipeline variables to environment variables for the subprocess.

5. **`condition: always()` on PublishTestResults**: Without this, if dbt tests fail (non-zero exit code), the next steps are skipped by default. `always()` ensures test results are always published so Azure DevOps shows a test summary even on failure.

### profiles.yml for CI Target

Your dbt `profiles.yml` should have a `ci` target reading from environment variables:

```yaml
# ~/.dbt/profiles.yml (or profiles.yml in dbt project)
my_project:
  target: dev
  outputs:
    ci:
      type: snowflake
      account: "{{ env_var('SNOWFLAKE_ACCOUNT') }}"
      user: "{{ env_var('SNOWFLAKE_USER') }}"
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: CI_ROLE
      warehouse: CI_WH
      database: CI_DATABASE
      schema: "dbt_ci_{{ env_var('BUILD_BUILDID', 'local') }}"
```

Using `BUILD_BUILDID` (Azure DevOps built-in variable) as the schema suffix ensures each PR run gets an isolated schema, preventing test collisions between concurrent PR runs.

</details>
</article>

---

<article data-difficulty="mid">

## Scenario 2: ADF Deployment Pipeline with Environment Promotion

Your team manages Azure Data Factory configurations in Git (using ADF's native Git integration). You need to create a CD pipeline that: (1) deploys to staging automatically when code merges to `adf_publish`, (2) requires manual approval before deploying to production, (3) overrides the Key Vault URL for each environment. Write the YAML and explain the ARM template override strategy.

<details>
<summary>✅ Solution</summary>

```yaml
# azure-pipelines-adf-cd.yml

trigger:
  branches:
    include:
      - adf_publish   # ADF Git integration creates/updates this branch on Publish

pool:
  vmImage: ubuntu-latest

variables:
  - group: ADF-Deployment-Config
    # Contains:
    # SERVICE_CONNECTION_STAGING, SERVICE_CONNECTION_PROD
    # RESOURCE_GROUP_STAGING, RESOURCE_GROUP_PROD
    # ADF_NAME_STAGING, ADF_NAME_PROD
    # KV_URL_STAGING, KV_URL_PROD

stages:
  # ── STAGING ────────────────────────────────────────────────────────────────
  - stage: Deploy_Staging
    displayName: Deploy to Staging
    jobs:
      - deployment: ADF_Staging
        displayName: Deploy ADF to Staging
        environment: staging    # No approval gate here — auto-deploys
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self   # Check out adf_publish branch

                - task: AzurePowerShell@5
                  displayName: Stop ADF Triggers (Staging)
                  inputs:
                    azureSubscription: $(SERVICE_CONNECTION_STAGING)
                    ScriptType: InlineScript
                    azurePowerShellVersion: LatestVersion
                    Inline: |
                      $triggers = Get-AzDataFactoryV2Trigger `
                        -ResourceGroupName "$(RESOURCE_GROUP_STAGING)" `
                        -DataFactoryName "$(ADF_NAME_STAGING)"
                      foreach ($trigger in $triggers) {
                        Stop-AzDataFactoryV2Trigger `
                          -ResourceGroupName "$(RESOURCE_GROUP_STAGING)" `
                          -DataFactoryName "$(ADF_NAME_STAGING)" `
                          -Name $trigger.Name -Force -ErrorAction SilentlyContinue
                      }
                      Write-Host "All triggers stopped"

                - task: AzureResourceManagerTemplateDeployment@3
                  displayName: Deploy ADF ARM Template to Staging
                  inputs:
                    deploymentScope: Resource Group
                    azureResourceManagerConnection: $(SERVICE_CONNECTION_STAGING)
                    action: Create Or Update Resource Group
                    resourceGroupName: $(RESOURCE_GROUP_STAGING)
                    location: East US
                    templateLocation: Linked artifact
                    csmFile: ARMTemplateForFactory.json
                    # Override parameters per environment:
                    overrideParameters: >-
                      -factoryName "$(ADF_NAME_STAGING)"
                      -AzureKeyVault_properties_typeProperties_baseUrl "$(KV_URL_STAGING)"
                      -AzureDataLakeStore_properties_typeProperties_dataLakeStoreUri "https://$(ADLS_NAME_STAGING).dfs.core.windows.net"

                - task: AzurePowerShell@5
                  displayName: Start ADF Triggers (Staging)
                  inputs:
                    azureSubscription: $(SERVICE_CONNECTION_STAGING)
                    ScriptType: InlineScript
                    azurePowerShellVersion: LatestVersion
                    Inline: |
                      $triggers = Get-AzDataFactoryV2Trigger `
                        -ResourceGroupName "$(RESOURCE_GROUP_STAGING)" `
                        -DataFactoryName "$(ADF_NAME_STAGING)"
                      foreach ($trigger in $triggers) {
                        Start-AzDataFactoryV2Trigger `
                          -ResourceGroupName "$(RESOURCE_GROUP_STAGING)" `
                          -DataFactoryName "$(ADF_NAME_STAGING)" `
                          -Name $trigger.Name -Force
                      }

  # ── STAGING SMOKE TEST ─────────────────────────────────────────────────────
  - stage: Staging_Smoke_Test
    displayName: Staging Smoke Tests
    dependsOn: Deploy_Staging
    jobs:
      - job: SmokeTest
        steps:
          - script: |
              pip install requests azure-identity
              python smoke_tests/test_adf_staging.py \
                --adf-name "$(ADF_NAME_STAGING)" \
                --resource-group "$(RESOURCE_GROUP_STAGING)"
            displayName: Run ADF smoke tests
            env:
              AZURE_CLIENT_ID: $(AZURE_CLIENT_ID)
              AZURE_CLIENT_SECRET: $(AZURE_CLIENT_SECRET)
              AZURE_TENANT_ID: $(AZURE_TENANT_ID)

  # ── PRODUCTION ─────────────────────────────────────────────────────────────
  - stage: Deploy_Production
    displayName: Deploy to Production
    dependsOn: Staging_Smoke_Test
    condition: succeeded()
    jobs:
      - deployment: ADF_Production
        displayName: Deploy ADF to Production
        environment: production    # ← Manual approval required here
        strategy:
          runOnce:
            deploy:
              steps:
                - checkout: self

                - task: AzurePowerShell@5
                  displayName: Stop ADF Triggers (Production)
                  inputs:
                    azureSubscription: $(SERVICE_CONNECTION_PROD)
                    ScriptType: InlineScript
                    azurePowerShellVersion: LatestVersion
                    Inline: |
                      Get-AzDataFactoryV2Trigger `
                        -ResourceGroupName "$(RESOURCE_GROUP_PROD)" `
                        -DataFactoryName "$(ADF_NAME_PROD)" |
                      Stop-AzDataFactoryV2Trigger -Force

                - task: AzureResourceManagerTemplateDeployment@3
                  displayName: Deploy ADF ARM Template to Production
                  inputs:
                    azureResourceManagerConnection: $(SERVICE_CONNECTION_PROD)
                    resourceGroupName: $(RESOURCE_GROUP_PROD)
                    templateLocation: Linked artifact
                    csmFile: ARMTemplateForFactory.json
                    overrideParameters: >-
                      -factoryName "$(ADF_NAME_PROD)"
                      -AzureKeyVault_properties_typeProperties_baseUrl "$(KV_URL_PROD)"
                      -AzureDataLakeStore_properties_typeProperties_dataLakeStoreUri "https://$(ADLS_NAME_PROD).dfs.core.windows.net"

                - task: AzurePowerShell@5
                  displayName: Start ADF Triggers (Production)
                  inputs:
                    azureSubscription: $(SERVICE_CONNECTION_PROD)
                    ScriptType: InlineScript
                    azurePowerShellVersion: LatestVersion
                    Inline: |
                      Get-AzDataFactoryV2Trigger `
                        -ResourceGroupName "$(RESOURCE_GROUP_PROD)" `
                        -DataFactoryName "$(ADF_NAME_PROD)" |
                      Start-AzDataFactoryV2Trigger -Force
```

### ARM Template Override Strategy

ADF exports a single `ARMTemplateForFactory.json` with all LinkedService definitions. Connection strings, Key Vault URLs, and storage account names differ per environment. The `overrideParameters` field replaces parameter values at deployment time without modifying the JSON file:

```
ARMTemplateParametersForFactory.json (committed to repo — staging values as default)
├── factoryName: "adf-dev"            → overridden to "adf-staging" or "adf-prod"
├── AzureKeyVault_baseUrl: "https://kv-dev..." → overridden per environment
└── ADLS_Uri: "https://adls-dev..."   → overridden per environment
```

**Why not use separate parameter files?** ADF's auto-generated `ARMTemplateParametersForFactory.json` only contains one set of values. Using `overrideParameters` in the pipeline is cleaner than maintaining environment-specific parameter files that drift over time.

### Setting Up the Production Approval Gate

In Azure DevOps:
1. Pipelines → Environments → `production` → Add resource
2. Approvals and checks → + Add → Approvals
3. Add approvers (e.g., Tech Lead + On-call SRE)
4. Set "Instructions for approvers" to include a checklist: staging tests green, change ticket raised, change window is current.

</details>
</article>

---

<article data-difficulty="senior">

## Scenario 3: Designing a Reusable CI/CD Framework for a Multi-Team Data Platform

You're the platform engineering lead. Your organisation has 6 data engineering teams, each with their own dbt projects, ADF pipelines, and Databricks jobs. Currently each team maintains their own ad-hoc Azure DevOps pipelines, leading to inconsistent security practices (some pipelines have hardcoded secrets), no standardised test gates, and duplicated YAML. Design a reusable CI/CD framework using YAML templates and explain how you would enforce adoption.

<details>
<summary>✅ Solution</summary>

### Architecture: Centralised Template Library

```
azure-devops-templates/ (shared repository, owned by platform team)
├── stages/
│   ├── dbt-ci.yml                 # Reusable dbt CI stage
│   ├── adf-cd.yml                 # Reusable ADF CD stage
│   └── databricks-cd.yml          # Reusable Databricks CD stage
├── jobs/
│   ├── security-scan.yml          # Mandatory security scanning job
│   └── compliance-check.yml       # Policy compliance checks
├── steps/
│   ├── install-dbt.yml
│   ├── configure-databricks.yml
│   └── notify-teams.yml
└── CHANGELOG.md                   # Template version history
```

### Reusable dbt CI Stage Template

```yaml
# stages/dbt-ci.yml

parameters:
  - name: dbtAdapterPackage
    type: string             # e.g., 'dbt-snowflake==1.7.2'
  - name: dbtProjectDir
    type: string
    default: 'dbt/'
  - name: dbtCITarget
    type: string
    default: 'ci'
  - name: credentialVariableGroup
    type: string             # Must be a Key Vault-linked variable group
  - name: artifactStorageAccount
    type: string
  - name: azureServiceConnection
    type: string

stages:
  - stage: dbt_CI
    displayName: dbt CI
    variables:
      - group: ${{ parameters.credentialVariableGroup }}
      - group: platform-shared-config   # Platform-wide config (non-secret)
    jobs:
      # Mandatory security scan (cannot be removed by consuming teams)
      - template: ../jobs/security-scan.yml
        parameters:
          scanPath: ${{ parameters.dbtProjectDir }}

      - job: dbt_build_test
        dependsOn: security_scan
        pool:
          vmImage: ubuntu-latest
        steps:
          - checkout: self
            fetchDepth: 0   # Full history for dbt state comparison

          - script: pip install ${{ parameters.dbtAdapterPackage }}
            displayName: Install dbt adapter

          - task: AzureCLI@2
            displayName: Download production manifest
            inputs:
              azureSubscription: ${{ parameters.azureServiceConnection }}
              scriptType: bash
              inlineScript: |
                az storage blob download \
                  --account-name ${{ parameters.artifactStorageAccount }} \
                  --container-name dbt-artifacts \
                  --name prod/manifest.json \
                  --file manifest.json \
                  --auth-mode login 2>/dev/null \
                  && echo "Production manifest downloaded" \
                  || echo "No production manifest — running full build"

          - script: |
              cd ${{ parameters.dbtProjectDir }}
              dbt deps

              if [ -f ../manifest.json ]; then
                echo "Running slim CI (changed models only)"
                dbt build \
                  --target ${{ parameters.dbtCITarget }} \
                  --select state:modified+ \
                  --state ..
              else
                echo "Running full CI (no baseline manifest)"
                dbt build --target ${{ parameters.dbtCITarget }}
              fi
            displayName: dbt build and test
            env:
              # Injected from Key Vault-linked variable group
              SNOWFLAKE_PASSWORD: $(SNOWFLAKE_CI_PASSWORD)
              SNOWFLAKE_ACCOUNT: $(SNOWFLAKE_ACCOUNT)
              SNOWFLAKE_USER: $(SNOWFLAKE_CI_USER)
              BUILD_BUILDID: $(Build.BuildId)

          - task: PublishTestResults@2
            condition: always()
            inputs:
              testResultsFiles: '${{ parameters.dbtProjectDir }}target/junit/*.xml'
              testRunTitle: 'dbt - $(Build.Repository.Name) PR $(System.PullRequest.PullRequestNumber)'

          - task: PublishPipelineArtifact@1
            condition: succeeded()
            inputs:
              targetPath: '${{ parameters.dbtProjectDir }}target/'
              artifact: dbt-target
```

### Mandatory Security Scan Job

```yaml
# jobs/security-scan.yml

parameters:
  - name: scanPath
    type: string
    default: '.'

jobs:
  - job: security_scan
    displayName: Security Scan (Mandatory)
    pool:
      vmImage: ubuntu-latest
    steps:
      - checkout: self

      # Check for hardcoded secrets with Gitleaks
      - script: |
          docker run --rm \
            -v $(Build.SourcesDirectory):/path \
            ghcr.io/gitleaks/gitleaks:latest detect \
            --source=/path/${{ parameters.scanPath }} \
            --exit-code=1 \
            --report-format=json \
            --report-path=/path/gitleaks-report.json
        displayName: Gitleaks secret scanning
        continueOnError: false   # HARD BLOCK — no secrets allowed

      # Python security check (if Python files present)
      - script: |
          if find ${{ parameters.scanPath }} -name "*.py" | head -1 | grep -q py; then
            pip install bandit safety
            bandit -r ${{ parameters.scanPath }} \
              --severity-level high \
              --confidence-level medium \
              -f json -o bandit-report.json || true
            # Fail on HIGH severity issues
            python -c "
          import json, sys
          with open('bandit-report.json') as f:
              r = json.load(f)
          highs = [i for i in r['results'] if i['issue_severity'] == 'HIGH']
          if highs:
              print(f'FAILED: {len(highs)} HIGH severity issues found')
              sys.exit(1)
            "
          fi
        displayName: Python security scan (bandit)

      - task: PublishPipelineArtifact@1
        condition: always()
        inputs:
          targetPath: '*-report.json'
          artifact: security-reports
```

### Consuming Team's Pipeline (Minimal YAML)

Teams are given a template and only need to fill in parameters:

```yaml
# azure-pipelines.yml (in each team's repository)

pr:
  branches:
    include: [main]

resources:
  repositories:
    - repository: platform-templates
      type: git
      name: platform/azure-devops-templates
      ref: refs/tags/v3.0.0   # Pin to stable version

extends:
  template: stages/dbt-ci.yml@platform-templates
  parameters:
    dbtAdapterPackage: 'dbt-snowflake==1.7.2'
    dbtProjectDir: 'analytics/'
    dbtCITarget: 'ci'
    credentialVariableGroup: 'fintech-team-snowflake-ci'
    artifactStorageAccount: 'companydataplatform'
    azureServiceConnection: 'platform-azure'
```

### Enforcing Adoption

**Technical enforcement:**
1. **Required pipeline policy**: Azure DevOps branch policy requires the platform-managed pipeline to pass before any PR can merge to `main`.
2. **`extends` restriction**: Configure the organization policy to only allow pipelines that `extends` an approved template (Azure DevOps "Required template" policy). Teams cannot create pipelines that bypass the security scan.

**Governance enforcement:**
3. **Service connection restriction**: Production service connections are only accessible from pipelines using the approved templates.
4. **Key Vault access**: Only Key Vault-linked variable groups are allowed to hold production secrets — hardcoded secrets in YAML are blocked by Gitleaks (which is mandatory in the template).

**Process enforcement:**
5. **Template versioning**: Templates use Git tags (semver). Breaking changes require a major version bump. Teams get 90 days to upgrade before old tags are deleted.
6. **Monthly platform review**: Showcase template improvements in all-hands; collect feedback to make the templates better than teams would build themselves.

### Result Metrics After 6 Months

- Secret scanning coverage: 0% → 100% of data engineering PRs
- Median CI pipeline time: reduced from 22 min → 8 min (slim CI adoption)
- Security incidents from hardcoded credentials: reduced from 3/year → 0
- YAML duplication: 6 teams × ~200 lines → 6 teams × ~25 lines each

</details>
</article>
