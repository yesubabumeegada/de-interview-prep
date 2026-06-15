---
title: "Terraform for Data Infrastructure - Senior Deep Dive"
topic: ci-cd
subtopic: terraform-for-data
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [ci-cd, terraform, atlantis, terraform-cloud, gitops, policy-as-code, sentinel, opa]
---

# Terraform for Data Infrastructure — Senior Deep Dive

## GitOps Pattern for Infrastructure

GitOps treats your Git repository as the single source of truth for infrastructure state. The workflow:

1. Engineer opens a PR with infrastructure changes
2. CI runs `terraform plan` and posts the output as a PR comment
3. Team reviews the plan (reviewing HCL diff AND the plan output)
4. PR is approved and merged
5. CD automatically runs `terraform apply` on the main branch

This pattern eliminates ad-hoc `terraform apply` runs from local machines, which is critical for audit trails and preventing "works on my machine" infrastructure drift.

### Directory Structure for GitOps

```
infra/
  modules/
    data-product/
    snowflake-warehouse/
    databricks-job/
  environments/
    dev/
      backend.tf
      main.tf
      terraform.tfvars
    staging/
      backend.tf
      main.tf
      terraform.tfvars
    prod/
      backend.tf
      main.tf
      terraform.tfvars
  .github/
    workflows/
      terraform-plan.yml
      terraform-apply.yml
```

### GitHub Actions: Plan on PR

```yaml
# .github/workflows/terraform-plan.yml
name: Terraform Plan

on:
  pull_request:
    paths:
      - 'infra/**'

jobs:
  plan:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        environment: [dev, staging, prod]

    permissions:
      contents: read
      pull-requests: write
      id-token: write  # For OIDC auth to AWS

    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials (OIDC)
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::${{ secrets.AWS_ACCOUNT_ID }}:role/terraform-ci-role
          aws-region: us-east-1

      - name: Setup Terraform
        uses: hashicorp/setup-terraform@v3
        with:
          terraform_version: "1.7.0"

      - name: Terraform Init
        working-directory: infra/environments/${{ matrix.environment }}
        run: terraform init

      - name: Terraform Plan
        id: plan
        working-directory: infra/environments/${{ matrix.environment }}
        run: |
          terraform plan -no-color -out=tfplan 2>&1 | tee plan_output.txt
          echo "plan_output<<EOF" >> $GITHUB_OUTPUT
          cat plan_output.txt >> $GITHUB_OUTPUT
          echo "EOF" >> $GITHUB_OUTPUT

      - name: Comment PR with Plan
        uses: actions/github-script@v7
        with:
          script: |
            const plan = `${{ steps.plan.outputs.plan_output }}`;
            const body = `## Terraform Plan — ${{ matrix.environment }}\n\`\`\`\n${plan}\n\`\`\``;
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: body
            });
```

```yaml
# .github/workflows/terraform-apply.yml
name: Terraform Apply

on:
  push:
    branches: [main]
    paths:
      - 'infra/**'

jobs:
  apply:
    runs-on: ubuntu-latest
    environment: production  # Requires manual approval in GitHub

    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: arn:aws:iam::${{ secrets.AWS_ACCOUNT_ID }}:role/terraform-deploy-role
          aws-region: us-east-1

      - name: Setup Terraform
        uses: hashicorp/setup-terraform@v3

      - name: Terraform Init & Apply
        working-directory: infra/environments/prod
        run: |
          terraform init
          terraform apply -auto-approve
```

## Atlantis: Automated PR Plans and Applies

Atlantis is a self-hosted application that automates Terraform workflows directly in pull request comments. It's the open-source alternative to Terraform Cloud and is widely used in enterprises.

### How Atlantis Works

Atlantis installs a webhook on your GitHub/GitLab repository. When a PR is opened or updated, Atlantis automatically runs `terraform plan`. Engineers then comment on the PR to trigger applies:

```
# In a PR comment:
atlantis plan -d infra/environments/prod    # Run plan for prod
atlantis apply -d infra/environments/prod   # Run apply after approval
```

### atlantis.yaml Configuration

```yaml
# atlantis.yaml (repo root)
version: 3

projects:
  - name: data-platform-dev
    dir: infra/environments/dev
    workspace: default
    autoplan:
      enabled: true
      when_modified:
        - "**/*.tf"
        - "../../modules/**/*.tf"
    apply_requirements:
      - approved

  - name: data-platform-prod
    dir: infra/environments/prod
    workspace: default
    autoplan:
      enabled: true
      when_modified:
        - "**/*.tf"
        - "../../modules/**/*.tf"
    apply_requirements:
      - approved
      - mergeable
    workflow: prod-workflow

workflows:
  prod-workflow:
    plan:
      steps:
        - init
        - plan:
            extra_args: ["-var-file=prod.tfvars"]
    apply:
      steps:
        - apply
```

## Terraform Cloud for Team Workflows

Terraform Cloud (now part of HCP Terraform) provides managed state storage, run history, policy enforcement, and team access controls. For data platform teams, the key advantages are:

- **Remote execution**: Plans and applies run in Terraform Cloud, not on CI runners or laptops
- **State management**: Built-in remote state with history and locking
- **Sentinel policies**: Policy as code to prevent dangerous changes
- **Variable sets**: Shared variables (API keys, account IDs) across workspaces

### Sentinel Policy Example

Sentinel policies enforce governance rules before apply:

```python
# policy/require-tags.sentinel
import "tfplan/v2" as tfplan

# Require all S3 buckets to have Environment and Team tags
required_tags = ["Environment", "Team", "ManagedBy"]

s3_buckets = filter tfplan.resource_changes as _, rc {
  rc.type is "aws_s3_bucket" and
  (rc.change.actions contains "create" or rc.change.actions contains "update")
}

violations = filter s3_buckets as _, bucket {
  any required_tags as tag {
    not (tag in keys(bucket.change.after.tags))
  }
}

main = rule {
  length(violations) is 0
}
```

## Advanced State Operations

Senior engineers must be comfortable with surgical state manipulation for recovery scenarios:

```bash
# List all resources in state
terraform state list

# Show details of a specific resource
terraform state show aws_s3_bucket.raw_zone

# Move resource in state (renaming a resource without destroying it)
terraform state mv aws_s3_bucket.raw_data aws_s3_bucket.raw_zone

# Move resource to a different state file (splitting configurations)
terraform state mv -state-out=../new-config/terraform.tfstate \
  module.orders_product module.orders_product

# Remove resource from state WITHOUT destroying it (orphaning)
# Use when you want Terraform to forget about a resource
terraform state rm aws_s3_bucket.legacy_manual_bucket

# Pull remote state locally for inspection
terraform state pull > current_state.json

# Force-unlock stuck state (when a previous run crashed holding the lock)
terraform force-unlock <LOCK_ID>
```

## Managing Snowflake and Databricks at Scale

```hcl
# Snowflake warehouse with auto-scaling
resource "snowflake_warehouse" "etl" {
  name           = "ETL_${upper(var.environment)}"
  warehouse_size = var.environment == "prod" ? "LARGE" : "X-SMALL"
  auto_suspend   = 60   # Suspend after 60 seconds of inactivity
  auto_resume    = true
  max_cluster_count = var.environment == "prod" ? 3 : 1
  min_cluster_count = 1
  scaling_policy = "ECONOMY"

  # Prevent accidental destruction of prod warehouse
  lifecycle {
    prevent_destroy = var.environment == "prod"
  }
}

# Databricks Unity Catalog metastore assignment
resource "databricks_metastore_assignment" "this" {
  metastore_id = var.unity_catalog_metastore_id
  workspace_id = databricks_mws_workspaces.data_platform.workspace_id
}

# Databricks job with retry and alerting
resource "databricks_job" "daily_etl" {
  name = "daily-etl-${var.environment}"

  task {
    task_key = "transform"

    notebook_task {
      notebook_path = "/Shared/ETL/daily_transform"
      base_parameters = {
        environment = var.environment
        date        = "{{ds}}"
      }
    }

    new_cluster {
      num_workers   = var.environment == "prod" ? 8 : 2
      spark_version = "14.3.x-scala2.12"
      node_type_id  = "i3.xlarge"
    }

    retry_on_timeout = true
    max_retries      = 2
  }

  email_notifications {
    on_failure = [var.oncall_email]
  }

  schedule {
    quartz_cron_expression = "0 0 3 * * ?"  # Daily at 3am
    timezone_id            = "UTC"
  }
}
```

## Key Senior Interview Takeaways

- **GitOps with plan-on-PR** is the production standard — never apply from laptops, always from CI
- **Atlantis** is the open-source workhorse for automated Terraform PR workflows; know its `atlantis.yaml` config
- **Terraform Cloud/HCP Terraform** adds policy enforcement (Sentinel/OPA), audit logs, and managed execution
- **State manipulation** commands (`mv`, `rm`, `import`) are essential for incident recovery and refactoring
- `lifecycle { prevent_destroy = true }` is a production safety net for critical data stores
- OIDC authentication to cloud providers (no long-lived secrets in CI) is the security best practice
