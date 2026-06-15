---
title: "Terraform for Data Infrastructure - Scenario Questions"
topic: ci-cd
subtopic: terraform-for-data
content_type: scenario_question
tags: [ci-cd, terraform, iac, state-management, modules, gitops, snowflake, databricks]
---

# Terraform for Data Infrastructure — Scenario Questions

<article data-difficulty="junior">

## Scenario 1: Your First Terraform Data Resource

You're a junior data engineer at a startup. The team has been manually creating S3 buckets through the AWS console, and your tech lead asks you to start managing new buckets with Terraform. Your task: write Terraform configuration to create a raw data ingestion bucket for the `orders` domain with versioning enabled and a lifecycle rule that moves objects to Glacier after 90 days. The bucket must be tagged with `Team: data-engineering` and `Environment: prod`.

**Requirements:**
- Use variables for environment and project name
- Enable versioning
- Add a Glacier transition lifecycle rule
- Tag the bucket appropriately
- Show the bucket name as an output

<details>
<summary>✅ Solution</summary>

```hcl
# variables.tf
variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "prod"
}

variable "project" {
  description = "Project name prefix"
  type        = string
  default     = "mycompany"
}

# main.tf
resource "aws_s3_bucket" "orders_raw" {
  bucket = "${var.project}-orders-raw-${var.environment}"

  tags = {
    Team        = "data-engineering"
    Environment = var.environment
    Domain      = "orders"
    ManagedBy   = "terraform"
  }
}

resource "aws_s3_bucket_versioning" "orders_raw" {
  bucket = aws_s3_bucket.orders_raw.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "orders_raw" {
  bucket = aws_s3_bucket.orders_raw.id

  rule {
    id     = "archive-to-glacier"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}

# outputs.tf
output "orders_raw_bucket_name" {
  description = "Name of the orders raw S3 bucket"
  value       = aws_s3_bucket.orders_raw.bucket
}

output "orders_raw_bucket_arn" {
  description = "ARN of the orders raw S3 bucket"
  value       = aws_s3_bucket.orders_raw.arn
}
```

**Key concepts demonstrated:**
- Separate resource blocks for bucket, versioning, and lifecycle (modern AWS provider pattern)
- Variable usage for reusability across environments
- Resource references (`aws_s3_bucket.orders_raw.id`) instead of hardcoded names
- Meaningful tags including `ManagedBy: terraform` for operational visibility

**Workflow to deploy:**
```bash
terraform init        # Download AWS provider
terraform plan        # Review — should show 3 resources to create
terraform apply       # Create the resources
```

**Common junior mistake to avoid:** Using `aws_s3_bucket.orders_raw.bucket_name` — the attribute is `.bucket`, not `.bucket_name`. Always check provider docs for the correct attribute names.

</details>
</article>

<article data-difficulty="mid">

## Scenario 2: Team Conflict — Two Engineers Applied at the Same Time

Your team's data platform Terraform config uses local state. Two engineers both ran `terraform apply` at the same time today. One created a new Glue database; the other modified an S3 bucket policy. Now the state is corrupted and `terraform plan` shows resources that already exist as needing to be created again. Your manager asks you to fix this and prevent it from happening in the future.

**Part 1:** How do you recover from the corrupted state today?
**Part 2:** Design the remote state setup that prevents this forever.
**Part 3:** What's the PR-based workflow you'd put in place going forward?

<details>
<summary>✅ Solution</summary>

**Part 1: Recovery**

```bash
# 1. Do NOT run terraform apply — it will try to create duplicate resources
# 2. Pull the current AWS reality using data sources / state refresh
terraform refresh  # Updates state to match real resources (deprecated in TF 1.x, use plan -refresh-only)
terraform plan -refresh-only

# 3. If state is corrupt, manually reconstruct it
# First, list what actually exists in AWS
aws s3api list-buckets | grep data-platform
aws glue get-databases

# 4. Import the real resources into a clean state
terraform state list  # See what state thinks it has
terraform import aws_glue_catalog_database.analytics 123456789:database/analytics_prod
terraform import aws_s3_bucket.raw_data company-raw-data-prod

# 5. Run plan — if HCL matches reality, plan should show no changes
terraform plan
# If it shows changes, your HCL config doesn't match what's deployed
# Fix the HCL before applying anything
```

**Part 2: Remote State with S3 + DynamoDB**

```hcl
# bootstrap/main.tf — apply this ONCE manually
resource "aws_s3_bucket" "terraform_state" {
  bucket = "mycompany-terraform-state-${data.aws_caller_identity.current.account_id}"
}

resource "aws_s3_bucket_versioning" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_dynamodb_table" "terraform_locks" {
  name         = "terraform-state-locks"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }
}

# After bootstrap, update your main backend.tf:
# backend.tf
terraform {
  backend "s3" {
    bucket         = "mycompany-terraform-state-123456789012"
    key            = "data-platform/prod/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-state-locks"
  }
}
```

When Engineer A runs `terraform plan`, DynamoDB writes a lock entry. If Engineer B tries to run simultaneously, they see:
```
Error: Error locking state: Error acquiring the state lock
```

They must wait for Engineer A to finish. Lock is released automatically on completion (or manually with `terraform force-unlock <LOCK_ID>` if the previous run crashed).

**Part 3: PR-Based Workflow**

```yaml
# Branch protection rules on main:
# - Require PR reviews: 1 minimum
# - Require status checks: terraform-plan must pass
# - No direct pushes to main

# .github/workflows/terraform.yml
on:
  pull_request:   # Plan on PR
    paths: ['infra/**']
  push:
    branches: [main]  # Apply on merge

jobs:
  plan:
    if: github.event_name == 'pull_request'
    steps:
      - run: terraform plan -no-color 2>&1 | tee plan.txt
      - uses: actions/github-script@v7
        with:
          script: |
            const plan = require('fs').readFileSync('plan.txt', 'utf8');
            github.rest.issues.createComment({
              issue_number: context.issue.number,
              owner: context.repo.owner,
              repo: context.repo.repo,
              body: `## Terraform Plan\n\`\`\`\n${plan}\n\`\`\``
            });

  apply:
    if: github.event_name == 'push'
    steps:
      - run: terraform apply -auto-approve
```

**Result:** Only one apply can run at a time (state lock), all changes are reviewed before apply, and the full plan is visible in the PR for audit purposes.

</details>
</article>

<article data-difficulty="senior">

## Scenario 3: Designing a Self-Service Data Product Infrastructure Platform

You're the lead data platform engineer at a company with 15 data product teams. Each team needs: an S3 data lake zone, a Snowflake warehouse, a Databricks job namespace (folder + service principal), and IAM roles following least privilege. Teams should be able to request new infrastructure through a PR without needing to understand Terraform details. Design the Terraform architecture for this self-service model, including how teams interact with it, how you enforce governance, and how you handle the CI/CD for 15 teams' worth of infrastructure.

<details>
<summary>✅ Solution</summary>

**Architecture: Module + YAML Config Pattern**

The insight: data product teams shouldn't write HCL. Instead, they submit YAML configuration files, and the platform team's Terraform reads those files and creates infrastructure.

```yaml
# data-products/orders.yaml (submitted by Orders team via PR)
name: orders
team: checkout-engineering
oncall_email: checkout-oncall@company.com
snowflake_warehouse_size: LARGE
databricks_workers: 8
s3_retention_days: 90
environments:
  - dev
  - staging
  - prod
```

```hcl
# platform/main.tf
locals {
  # Read all YAML files in data-products/
  data_products = {
    for f in fileset("${path.root}/../data-products", "*.yaml") :
    trimsuffix(f, ".yaml") => yamldecode(file("${path.root}/../data-products/${f}"))
  }
}

# Instantiate module for each data product
module "data_products" {
  for_each = local.data_products
  source   = "../modules/data-product"

  name                      = each.value.name
  team                      = each.value.team
  oncall_email              = each.value.oncall_email
  snowflake_warehouse_size  = try(each.value.snowflake_warehouse_size, "SMALL")
  databricks_workers        = try(each.value.databricks_workers, 2)
  s3_retention_days         = try(each.value.s3_retention_days, 30)
  environment               = var.environment
}
```

**The Data Product Module**

```hcl
# modules/data-product/main.tf

# S3 data lake zone
resource "aws_s3_bucket" "data" {
  bucket = "${var.name}-${var.environment}-data"
  tags   = { Team = var.team, DataProduct = var.name, Environment = var.environment }
}

# Scoped IAM role for this data product only
resource "aws_iam_role" "data_product" {
  name = "dp-${var.name}-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = ["glue.amazonaws.com", "lambda.amazonaws.com"] }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "data_product_s3" {
  role = aws_iam_role.data_product.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:GetObject", "s3:PutObject", "s3:ListBucket"]
      Resource = [aws_s3_bucket.data.arn, "${aws_s3_bucket.data.arn}/*"]
      # NOTE: Only this product's bucket — not a wildcard!
    }]
  })
}

# Snowflake warehouse
resource "snowflake_warehouse" "data_product" {
  name           = upper("${var.name}_${var.environment}_WH")
  warehouse_size = var.snowflake_warehouse_size
  auto_suspend   = 300
  auto_resume    = true
}

# Snowflake role for this data product
resource "snowflake_role" "data_product_write" {
  name = upper("${var.name}_${var.environment}_WRITE")
}

# Databricks service principal
resource "databricks_service_principal" "data_product" {
  display_name = "sp-${var.name}-${var.environment}"
}

resource "databricks_directory" "data_product" {
  path = "/DataProducts/${var.name}"
}

resource "databricks_permissions" "data_product_dir" {
  directory_path = databricks_directory.data_product.path

  access_control {
    service_principal_name = databricks_service_principal.data_product.application_id
    permission_level       = "CAN_MANAGE"
  }
}
```

**CI/CD: Governed Pipeline with Policy Checks**

```yaml
# .github/workflows/data-product-infra.yml
on:
  pull_request:
    paths:
      - 'data-products/**'
      - 'modules/**'
      - 'platform/**'

jobs:
  validate-yaml:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Validate data product YAML schemas
        run: |
          pip install jsonschema pyyaml
          python scripts/validate_data_product_yamls.py

  terraform-plan:
    needs: validate-yaml
    strategy:
      matrix:
        environment: [dev, staging, prod]
    steps:
      - name: Plan ${{ matrix.environment }}
        run: |
          cd platform/
          terraform workspace select ${{ matrix.environment }}
          terraform plan -no-color

  policy-check:
    needs: validate-yaml
    steps:
      - name: Check warehouse size limits
        run: |
          python scripts/check_warehouse_limits.py
          # Blocks XLARGE warehouses in dev, requires senior approval for LARGE in prod
```

**Governance Controls Built In:**

1. **YAML schema validation**: PR fails if team requests unsupported warehouse sizes or missing required fields
2. **Cost controls**: Policy check blocks oversized resources without explicit approval label on PR
3. **Naming conventions**: Module enforces `{product}-{env}` naming — teams can't override it
4. **Least-privilege IAM**: Module generates scoped policies; teams can't request wildcard access
5. **Audit trail**: Every infrastructure change is a reviewed PR with Terraform plan output

**Handling 15 Teams at Scale:**

- Use `for_each` over the YAML configs — adding a new team is one YAML file PR, not new HCL
- Keep module changes separate from data product changes (different reviewers: platform team reviews module changes)
- Run plan on all 15 products in parallel using `matrix` strategy in CI — typically under 5 minutes total
- Use Terraform `moved` blocks when refactoring modules to avoid destroying/recreating resources

This architecture scales to 50+ data products without growing the platform team, because the self-service interface is intentionally simple (YAML), while the complexity lives in the module.

</details>
</article>
