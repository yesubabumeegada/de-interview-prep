---
title: "Terraform for Data Infrastructure - Intermediate"
topic: ci-cd
subtopic: terraform-for-data
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [ci-cd, terraform, modules, remote-state, workspaces, iam, glue, s3]
---

# Terraform for Data Infrastructure — Intermediate

## Remote State Management

When multiple engineers work on the same infrastructure, local state files are a disaster waiting to happen. Two engineers running `terraform apply` simultaneously can corrupt state or create duplicate resources. The solution is **remote state** stored in a shared backend with **state locking**.

### S3 Backend with DynamoDB Locking (AWS)

```hcl
# backend.tf
terraform {
  backend "s3" {
    bucket         = "my-company-terraform-state"
    key            = "data-platform/prod/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-state-locks"
  }
}
```

The DynamoDB table must have a partition key named `LockID` (type String). When any engineer runs `terraform plan` or `terraform apply`, Terraform writes a lock to DynamoDB. If another apply is already running, the second engineer sees an error rather than silently corrupting state.

```bash
# Create the DynamoDB lock table (bootstrap — done once manually or via a separate "bootstrap" config)
aws dynamodb create-table \
  --table-name terraform-state-locks \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST
```

### GCS Backend (GCP)

```hcl
terraform {
  backend "gcs" {
    bucket = "my-company-terraform-state"
    prefix = "data-platform/prod"
  }
}
```

GCS uses native object locking — no separate lock table needed.

## Terraform Modules

Modules are the primary mechanism for **reusability** in Terraform. A module is simply a directory of `.tf` files with input variables and outputs. You define a pattern once and instantiate it multiple times with different parameters.

### Writing a Data Product Module

A common pattern is a "data product module" that bundles together all the infrastructure a single data product needs: an S3 bucket, IAM roles, a Glue database, and a Glue crawler.

```
modules/
  data-product/
    main.tf
    variables.tf
    outputs.tf
    iam.tf
```

```hcl
# modules/data-product/variables.tf
variable "product_name" {
  description = "Name of the data product (e.g., 'orders', 'inventory')"
  type        = string
}

variable "environment" {
  type = string
}

variable "glue_role_arn" {
  description = "IAM role ARN for Glue crawler and ETL jobs"
  type        = string
}

# modules/data-product/main.tf
resource "aws_s3_bucket" "data" {
  bucket = "${var.product_name}-${var.environment}-data"

  tags = {
    DataProduct = var.product_name
    Environment = var.environment
    ManagedBy   = "terraform"
  }
}

resource "aws_glue_catalog_database" "data" {
  name = "${var.product_name}_${var.environment}"
}

resource "aws_glue_crawler" "data" {
  name          = "${var.product_name}-${var.environment}-crawler"
  role          = var.glue_role_arn
  database_name = aws_glue_catalog_database.data.name

  s3_target {
    path = "s3://${aws_s3_bucket.data.bucket}/raw/"
  }

  schedule = "cron(0 2 * * ? *)"  # Daily at 2am UTC
}

# modules/data-product/outputs.tf
output "bucket_arn" {
  value = aws_s3_bucket.data.arn
}

output "glue_database_name" {
  value = aws_glue_catalog_database.data.name
}
```

### Calling the Module

```hcl
# In your root configuration (environments/prod/main.tf)
module "orders_product" {
  source = "../../modules/data-product"

  product_name  = "orders"
  environment   = "prod"
  glue_role_arn = aws_iam_role.glue_service.arn
}

module "inventory_product" {
  source = "../../modules/data-product"

  product_name  = "inventory"
  environment   = "prod"
  glue_role_arn = aws_iam_role.glue_service.arn
}

# Reference module outputs
output "orders_bucket_arn" {
  value = module.orders_product.bucket_arn
}
```

Modules can also be sourced from Terraform Registry, GitHub, or a private registry — enabling platform teams to publish approved infrastructure patterns that product teams consume.

## Workspaces for Environment Separation

Terraform **workspaces** let you manage multiple state files from a single configuration, which is useful for environment separation:

```bash
# Create and switch workspaces
terraform workspace new dev
terraform workspace new staging
terraform workspace new prod

# List workspaces
terraform workspace list

# Switch workspace
terraform workspace select prod

# Current workspace available in HCL as terraform.workspace
```

```hcl
# Use workspace name to vary resource configuration
resource "aws_s3_bucket" "data_lake" {
  bucket = "company-data-lake-${terraform.workspace}"
}

# Use locals for environment-specific values
locals {
  is_prod = terraform.workspace == "prod"

  databricks_node_type = local.is_prod ? "i3.2xlarge" : "m5.large"
  databricks_num_workers = local.is_prod ? 10 : 2
}

resource "databricks_cluster" "etl" {
  cluster_name  = "etl-${terraform.workspace}"
  node_type_id  = local.databricks_node_type
  num_workers   = local.databricks_num_workers
}
```

**Workspace vs. separate directories**: Workspaces share the same code but have separate state; separate directories have separate code and state. For significantly different environments (different account IDs, different regions), separate directories are often cleaner. For identical configurations that only vary by scale, workspaces work well.

## IAM Roles and Policies for Data Services

Managing IAM in Terraform is one of the most impactful things a data engineer can do — replacing manually created roles with auditable, version-controlled definitions:

```hcl
# Glue service role
resource "aws_iam_role" "glue_service" {
  name = "glue-service-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "glue-s3-access"
  role = aws_iam_role.glue_service.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.raw_zone.arn,
          "${aws_s3_bucket.raw_zone.arn}/*",
          aws_s3_bucket.curated_zone.arn,
          "${aws_s3_bucket.curated_zone.arn}/*"
        ]
      }
    ]
  })
}

# Attach AWS managed policy for Glue service
resource "aws_iam_role_policy_attachment" "glue_service_managed" {
  role       = aws_iam_role.glue_service.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}
```

## Importing Existing Resources

When you take over a data platform that was built manually, you need to bring existing resources under Terraform management without destroying and recreating them:

```bash
# Terraform 1.5+ import block (preferred — shows in plan)
# In your .tf file:
import {
  to = aws_s3_bucket.legacy_raw
  id = "company-legacy-raw-data"
}

# Then run terraform plan — it will show what it would import
terraform plan

# Then apply to import
terraform apply
```

For older Terraform versions, use the CLI import command:

```bash
terraform import aws_s3_bucket.legacy_raw company-legacy-raw-data
terraform import aws_glue_catalog_database.analytics arn:aws:glue:us-east-1:123456789:database/analytics_prod
```

After importing, run `terraform plan` — it should show "No changes" if your HCL accurately describes the existing resource. Any drift shown means your configuration doesn't match reality and needs to be reconciled.

## Key Interview Takeaways

- **Remote state with locking** is non-negotiable for team environments — S3 + DynamoDB (AWS) or GCS (GCP)
- **Modules** are how you enforce consistent infrastructure patterns across data products — one module, many instances
- **Workspaces** provide environment isolation with a single codebase; separate directories are better for fundamentally different environments
- **Import** is how you bring legacy manually-created infrastructure under Terraform control
- IAM policies in Terraform are reviewed in PRs — this is a major security improvement over console-created policies
