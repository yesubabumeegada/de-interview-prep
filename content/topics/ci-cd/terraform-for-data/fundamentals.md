---
title: "Terraform for Data Infrastructure - Fundamentals"
topic: ci-cd
subtopic: terraform-for-data
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [ci-cd, terraform, iac, infrastructure-as-code, data-infrastructure, hcl]
---

# Terraform for Data Infrastructure — Fundamentals

## What Is Terraform and Why Does It Matter for Data Engineers?

Terraform is an open-source Infrastructure as Code (IaC) tool created by HashiCorp that lets you define, provision, and manage cloud infrastructure using a declarative configuration language called HCL (HashiCorp Configuration Language). For data engineers, Terraform is the industry-standard tool for managing the underlying infrastructure that powers data platforms — S3 buckets, IAM roles, Glue databases, BigQuery datasets, Snowflake warehouses, Databricks clusters, and Kafka topics are all managed as code rather than through manual console clicks.

The core principle: infrastructure should be **version-controlled, reproducible, and automated**, just like application code. When you provision a Redshift cluster by hand in the AWS console, that knowledge lives in someone's head or a wiki. When you define it in Terraform, it lives in Git, can be reviewed in a PR, rolled back if something breaks, and reproduced identically across environments.

## Core Concepts

### Providers

Providers are plugins that allow Terraform to interact with APIs of cloud platforms and SaaS tools. For data infrastructure, you'll work with providers almost daily:

```hcl
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.87"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.30"
    }
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 1.76"
    }
  }
}

provider "aws" {
  region = "us-east-1"
}

provider "snowflake" {
  account  = var.snowflake_account
  username = var.snowflake_username
  password = var.snowflake_password
  role     = "SYSADMIN"
}
```

Each provider exposes a set of `resource` and `data` blocks specific to that platform. The Databricks provider, for instance, can manage clusters, notebooks, jobs, secrets, and Unity Catalog objects.

### Resources vs Data Sources

This is a fundamental distinction you must understand clearly:

- **`resource` blocks** declare infrastructure that Terraform will **create, update, and destroy**. You own the lifecycle.
- **`data` blocks** read information about **existing infrastructure** without managing it. They are read-only lookups.

```hcl
# RESOURCE: Terraform owns this S3 bucket — it will create and can destroy it
resource "aws_s3_bucket" "raw_data" {
  bucket = "my-company-raw-data-${var.environment}"

  tags = {
    Environment = var.environment
    Team        = "data-engineering"
    ManagedBy   = "terraform"
  }
}

# DATA SOURCE: This IAM role already exists — Terraform just looks it up
data "aws_iam_role" "existing_glue_role" {
  name = "GlueServiceRole-existing"
}

# Reference the data source in a resource
resource "aws_glue_database" "analytics" {
  name = "analytics_${var.environment}"

  # Use the looked-up ARN from the data source
  catalog_id = data.aws_iam_role.existing_glue_role.id
}
```

A common interview question: *"When would you use a `data` source instead of a `resource`?"* — Answer: when the infrastructure was created outside of Terraform (by another team, another tool, or manually) and you only need to reference it.

### State

Terraform maintains a **state file** (`terraform.tfstate`) that maps your configuration to real-world resources. This is how Terraform knows what exists, what needs to be created, and what needs to be destroyed.

The state file is critical — losing it means Terraform no longer knows what it manages. By default, state is stored locally, but in team environments this causes conflicts when multiple engineers run Terraform simultaneously.

## Basic Workflow

The four core commands every data engineer should know:

```bash
# Initialize the working directory — downloads providers, sets up backend
terraform init

# Preview what changes would be made WITHOUT applying them
# This is run in CI for PR reviews
terraform plan

# Apply the changes shown in the plan
terraform apply

# Destroy all resources managed by this configuration
terraform destroy
```

`terraform plan` is your safety net. Always review the plan before applying. In a CI/CD pipeline, you typically run `plan` on every PR and `apply` only on merge to main.

## Defining a Simple Data Resource

Here is a realistic example: an S3 bucket for raw data ingestion with versioning, encryption, and lifecycle rules:

```hcl
resource "aws_s3_bucket" "raw_zone" {
  bucket = "${var.project}-raw-${var.environment}"
}

resource "aws_s3_bucket_versioning" "raw_zone" {
  bucket = aws_s3_bucket.raw_zone.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_zone" {
  bucket = aws_s3_bucket.raw_zone.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_zone" {
  bucket = aws_s3_bucket.raw_zone.id

  rule {
    id     = "transition-to-ia"
    status = "Enabled"

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}
```

Notice how each capability is a separate resource in modern AWS provider versions — this is important for interview discussions about breaking changes in provider upgrades.

## Variables and Outputs

Terraform uses **input variables** to make configurations reusable across environments and **outputs** to expose values for use by other configurations or for display:

```hcl
# variables.tf
variable "environment" {
  description = "Deployment environment: dev, staging, prod"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be dev, staging, or prod."
  }
}

variable "project" {
  description = "Project name prefix for all resources"
  type        = string
}

# outputs.tf
output "raw_bucket_arn" {
  description = "ARN of the raw data S3 bucket"
  value       = aws_s3_bucket.raw_zone.arn
}

output "raw_bucket_name" {
  description = "Name of the raw data S3 bucket"
  value       = aws_s3_bucket.raw_zone.bucket
}
```

## Key Interview Takeaways

- Terraform is the standard IaC tool for data platforms — knowing it separates senior data engineers from mid-level ones
- The **plan/apply workflow** is what makes Terraform safe for production use
- `resource` blocks manage infrastructure; `data` blocks query existing infrastructure
- State is the source of truth — protect it, back it up, and never edit it manually
- Every data infrastructure component (S3, IAM, Glue, BigQuery, Snowflake, Databricks) can be managed through provider-specific resources
