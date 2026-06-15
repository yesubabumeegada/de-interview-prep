---
title: "Terraform for Data Infrastructure - Real-World Patterns"
topic: ci-cd
subtopic: terraform-for-data
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [ci-cd, terraform, data-platform, production-patterns, multi-account, cost]
---

# Terraform for Data Infrastructure — Real-World Patterns

## The Data Platform Terraform Monorepo

At production scale, data infrastructure typically lives in a dedicated monorepo (or a dedicated directory in an infra monorepo) with clear separation of concerns:

```
data-platform-infra/
  bootstrap/            # S3 state bucket, DynamoDB table — created manually ONCE
  modules/
    data-lake/          # S3 zones + lifecycle + encryption
    data-product/       # Per-product resources
    glue-etl-job/       # Glue job + trigger + crawler
    databricks-cluster/ # Cluster + instance pool + job
    snowflake-rbac/     # Roles, grants, warehouse per team
    kafka-topic/        # Confluent Cloud topic + schema registry subject
  environments/
    dev/
    staging/
    prod/
  shared/               # Cross-environment resources (Route53, ACM, shared VPC)
  scripts/
    validate.sh         # Run fmt check + validate on all modules
    plan-all.sh         # Plan all environments
```

### Bootstrap Problem

You can't use Terraform to create the S3 bucket that holds Terraform state — it's a chicken-and-egg problem. The standard solution is a small `bootstrap/` directory that's applied manually once and never touched again:

```bash
# Run once by a platform administrator
cd bootstrap/
terraform init -backend=false  # Local state only for bootstrap
terraform apply
# Creates: terraform-state-<account-id> S3 bucket + terraform-locks DynamoDB table
# Then copy the state file to S3 manually — or just leave bootstrap in local state
```

## Real Pattern: Multi-Account Data Platform on AWS

Enterprise data platforms often use separate AWS accounts per environment for blast radius isolation. Terraform handles this through provider aliases:

```hcl
# providers.tf
provider "aws" {
  alias  = "dev"
  region = "us-east-1"
  assume_role {
    role_arn = "arn:aws:iam::111111111111:role/TerraformDeployRole"
  }
}

provider "aws" {
  alias  = "prod"
  region = "us-east-1"
  assume_role {
    role_arn = "arn:aws:iam::999999999999:role/TerraformDeployRole"
  }
}

# Resources explicitly use a provider alias
resource "aws_s3_bucket" "prod_raw" {
  provider = aws.prod
  bucket   = "company-prod-raw-data"
}
```

The CI role in each account only has the permissions needed for Terraform — not full admin access. This is enforced using the least-privilege IAM principle in the `TerraformDeployRole` trust policy.

## Real Pattern: Snowflake RBAC as Code

One of the highest-value uses of the Snowflake Terraform provider is encoding Role-Based Access Control (RBAC). Snowflake's permission model is complex and error-prone when managed manually:

```hcl
# Warehouse per team
resource "snowflake_warehouse" "analytics_team" {
  name           = "ANALYTICS_WH"
  warehouse_size = "MEDIUM"
  auto_suspend   = 300
  auto_resume    = true
}

# Database and schema
resource "snowflake_database" "analytics" {
  name = "ANALYTICS"
}

resource "snowflake_schema" "curated" {
  database = snowflake_database.analytics.name
  name     = "CURATED"
}

# Role for analytics team
resource "snowflake_role" "analytics_read" {
  name = "ANALYTICS_READ"
}

resource "snowflake_role" "analytics_write" {
  name = "ANALYTICS_WRITE"
}

# Grant warehouse usage
resource "snowflake_warehouse_grant" "analytics_usage" {
  warehouse_name = snowflake_warehouse.analytics_team.name
  privilege      = "USAGE"
  roles          = [snowflake_role.analytics_read.name, snowflake_role.analytics_write.name]
}

# Grant schema privileges
resource "snowflake_schema_grant" "analytics_read" {
  database_name = snowflake_database.analytics.name
  schema_name   = snowflake_schema.curated.name
  privilege     = "USAGE"
  roles         = [snowflake_role.analytics_read.name]
}

# Assign role to user
resource "snowflake_role_grants" "analytics_members" {
  role_name = snowflake_role.analytics_read.name
  users     = var.analytics_team_users
}
```

This pattern means a new analyst getting access requires a PR that goes through code review — creating an audit trail and preventing privilege creep.

## Real Pattern: Confluent Cloud Kafka Topics

Managing Kafka topics manually leads to undocumented configurations, wrong retention settings, and replication factors that don't match production requirements:

```hcl
provider "confluent" {
  cloud_api_key    = var.confluent_api_key
  cloud_api_secret = var.confluent_api_secret
}

resource "confluent_kafka_topic" "orders_raw" {
  kafka_cluster {
    id = var.kafka_cluster_id
  }

  topic_name       = "orders.raw.v1"
  partitions_count = var.environment == "prod" ? 24 : 3
  rest_endpoint    = var.kafka_rest_endpoint

  config = {
    "retention.ms"          = "604800000"   # 7 days
    "cleanup.policy"        = "delete"
    "min.insync.replicas"   = "2"
    "compression.type"      = "lz4"
    "max.message.bytes"     = "1048576"     # 1MB
  }

  lifecycle {
    prevent_destroy = var.environment == "prod"
  }
}

# Schema Registry subject
resource "confluent_schema" "orders_raw_v1" {
  schema_registry_cluster {
    id = var.schema_registry_id
  }

  subject_name = "${confluent_kafka_topic.orders_raw.topic_name}-value"
  format       = "AVRO"
  schema       = file("${path.module}/schemas/orders.avsc")
}
```

## Common Production Pitfalls and How to Avoid Them

### 1. Drift Detection

Infrastructure drift happens when someone makes a manual change in the console. Set up scheduled drift detection:

```yaml
# .github/workflows/drift-detect.yml
on:
  schedule:
    - cron: '0 6 * * *'  # Daily at 6am

jobs:
  drift-check:
    steps:
      - name: Terraform Plan (detect drift)
        run: terraform plan -detailed-exitcode
        # Exit code 2 means changes detected (drift)
        # Exit code 0 means no changes
        continue-on-error: true

      - name: Alert on drift
        if: steps.plan.outputs.exitcode == '2'
        run: |
          curl -X POST ${{ secrets.SLACK_WEBHOOK }} \
            -d '{"text": "Infrastructure drift detected in prod! Check terraform plan."}'
```

### 2. Terraform Version Pinning

Provider and Terraform version drift between engineers and CI causes subtle bugs:

```hcl
terraform {
  required_version = "~> 1.7.0"  # Patch updates only
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "= 5.31.0"  # Exact pin for stability
    }
  }
}
```

Use `.terraform-version` file with `tfenv` for local version management:
```
1.7.0
```

### 3. Protecting Production Data Stores

```hcl
resource "aws_s3_bucket" "prod_data_lake" {
  bucket = "company-prod-data-lake"

  lifecycle {
    prevent_destroy = true
  }
}

# Even with prevent_destroy, use S3 Object Lock for true protection
resource "aws_s3_bucket_object_lock_configuration" "prod_data_lake" {
  bucket = aws_s3_bucket.prod_data_lake.id

  rule {
    default_retention {
      mode = "COMPLIANCE"
      days = 365
    }
  }
}
```

## Interview Story: Migrating a Data Platform to Terraform

A strong real-world narrative for interviews: "We inherited a data platform built entirely through the AWS console over 3 years. No one knew exactly what existed. We ran `aws resourcegroupstaggingapi get-resources` to enumerate all resources, then used `terraform import` to bring them under management one resource type at a time — starting with S3 buckets, then IAM roles, then Glue resources. Each import was a PR with a plan showing 'No changes' to verify the HCL matched reality. After 8 weeks, 340 resources were under Terraform management. We immediately found 23 S3 buckets with no encryption, 17 IAM roles with admin access that should have been scoped, and 8 Glue databases that nobody owned. The visibility alone justified the effort."

## Key Real-World Takeaways

- The bootstrap problem is real — solve it with a manual one-time `bootstrap/` module or scripts
- Multi-account setups use provider aliases and assumed roles — never hard-coded credentials
- Snowflake RBAC as code solves the audit trail problem for access management
- Drift detection as a scheduled CI job catches manual console changes before they cause incidents
- The migration story (importing existing infra) is a common interview topic — know the process
