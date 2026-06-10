---
title: "Repos and CI/CD - Senior Deep Dive"
topic: databricks
subtopic: repos-and-ci-cd
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [databricks, repos, ci-cd, production, terraform, gitops, governance]
---

# Databricks Repos and CI/CD — Senior-Level Deep Dive

## GitOps for Databricks

All infrastructure and pipeline configuration managed as code:

```hcl
# terraform/main.tf — complete Databricks infrastructure

# Catalogs and schemas
resource "databricks_catalog" "production" { name = "production" }
resource "databricks_schema" "bronze" { catalog_name = "production"; name = "bronze" }
resource "databricks_schema" "silver" { catalog_name = "production"; name = "silver" }
resource "databricks_schema" "gold" { catalog_name = "production"; name = "gold" }

# Permissions
resource "databricks_grants" "analysts" {
  schema = "production.gold"
  grant { principal = "data-analysts"; privileges = ["USE_SCHEMA", "SELECT"] }
}

# Jobs
resource "databricks_job" "daily_etl" {
  name = "daily_etl"
  schedule { quartz_cron_expression = "0 0 6 * * ?" }
  
  task {
    task_key = "ingest"
    notebook_task { notebook_path = "/Repos/production/pipelines/src/bronze/ingest" }
    job_cluster_key = "etl"
  }
  
  job_cluster {
    job_cluster_key = "etl"
    new_cluster {
      spark_version = "14.3.x-photon-scala2.12"
      node_type_id  = "i3.xlarge"
      autoscale { min_workers = 4; max_workers = 12 }
      aws_attributes { availability = "SPOT_WITH_FALLBACK" }
    }
  }
}

# Cluster policies
resource "databricks_cluster_policy" "standard" {
  name = "standard-etl"
  definition = jsonencode({
    "autotermination_minutes": {"type": "range", "minValue": 15, "maxValue": 60}
    "node_type_id": {"type": "allowlist", "values": ["i3.xlarge", "m5.xlarge"]}
  })
}
```

### Multi-Environment with Terraform Workspaces

```hcl
# Same Terraform code, different variables per environment:
# terraform workspace select staging → applies to staging workspace
# terraform workspace select production → applies to production workspace

variable "environment" { default = "production" }
variable "catalog_name" { default = "production" }
variable "cluster_size" {
  default = { min = 4, max = 16 }  # Overridden per workspace
}

# CI/CD pipeline:
# 1. terraform plan (show changes)
# 2. terraform apply -target=staging (deploy to staging first)
# 3. Validate staging
# 4. terraform apply -target=production (deploy to production)
```

---

## Advanced CI/CD Patterns

### Blue-Green Deployment

```python
# Deploy new version alongside old (blue-green):
# 1. Deploy new code to /Repos/production-green/ (new version)
# 2. Update job to point to production-green
# 3. Run one cycle → validate
# 4. If good: rename green→production, delete old
# 5. If bad: revert job to original /Repos/production/ (instant rollback)

def blue_green_deploy(new_branch: str):
    # Step 1: Create green repo
    create_repo("/Repos/production-green/pipelines", branch=new_branch)
    
    # Step 2: Update job to use green
    update_job_notebook_path(job_id, "/Repos/production-green/pipelines/src/ingest")
    
    # Step 3: Run and validate
    run_id = trigger_job(job_id)
    wait_for_completion(run_id)
    
    if validate_output(run_id):
        # Step 4a: Promote green to production
        delete_repo("/Repos/production/pipelines")
        rename_repo("/Repos/production-green/pipelines", "/Repos/production/pipelines")
    else:
        # Step 4b: Rollback
        update_job_notebook_path(job_id, "/Repos/production/pipelines/src/ingest")
        delete_repo("/Repos/production-green/pipelines")
        alert("Deployment failed! Rolled back to previous version.")
```

### Canary Deployment for Pipelines

```python
# Run new code on 10% of data first, compare results with old code

def canary_deploy(job_id: int, new_code_path: str, sample_pct: float = 0.1):
    """Deploy new code on a sample, validate, then roll out fully."""
    
    # Run old code on full data (baseline)
    old_result = run_job_with_params(job_id, {"code_path": OLD_PATH, "sample": "1.0"})
    
    # Run new code on 10% sample
    new_result = run_job_with_params(job_id, {"code_path": new_code_path, "sample": str(sample_pct)})
    
    # Compare results (row counts, aggregates should be proportional)
    if validate_canary(old_result, new_result, sample_pct):
        # Promote: new code on full data
        update_job(job_id, notebook_path=new_code_path)
        print("Canary passed! New code deployed to production.")
    else:
        alert("Canary FAILED! New code produces different results. Investigate.")
```

---

## Release Management

```python
# Versioned releases with Git tags

RELEASE_PROCESS = {
    "1_prepare": "Create release branch: release/v2.3.0",
    "2_test": "Run full test suite on release branch",
    "3_tag": "git tag v2.3.0 → immutable release marker",
    "4_deploy_staging": "Update /Repos/staging/ to tag v2.3.0",
    "5_validate": "Run staging pipeline, validate outputs",
    "6_deploy_production": "Update /Repos/production/ to tag v2.3.0",
    "7_monitor": "Monitor production for 24 hours",
    "8_rollback_if_needed": "Update /Repos/production/ to tag v2.2.0 (previous)",
}

# Rollback is instant: just point Repos to previous tag
# databricks repos update --path /Repos/production/pipelines --tag v2.2.0
```

---

## Monorepo vs Multi-Repo

| Aspect | Monorepo (all pipelines in one repo) | Multi-Repo (one repo per pipeline) |
|--------|--------------------------------------|--------------------------------------|
| Shared code | Easy (same repo) | Requires packages/wheels |
| Team autonomy | Lower (everyone in same repo) | Higher (own repo, own schedule) |
| CI speed | Slower (tests all pipelines) | Faster (only affected pipeline) |
| Deployment | All-or-nothing (or path-based triggers) | Independent per pipeline |
| Best for | Small-medium teams (<20 engineers) | Large teams (20+), strong boundaries |

```yaml
# Monorepo with path-based CI triggers:
on:
  push:
    paths:
      - 'pipelines/orders/**'  # Only run if orders pipeline changed
      - 'lib/**'               # Or shared library changed

# This gives monorepo convenience with pipeline-specific CI speed
```

---

## Automated Rollback

```python
# Automatically rollback if production pipeline fails after deploy

class AutoRollback:
    def deploy_with_safety(self, new_version: str):
        """Deploy and auto-rollback on failure."""
        old_version = self.get_current_version()
        
        # Deploy new version
        self.update_repos("production", new_version)
        
        # Run pipeline
        run = self.trigger_and_wait(self.production_job_id)
        
        if run.status == "FAILED":
            # Auto-rollback!
            self.update_repos("production", old_version)
            self.alert(f"Auto-rollback: {new_version} → {old_version} (pipeline failed)")
            raise DeploymentFailed(f"Version {new_version} failed, rolled back to {old_version}")
        
        # Validate output quality
        quality = self.validate_output()
        if quality["score"] < 0.95:
            self.update_repos("production", old_version)
            self.alert(f"Auto-rollback: quality {quality['score']:.2f} below 0.95 threshold")
            raise QualityRegression(f"Quality dropped to {quality['score']:.2f}")
        
        self.log_successful_deploy(new_version)
```

---

## Interview Tips

> **Tip 1:** "How do you manage Databricks as code?" — GitOps with Terraform: all infrastructure (catalogs, permissions, jobs, clusters, policies) defined in Terraform config. CI/CD applies changes: staging first, then production (with approval gate). Repos syncs notebook code from Git. Everything is version-controlled, reviewed, and auditable.

> **Tip 2:** "How do you handle rollbacks?" — For code: update Repos to previous Git tag/commit (instant, one command). For infrastructure: `terraform apply` with previous state (reverts job configs). For data: Delta time travel (RESTORE TABLE to previous version). For complete rollback: all three together (code + infra + data).

> **Tip 3:** "Monorepo vs multi-repo for data pipelines?" — Monorepo for <20 engineers: simpler shared code, single CI/CD pipeline, one source of truth. Multi-repo for >20 engineers: team autonomy, independent deployment schedules, faster CI (only test affected pipeline). Compromise: monorepo with path-based CI triggers (fast CI + shared code).

## ⚡ Cheat Sheet

**Repos vs Workspace files**
| Feature | Repos | Workspace files |
|---|---|---|
| Git sync | Yes (per commit/branch) | No |
| PR workflow | Via Git provider | No |
| File types | All (notebooks, .py, .sql) | All |
| Recommended | Production code | Quick experiments |

**CI/CD pipeline pattern**
```
Dev branch → PR → CI (lint + unit tests) → merge to main
→ CD (deploy bundle to staging → integration test → promote to prod)
```

**Databricks Asset Bundles (DAB)**
- `databricks.yml`: defines jobs, pipelines, clusters, permissions as code
- `databricks bundle deploy --target prod`: deploys all resources
- `databricks bundle run job_name`: triggers a job run
- Supports variable substitution per target (`dev`, `staging`, `prod`)

**Testing strategy**
- Unit tests: `pytest` with `pyspark.testing` or `chispa`; use `spark.builder.master("local[*]")`
- Integration tests: deploy to staging cluster; run against real Delta tables
- DQ tests: dbt tests or Great Expectations in CI before promoting models

**Secrets in CI/CD**
- Databricks secrets: `dbutils.secrets.get(scope, key)` — inject at runtime, never in code
- GitHub Actions: store PAT as `DATABRICKS_TOKEN` secret; use `databricks-labs/deco` action
- `.databrickscfg`: local only; never commit

**Branching strategy**
- Feature branches → dev workspace (personal); main → staging; tags/releases → prod
- Repos API: `/api/2.0/repos/{id}` → checkout branch programmatically in CI
- Notebook outputs: strip with `nbstripout` pre-commit hook to keep diffs clean
