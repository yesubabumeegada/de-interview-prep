---
title: "Repos and CI/CD - Scenario Questions"
topic: databricks
subtopic: repos-and-ci-cd
content_type: scenario_question
tags: [databricks, repos, ci-cd, interview, scenarios]
---

# Scenario Questions — Repos and CI/CD

<article data-difficulty="junior">

## 🟢 Junior: Setting Up Version Control

**Scenario:** Your team currently edits notebooks directly in the Databricks workspace (no version control). The CTO says "we need Git." Set up Repos for the team.

<details>
<summary>💡 Hint</summary>
Create a GitHub repo, connect it via Databricks Repos, establish a folder structure for pipelines, and set up branches for dev/staging/production.
</details>

<details>
<summary>✅ Solution</summary>

```python
# Step 1: Create GitHub repository
# github.com/company/data-pipelines (private repo)

# Step 2: Set up repo structure
"""
data-pipelines/
├── src/bronze/ingest_orders.py
├── src/silver/transform_orders.py
├── src/gold/aggregate_revenue.py
├── lib/common.py
├── tests/test_transforms.py
└── README.md
"""

# Step 3: Connect to Databricks Repos (via UI or CLI)
# Workspace → Repos → Add Repo → paste GitHub URL
# Creates: /Repos/username/data-pipelines

# Step 4: Set up environment folders
# /Repos/production/data-pipelines → always on `main` branch
# /Repos/staging/data-pipelines → always on `main` branch (updated before prod)
# /Repos/dev-alice/data-pipelines → Alice's feature branch

# Step 5: Update production Workflow to use Repos path
# Old: notebook_path = "/Users/alice/ingest_orders"
# New: notebook_path = "/Repos/production/data-pipelines/src/bronze/ingest_orders"

# Step 6: Migrate existing notebooks to Git
# Export notebooks → commit to GitHub → reference from Repos
```

**Key Points:**
- Code lives in Git (source of truth), synced to Databricks via Repos
- Three Repos folders: production (main), staging (main), dev (feature branches)
- Workflows reference /Repos/production/ paths (always run latest main branch code)
- Developers work on feature branches in their personal /Repos/ folder
- No more editing production notebooks directly (changes go through PRs)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: CI/CD Pipeline Design

**Scenario:** Design a CI/CD pipeline that: (1) runs unit tests on every PR, (2) deploys to staging and validates on merge, (3) deploys to production with approval. Include rollback capability.

<details>
<summary>💡 Hint</summary>
GitHub Actions with three jobs: test (PR), deploy-staging (push to main), deploy-production (manual approval via environment protection). Rollback: update Repos to previous tag.
</details>

<details>
<summary>✅ Solution</summary>

```yaml
# .github/workflows/cicd.yml
name: Data Pipeline CI/CD
on:
  pull_request: { branches: [main] }
  push: { branches: [main] }

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install -r requirements-dev.txt
      - run: pytest tests/ -v

  staging:
    if: github.ref == 'refs/heads/main'
    needs: test
    runs-on: ubuntu-latest
    steps:
      - run: databricks repos update --path /Repos/staging/pipelines --branch main
      - run: databricks jobs run-now --job-id $STAGING_JOB --wait
      - run: python scripts/validate_staging.py

  production:
    needs: staging
    runs-on: ubuntu-latest
    environment: production  # Requires approval!
    steps:
      - run: |
          # Tag the release for rollback
          git tag v$(date +%Y%m%d.%H%M)
          git push --tags
      - run: databricks repos update --path /Repos/production/pipelines --branch main
```

```bash
# ROLLBACK (if production breaks):
# Option 1: Revert to previous tag
databricks repos update --path /Repos/production/pipelines --tag v20240315.0600

# Option 2: Revert the Git commit
git revert HEAD
git push  # Triggers CI/CD again with the reverted code
```

**Key Points:**
- PR → test (automated, fast) → merge requires approval + passing tests
- Merge to main → staging auto-deploy → validate → production deploy (with approval gate)
- Git tags on each production deploy enable instant rollback
- Rollback: one command (`databricks repos update --tag vPREVIOUS`)
- Environment protection in GitHub: requires manual approval for production
- Total pipeline: PR to production in 30 minutes (including staging validation)

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Enterprise CI/CD Architecture

**Scenario:** Design CI/CD for 50 engineers across 5 teams, each owning 5-10 pipelines. Requirements: team autonomy (deploy independently), quality gates (tests + staging), cost governance (no accidental large clusters), and audit trail.

<details>
<summary>💡 Hint</summary>
Monorepo with path-based triggers (per-team CI), shared quality gate library, Terraform for infrastructure governance, and deployment audit via Git tags + system tables.
</details>

<details>
<summary>✅ Solution</summary>

```python
ENTERPRISE_CICD = {
    "repo_structure": "Monorepo with team folders (teams/sales/, teams/marketing/, etc.)",
    
    "ci_per_team": {
        "trigger": "Path-based (teams/sales/** → only sales CI runs)",
        "jobs": "lint → unit tests → staging deploy → validate → production deploy",
        "duration": "~15 minutes end-to-end",
    },
    
    "quality_gates": {
        "mandatory": [
            "Unit tests pass (pytest)",
            "Lint passes (ruff/black)",
            "Staging pipeline succeeds",
            "Data quality checks pass (row counts, null rates, schema match)",
        ],
        "blocking": "Production deploy blocked until ALL gates pass",
        "shared_library": "lib/quality_gates.py (reused by all teams)",
    },
    
    "infrastructure_governance": {
        "method": "Terraform with team-specific variables",
        "controls": [
            "Cluster policies (max workers, instance types, spot enforcement)",
            "Job budget limits (alert if team exceeds monthly threshold)",
            "Required tags (team, cost_center) on all resources",
        ],
        "approval": "Terraform changes to production require platform team review",
    },
    
    "audit_trail": {
        "who_deployed": "Git commit author + GitHub Actions run ID",
        "what_deployed": "Git tag (v20240315.0600) → exact code version",
        "when_deployed": "GitHub Actions timestamp + system.lakeflow.job_run_timeline",
        "rollback_history": "Git tags show all deployments (revert = point to older tag)",
    },
    
    "team_autonomy": {
        "own_ci": "Each team's CI/CD triggers independently (path-based)",
        "own_schedule": "Teams choose their deployment cadence",
        "own_testing": "Teams add custom quality checks beyond shared baseline",
        "guardrails": "Platform team sets boundaries, teams operate freely within them",
    },
}
```

**Key Points:**
- Monorepo + path triggers: team autonomy with shared code (best of both worlds)
- Shared quality gate library: baseline checks everyone must pass (consistency)
- Terraform for governance: cluster policies prevent cost explosions
- Git tags = deployment audit trail (who, what, when, easy rollback)
- Platform team sets guardrails; teams self-serve within them
- Each team can deploy independently (one team's bad deploy doesn't block others)
- Total setup: 2-3 weeks for platform team to build, then self-service for all teams

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What are Databricks Repos and what problem do they solve?**
A: Databricks Repos integrates Git repositories directly into the Databricks workspace, allowing notebooks and files to be version-controlled, branched, and collaborated on using standard Git workflows. It solves the problem of notebooks being isolated in the workspace with no version history or code review process.

**Q: What is the difference between Databricks Repos and Databricks Files (workspace files)?**
A: Repos syncs content from a remote Git repository and supports full Git operations (branch, commit, push, pull, merge). Workspace files are non-Git-backed files in the workspace. Repos is the preferred approach for production code that needs version control and CI/CD integration.

**Q: How do you implement CI/CD for Databricks notebooks and jobs?**
A: Use a Git repository as the source of truth, run automated tests in CI (unit tests with pytest, integration tests on a test cluster), deploy configuration via the Databricks REST API or Terraform, and promote code through environments (dev → staging → prod) using branch-based or tag-based deployment strategies.

**Q: What is the Databricks Asset Bundles (DAB) framework?**
A: Databricks Asset Bundles is a YAML-based configuration framework for defining, deploying, and managing Databricks resources (jobs, pipelines, clusters, permissions) as code. It replaces ad-hoc REST API scripts with a structured, version-controlled, environment-aware deployment system.

**Q: How do you run automated tests for Databricks notebooks in CI?**
A: Extract testable logic from notebooks into Python modules, write unit tests with pytest that run locally or in CI without a Databricks cluster, and write integration tests that run on a Databricks cluster via the REST API Jobs API or `databricks-connect`. Nutter is a framework specifically for testing Databricks notebooks.

**Q: What is databricks-connect and how is it used in CI/CD?**
A: `databricks-connect` is a library that redirects local Spark API calls to a remote Databricks cluster, allowing you to develop and test Spark code locally in an IDE while executing on a real cluster. In CI/CD, it enables running integration tests against a real cluster without uploading notebooks.

**Q: How do you manage environment-specific configuration in a Databricks CI/CD pipeline?**
A: Use Databricks Asset Bundles targets (dev, staging, prod) with environment-specific variable substitution for cluster sizes, table names, and secrets. Store sensitive configuration in Databricks Secrets backed by a secrets manager (AWS Secrets Manager, Azure Key Vault, HashiCorp Vault).

**Q: What are the key stages of a Databricks CI/CD pipeline?**
A: Code commit → PR/code review with static analysis (flake8, mypy) → CI: unit tests (local), integration tests (test cluster via Jobs API) → artifact build (wheel or bundle) → staging deployment (Databricks Asset Bundles deploy) → automated smoke test → production deployment with approval gate.

---

## 💼 Interview Tips

- Show that you treat notebooks as code artifacts subject to the same engineering discipline as application code — version control, code review, testing, and deployment pipelines.
- Know Databricks Asset Bundles — it is the modern standard for infrastructure-as-code in Databricks and asking about it is common in senior interviews.
- Be ready to discuss testing strategy: how you separate unit-testable logic from cluster-dependent code is a key design discipline that senior interviewers probe.
- Mention the promotion workflow through environments (dev → staging → prod) with appropriate approval gates — this shows production engineering maturity.
- Common mistake: using the workspace UI as the source of truth for production notebooks — all production code must live in Git, deployed via CI/CD.
- Senior interviewers at engineering-mature organizations will ask about secrets management — know how Databricks Secrets integrates with cloud key vaults.
