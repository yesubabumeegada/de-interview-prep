---
title: "Notebooks & Collaboration - Scenario Questions"
topic: databricks
subtopic: notebooks-collaboration
content_type: scenario_question
tags: [databricks, notebooks, scenarios, interview, collaboration, best-practices]
---

# Scenario Questions — Notebooks & Collaboration

<article data-difficulty="junior">

## 🟢 Junior: Build a Parameterized Data Quality Notebook

**Scenario:** Your team runs manual data quality checks on 10 different tables, each with the same validation logic. Currently there are 10 separate notebooks with copy-pasted code. Consolidate them into one parameterized notebook that can validate any table.

<details>
<summary>✅ Solution</summary>

```python
# --- data_quality_check.py ---

# 1. Parameters — set by user or Workflow
dbutils.widgets.text("table_name", "prod.sales.orders", "Table (catalog.schema.table)")
dbutils.widgets.text("primary_key", "order_id", "Primary Key Column")
dbutils.widgets.text("not_null_columns", "order_id,customer_id,amount", "Required Non-Null Columns (comma-separated)")
dbutils.widgets.text("min_row_count", "1000", "Minimum Expected Row Count")

table_name = dbutils.widgets.get("table_name")
primary_key = dbutils.widgets.get("primary_key")
not_null_cols = [c.strip() for c in dbutils.widgets.get("not_null_columns").split(",")]
min_rows = int(dbutils.widgets.get("min_row_count"))

print(f"Validating: {table_name}")

# 2. Run validation checks
df = spark.table(table_name)
total_rows = df.count()

checks = {}

# Check 1: Row count
checks["min_row_count"] = {
    "expected": f">= {min_rows}",
    "actual": total_rows,
    "passed": total_rows >= min_rows
}

# Check 2: Null checks
for col in not_null_cols:
    null_count = df.filter(f"{col} IS NULL").count()
    checks[f"not_null_{col}"] = {
        "expected": 0,
        "actual": null_count,
        "passed": null_count == 0
    }

# Check 3: Duplicate primary key
dup_count = df.groupBy(primary_key).count().filter("count > 1").count()
checks["no_duplicate_pk"] = {
    "expected": 0,
    "actual": dup_count,
    "passed": dup_count == 0
}

# 3. Display results
import pyspark.sql.functions as F
results_df = spark.createDataFrame([
    {"check": k, "expected": str(v["expected"]), "actual": str(v["actual"]),
     "status": "PASS" if v["passed"] else "FAIL"}
    for k, v in checks.items()
])
display(results_df)

# 4. Exit with summary
failed = [k for k, v in checks.items() if not v["passed"]]
if failed:
    dbutils.notebook.exit(f"FAIL: {', '.join(failed)}")
else:
    dbutils.notebook.exit(f"PASS: {len(checks)} checks, {total_rows:,} rows")
```

**Run for any table without code changes:**
```python
# From an orchestrator notebook or Workflow
for table_config in [
    {"table_name": "prod.sales.orders", "primary_key": "order_id",
     "not_null_columns": "order_id,customer_id,amount", "min_row_count": "1000"},
    {"table_name": "prod.customers.profiles", "primary_key": "customer_id",
     "not_null_columns": "customer_id,email", "min_row_count": "500"},
]:
    result = dbutils.notebook.run("/Shared/validation/data_quality_check",
                                   timeout_seconds=300, arguments=table_config)
    print(f"{table_config['table_name']}: {result}")
```

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Set Up Git Workflow for Notebook Development

**Scenario:** Your team has 5 data engineers working on notebooks in a shared Databricks workspace. Currently they edit notebooks directly — no versioning, no code review, frequent conflicts. Design a Git-based collaboration workflow.

<details>
<summary>✅ Solution</summary>

**Architecture:**

```
GitHub: org/de-pipelines
  main branch (protected)
  feature branches per engineer

Databricks Repos:
  /Repos/org/de-pipelines/
    notebooks/         ← pipeline notebooks
    src/               ← shared Python utilities
    tests/             ← test notebooks
    .github/workflows/ ← CI/CD
```

**Step 1: Connect Repos to GitHub**
```bash
# In Databricks UI: Repos → Add Repo → GitHub URL
# Or via CLI:
databricks repos create \
  --url https://github.com/org/de-pipelines \
  --provider gitHub \
  --path /Repos/org/de-pipelines
```

**Step 2: Branching workflow**
```bash
# Each engineer works on a feature branch
# (Done in Repos UI: Branch dropdown → Create branch)
# Engineer Alice:
git checkout -b feature/new-customer-pipeline

# Make changes to notebooks
# Commit and push from Repos UI → Commit & Push button

# Or from CLI:
databricks repos update \
  --repo-id <id> \
  --branch feature/new-customer-pipeline
```

**Step 3: Notebook as Python file (clean diffs)**

Databricks Repos stores notebooks as `.py` files:
```python
# Databricks notebook source - DO NOT EDIT DIRECTLY
# MAGIC %md
# MAGIC # Customer Pipeline
# MAGIC Owner: alice@company.com

# COMMAND ----------

import pyspark.sql.functions as F

df = spark.table("prod.customers.profiles")

# COMMAND ----------

%sql
SELECT COUNT(*) FROM prod.customers.profiles
```

Git diff of a notebook change:
```diff
- df = spark.table("prod.customers.profiles")
+ df = spark.table("prod.customers.profiles").filter("status = 'active'")
```
Clean, reviewable — just like code.

**Step 4: Branch protection + PR review**
```yaml
# GitHub branch protection rules:
# - Require 1 reviewer approval before merge
# - Require CI status checks to pass
# - No direct pushes to main
```

**Step 5: Per-engineer workflow**
```
1. Pull latest main → create feature branch in Repos UI
2. Develop and test notebook in personal/dev cluster
3. Commit via Repos UI → push to GitHub
4. Open PR → peer review comments on specific lines
5. CI runs integration tests on staging cluster
6. Merge to main → Workflow automatically deploys from main branch
```

**Key benefits over unversioned notebooks:**
- Every change is attributable (who, when, why)
- Rollback is one `git revert` away
- Code review catches bugs before production
- No more "who changed what" mystery

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design a Notebook Lifecycle Policy for a Growing Team

**Scenario:** Your 20-person DE team has accumulated 3,000+ notebooks over 2 years. Problems: (1) nobody knows which notebooks are still active, (2) critical production notebooks have no owners, (3) data scientists modify production notebooks without review, (4) secrets are hardcoded in several notebooks discovered during a security audit. Design a lifecycle management policy.

<details>
<summary>✅ Solution</summary>

**Policy framework: three notebook tiers**

```
Tier 1: Production (critical)
  Location: /Repos/org/de-pipelines/notebooks/
  Requirements:
    - Git-based (in Repos, not /Users or /Shared)
    - PR review required (2 approvers)
    - CI/CD tests must pass before merge
    - Header comment: owner, schedule, output table, last-modified
    - No hardcoded secrets (enforced by CI secret scanner)
    - Monitored in Databricks Workflows

Tier 2: Shared utilities (reference)
  Location: /Repos/org/de-pipelines/utilities/
  Requirements:
    - Git-based
    - 1 PR reviewer required
    - Owner tag in header

Tier 3: Exploratory (ephemeral)
  Location: /Users/{user}/ or /Shared/sandbox/
  Requirements:
    - Clearly labeled "EXPLORATORY — do not use in production"
    - Auto-archived after 90 days of no edits
    - Not referenced by any Workflow
```

**Secret scanning in CI:**

```python
# .github/workflows/security_check.py (runs in CI on every PR)
import re, sys

PATTERNS = [
    (r'sk-[A-Za-z0-9]{32,}', "OpenAI API key"),
    (r'[A-Za-z0-9+/]{40}={0,2}', "potential AWS key"),
    (r'https://hooks\.slack\.com/services/[A-Z0-9/]+', "Slack webhook"),
    (r'dbutils\.secrets.*hardcoded', "hardcoded secret reference"),
    (r"password\s*=\s*['\"][^'\"]{6,}", "hardcoded password"),
]

found_secrets = []
for filepath in sys.argv[1:]:
    with open(filepath) as f:
        content = f.read()
    for pattern, description in PATTERNS:
        if re.search(pattern, content):
            found_secrets.append(f"{filepath}: {description}")

if found_secrets:
    print("SECURITY: Hardcoded secrets detected!")
    for s in found_secrets:
        print(f"  - {s}")
    sys.exit(1)
print("Security check passed")
```

**Notebook ownership audit (weekly):**

```python
# Query notebook metadata via Databricks SDK
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Find all notebooks without owner tag in header
def check_notebook_header(path: str) -> dict:
    try:
        content = w.workspace.export(path, format="SOURCE").content.decode()
        has_owner = "owner:" in content.lower() or "Owner:" in content
        has_schedule = "Schedule:" in content
        has_output = "Output:" in content
        return {"path": path, "has_owner": has_owner,
                "has_schedule": has_schedule, "compliant": has_owner and has_schedule}
    except Exception:
        return {"path": path, "has_owner": False, "compliant": False}

# Scan production notebooks
production_notebooks = w.workspace.list("/Repos/org/de-pipelines/notebooks/", recursive=True)
results = [check_notebook_header(nb.path) for nb in production_notebooks if nb.object_type.name == "NOTEBOOK"]

non_compliant = [r for r in results if not r["compliant"]]
print(f"Non-compliant: {len(non_compliant)}/{len(results)}")

# Send weekly report to team lead
import requests
if non_compliant:
    msg = f"📋 Weekly governance: {len(non_compliant)} notebooks missing owner/schedule header\n"
    msg += "\n".join([f"  - {r['path']}" for r in non_compliant[:10]])
    requests.post(webhook_url, json={"text": msg})
```

**Migration plan for existing notebooks:**

```
Week 1: Audit — identify all 3,000 notebooks
Week 2: Classify — tier 1/2/3 labels + auto-archive flag
Week 3: Secret remediation — fix all hardcoded secrets
Week 4: Move tier 1 to Repos (migrate from /Users to git-backed Repos)
Week 5: Set up CI/CD for tier 1 notebooks
Week 6: Enforce branch protection + mandatory PR review
Ongoing: Monthly audit, quarterly cleanup of Tier 3
```

**Outcome targets:**
- 0 hardcoded secrets in tier 1/2 notebooks (audited in CI)
- 100% of production notebooks have owner + schedule header
- All production changes go through PR review
- Stale notebooks auto-archived after 90 days

</details>
</article>
