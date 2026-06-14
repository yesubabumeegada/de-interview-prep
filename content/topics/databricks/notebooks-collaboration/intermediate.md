---
title: "Notebooks & Collaboration - Intermediate"
topic: databricks
subtopic: notebooks-collaboration
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, notebooks, git, repos, testing, notebook-workflows, best-practices]
---

# Notebooks & Collaboration — Intermediate Concepts

## Databricks Repos (Git Integration)

Databricks Repos connects notebooks directly to Git — version control, branching, and code review:

```
Repos/
  my-org/
    de-pipeline-repo/     ← cloned from GitHub/GitLab/Azure DevOps
      notebooks/
        ingestion.py
        transformation.py
        validation.py
      src/
        utils/
          helpers.py
      tests/
        test_helpers.py
```

**Working with Repos:**
```python
# In the Repos UI:
# Add Repo → paste GitHub URL → clone
# Then work just like a local git repo: branch, commit, push, PR

# From CLI (Databricks CLI):
databricks repos create --url https://github.com/org/repo --provider gitHub
databricks repos update --repo-id 123 --branch feature/new-pipeline
databricks repos list
```

**Best practices:**
- Never commit to `main` directly — use feature branches
- Notebooks in Repos are Python files (`.py`) not `.ipynb` — cleaner diffs
- Pull before working to avoid conflicts

---

## Notebook as a Python Module

When using Repos, notebooks can import from adjacent Python files:

```python
# src/utils/data_quality.py
def validate_not_null(df, columns: list) -> dict:
    results = {}
    for col in columns:
        null_count = df.filter(f"{col} IS NULL").count()
        results[col] = {"null_count": null_count, "valid": null_count == 0}
    return results

def validate_schema(df, expected_schema: dict) -> list:
    errors = []
    for col, dtype in expected_schema.items():
        if col not in df.columns:
            errors.append(f"Missing column: {col}")
        elif str(df.schema[col].dataType) != dtype:
            errors.append(f"Wrong type for {col}: expected {dtype}, got {df.schema[col].dataType}")
    return errors
```

```python
# In a notebook (same Repo)
import sys
sys.path.append("/Workspace/Repos/org/repo/src")

from utils.data_quality import validate_not_null, validate_schema

df = spark.table("prod.sales.orders")
results = validate_not_null(df, ["order_id", "customer_id", "amount"])
print(results)
```

---

## Running Notebooks from Other Notebooks

```python
# Run a child notebook and get the return value
result = dbutils.notebook.run(
    path="/Workspace/Shared/pipelines/data_quality_check",
    timeout_seconds=300,
    arguments={
        "table_name": "prod.sales.orders",
        "date": "2024-01-15",
        "fail_on_error": "true"
    }
)
print(f"Data quality result: {result}")
# result is the string passed to dbutils.notebook.exit() in the child

# Pattern: orchestrator notebook calls multiple child notebooks
tables = ["orders", "customers", "products"]
for table in tables:
    result = dbutils.notebook.run(
        f"/Shared/validation/validate_table",
        timeout_seconds=120,
        arguments={"table": f"prod.sales.{table}"}
    )
    if result != "passed":
        raise Exception(f"Validation failed for {table}: {result}")
```

---

## Notebook Testing Patterns

```python
# Test helper functions directly in notebooks
import unittest

class TestDataTransformations(unittest.TestCase):

    def setUp(self):
        self.sample_data = spark.createDataFrame([
            (1, "alice@test.com", 100.0, "2024-01-01"),
            (2, None, 50.0, "2024-01-02"),
            (3, "bob@test.com", -10.0, "2024-01-03"),
        ], ["id", "email", "amount", "date"])

    def test_null_check(self):
        from utils.data_quality import validate_not_null
        results = validate_not_null(self.sample_data, ["id", "email"])
        self.assertTrue(results["id"]["valid"])
        self.assertFalse(results["email"]["valid"])

    def test_negative_amounts(self):
        invalid = self.sample_data.filter("amount < 0")
        self.assertEqual(invalid.count(), 1)

# Run tests in notebook
suite = unittest.TestLoader().loadTestsFromTestCase(TestDataTransformations)
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)
```

---

## Notebook Output and Reporting

```python
# Rich outputs in notebooks

# 1. Formatted table with colors
from IPython.display import HTML

html_table = """
<table style='border-collapse: collapse;'>
  <tr style='background: #4CAF50; color: white;'>
    <th>Region</th><th>Revenue</th><th>Status</th>
  </tr>
  <tr><td>US East</td><td>$1.2M</td><td>✅ Above target</td></tr>
  <tr><td>EU</td><td>$0.8M</td><td>⚠️ At target</td></tr>
</table>
"""
displayHTML(html_table)  # Databricks-specific

# 2. Send notebook results to email/Slack on completion
import requests

def notify_slack(message: str, webhook_url: str):
    requests.post(webhook_url, json={"text": message})

# At end of notebook:
notify_slack(
    f"✅ Daily report complete: {row_count:,} rows processed",
    webhook_url=dbutils.secrets.get("slack", "webhook-url")
)
```

---

## Collaboration Best Practices

```python
# 1. Notebook header — context for collaborators
# Add as first markdown cell:
"""
# Daily Revenue Report
**Owner:** data-team@company.com  
**Schedule:** Runs daily at 6am UTC via Workflow  
**Parameters:** date (default: yesterday), region (default: all)  
**Output:** prod.reporting.daily_revenue  
**Last updated:** 2024-01-15 by Alice  
"""

# 2. Section headers with clear purpose
# %md ## 1. Data Ingestion
# %md ## 2. Transformation
# %md ## 3. Quality Validation
# %md ## 4. Output

# 3. Defensive parameter handling
date = dbutils.widgets.get("date")
if not date:
    from datetime import date as dt, timedelta
    date = str(dt.today() - timedelta(days=1))  # default: yesterday

print(f"Running for date: {date}")  # always log parameters at start

# 4. Checkpoint cells for long pipelines
# After each major step, check if output already exists
from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, f"dbfs:/checkpoints/{date}/step1"):
    print("Step 1 already complete, skipping")
else:
    # run step 1
    pass
```

---

## Interview Tips

> **Tip 1:** "How do you version control Databricks notebooks?" — "Use Databricks Repos — connect your workspace to GitHub/GitLab/Azure DevOps. Notebooks in Repos are stored as Python files (.py with special cell delimiters) for clean git diffs. Use feature branches, commit messages, and PRs just like regular code. Never develop directly on main."

> **Tip 2:** "How do you share code across notebooks without copy-pasting?" — "Three options: (1) Put shared functions in a Python module in the Repos directory and import with `sys.path.append`. (2) Create a shared utility notebook and `%run` it (imports all definitions into the current notebook's namespace). (3) Package shared code as a library installed on the cluster. For production, prefer option 3 (library) as it's versioned and testable."

> **Tip 3:** "What's the difference between `%run` and `dbutils.notebook.run()`?" — "`%run ./utils` executes another notebook inline in the current context — it imports all variables and functions into the calling notebook's namespace. `dbutils.notebook.run()` runs a notebook as a subprocess — isolated namespace, passes parameters, returns a string result. Use `%run` for importing shared utilities; use `dbutils.notebook.run()` for orchestrating independent pipeline steps."
