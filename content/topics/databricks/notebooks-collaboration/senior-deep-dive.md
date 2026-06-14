---
title: "Notebooks & Collaboration - Senior Deep Dive"
topic: databricks
subtopic: notebooks-collaboration
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [databricks, notebooks, production, ci-cd, notebook-testing, mlflow, code-quality]
---

# Notebooks & Collaboration — Senior Deep Dive

## When to Use Notebooks vs Python Modules

Notebooks are not always the right tool. Senior engineers know when to use each:

| Use Case | Notebooks | Python Modules/Jobs |
|----------|-----------|---------------------|
| Exploratory analysis | ✅ Best | ❌ Overkill |
| One-time data fix | ✅ Fine | ❌ Overkill |
| Production pipeline | ⚠️ Only if parameterized + in Repos | ✅ Preferred |
| Reusable utilities | ❌ Avoid | ✅ Required |
| ML experiment tracking | ✅ Works well | ✅ Also good |
| Team collaboration / review | ✅ With Repos | ✅ Both work |
| CI/CD and automated testing | ❌ Hard to test | ✅ Required |

**Rule of thumb:** If a notebook runs in production and is critical to business operations, it should be in a Repo with unit tests, a PR review, and version history.

---

## Notebook-Based CI/CD Pipeline

```yaml
# .github/workflows/databricks-ci.yml
name: Databricks Pipeline CI

on:
  pull_request:
    branches: [main]
    paths: ['notebooks/**', 'src/**']

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Install dependencies
        run: pip install databricks-sdk pytest pyspark

      - name: Run unit tests (local)
        run: pytest tests/ -v

      - name: Deploy to staging and run integration test
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: |
          databricks repos update \
            --repo-id ${{ secrets.STAGING_REPO_ID }} \
            --branch ${{ github.head_ref }}

          databricks jobs run-now \
            --job-id ${{ secrets.INTEGRATION_TEST_JOB_ID }} \
            --notebook-params '{"env": "staging", "branch": "${{ github.head_ref }}"}'
```

---

## Notebook Quality Gates

```python
# quality_gate.py — run at end of each notebook in production
class NotebookQualityGate:
    """Enforce output quality before notebook completes."""

    def __init__(self, notebook_name: str):
        self.notebook_name = notebook_name
        self.checks = []

    def assert_row_count(self, df, min_rows: int, description: str):
        count = df.count()
        passed = count >= min_rows
        self.checks.append({
            "check": description,
            "expected": f">= {min_rows}",
            "actual": count,
            "passed": passed
        })
        if not passed:
            raise AssertionError(f"FAIL {description}: {count} rows < {min_rows}")
        return self

    def assert_no_nulls(self, df, column: str):
        null_count = df.filter(f"{column} IS NULL").count()
        passed = null_count == 0
        self.checks.append({
            "check": f"No nulls in {column}",
            "expected": 0,
            "actual": null_count,
            "passed": passed
        })
        if not passed:
            raise AssertionError(f"FAIL: {null_count} nulls in {column}")
        return self

    def assert_revenue_range(self, df, amount_col: str, min_val: float, max_val: float):
        stats = df.select(F.min(amount_col).alias("min"), F.max(amount_col).alias("max")).collect()[0]
        passed = stats["min"] >= min_val and stats["max"] <= max_val
        self.checks.append({
            "check": f"{amount_col} in [{min_val}, {max_val}]",
            "passed": passed
        })
        if not passed:
            raise AssertionError(f"FAIL: {amount_col} range [{stats['min']}, {stats['max']}] outside [{min_val}, {max_val}]")
        return self

    def report(self):
        passed = sum(1 for c in self.checks if c["passed"])
        print(f"\nQuality Gate: {passed}/{len(self.checks)} checks passed")
        for c in self.checks:
            status = "✅" if c["passed"] else "❌"
            print(f"  {status} {c['check']}")

# Usage in production notebook
gate = NotebookQualityGate("daily_revenue_report")
(gate
    .assert_row_count(result_df, 1000, "At least 1000 revenue rows")
    .assert_no_nulls(result_df, "customer_id")
    .assert_revenue_range(result_df, "amount", 0, 1_000_000)
    .report())

dbutils.notebook.exit(f"success: {result_df.count()} rows")
```

---

## MLflow Integration with Notebooks

```python
# Best practice: one MLflow run per notebook execution
import mlflow

# Use notebook context for automatic run naming
context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
notebook_path = context.notebookPath().get()
cluster_id = context.clusterId().get()

with mlflow.start_run(
    run_name=f"{notebook_path.split('/')[-1]}-{date}",
    tags={
        "notebook_path": notebook_path,
        "cluster_id": cluster_id,
        "run_date": date,
        "triggered_by": "scheduled_workflow"  # or "manual"
    }
):
    mlflow.log_param("date", date)
    mlflow.log_param("source_table", "prod.sales.orders")

    # Log data quality metrics
    mlflow.log_metric("input_rows", input_df.count())
    mlflow.log_metric("null_rate", null_count / total_count)

    # Run transformations
    result = transform(input_df)

    mlflow.log_metric("output_rows", result.count())
    mlflow.log_artifact("/tmp/quality_report.html")
```

---

## Avoiding Common Notebook Anti-Patterns

```python
# ❌ Anti-pattern 1: Hardcoded credentials
api_key = "sk-abc123xyz"  # NEVER do this

# ✅ Fix: Use secrets
api_key = dbutils.secrets.get(scope="prod-secrets", key="openai-api-key")

# ❌ Anti-pattern 2: collect() on large DataFrames
all_records = large_df.collect()  # brings 1B rows to driver → OOM

# ✅ Fix: Use Spark operations or limit
sample = large_df.limit(1000).toPandas()

# ❌ Anti-pattern 3: Hardcoded paths
data = spark.read.parquet("/mnt/data/2024-01-15/")  # breaks in staging

# ✅ Fix: Parameterize
date = dbutils.widgets.get("date")
data = spark.read.parquet(f"/mnt/data/{date}/")

# ❌ Anti-pattern 4: No error handling in orchestrator notebooks
dbutils.notebook.run("child")  # if this fails, entire notebook fails silently

# ✅ Fix: Handle failures explicitly
try:
    result = dbutils.notebook.run("child", timeout_seconds=300)
except Exception as e:
    notify_slack(f"⚠️ child notebook failed: {str(e)}")
    raise  # still fail the run, but after notification

# ❌ Anti-pattern 5: Recomputing expensive operations
df1 = spark.table("big_table").filter("status = 'active'")  # computed here
df2 = df1.groupBy("region").agg(...)  # recomputes df1
df3 = df1.filter("amount > 100")     # recomputes df1 AGAIN

# ✅ Fix: Cache the expensive base DataFrame
df1 = spark.table("big_table").filter("status = 'active'").cache()
df1.count()  # materialize
df2 = df1.groupBy("region").agg(...)  # from cache
df3 = df1.filter("amount > 100")     # from cache
df1.unpersist()
```

---

## Interview Tips

> **Tip 1:** "What anti-patterns do you watch for in Databricks notebooks?" — "Five common ones: (1) Hardcoded credentials — use `dbutils.secrets`. (2) `collect()` on large DataFrames — use Spark operations instead. (3) No error handling in orchestrators — wrap `dbutils.notebook.run()` in try/except with alerting. (4) Hardcoded paths and dates — parameterize with widgets. (5) Recomputing expensive DataFrames — cache intermediate results."

> **Tip 2:** "How do you test notebooks?" — "Three levels: (1) Unit tests for pure Python functions in the src/ directory — run with pytest in CI, no Spark needed. (2) Notebook integration tests — `dbutils.notebook.run()` the notebook from a test notebook, check the exit value and output table row counts. (3) Quality gates at the end of each notebook — assert data contracts (row counts, null rates, value ranges) before `dbutils.notebook.exit('success')`."

> **Tip 3:** "When would you NOT use a notebook for a production pipeline?" — "When: (1) The pipeline needs unit tests that run in CI without a cluster. (2) Multiple engineers are working on the same file (notebook merge conflicts are painful). (3) The pipeline is a library/utility used across many notebooks. (4) The code needs strict static analysis (type checking, linters). In those cases, write Python modules in a Repo, test with pytest, and deploy as a Databricks job using a Python script entry point instead of a notebook."
