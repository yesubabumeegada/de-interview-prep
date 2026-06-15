---
title: "Airflow Testing - Real World"
topic: airflow
subtopic: airflow-testing
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [airflow, testing, github-actions, pre-commit, staging, incidents, tdd]
---

# Airflow Testing — Real World

## Production Testing Setup: GitHub Actions Workflow

A complete CI/CD pipeline for Airflow DAGs with multiple test stages:

```yaml
# .github/workflows/airflow-ci.yml
name: Airflow DAG CI

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  # Stage 1: Fast checks on every commit (< 2 minutes)
  dag-integrity:
    name: DAG Integrity Tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python 3.11
        uses: actions/setup-python@v4
        with:
          python-version: "3.11"
          cache: pip
      
      - name: Install Airflow and deps
        run: |
          pip install "apache-airflow==2.8.0" --constraint constraints.txt
          pip install -r requirements.txt pytest pytest-cov
      
      - name: Run integrity tests (fast)
        run: pytest tests/test_dag_integrity.py tests/unit/ -v --tb=short --timeout=60
        env:
          AIRFLOW__CORE__LOAD_EXAMPLES: "False"
          AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: sqlite:///test.db
          AIRFLOW__CORE__UNIT_TEST_MODE: "True"

  # Stage 2: Integration tests on PR (5-10 minutes)
  integration:
    name: Integration Tests
    runs-on: ubuntu-latest
    needs: dag-integrity
    if: github.event_name == 'pull_request'
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_DB: airflow_test
          POSTGRES_USER: airflow
          POSTGRES_PASSWORD: airflow
        options: >-
          --health-cmd pg_isready
          --health-interval 5s
          --health-retries 5
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up Python 3.11
        uses: actions/setup-python@v4
        with:
          python-version: "3.11"
          cache: pip
      
      - name: Install dependencies
        run: |
          pip install "apache-airflow==2.8.0" --constraint constraints.txt
          pip install -r requirements.txt pytest
      
      - name: Initialize Airflow DB
        run: airflow db init
        env:
          AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql://airflow:airflow@localhost/airflow_test
          AIRFLOW__CORE__LOAD_EXAMPLES: "False"
      
      - name: Run integration tests
        run: pytest tests/integration/ -v --tb=short --timeout=300
        env:
          AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql://airflow:airflow@localhost/airflow_test
          AIRFLOW__CORE__EXECUTOR: LocalExecutor
          AIRFLOW__CORE__LOAD_EXAMPLES: "False"
      
      - name: Upload test results
        if: always()
        uses: actions/upload-artifact@v3
        with:
          name: test-results
          path: test-results.xml
```

---

## Pre-Commit Hooks for DAG Validation

Pre-commit hooks catch issues before code even reaches CI:

```yaml
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.5.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-merge-conflict

  - repo: https://github.com/astral-sh/ruff-pre-commit
    rev: v0.1.0
    hooks:
      - id: ruff  # Linting
      - id: ruff-format  # Formatting

  - repo: local
    hooks:
      - id: airflow-dag-integrity
        name: Airflow DAG Integrity Check
        language: python
        entry: python
        args: ["-c"]
        # Inline script: import all changed DAG files and report errors
        pass_filenames: false
        always_run: false
        files: "^dags/.*\\.py$"
        additional_dependencies: ["apache-airflow==2.8.0"]
        # Alternative: call a script
        # entry: scripts/validate_dags.sh
```

```bash
# scripts/validate_dags.sh
#!/bin/bash
set -e

echo "Checking DAG imports..."
python -c "
import sys
from airflow.models import DagBag

db = DagBag(dag_folder='dags/', include_examples=False)

if db.import_errors:
    print('DAG IMPORT ERRORS DETECTED:')
    for filename, error in db.import_errors.items():
        print(f'  {filename}: {error}')
    sys.exit(1)

print(f'OK: {len(db.dags)} DAGs loaded without errors')
"
```

---

## Staging Environment DAG Runs Before Production

A staging environment catches real integration issues that unit tests miss (missing connections, permissions, wrong table names):

```python
# dags/platform/promote_to_prod.py
"""DAG that runs in staging and blocks promotion to prod until validated."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

CANDIDATE_DAG_ID = "daily_revenue_etl"

def validate_staging_run(**context):
    """Check that the DAG ran successfully in staging."""
    from airflow.models import DagRun
    from airflow.utils.state import State
    
    staging_runs = DagRun.find(
        dag_id=f"staging_{CANDIDATE_DAG_ID}",
        execution_date=context["execution_date"],
    )
    
    if not staging_runs:
        raise ValueError(f"No staging run found for {CANDIDATE_DAG_ID}")
    
    run = staging_runs[0]
    if run.state != State.SUCCESS:
        raise ValueError(f"Staging run status: {run.state} (expected SUCCESS)")
    
    print(f"Staging validation passed — promoting {CANDIDATE_DAG_ID} to production")

with DAG(
    dag_id=f"promote_{CANDIDATE_DAG_ID}",
    schedule_interval=None,  # Triggered manually
    start_date=datetime(2024, 1, 1),
    tags=["platform"],
) as dag:
    
    wait_for_staging = ExternalTaskSensor(
        task_id="wait_for_staging_run",
        external_dag_id=f"staging_{CANDIDATE_DAG_ID}",
        timeout=3600,
        mode="reschedule",
    )
    
    validate = PythonOperator(
        task_id="validate_staging",
        python_callable=validate_staging_run,
    )
    
    wait_for_staging >> validate
```

---

## Real Incidents from Untested DAGs

### Incident 1: Duplicate Data from Non-Idempotent Task

**What happened:** A `load_orders` task used `INSERT INTO orders SELECT ...`. After a network timeout caused a retry, the rows were inserted twice. The business reported "order counts are doubled" three weeks later.

**Root cause:** No idempotency test existed. The task had been running for months before the first retry occurred.

```python
# BEFORE (broken — not idempotent)
def load_orders(**context):
    execute("INSERT INTO orders SELECT * FROM staging.orders WHERE date = %s", (context["ds"],))

# AFTER (idempotent — DELETE first)
def load_orders(**context):
    execute("DELETE FROM orders WHERE order_date = %s", (context["ds"],))
    execute("INSERT INTO orders SELECT * FROM staging.orders WHERE date = %s", (context["ds"],))

# TEST that prevents regression
def test_load_orders_idempotent():
    context = {"ds": "2024-01-15", "task_instance": MagicMock()}
    load_orders(**context)
    count1 = query("SELECT COUNT(*) FROM orders WHERE order_date = '2024-01-15'")[0][0]
    load_orders(**context)
    count2 = query("SELECT COUNT(*) FROM orders WHERE order_date = '2024-01-15'")[0][0]
    assert count1 == count2, "Load task is not idempotent"
```

### Incident 2: Wrong Partition Overwritten

**What happened:** A dynamic DAG that processed data by `region` had a bug in the BranchPythonOperator — it always returned `"process_us"` regardless of the input. European data was loaded into the US partition. No branch logic test existed.

```python
# BEFORE (bug — always returns "process_us")
def choose_region(**context):
    region = context["task_instance"].xcom_pull(key="region")
    if region = "us":   # ← SyntaxError caught at import, but logic bugs are silent
        return "process_us"
    return "process_eu"

# TEST that would have caught this
@pytest.mark.parametrize("region,expected", [
    ("us", "process_us"),
    ("eu", "process_eu"),
    ("apac", "process_apac"),
])
def test_region_branch(region, expected):
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = region
    result = choose_region(task_instance=mock_ti)
    assert result == expected, f"For region={region}, expected {expected} got {result}"
```

---

## Test-Driven DAG Development Workflow

TDD for Airflow: write the test before writing the task function.

```
1. Write the test (defines expected behavior)
   └── test_extract_returns_record_count()
   └── test_load_is_idempotent()
   └── test_branch_routes_correctly()

2. Run tests — all fail (red)

3. Write the minimal task code to make tests pass (green)

4. Refactor — clean up code without breaking tests

5. DAG integrity test — run DagBag load test

6. Commit — pre-commit hooks run DAG validation
```

```python
# Example TDD cycle for a new extract task

# Step 1: Write the test first
def test_extract_customer_data_returns_count():
    """extract_customer_data should return the number of rows fetched."""
    from unittest.mock import MagicMock, patch
    
    context = {"ds": "2024-01-15", "task_instance": MagicMock()}
    
    with patch("dags.customer_dag.get_source_connection") as mock_conn:
        mock_conn.return_value.execute.return_value = [{"id": 1}, {"id": 2}]
        # This import will FAIL until we write the function
        from dags.customer_dag import extract_customer_data
        result = extract_customer_data(**context)
    
    assert result == 2  # Returns row count

# Step 2: Write the function to make it pass
# (in dags/customer_dag.py)
def extract_customer_data(**context):
    conn = get_source_connection()
    rows = conn.execute(f"SELECT id FROM customers WHERE updated_date = '{context['ds']}'")
    return len(rows)
```

---

## Key Takeaways

- GitHub Actions CI should have two stages: fast integrity tests on every commit, integration tests on PRs only
- Pre-commit hooks with `DagBag` validation catch import errors before code reaches CI
- Staging environments catch real integration issues (wrong connections, permissions, missing tables) that mocks cannot
- The two most common untested-DAG incidents are non-idempotent loads (duplicate data) and uncovered branch logic (wrong partition)
- TDD for Airflow: write the test first, then write the task function — ensures every callable has a corresponding test from day one
