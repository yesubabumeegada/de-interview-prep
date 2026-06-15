---
title: "Airflow Testing - Fundamentals"
topic: airflow
subtopic: airflow-testing
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [airflow, testing, pytest, dagbag, ci-cd, dag-integrity]
---

# Airflow Testing — Fundamentals

## 🎯 Analogy

Testing an Airflow DAG without running it is like proofreading a recipe before cooking — you catch typos, missing steps, and impossible instructions before you've wasted ingredients. DAG integrity tests are your proofreading pass.

---

## Why DAG Testing Matters

DAGs fail in production for predictable, preventable reasons:

| Failure Type | Example | Caught By |
|---|---|---|
| Import error | `from mypackage import helper` (package not installed) | DAG load test |
| Cycle | Task A depends on Task B depends on Task A | DAG load test |
| Invalid `default_args` | `retry_delay=5` (must be timedelta) | DAG load test |
| Wrong schedule | `schedule_interval="daily"` (invalid cron) | DAG load test |
| Missing connection | Connection `my_postgres` not configured | Integration test |
| Logic error | BranchOperator returns wrong task_id | Unit test |

The first three are free to catch — they're syntax errors discovered just by importing the DAG file.

---

## DagBag Validation Patterns

`DagBag` is Airflow's internal class that loads and parses DAG files. Use it in tests to validate all your DAGs at once.

```python
# tests/test_dag_integrity.py
import pytest
from airflow.models import DagBag

@pytest.fixture(scope="session")
def dagbag():
    """Load all DAGs from the dags/ folder once for the entire test session."""
    return DagBag(dag_folder="dags/", include_examples=False)

def test_no_import_errors(dagbag):
    """Ensure no DAG files have import errors."""
    assert dagbag.import_errors == {}, (
        f"DAG import errors:\n"
        + "\n".join(f"  {file}: {err}" for file, err in dagbag.import_errors.items())
    )

def test_dagbag_loaded_dags(dagbag):
    """Ensure we have at least some DAGs loaded."""
    assert len(dagbag.dags) > 0, "No DAGs found in dags/ folder"
```

---

## DAG Integrity Tests: Structure Validation

Beyond import errors, validate that each DAG has the expected structure:

```python
# tests/test_dag_integrity.py (continued)

EXPECTED_DAGS = [
    "daily_revenue_etl",
    "customer_360_refresh", 
    "inventory_sync",
]

@pytest.mark.parametrize("dag_id", EXPECTED_DAGS)
def test_dag_exists(dagbag, dag_id):
    """Each expected DAG is present in the DagBag."""
    assert dag_id in dagbag.dags, f"DAG '{dag_id}' not found"

@pytest.mark.parametrize("dag_id", EXPECTED_DAGS)
def test_dag_has_no_cycles(dagbag, dag_id):
    """DAG has valid structure (no cycles — Airflow enforces this, but be explicit)."""
    dag = dagbag.get_dag(dag_id)
    assert dag is not None
    # test() validates the DAG structure including cycle detection
    dag.test_cycle()  # Raises CycleError if a cycle is found

@pytest.mark.parametrize("dag_id", EXPECTED_DAGS)
def test_dag_has_required_tags(dagbag, dag_id):
    """Every production DAG must have at least one team tag."""
    dag = dagbag.get_dag(dag_id)
    assert dag.tags, f"DAG '{dag_id}' has no tags — add a team tag"

@pytest.mark.parametrize("dag_id", EXPECTED_DAGS)
def test_dag_has_owner(dagbag, dag_id):
    """DAGs must have an owner set in default_args."""
    dag = dagbag.get_dag(dag_id)
    assert dag.owner != "airflow", (
        f"DAG '{dag_id}' uses default owner 'airflow' — set a real team/person"
    )
```

---

## Task Count and Dependencies

Validate that a DAG has the expected tasks and dependency structure:

```python
def test_revenue_etl_task_structure(dagbag):
    """daily_revenue_etl must have exactly these tasks in this order."""
    dag = dagbag.get_dag("daily_revenue_etl")
    
    expected_tasks = {"extract", "validate", "transform", "load", "notify"}
    actual_tasks = {task.task_id for task in dag.tasks}
    
    assert actual_tasks == expected_tasks, (
        f"Task mismatch.\nExpected: {expected_tasks}\nActual: {actual_tasks}"
    )

def test_revenue_etl_dependencies(dagbag):
    """Validate task dependency order."""
    dag = dagbag.get_dag("daily_revenue_etl")
    
    # Check that 'load' depends on 'transform'
    load_task = dag.get_task("load")
    upstream_ids = {t.task_id for t in load_task.upstream_list}
    assert "transform" in upstream_ids, "'load' must depend on 'transform'"
    
    # Check that 'notify' is the final task (no downstream)
    notify_task = dag.get_task("notify")
    assert notify_task.downstream_list == [], "'notify' should have no downstream tasks"

def test_schedule_interval(dagbag):
    """Validate the schedule is what we expect."""
    dag = dagbag.get_dag("daily_revenue_etl")
    assert dag.schedule_interval == "0 6 * * *", "Revenue ETL should run at 6am UTC"
```

---

## pytest Basics for Airflow

Set up a `conftest.py` at the tests root to configure Airflow for testing:

```python
# tests/conftest.py
import os
import pytest

# Point to a test configuration — use SQLite for test DB
os.environ.setdefault("AIRFLOW__CORE__UNIT_TEST_MODE", "True")
os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", "sqlite:///tests/test.db")
os.environ.setdefault("AIRFLOW__CORE__DAGS_FOLDER", "dags/")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

@pytest.fixture(scope="session", autouse=True)
def initialize_test_db():
    """Initialize Airflow metadata DB for tests."""
    from airflow.utils.db import initdb
    initdb()
    yield
    # Cleanup happens automatically with SQLite test file
```

Run tests:

```bash
# Install test dependencies
pip install pytest apache-airflow

# Run all tests
pytest tests/ -v

# Run only DAG integrity tests
pytest tests/test_dag_integrity.py -v

# Run with coverage
pytest tests/ --cov=dags --cov-report=html
```

---

## Testing in CI/CD Pipeline

A GitHub Actions workflow that runs DAG tests on every pull request:

```yaml
# .github/workflows/dag-tests.yml
name: DAG Tests

on:
  pull_request:
    paths:
      - "dags/**"
      - "tests/**"
      - "requirements.txt"

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python 3.11
        uses: actions/setup-python@v4
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          pip install apache-airflow==2.8.0 pytest pytest-cov
          pip install -r requirements.txt

      - name: Initialize Airflow DB
        run: airflow db init
        env:
          AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: sqlite:///test.db
          AIRFLOW__CORE__LOAD_EXAMPLES: "False"

      - name: Run DAG integrity tests
        run: pytest tests/ -v --tb=short
        env:
          AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: sqlite:///test.db
          AIRFLOW__CORE__DAGS_FOLDER: dags/
          AIRFLOW__CORE__LOAD_EXAMPLES: "False"

      - name: Upload coverage
        uses: codecov/codecov-action@v3
```

---

## Key Takeaways

- Import errors, cycles, and invalid config are caught for free by loading the DagBag — no execution needed
- `DagBag(dag_folder="dags/", include_examples=False)` loads all DAGs; `dagbag.import_errors` reveals broken files
- Test task count, task IDs, dependencies, schedule interval, tags, and owner for every production DAG
- `conftest.py` with `AIRFLOW__CORE__UNIT_TEST_MODE=True` and a SQLite test DB enables lightweight Airflow testing
- CI/CD should run DAG tests on every PR that touches the `dags/` folder
