---
title: "Airflow Testing - Senior Deep Dive"
topic: airflow
subtopic: airflow-testing
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [airflow, testing, integration, idempotency, performance, mutation-testing, ci]
---

# Airflow Testing — Senior Deep Dive

## Integration Testing with LocalExecutor in CI

Integration tests run actual DAG executions against a real (test) database. Use `dag.test()` — it runs a DAG synchronously in the current process without a scheduler.

```python
# tests/integration/test_dag_execution.py
import pytest
from datetime import datetime
from airflow.models import DagBag, DagRun
from airflow.utils.state import State

@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)

def test_daily_etl_runs_successfully(dagbag):
    """Run the full DAG in-process and verify all tasks succeed."""
    dag = dagbag.get_dag("daily_etl")
    assert dag is not None, "DAG not found"
    
    execution_date = datetime(2024, 1, 15)
    
    # dag.test() runs tasks synchronously using LocalExecutor logic
    # It will raise if any task fails
    dag.test(execution_date=execution_date)
    
    # Verify final state
    dag_runs = DagRun.find(dag_id="daily_etl", execution_date=execution_date)
    assert len(dag_runs) == 1
    assert dag_runs[0].state == State.SUCCESS
```

Docker Compose for CI integration tests:

```yaml
# docker-compose.test.yml
services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_DB: airflow_test
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5

  airflow-test:
    build: .
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql://airflow:airflow@postgres/airflow_test
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__CORE__LOAD_EXAMPLES: "False"
    command: >
      bash -c "
        airflow db init &&
        pytest tests/integration/ -v --tb=short
      "
```

```yaml
# .github/workflows/integration-tests.yml
- name: Run integration tests
  run: docker-compose -f docker-compose.test.yml run --rm airflow-test
```

---

## Testing Idempotency

A task is idempotent if running it multiple times with the same inputs produces the same result. This is the most important correctness property for production pipelines.

```python
# tests/integration/test_idempotency.py

def test_load_task_is_idempotent(dagbag):
    """Running the load task twice for the same date produces identical results."""
    from airflow.operators.python import PythonOperator
    from dags.daily_etl import load_to_warehouse
    from unittest.mock import MagicMock
    import pandas as pd
    
    execution_date = datetime(2024, 1, 15)
    context = {
        "ds": "2024-01-15",
        "task_instance": MagicMock(),
        "execution_date": execution_date,
    }
    
    # Run 1
    load_to_warehouse(**context)
    count_after_run1 = query_db("SELECT COUNT(*) FROM fact_sales WHERE sale_date = '2024-01-15'")
    
    # Run 2 (simulates retry)
    load_to_warehouse(**context)
    count_after_run2 = query_db("SELECT COUNT(*) FROM fact_sales WHERE sale_date = '2024-01-15'")
    
    assert count_after_run1 == count_after_run2, (
        f"NOT IDEMPOTENT: First run: {count_after_run1} rows, second run: {count_after_run2} rows"
    )

def test_dag_full_run_idempotent(dagbag):
    """Full DAG run twice for same date gives same output rows."""
    dag = dagbag.get_dag("daily_etl")
    execution_date = datetime(2024, 1, 15)
    
    dag.test(execution_date=execution_date)
    result1 = get_output_table_hash("fact_sales", "2024-01-15")
    
    dag.test(execution_date=execution_date)  # Run again
    result2 = get_output_table_hash("fact_sales", "2024-01-15")
    
    assert result1 == result2, "DAG is not idempotent — output differs between runs"

def get_output_table_hash(table: str, date: str) -> str:
    """Hash the contents of an output table partition for comparison."""
    import hashlib
    rows = query_db(f"SELECT * FROM {table} WHERE sale_date = '{date}' ORDER BY 1,2,3")
    return hashlib.md5(str(rows).encode()).hexdigest()
```

---

## Data Contract Testing

Validate that a task's output table matches the expected schema and constraints:

```python
# tests/integration/test_data_contracts.py
import pytest
import pandas as pd

FACT_SALES_CONTRACT = {
    "required_columns": ["sale_id", "sale_date", "customer_id", "amount", "created_at"],
    "not_null_columns": ["sale_id", "sale_date", "amount"],
    "unique_columns": ["sale_id"],
    "numeric_columns": {"amount": {"min": 0, "max": 1_000_000}},
}

def test_fact_sales_schema(get_test_db_connection):
    """Validate output table schema after ETL runs."""
    conn = get_test_db_connection()
    df = pd.read_sql("SELECT * FROM fact_sales WHERE sale_date = '2024-01-15'", conn)
    
    # Column presence
    for col in FACT_SALES_CONTRACT["required_columns"]:
        assert col in df.columns, f"Missing required column: {col}"
    
    # Not null
    for col in FACT_SALES_CONTRACT["not_null_columns"]:
        null_count = df[col].isna().sum()
        assert null_count == 0, f"Column '{col}' has {null_count} null values"
    
    # Uniqueness
    for col in FACT_SALES_CONTRACT["unique_columns"]:
        dup_count = df[col].duplicated().sum()
        assert dup_count == 0, f"Column '{col}' has {dup_count} duplicate values"
    
    # Range checks
    for col, bounds in FACT_SALES_CONTRACT["numeric_columns"].items():
        assert df[col].min() >= bounds["min"], f"{col} has values below {bounds['min']}"
        assert df[col].max() <= bounds["max"], f"{col} has values above {bounds['max']}"
```

---

## Testing DAG Backfill Behavior

Backfills run historical dates. Test that backfilling 7 days doesn't create cross-date contamination:

```python
def test_backfill_no_cross_date_contamination(dagbag):
    """Running backfill for 3 days populates exactly 3 days, no overlap."""
    from airflow.models import BackfillJob
    from datetime import timedelta
    
    dag = dagbag.get_dag("daily_etl")
    start = datetime(2024, 1, 13)
    end = datetime(2024, 1, 15)
    
    # Run backfill
    job = BackfillJob(dag=dag, start_date=start, end_date=end, ignore_first_depends_on_past=True)
    job.run()
    
    # Verify exactly 3 dates loaded
    counts = query_db("""
        SELECT sale_date, COUNT(*) as rows
        FROM fact_sales
        WHERE sale_date BETWEEN '2024-01-13' AND '2024-01-15'
        GROUP BY sale_date
        ORDER BY sale_date
    """)
    
    assert len(counts) == 3, f"Expected 3 date partitions, got {len(counts)}"
    dates = [row["sale_date"] for row in counts]
    assert "2024-01-13" in dates
    assert "2024-01-14" in dates
    assert "2024-01-15" in dates
```

---

## Performance Testing: DAG Parse Time

Airflow re-parses all DAG files on a configurable interval. Slow parse times starve the scheduler.

```python
# tests/performance/test_dag_parse_time.py
import time
import pytest

MAX_PARSE_SECONDS = 30  # Org standard: no DAG file should take > 30s to parse

@pytest.mark.parametrize("dag_file", [
    "dags/daily_etl.py",
    "dags/customer_360.py",
    "dags/inventory_sync.py",
])
def test_dag_parse_time(dag_file):
    """Each DAG file must parse (import) in under 30 seconds."""
    start = time.perf_counter()
    
    import importlib.util
    spec = importlib.util.spec_from_file_location("dag_module", dag_file)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    
    elapsed = time.perf_counter() - start
    
    assert elapsed < MAX_PARSE_SECONDS, (
        f"{dag_file} took {elapsed:.2f}s to parse — exceeds {MAX_PARSE_SECONDS}s limit.\n"
        "Common causes: DB calls at module level, heavy imports, dynamic DAG generation with API calls."
    )
```

> **Common slow-parse culprits:** DB queries at module level (e.g., `Variable.get()` at import time), imports of heavy ML libraries, API calls to generate dynamic DAGs. Move all such calls inside task functions or use Jinja templates.

---

## Test Coverage Strategy for 50-DAG Codebase

```
Total testing budget: 100%

Unit tests (70% of effort, fastest to run):
├── All operator.execute() methods — mock external I/O
├── All Python callable functions — pure logic tests  
├── All BranchPythonOperator branches — parametrize boundary values
├── All XCom push/pull contracts
└── All utility functions in plugins/

Integration tests (20% of effort, run in CI on PR merge):
├── dag.test() for top 10 most critical DAGs
├── Idempotency test for all tasks that write to storage
├── Data contract validation for all output tables
└── End-to-end backfill test for 1 representative DAG

DAG integrity tests (10% of effort, fastest to write, run on every PR):
├── DagBag loads without import errors
├── Task structure validation for every DAG
├── Schedule interval correctness
├── Owner and tags present
└── retries >= 1 and catchup=False

Performance tests (continuous monitoring):
├── DAG parse time < 30s per file
└── DagBag load time < 60s total
```

```python
# pytest.ini — configure test markers
[pytest]
markers =
    unit: Unit tests (fast, no external I/O)
    integration: Integration tests (require test database)
    performance: Performance tests (measure parse/run time)
    slow: Tests that take > 30 seconds

# Run only unit tests in pre-commit:
# pytest -m "unit" --timeout=60

# Run all tests in CI:
# pytest -m "unit or integration" --timeout=300
```

---

## Key Takeaways

- `dag.test()` runs a DAG synchronously in the current process — the best tool for integration testing without a full Airflow deployment
- Idempotency tests run the same task/DAG twice and assert identical output — catch this before production or face duplicate data
- Data contract tests validate schema, nullability, uniqueness, and range constraints on output tables post-execution
- DAG parse time must be < 30s per file — move all I/O (DB queries, API calls) out of module-level code
- A 50-DAG codebase needs 70% unit / 20% integration / 10% integrity tests; run integrity tests on every PR, integration tests on merge
