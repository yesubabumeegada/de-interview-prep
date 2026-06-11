---
title: "Pipeline Testing - Intermediate"
topic: ci-cd
subtopic: pipeline-testing
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [ci-cd, testing, integration-tests, mocking, great-expectations]
---

# Pipeline Testing — Intermediate

## Integration Testing with a Real Database

```python
# conftest.py — shared pytest fixtures
import pytest
import sqlalchemy as sa
from sqlalchemy import create_engine, text

@pytest.fixture(scope="session")
def test_engine():
    """Create an in-memory SQLite DB for integration tests."""
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                customer_id INTEGER,
                amount FLOAT,
                status TEXT,
                order_date DATE
            )
        """))
        conn.execute(text("""
            INSERT INTO orders VALUES
            (1, 101, 150.0, 'completed', '2024-01-01'),
            (2, 102, 200.0, 'completed', '2024-01-01'),
            (3, 103, 75.0,  'cancelled', '2024-01-01'),
            (4, 101, 300.0, 'completed', '2024-01-02')
        """))
        conn.commit()
    return engine

# test_integration.py
from pipelines.revenue import calculate_daily_revenue

def test_daily_revenue_excludes_cancelled(test_engine):
    results = calculate_daily_revenue(test_engine, date="2024-01-01")
    assert results["total_revenue"] == 350.0  # 150 + 200, not 75
    assert results["order_count"] == 2

def test_daily_revenue_groups_by_date(test_engine):
    results = calculate_daily_revenue(test_engine, date="2024-01-02")
    assert results["total_revenue"] == 300.0
```

---

## Testing with Mocks for External Services

```python
from unittest.mock import patch, MagicMock
from pipelines.s3_extract import extract_from_s3

def test_extract_calls_s3_with_correct_key():
    mock_s3 = MagicMock()
    mock_s3.get_object.return_value = {
        "Body": MagicMock(read=lambda: b'order_id,amount\n1,100\n2,200')
    }
    
    with patch("pipelines.s3_extract.boto3.client", return_value=mock_s3):
        result = extract_from_s3(bucket="my-bucket", key="orders/2024-01-01.csv")
    
    mock_s3.get_object.assert_called_once_with(
        Bucket="my-bucket", Key="orders/2024-01-01.csv"
    )
    assert len(result) == 2

# When NOT to mock: avoid mocking your own database interactions
# — use a real test DB instead. Mocks can mask real query bugs.
```

---

## DataFrame Testing with pandera

```python
import pandera as pa
import pandas as pd
from pandera import Column, DataFrameSchema

# Define schema contract
orders_schema = DataFrameSchema({
    "order_id": Column(int, nullable=False, unique=True),
    "amount": Column(float, pa.Check.greater_than(0)),
    "status": Column(str, pa.Check.isin(["completed", "pending", "cancelled"])),
    "order_date": Column(pa.DateTime, nullable=False),
})

def transform_orders(df: pd.DataFrame) -> pd.DataFrame:
    # Validate input
    orders_schema.validate(df)
    
    result = df[df["status"] == "completed"].copy()
    result["revenue"] = result["amount"] * 1.1
    return result

# Test
def test_transform_rejects_invalid_status():
    bad_df = pd.DataFrame({
        "order_id": [1],
        "amount": [100.0],
        "status": ["UNKNOWN"],  # invalid
        "order_date": ["2024-01-01"],
    })
    with pytest.raises(pa.errors.SchemaError):
        transform_orders(bad_df)
```

---

## Great Expectations in CI

```python
# great_expectations/checkpoints/orders_checkpoint.yml
# Run in CI after pipeline: ge checkpoint run orders_checkpoint

# Programmatically:
import great_expectations as gx

context = gx.get_context()
checkpoint_result = context.run_checkpoint(checkpoint_name="orders_checkpoint")

if not checkpoint_result["success"]:
    raise RuntimeError(f"Data quality failed: {checkpoint_result}")
```

```yaml
# In GitHub Actions:
- name: Run data quality checks
  run: |
    great_expectations checkpoint run orders_daily_checkpoint
  env:
    GE_CLOUD_DATACONTEXT_ID: ${{ secrets.GE_CLOUD_ID }}
```

---

## Common Testing Pitfalls

| Pitfall | Fix |
|---|---|
| Testing only happy paths | Add tests for nulls, edge cases, bad data types |
| Mocking too aggressively | Use real test DB for SQL logic |
| Slow integration tests blocking CI | Use `pytest-xdist` for parallel test execution |
| Tests coupled to prod database | Always use isolated test fixtures |
| No assertion on row counts | Assert output row count, not just no-error |
| Ignoring test coverage | Set `--cov-fail-under=80` in CI |

---

## pytest Configuration

```ini
# pytest.ini
[pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = --tb=short --strict-markers -q
markers =
    unit: fast unit tests
    integration: tests requiring DB/external services
    slow: tests taking > 5 seconds

# Run only fast tests in pre-commit:
# pytest -m "unit"
# Run all in CI:
# pytest -m "unit or integration"
```
