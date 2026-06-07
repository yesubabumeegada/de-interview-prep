---
title: "Python Testing with pytest - Intermediate"
topic: python
subtopic: testing-pytest
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [python, testing, pytest, mocking, patch, magicmock, conftest, markers, coverage]
---

# Python Testing with pytest — Intermediate Concepts

## Mocking — Isolating Code from External Dependencies

**The analogy:** Mocking is like using a stunt double in a movie. You replace the dangerous actor (real database, API) with a controlled substitute that behaves predictably for testing purposes.

### unittest.mock.patch — Replace During Test

```python
from unittest.mock import patch, MagicMock
import pytest

# Function under test
def fetch_user_data(user_id: str) -> dict:
    """Fetches from an API — we don't want real network calls in tests."""
    import requests
    response = requests.get(f"https://api.example.com/users/{user_id}")
    response.raise_for_status()
    return response.json()

# Test with patch
@patch("requests.get")
def test_fetch_user_data_success(mock_get):
    """Patch replaces requests.get with a mock."""
    # Configure mock response
    mock_response = MagicMock()
    mock_response.json.return_value = {"id": "u1", "name": "Alice"}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response
    
    # Call function — it uses the mock instead of real requests.get
    result = fetch_user_data("u1")
    
    assert result == {"id": "u1", "name": "Alice"}
    mock_get.assert_called_once_with("https://api.example.com/users/u1")

@patch("requests.get")
def test_fetch_user_data_handles_timeout(mock_get):
    """Test error handling with mocked failure."""
    import requests
    mock_get.side_effect = requests.Timeout("Connection timed out")
    
    with pytest.raises(requests.Timeout):
        fetch_user_data("u1")
```

### MagicMock — Flexible Fake Objects

```python
from unittest.mock import MagicMock, call

def test_pipeline_writes_to_database():
    """Use MagicMock to simulate database operations."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    
    # Run the function
    records = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    load_records(mock_conn, records, table="users")
    
    # Verify the right SQL was executed
    mock_cursor.executemany.assert_called_once()
    call_args = mock_cursor.executemany.call_args
    assert "INSERT INTO users" in call_args[0][0]
    mock_conn.commit.assert_called_once()
```

---

## Patching External Services — Where to Patch

The golden rule: **patch where it's used, not where it's defined.**

```python
# src/pipeline/extract.py
import boto3

def read_from_s3(bucket: str, key: str) -> str:
    client = boto3.client("s3")
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode()

# tests/test_extract.py
from unittest.mock import patch, MagicMock

# Patch WHERE boto3 is used (in extract module), not where it's defined
@patch("pipeline.extract.boto3.client")
def test_read_from_s3(mock_boto_client):
    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3
    
    mock_body = MagicMock()
    mock_body.read.return_value = b'{"user": "alice"}'
    mock_s3.get_object.return_value = {"Body": mock_body}
    
    result = read_from_s3("my-bucket", "data/file.json")
    
    assert result == '{"user": "alice"}'
    mock_s3.get_object.assert_called_once_with(Bucket="my-bucket", Key="data/file.json")
```

---

## conftest.py — Shared Test Infrastructure

```python
# tests/conftest.py
import pytest
from unittest.mock import MagicMock

@pytest.fixture
def mock_s3_client():
    """Reusable S3 client mock across all tests."""
    client = MagicMock()
    
    # Configure common behaviors
    client.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "data/part-001.parquet", "Size": 1024},
            {"Key": "data/part-002.parquet", "Size": 2048},
        ]
    }
    
    return client

@pytest.fixture
def mock_db_connection():
    """Reusable database connection mock."""
    conn = MagicMock()
    cursor = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cursor)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    conn.cursor_instance = cursor  # Convenient access in tests
    return conn

@pytest.fixture
def sample_events():
    """Standard test dataset."""
    return [
        {"user_id": "u1", "event": "login", "ts": "2024-01-15T10:00:00"},
        {"user_id": "u2", "event": "purchase", "ts": "2024-01-15T11:00:00"},
        {"user_id": "u1", "event": "logout", "ts": "2024-01-15T12:00:00"},
    ]
```

---

## Markers — Categorizing Tests

```python
import pytest

@pytest.mark.unit
def test_transform_logic():
    """Fast, no external dependencies."""
    assert transform_record({"amount": "100"}) == {"amount": 100.0}

@pytest.mark.integration
def test_full_pipeline():
    """Slower, hits real resources."""
    result = run_pipeline(config)
    assert result.success_count > 0

@pytest.mark.slow
def test_large_dataset_processing():
    """Performance test with large data."""
    records = generate_test_records(1_000_000)
    result = process_batch(records)
    assert result.duration_seconds < 60

# Custom marker for data quality tests
@pytest.mark.data_quality
def test_no_null_primary_keys():
    result = run_quality_check("null_pk_check")
    assert result.passed
```

```bash
# Run only unit tests
pytest -m unit

# Run everything except slow tests
pytest -m "not slow"

# Run integration tests with verbose output
pytest -m integration -v
```

---

## Testing with Side Effects — Simulating Sequences

```python
from unittest.mock import patch, MagicMock

@patch("pipeline.extract.requests.get")
def test_retry_on_transient_failure(mock_get):
    """Simulate: fail twice, succeed on third try."""
    import requests
    
    # First two calls fail, third succeeds
    mock_get.side_effect = [
        requests.ConnectionError("Connection refused"),
        requests.Timeout("Timed out"),
        MagicMock(
            status_code=200,
            json=MagicMock(return_value={"data": [1, 2, 3]})
        )
    ]
    
    result = fetch_with_retry("https://api.example.com/data", max_retries=3)
    
    assert result == {"data": [1, 2, 3]}
    assert mock_get.call_count == 3

@patch("time.sleep")  # Don't actually sleep in tests!
@patch("pipeline.extract.requests.get")
def test_backoff_timing(mock_get, mock_sleep):
    """Verify exponential backoff delays."""
    import requests
    
    mock_get.side_effect = [
        requests.Timeout(),
        requests.Timeout(),
        MagicMock(status_code=200, json=MagicMock(return_value={}))
    ]
    
    fetch_with_retry("https://api.example.com/data")
    
    # Verify backoff delays were applied
    sleep_calls = [call[0][0] for call in mock_sleep.call_args_list]
    assert sleep_calls[0] < sleep_calls[1]  # Exponential growth
```

---

## Code Coverage

```bash
# Run with coverage
pytest --cov=src --cov-report=html --cov-report=term-missing

# Coverage output:
# Name                    Stmts   Miss  Cover   Missing
# src/pipeline/extract.py    45      3    93%   67-69
# src/pipeline/transform.py  32      0   100%
# src/pipeline/load.py       28      5    82%   34-38
```

```ini
# .coveragerc or pyproject.toml
[tool.coverage.run]
source = ["src"]
omit = ["tests/*", "**/__init__.py"]

[tool.coverage.report]
fail_under = 85
show_missing = true
exclude_lines = [
    "pragma: no cover",
    "if __name__ == .__main__.",
    "raise NotImplementedError",
]
```

---

## Fixture Factories — Dynamic Test Data

```python
import pytest
from datetime import datetime, timedelta

@pytest.fixture
def make_event():
    """Factory fixture — create events with custom attributes."""
    def _make_event(
        user_id: str = "u1",
        event_type: str = "pageview",
        days_ago: int = 0,
        amount: float = None
    ):
        return {
            "user_id": user_id,
            "event_type": event_type,
            "timestamp": (datetime.now() - timedelta(days=days_ago)).isoformat(),
            "amount": amount,
        }
    return _make_event

def test_filter_recent_events(make_event):
    """Use factory to create specific test scenarios."""
    events = [
        make_event(days_ago=0),   # Today
        make_event(days_ago=5),   # 5 days ago
        make_event(days_ago=30),  # A month ago
    ]
    
    recent = filter_last_n_days(events, days=7)
    assert len(recent) == 2

def test_revenue_calculation(make_event):
    """Factory makes it easy to set up specific amounts."""
    events = [
        make_event(event_type="purchase", amount=100.0),
        make_event(event_type="purchase", amount=50.0),
        make_event(event_type="refund", amount=-25.0),
    ]
    
    assert calculate_revenue(events) == 125.0
```

---

## Testing Async Code

```python
import pytest
import asyncio

@pytest.mark.asyncio
async def test_async_data_fetch():
    """Test async functions with pytest-asyncio."""
    result = await fetch_data_async("https://api.example.com/data")
    assert "records" in result
    assert len(result["records"]) > 0

@pytest.fixture
async def async_db_pool():
    """Async fixture for database pool."""
    import asyncpg
    pool = await asyncpg.create_pool("postgresql://test:5432/testdb")
    yield pool
    await pool.close()
```

---

## Interview Tips

> **Tip 1:** When discussing mocking strategy, explain the boundary principle: "I mock at the boundary between my code and external systems (APIs, databases, file systems). Everything inside that boundary is tested with real logic. This gives confidence that my transformations work correctly while keeping tests fast and deterministic."

> **Tip 2:** Know the `patch` location rule — "patch where it's imported, not where it's defined." This trips up many candidates. If your module does `from boto3 import client`, you patch `your_module.client`, not `boto3.client`. Getting this right in an interview shows practical testing experience.

> **Tip 3:** Mention conftest.py as your fixture organization tool. "I put reusable fixtures in conftest.py at the appropriate directory level — test-wide fixtures at the root, integration-specific ones in the integration subdirectory. pytest auto-discovers them by directory hierarchy."
