---
title: "Airflow Testing - Intermediate"
topic: airflow
subtopic: airflow-testing
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [airflow, testing, pytest, mocking, xcom, branch, custom-operator, parametrize]
---

# Airflow Testing — Intermediate

## Unit Testing Operators and Hooks

For custom operators and hooks, unit tests mock external systems so tests run fast without network access.

```python
# dags/operators/http_to_s3.py
import boto3
import requests
from airflow.models import BaseOperator

class HttpToS3Operator(BaseOperator):
    def __init__(self, endpoint: str, s3_bucket: str, s3_key: str, **kwargs):
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        self.log.info(f"Fetching {self.endpoint}")
        response = requests.get(self.endpoint, timeout=30)
        response.raise_for_status()
        data = response.json()

        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=self.s3_bucket,
            Key=self.s3_key,
            Body=str(data).encode(),
        )
        self.log.info(f"Saved {len(str(data))} bytes to s3://{self.s3_bucket}/{self.s3_key}")
        return {"records": len(data), "s3_key": self.s3_key}
```

```python
# tests/operators/test_http_to_s3.py
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from airflow.models import DagRun, TaskInstance
from airflow.utils.state import State
from dags.operators.http_to_s3 import HttpToS3Operator

@pytest.fixture
def mock_context():
    """Minimal Airflow context for operator.execute()."""
    return {
        "ds": "2024-01-15",
        "execution_date": datetime(2024, 1, 15),
        "task_instance": MagicMock(),
    }

@patch("dags.operators.http_to_s3.boto3.client")
@patch("dags.operators.http_to_s3.requests.get")
def test_http_to_s3_success(mock_requests, mock_boto, mock_context):
    """Test happy path: API returns data, S3 upload succeeds."""
    # Arrange: mock API response
    mock_response = MagicMock()
    mock_response.json.return_value = [{"id": 1}, {"id": 2}, {"id": 3}]
    mock_response.raise_for_status.return_value = None
    mock_requests.return_value = mock_response

    # Arrange: mock S3 client
    mock_s3 = MagicMock()
    mock_boto.return_value = mock_s3

    # Act
    operator = HttpToS3Operator(
        task_id="test_task",
        endpoint="https://api.example.com/data",
        s3_bucket="my-bucket",
        s3_key="data/2024-01-15.json",
    )
    result = operator.execute(mock_context)

    # Assert
    assert result["records"] == 3
    assert result["s3_key"] == "data/2024-01-15.json"
    mock_requests.assert_called_once_with("https://api.example.com/data", timeout=30)
    mock_s3.put_object.assert_called_once_with(
        Bucket="my-bucket",
        Key="data/2024-01-15.json",
        Body=mock_response.json.return_value.__str__().encode(),
    )

@patch("dags.operators.http_to_s3.requests.get")
def test_http_to_s3_api_failure(mock_requests, mock_context):
    """Test that HTTP errors propagate as exceptions."""
    import requests as req_lib
    mock_requests.return_value.raise_for_status.side_effect = req_lib.HTTPError("404 Not Found")

    operator = HttpToS3Operator(
        task_id="test_task",
        endpoint="https://api.example.com/data",
        s3_bucket="my-bucket",
        s3_key="data/test.json",
    )
    with pytest.raises(req_lib.HTTPError):
        operator.execute(mock_context)
```

---

## pytest Fixtures for Airflow

```python
# tests/conftest.py
import pytest
from airflow.models import DagBag, DAG, TaskInstance
from airflow.utils.session import create_session
from datetime import datetime

@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)

@pytest.fixture
def session():
    """Provide an Airflow database session, rolled back after each test."""
    with create_session() as s:
        yield s
        s.rollback()

@pytest.fixture
def execution_date():
    return datetime(2024, 1, 15)

@pytest.fixture
def make_task_instance(session, execution_date):
    """Factory fixture to create TaskInstance objects for testing."""
    created = []
    
    def _make(dag_id: str, task_id: str, state: str = "success"):
        from airflow.models import DagRun
        dag_run = DagRun(
            dag_id=dag_id,
            run_id=f"test__{execution_date.isoformat()}",
            execution_date=execution_date,
            state="running",
        )
        session.add(dag_run)
        session.flush()
        
        ti = TaskInstance(
            task=MagicMock(task_id=task_id, dag_id=dag_id),
            execution_date=execution_date,
        )
        ti.dag_id = dag_id
        ti.state = state
        session.add(ti)
        session.flush()
        created.append(ti)
        return ti
    
    yield _make
```

---

## Testing XCom Push/Pull

```python
# tests/test_xcom.py
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime

def test_task_pushes_correct_xcom(dagbag):
    """Verify the extract task pushes record count to XCom."""
    from dags.daily_etl import extract_records
    
    mock_ti = MagicMock()
    context = {
        "ds": "2024-01-15",
        "task_instance": mock_ti,
        "execution_date": datetime(2024, 1, 15),
    }

    with patch("dags.daily_etl.get_db_connection") as mock_db:
        mock_db.return_value.execute.return_value.rowcount = 42
        extract_records(**context)
    
    # Verify XCom was pushed with correct key and value
    mock_ti.xcom_push.assert_called_once_with(key="record_count", value=42)

def test_downstream_reads_xcom(dagbag):
    """Verify the validate task reads record_count from XCom."""
    from dags.daily_etl import validate_records
    
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = 42  # Simulates upstream task pushed 42
    
    context = {
        "ds": "2024-01-15",
        "task_instance": mock_ti,
        "execution_date": datetime(2024, 1, 15),
    }

    result = validate_records(**context)
    
    mock_ti.xcom_pull.assert_called_with(task_ids="extract_records", key="record_count")
    assert result["validated"] == True
```

---

## Testing BranchPythonOperator Logic

```python
# dags/branching_dag.py
def choose_processing_path(**context):
    """Route to fast or slow processing based on record count."""
    record_count = context["task_instance"].xcom_pull(
        task_ids="count_records", key="count"
    )
    if record_count < 10_000:
        return "fast_process"
    elif record_count < 1_000_000:
        return "standard_process"
    else:
        return "bulk_process"
```

```python
# tests/test_branching.py
import pytest
from unittest.mock import MagicMock

def _make_context(xcom_value):
    mock_ti = MagicMock()
    mock_ti.xcom_pull.return_value = xcom_value
    return {"task_instance": mock_ti}

@pytest.mark.parametrize("record_count,expected_branch", [
    (0,          "fast_process"),
    (9_999,      "fast_process"),
    (10_000,     "standard_process"),
    (999_999,    "standard_process"),
    (1_000_000,  "bulk_process"),
    (5_000_000,  "bulk_process"),
])
def test_branch_routing(record_count, expected_branch):
    """Validate all branch conditions."""
    from dags.branching_dag import choose_processing_path
    
    context = _make_context(record_count)
    result = choose_processing_path(**context)
    
    assert result == expected_branch, (
        f"For {record_count} records, expected '{expected_branch}' but got '{result}'"
    )
```

---

## Mocking External APIs with `responses` Library

```python
# pip install responses
import responses as responses_lib
import requests

# dags/api_tasks.py
def fetch_exchange_rates(**context):
    """Fetch exchange rates from external API."""
    date = context["ds"]
    response = requests.get(f"https://api.exchangerate.host/{date}")
    response.raise_for_status()
    rates = response.json()["rates"]
    return rates
```

```python
# tests/test_api_tasks.py
import pytest
import responses as resp

@resp.activate
def test_fetch_exchange_rates_success():
    """Mock the exchange rate API and validate the return value."""
    resp.add(
        resp.GET,
        "https://api.exchangerate.host/2024-01-15",
        json={"success": True, "date": "2024-01-15", "rates": {"EUR": 0.92, "GBP": 0.79}},
        status=200,
    )
    
    from dags.api_tasks import fetch_exchange_rates
    from unittest.mock import MagicMock
    
    context = {"ds": "2024-01-15", "task_instance": MagicMock()}
    result = fetch_exchange_rates(**context)
    
    assert result["EUR"] == 0.92
    assert result["GBP"] == 0.79

@resp.activate  
def test_fetch_exchange_rates_api_error():
    """API returning 500 should raise an exception."""
    resp.add(
        resp.GET,
        "https://api.exchangerate.host/2024-01-15",
        status=500,
    )
    
    from dags.api_tasks import fetch_exchange_rates
    from unittest.mock import MagicMock
    
    context = {"ds": "2024-01-15", "task_instance": MagicMock()}
    with pytest.raises(requests.HTTPError):
        fetch_exchange_rates(**context)
```

---

## Parametrized Tests for Multiple DAG Configs

```python
# tests/test_dag_configs.py

# Test all DAGs in the DagBag meet organizational standards
@pytest.mark.parametrize("dag_id,dag", [
    pytest.param(dag_id, dag, id=dag_id)
    for dag_id, dag in DagBag(dag_folder="dags/", include_examples=False).dags.items()
])
def test_all_dags_have_tags(dag_id, dag):
    assert len(dag.tags) > 0, f"{dag_id}: missing tags"

@pytest.mark.parametrize("dag_id,dag", [
    pytest.param(dag_id, dag, id=dag_id)
    for dag_id, dag in DagBag(dag_folder="dags/", include_examples=False).dags.items()
])
def test_all_dags_have_retries(dag_id, dag):
    for task in dag.tasks:
        assert task.retries >= 1, (
            f"{dag_id}.{task.task_id}: tasks should have at least 1 retry"
        )

@pytest.mark.parametrize("dag_id,dag", [
    pytest.param(dag_id, dag, id=dag_id)
    for dag_id, dag in DagBag(dag_folder="dags/", include_examples=False).dags.items()
])
def test_all_dags_catchup_disabled(dag_id, dag):
    assert dag.catchup == False, (
        f"{dag_id}: catchup should be False to prevent historical backfill on accidental deploy"
    )
```

---

## Key Takeaways

- Unit test operators by patching external calls (`requests.get`, `boto3.client`) at the module path where they're imported
- Use `MagicMock()` for the task_instance in context — it automatically stubs `xcom_push` and `xcom_pull`
- Test BranchPythonOperator with `@pytest.mark.parametrize` covering all branch boundaries (especially edge cases like exact boundary values)
- The `responses` library (`@responses.activate`) intercepts real HTTP calls for clean API mocking without network access
- Parametrize tests that must apply to ALL DAGs (tags, retries, catchup) — they automatically pick up new DAGs when added
