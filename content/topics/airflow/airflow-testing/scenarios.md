---
title: "Airflow Testing - Scenario Questions"
topic: airflow
subtopic: airflow-testing
content_type: scenario_question
tags: [airflow, testing, pytest, dagbag, idempotency, ci, integration, interview, scenarios]
---

# Scenario Questions — Airflow Testing

<article data-difficulty="junior">

## 🟢 Junior: Write a Test That Validates a DAG Loads and Has Correct Schedule

**Scenario:** Your team has a DAG `weekly_sales_report` that should run every Monday at 7am UTC (`0 7 * * 1`). Write a pytest test suite that validates: (1) the DAG loads without import errors, (2) it has the correct schedule, (3) it has the expected tasks.

<details>
<summary>✅ Solution</summary>

```python
# tests/test_weekly_sales_report.py
import pytest
from airflow.models import DagBag

DAG_ID = "weekly_sales_report"
EXPECTED_SCHEDULE = "0 7 * * 1"  # Monday 7am UTC
EXPECTED_TASKS = {"extract_sales", "aggregate", "generate_report", "send_email"}

@pytest.fixture(scope="module")
def dagbag():
    """Load DAGs once for this test module."""
    return DagBag(dag_folder="dags/", include_examples=False)

@pytest.fixture(scope="module")
def dag(dagbag):
    """Get the specific DAG under test."""
    return dagbag.get_dag(DAG_ID)


class TestWeeklySalesReportDAG:
    
    def test_dag_loads_without_import_errors(self, dagbag):
        """DAG file must import without errors."""
        # If there's an import error, it shows up here, not in get_dag()
        assert DAG_ID not in dagbag.import_errors.values(), (
            f"Import error in {DAG_ID}: {dagbag.import_errors}"
        )
        # More robust: check the full import_errors dict
        assert dagbag.import_errors == {} or all(
            DAG_ID not in str(v) for v in dagbag.import_errors.values()
        )
    
    def test_dag_exists(self, dag):
        """The DAG must be findable in the DagBag."""
        assert dag is not None, (
            f"DAG '{DAG_ID}' not found in dags/. "
            f"Available DAGs: {list(DagBag('dags/', include_examples=False).dags.keys())}"
        )
    
    def test_correct_schedule_interval(self, dag):
        """DAG must run on Mondays at 7am UTC."""
        assert dag.schedule_interval == EXPECTED_SCHEDULE, (
            f"Expected schedule '{EXPECTED_SCHEDULE}', "
            f"got '{dag.schedule_interval}'"
        )
    
    def test_expected_tasks_present(self, dag):
        """All expected tasks must exist in the DAG."""
        actual_tasks = {task.task_id for task in dag.tasks}
        assert EXPECTED_TASKS == actual_tasks, (
            f"Task mismatch.\n"
            f"Expected: {sorted(EXPECTED_TASKS)}\n"
            f"Actual:   {sorted(actual_tasks)}\n"
            f"Missing:  {EXPECTED_TASKS - actual_tasks}\n"
            f"Extra:    {actual_tasks - EXPECTED_TASKS}"
        )
    
    def test_correct_task_count(self, dag):
        """Sanity check: task count matches expected."""
        assert len(dag.tasks) == len(EXPECTED_TASKS), (
            f"Expected {len(EXPECTED_TASKS)} tasks, got {len(dag.tasks)}"
        )
    
    def test_email_task_is_last(self, dag):
        """send_email must be the terminal task with no downstream dependencies."""
        email_task = dag.get_task("send_email")
        assert email_task.downstream_list == [], (
            f"send_email has unexpected downstream tasks: "
            f"{[t.task_id for t in email_task.downstream_list]}"
        )
    
    def test_extract_is_first(self, dag):
        """extract_sales must have no upstream dependencies."""
        extract_task = dag.get_task("extract_sales")
        assert extract_task.upstream_list == [], (
            f"extract_sales has unexpected upstream tasks: "
            f"{[t.task_id for t in extract_task.upstream_list]}"
        )
    
    def test_catchup_disabled(self, dag):
        """Catchup must be disabled to prevent massive historical backfill."""
        assert dag.catchup == False, (
            "DAG has catchup=True — this will trigger historical runs on deployment!"
        )
    
    def test_retries_configured(self, dag):
        """All tasks should have at least 1 retry."""
        for task in dag.tasks:
            assert task.retries >= 1, (
                f"Task '{task.task_id}' has no retries configured — "
                "set retries >= 1 in default_args"
            )
```

**Running the tests:**

```bash
# Install dependencies
pip install apache-airflow pytest

# Initialize test DB
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:///test.db
export AIRFLOW__CORE__LOAD_EXAMPLES=False
airflow db init

# Run tests
pytest tests/test_weekly_sales_report.py -v

# Expected output:
# PASSED test_dag_loads_without_import_errors
# PASSED test_dag_exists  
# PASSED test_correct_schedule_interval
# PASSED test_expected_tasks_present
# ...
```

</details>

</article>

<article data-difficulty="mid">

## 🟡 Mid: Write Unit Tests for a Custom Operator That Calls an External API with Retries

**Scenario:** You've written a `WeatherAPIOperator` that fetches weather data, handles rate limits (429 response), and retries up to 3 times with exponential backoff. Write comprehensive unit tests covering: success, rate limit with successful retry, permanent failure, and missing API key.

<details>
<summary>✅ Solution</summary>

```python
# dags/operators/weather_api.py
import time
import requests
from airflow.models import BaseOperator
from airflow.models import Variable

class WeatherAPIOperator(BaseOperator):
    def __init__(self, city: str, output_key: str = "weather_data", 
                 max_retries: int = 3, **kwargs):
        super().__init__(**kwargs)
        self.city = city
        self.output_key = output_key
        self.max_retries = max_retries

    def execute(self, context):
        api_key = Variable.get("weather_api_key")
        if not api_key:
            raise ValueError("weather_api_key variable is not set")

        url = f"https://api.weather.example.com/current?city={self.city}&key={api_key}"
        
        for attempt in range(1, self.max_retries + 1):
            self.log.info(f"Attempt {attempt}/{self.max_retries}: fetching weather for {self.city}")
            response = requests.get(url, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                context["task_instance"].xcom_push(key=self.output_key, value=data)
                self.log.info(f"Successfully fetched weather for {self.city}")
                return data
            
            elif response.status_code == 429:  # Rate limited
                wait = 2 ** attempt  # Exponential backoff: 2, 4, 8 seconds
                self.log.warning(f"Rate limited. Waiting {wait}s before retry {attempt + 1}")
                time.sleep(wait)
                continue
            
            else:
                response.raise_for_status()
        
        raise RuntimeError(f"Failed to fetch weather after {self.max_retries} attempts")
```

```python
# tests/operators/test_weather_api.py
import pytest
import time
from unittest.mock import MagicMock, patch, call
from datetime import datetime

@pytest.fixture
def context():
    mock_ti = MagicMock()
    return {
        "ds": "2024-01-15",
        "task_instance": mock_ti,
        "execution_date": datetime(2024, 1, 15),
    }

@pytest.fixture
def operator():
    return WeatherAPIOperator(
        task_id="fetch_weather",
        city="London",
        max_retries=3,
    )


class TestWeatherAPIOperator:

    @patch("dags.operators.weather_api.Variable.get")
    @patch("dags.operators.weather_api.requests.get")
    def test_success_on_first_attempt(self, mock_get, mock_var, operator, context):
        """Happy path: API returns 200 on first try."""
        mock_var.return_value = "test-api-key-123"
        
        weather_data = {"city": "London", "temp_c": 12.5, "condition": "Cloudy"}
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = weather_data
        mock_get.return_value = mock_response
        
        result = operator.execute(context)
        
        assert result == weather_data
        assert mock_get.call_count == 1  # Only called once
        context["task_instance"].xcom_push.assert_called_once_with(
            key="weather_data", value=weather_data
        )

    @patch("dags.operators.weather_api.time.sleep")
    @patch("dags.operators.weather_api.Variable.get")
    @patch("dags.operators.weather_api.requests.get")
    def test_rate_limit_then_success(self, mock_get, mock_var, mock_sleep, operator, context):
        """429 on first call, 200 on second — should succeed with backoff."""
        mock_var.return_value = "test-api-key-123"
        
        # First call: 429 (rate limited), second call: 200 (success)
        rate_limit_response = MagicMock()
        rate_limit_response.status_code = 429
        
        success_response = MagicMock()
        success_response.status_code = 200
        success_response.json.return_value = {"city": "London", "temp_c": 12.5}
        
        mock_get.side_effect = [rate_limit_response, success_response]
        
        result = operator.execute(context)
        
        assert result == {"city": "London", "temp_c": 12.5}
        assert mock_get.call_count == 2
        # Verify backoff: first retry waits 2^1 = 2 seconds
        mock_sleep.assert_called_once_with(2)

    @patch("dags.operators.weather_api.time.sleep")
    @patch("dags.operators.weather_api.Variable.get")
    @patch("dags.operators.weather_api.requests.get")
    def test_all_retries_exhausted_raises(self, mock_get, mock_var, mock_sleep, operator, context):
        """429 on every attempt — should raise RuntimeError after max_retries."""
        mock_var.return_value = "test-api-key-123"
        
        rate_limit_response = MagicMock()
        rate_limit_response.status_code = 429
        mock_get.return_value = rate_limit_response  # Always 429
        
        with pytest.raises(RuntimeError, match="Failed to fetch weather after 3 attempts"):
            operator.execute(context)
        
        assert mock_get.call_count == 3  # Tried 3 times
        # Verify exponential backoff: 2^1=2, 2^2=4, 2^3=8 — but only 2 sleeps before final raise
        assert mock_sleep.call_args_list == [call(2), call(4)]

    @patch("dags.operators.weather_api.Variable.get")
    @patch("dags.operators.weather_api.requests.get")
    def test_server_error_raises_immediately(self, mock_get, mock_var, operator, context):
        """500 error should raise immediately via raise_for_status()."""
        mock_var.return_value = "test-api-key-123"
        
        error_response = MagicMock()
        error_response.status_code = 500
        error_response.raise_for_status.side_effect = requests.HTTPError("500 Server Error")
        mock_get.return_value = error_response
        
        with pytest.raises(requests.HTTPError, match="500 Server Error"):
            operator.execute(context)
        
        assert mock_get.call_count == 1  # No retries on non-429 errors

    @patch("dags.operators.weather_api.Variable.get")
    def test_missing_api_key_raises_value_error(self, mock_var, operator, context):
        """Empty API key should raise ValueError before making any HTTP calls."""
        mock_var.return_value = ""  # Empty key
        
        with pytest.raises(ValueError, match="weather_api_key variable is not set"):
            operator.execute(context)

    @patch("dags.operators.weather_api.Variable.get")
    @patch("dags.operators.weather_api.requests.get")
    def test_custom_output_key(self, mock_get, mock_var, context):
        """Operator with custom output_key should push to that XCom key."""
        mock_var.return_value = "test-key"
        
        success_response = MagicMock()
        success_response.status_code = 200
        success_response.json.return_value = {"temp": 15}
        mock_get.return_value = success_response
        
        operator = WeatherAPIOperator(
            task_id="fetch_weather",
            city="Paris",
            output_key="paris_weather",  # Custom key
        )
        operator.execute(context)
        
        context["task_instance"].xcom_push.assert_called_once_with(
            key="paris_weather", value={"temp": 15}  # Uses custom key
        )
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Full Testing Strategy for 100 Production DAGs

**Scenario:** Your team maintains 100 production DAGs. Currently there are zero tests. You've just experienced two production incidents: (1) a non-idempotent load created duplicate rows, (2) an import error in a new DAG broke the scheduler's ability to parse the entire dags/ folder. Design and implement a comprehensive testing strategy with CI gates.

<details>
<summary>✅ Solution</summary>

**Testing pyramid for 100 DAGs:**

```
            [E2E: 2-3 critical paths]
           [Integration: 20 key DAGs]
          [Unit: all task functions]
         [Integrity: all 100 DAGs — fast]
```

**Layer 1: DAG Integrity (runs on every commit, < 3 minutes)**

```python
# tests/integrity/test_all_dags.py
import pytest
from airflow.models import DagBag

@pytest.fixture(scope="session")
def dagbag():
    return DagBag(dag_folder="dags/", include_examples=False)

def test_no_import_errors(dagbag):
    """CRITICAL: No import errors. One broken DAG can delay all others."""
    assert dagbag.import_errors == {}, (
        "IMPORT ERRORS DETECTED — fix before merging:\n"
        + "\n".join(f"  {f}: {e}" for f, e in dagbag.import_errors.items())
    )

# Parametrize over all DAGs automatically
all_dag_ids = list(DagBag("dags/", include_examples=False).dags.keys())

@pytest.mark.parametrize("dag_id", all_dag_ids)
def test_all_dags_have_owner(dagbag, dag_id):
    assert dagbag.get_dag(dag_id).owner != "airflow"

@pytest.mark.parametrize("dag_id", all_dag_ids)
def test_all_dags_catchup_false(dagbag, dag_id):
    assert dagbag.get_dag(dag_id).catchup == False

@pytest.mark.parametrize("dag_id", all_dag_ids)
def test_all_tasks_have_retries(dagbag, dag_id):
    dag = dagbag.get_dag(dag_id)
    for task in dag.tasks:
        assert task.retries >= 1, f"{dag_id}.{task.task_id}: no retries"
```

**Layer 2: Unit Tests (runs on every PR, < 5 minutes)**

```python
# Enforce 80% coverage on dags/ code
# pytest.ini
[pytest]
addopts = --cov=dags --cov-report=term-missing --cov-fail-under=80
```

```python
# Fixture for idempotency testing — apply to ALL write tasks
@pytest.fixture
def idempotency_checker():
    """Helper to run a task twice and compare output."""
    def check(task_callable, context, table, partition_col, partition_val):
        task_callable(**context)
        count1 = query_db(f"SELECT COUNT(*) FROM {table} WHERE {partition_col} = '{partition_val}'")
        task_callable(**context)
        count2 = query_db(f"SELECT COUNT(*) FROM {table} WHERE {partition_col} = '{partition_val}'")
        assert count1 == count2, f"NOT IDEMPOTENT: {count1} vs {count2} rows"
    return check

# Idempotency tests for the 10 highest-risk load tasks
LOAD_TASKS_TO_TEST = [
    ("dags.sales_etl", "load_orders", "fact_orders", "order_date", "2024-01-15"),
    ("dags.inventory_etl", "load_inventory", "fact_inventory", "snapshot_date", "2024-01-15"),
    # ... 8 more
]

@pytest.mark.parametrize("module,func_name,table,col,val", LOAD_TASKS_TO_TEST)
def test_load_tasks_idempotent(module, func_name, table, col, val, idempotency_checker):
    import importlib
    mod = importlib.import_module(module)
    func = getattr(mod, func_name)
    context = {"ds": val, "task_instance": MagicMock()}
    idempotency_checker(func, context, table, col, val)
```

**Layer 3: Integration Tests (runs on merge to main)**

```python
# tests/integration/test_critical_dags.py
CRITICAL_DAGS = ["daily_revenue_etl", "customer_360_refresh", "inventory_sync"]

@pytest.mark.parametrize("dag_id", CRITICAL_DAGS)
def test_dag_runs_successfully(dag_id, dagbag):
    dag = dagbag.get_dag(dag_id)
    dag.test(execution_date=datetime(2024, 1, 15))
    # If no exception raised, the DAG ran successfully
```

**CI pipeline configuration:**

```yaml
# .github/workflows/airflow-ci.yml
jobs:
  integrity:
    name: "Gate 1: DAG Integrity (blocks all PRs)"
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - run: pip install apache-airflow pytest
      - run: pytest tests/integrity/ -v --tb=short
      # This job MUST pass — if broken, no PR merges

  unit-tests:
    name: "Gate 2: Unit Tests (80% coverage required)"
    needs: integrity
    runs-on: ubuntu-latest
    steps:
      - run: pytest tests/unit/ --cov=dags --cov-fail-under=80

  integration-tests:
    name: "Gate 3: Integration Tests (on merge to main)"
    needs: unit-tests
    if: github.ref == 'refs/heads/main'
    services:
      postgres:
        image: postgres:15
        # ...
    steps:
      - run: pytest tests/integration/ -v --timeout=300
```

**Rollout plan for the 100-DAG codebase:**

```
Week 1: Integrity tests only
  → Unblock the "import error kills all DAGs" problem immediately
  → Add to CI as a required check

Week 2: Unit tests for new code (going forward)
  → Enforce "no new code without tests" via PR review
  → Add coverage reporting (start at 0%, improve over time)

Week 3-4: Idempotency tests for top 20 highest-risk load tasks
  → Prioritize tasks that write to shared tables
  → Each test added = one incident prevented

Week 5-8: Retrofit unit tests for existing task functions
  → Target 80% coverage on dags/ within 2 months
  → Assign each team responsibility for their DAGs

Month 3: Integration tests for critical paths
  → dag.test() for top 10 most business-critical DAGs
  → Data contract tests for output tables
```

</details>

</article>
