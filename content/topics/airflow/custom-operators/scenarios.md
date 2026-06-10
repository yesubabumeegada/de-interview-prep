---
title: "Airflow Custom Operators - Scenario Questions"
topic: airflow
subtopic: custom-operators
content_type: scenario_question
tags: [airflow, custom-operators, hooks, sensors, deferrable, interview, scenarios]
---

# Scenario Questions — Airflow Custom Operators

<article data-difficulty="junior">

## 🟢 Junior: Build a Simple File Validation Operator

**Scenario:** You need an Airflow operator that checks if a local file exists and is non-empty. If the file doesn't exist or is empty, the task should fail. If it exists and has content, the task should return the file size in bytes. Build this custom operator.

<details>
<summary>💡 Hint</summary>

Extend `BaseOperator` and implement `execute(context)`. Use `os.path.exists()` and `os.path.getsize()`. Raise `AirflowException` for failures and return the size for success. Add the file path to `template_fields` so users can use Jinja expressions like `{{ ds }}` in the path.

</details>

<details>
<summary>✅ Solution</summary>

```python
import os
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

class FileValidationOperator(BaseOperator):
    """
    Checks that a file exists and is non-empty.
    
    :param file_path: Path to the file to validate. Supports Jinja templating.
    """

    template_fields = ('file_path',)   # Allows {{ ds }} in file_path

    def __init__(self, file_path: str, **kwargs):
        super().__init__(**kwargs)   # Always pass **kwargs
        self.file_path = file_path

    def execute(self, context: dict):
        self.log.info(f"Validating file: {self.file_path}")

        if not os.path.exists(self.file_path):
            raise AirflowException(f"File does not exist: {self.file_path}")

        size = os.path.getsize(self.file_path)
        if size == 0:
            raise AirflowException(f"File is empty: {self.file_path}")

        self.log.info(f"File is valid: {self.file_path} ({size} bytes)")
        return size   # Pushed to XCom automatically
```

```python
# Usage
validate = FileValidationOperator(
    task_id='validate_daily_export',
    file_path='/data/exports/{{ ds }}/orders.csv',
    retries=2,
)
```

**Key points:**
- `**kwargs` passed to `super().__init__()` enables `retries`, `pool`, `on_failure_callback`, etc.
- `template_fields = ('file_path',)` enables `{{ ds }}` in the path
- Raising `AirflowException` marks the task as failed with a clear error message
- Return value goes to XCom (accessible by downstream tasks with `xcom_pull`)

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Custom Sensor for an Internal API

**Scenario:** Your company's internal data platform exposes a REST API endpoint `GET /api/jobs/{job_id}/status` that returns `{"status": "running"|"complete"|"failed"}`. Build a custom sensor that polls this endpoint until the job is complete.

<details>
<summary>💡 Hint</summary>

Extend `BaseSensorOperator` and implement `poke(context)`. The method should return `True` when the condition is met (job complete) and `False` to keep waiting. Use `mode='reschedule'` to free the worker slot between polls. Raise `AirflowException` if the job fails.

</details>

<details>
<summary>✅ Solution</summary>

```python
import requests
from airflow.sensors.base import BaseSensorOperator
from airflow.exceptions import AirflowException

class InternalJobSensor(BaseSensorOperator):
    """
    Polls the internal data platform API until a job completes.
    
    :param job_id: The job ID to poll.
    :param api_base_url: Base URL for the data platform API.
    :param api_token: Bearer token for authentication.
    """

    template_fields = ('job_id',)

    def __init__(
        self,
        job_id: str,
        api_base_url: str,
        api_token: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_id = job_id
        self.api_base_url = api_base_url
        self.api_token = api_token

    def poke(self, context: dict) -> bool:
        url = f"{self.api_base_url}/api/jobs/{self.job_id}/status"
        response = requests.get(
            url,
            headers={'Authorization': f'Bearer {self.api_token}'},
            timeout=30,
        )
        response.raise_for_status()
        status = response.json()['status']

        self.log.info(f"Job {self.job_id} status: {status}")

        if status == 'complete':
            return True   # Condition met — continue DAG
        elif status == 'failed':
            raise AirflowException(f"Job {self.job_id} failed")
        else:
            return False  # Still running — keep polling
```

```python
# Usage
wait_for_job = InternalJobSensor(
    task_id='wait_for_etl_job',
    job_id='{{ ti.xcom_pull("submit_job") }}',  # Job ID from upstream task
    api_base_url='https://data-platform.internal',
    api_token='{{ var.value.data_platform_token }}',
    mode='reschedule',      # Free worker slot between polls
    poke_interval=60,       # Check every minute
    timeout=7200,           # Give up after 2 hours
)
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Operator That Reads from Airflow Connections

**Scenario:** You're building a `SlackNotifyOperator` that posts a message to Slack. The Slack webhook URL should be stored in an Airflow Connection (not hardcoded), so different environments (dev, staging, prod) can use different channels. Implement the operator using the Hook pattern.

<details>
<summary>💡 Hint</summary>

Build a `SlackWebhookHook` that extends `BaseHook` and reads the webhook URL from the Airflow Connection's `host` field. The hook handles the HTTP call. The operator instantiates the hook and calls its method. This separates connection management (hook) from task logic (operator) and makes both independently testable.

</details>

<details>
<summary>✅ Solution</summary>

```python
# Step 1: The Hook
import requests
from airflow.hooks.base import BaseHook

class SlackWebhookHook(BaseHook):
    """Manages Slack webhook connection via Airflow Connection store."""

    conn_type = 'slack_webhook'

    def __init__(self, slack_conn_id: str = 'slack_default'):
        super().__init__()
        self.slack_conn_id = slack_conn_id

    def get_webhook_url(self) -> str:
        conn = self.get_connection(self.slack_conn_id)
        # Store full webhook URL in host field:
        # e.g. hooks.slack.com/services/T00/B00/XXXX
        return f"https://{conn.host}"

    def send(self, text: str, blocks: list = None) -> None:
        url = self.get_webhook_url()
        payload = {'text': text}
        if blocks:
            payload['blocks'] = blocks
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        if response.text != 'ok':
            raise Exception(f"Slack API error: {response.text}")


# Step 2: The Operator
from airflow.models import BaseOperator

class SlackNotifyOperator(BaseOperator):
    """Posts a message to Slack using an Airflow Connection."""

    template_fields = ('message',)   # Supports {{ ds }}, {{ ti }}, etc.

    def __init__(
        self,
        message: str,
        slack_conn_id: str = 'slack_default',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.message = message
        self.slack_conn_id = slack_conn_id

    def execute(self, context: dict):
        hook = SlackWebhookHook(slack_conn_id=self.slack_conn_id)
        hook.send(text=self.message)
        self.log.info(f"Slack notification sent: {self.message[:50]}...")
```

```python
# Usage — different connections per environment
notify = SlackNotifyOperator(
    task_id='notify_team',
    message='Pipeline for {{ ds }} completed — {{ ti.xcom_pull("load") }} rows loaded',
    slack_conn_id='slack_prod_alerts',   # Configured in Airflow UI
)
```

**Test the hook independently:**

```python
def test_slack_hook_sends_message():
    from unittest.mock import patch, MagicMock
    hook = SlackWebhookHook(slack_conn_id='test_conn')
    
    with patch.object(hook, 'get_connection') as mock_conn:
        mock_conn.return_value = MagicMock(host='hooks.slack.com/services/T/B/X')
        with patch('requests.post') as mock_post:
            mock_post.return_value = MagicMock(text='ok', raise_for_status=lambda: None)
            hook.send("Test message")
            mock_post.assert_called_once()
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Operator Skipping on No Data

**Scenario:** A downstream reporting operator should only run when there's data for the day. When data is absent, the task should be marked as **skipped** (not failed). Implement this using `AirflowSkipException` and explain why this is better than raising a normal exception.

<details>
<summary>💡 Hint</summary>

`AirflowSkipException` marks a task as `skipped` (green/yellow) instead of `failed` (red). Skipped tasks don't trigger `on_failure_callback` and don't count as failures for SLA monitoring. Use it when "no data today" is an expected, valid outcome — not an error.

</details>

<details>
<summary>✅ Solution</summary>

```python
from airflow.models import BaseOperator
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

class ConditionalReportOperator(BaseOperator):
    """
    Generates a daily report, but skips gracefully if no data is available.
    """

    template_fields = ('table', 'report_date')

    def __init__(self, table: str, conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.table = table
        self.conn_id = conn_id
        self.report_date = '{{ ds }}'

    def execute(self, context):
        hook = SnowflakeHook(snowflake_conn_id=self.conn_id)

        count_result = hook.get_first(
            f"SELECT COUNT(*) FROM {self.table} WHERE report_date = '{self.report_date}'"
        )
        row_count = count_result[0] if count_result else 0

        if row_count == 0:
            # ✅ Skip — expected on weekends, holidays, etc.
            raise AirflowSkipException(
                f"No data in {self.table} for {self.report_date} — skipping report generation"
            )

        self.log.info(f"Generating report for {row_count} rows")
        generate_report(self.table, self.report_date)
        return {'rows': row_count, 'date': self.report_date}
```

**Why `AirflowSkipException` over a regular `Exception`:**

| Behaviour | `AirflowSkipException` | Regular `Exception` |
|-----------|----------------------|---------------------|
| Task state | `skipped` (grey/yellow) | `failed` (red) |
| `on_failure_callback` | NOT triggered | Triggered |
| Downstream tasks | Skipped too (unless trigger_rule adjusted) | Set to `upstream_failed` |
| SLA monitoring | Not counted as failure | Counted as failure |
| Retry triggered | No | Yes (if retries > 0) |

Use `AirflowSkipException` when: data for a period genuinely doesn't exist and that's acceptable.  
Use `AirflowException` when: data should exist but something went wrong.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design a Reusable Operator Framework for Your Platform

**Scenario:** Your data platform team of 10 engineers supports 30+ internal teams building Airflow DAGs. You're seeing the same patterns: data quality checks, Snowflake loads, Slack notifications, and file sensors — copied and pasted across 200 DAGs. Design a reusable custom operator library that the 30 teams can consume as a pip package.

<details>
<summary>💡 Hint</summary>

This is an architectural design question. Consider: package structure, versioning, connection abstraction, testing strategy, documentation, and how you expose the library to consuming teams. Think about how to make the operators easy to use correctly (good defaults, clear template_fields) and hard to use incorrectly (type hints, validation in __init__).

</details>

<details>
<summary>✅ Solution</summary>

**Package structure:**

```
company-airflow-providers/
├── pyproject.toml
├── CHANGELOG.md
├── company_airflow/
│   ├── __init__.py
│   ├── hooks/
│   │   ├── snowflake_hook.py     # Extended Snowflake hook with retry/logging
│   │   ├── slack_hook.py
│   │   └── internal_api_hook.py
│   ├── operators/
│   │   ├── data_quality.py       # DataQualityOperator
│   │   ├── snowflake_load.py     # IdempotentSnowflakeLoadOperator
│   │   ├── notify.py             # MultiChannelNotifyOperator
│   │   └── conditional_task.py   # ConditionalRunOperator
│   ├── sensors/
│   │   ├── s3_sensor.py          # S3KeyWithMinSizeSensor
│   │   └── internal_job_sensor.py
│   └── utils/
│       ├── idempotency.py        # Shared idempotency helpers
│       └── alerting.py           # Shared alert formatting
├── tests/
│   ├── unit/
│   │   ├── test_data_quality.py
│   │   └── test_snowflake_load.py
│   └── integration/
│       └── test_end_to_end.py    # Against real Airflow test env
└── docs/
    └── operators.md
```

**Design principles enforced in the library:**

```python
# 1. Validate inputs in __init__, not execute() — fail fast
class IdempotentSnowflakeLoadOperator(BaseOperator):
    def __init__(self, target_table: str, source_path: str, partition_col: str, **kwargs):
        super().__init__(**kwargs)
        # Validate at construction time
        if '.' not in target_table:
            raise ValueError(
                f"target_table must be schema.table format, got: {target_table}"
            )
        self.target_table = target_table
        self.source_path = source_path
        self.partition_col = partition_col

# 2. Strong defaults aligned with company standards
class DataQualityOperator(BaseOperator):
    def __init__(
        self,
        table: str,
        checks: list,
        conn_id: str = 'snowflake_prod',   # Company default connection
        fail_on_zero_rows: bool = True,     # Safe default
        alert_on_failure: bool = True,      # Automatically alert team
        **kwargs,
    ):
        # ...

# 3. Structured logging for observability
class CompanyBaseOperator(BaseOperator):
    def execute(self, context):
        self.log.info(json.dumps({
            'operator': type(self).__name__,
            'dag_id': context['dag'].dag_id,
            'task_id': context['task_instance'].task_id,
            'execution_date': context['ds'],
            'run_id': context['run_id'],
        }))
        return self._execute(context)

    def _execute(self, context):
        raise NotImplementedError
```

**Release and versioning:**

```toml
# pyproject.toml
[project]
name = "company-airflow-providers"
version = "2.1.0"
dependencies = [
    "apache-airflow>=2.6.0",
    "apache-airflow-providers-snowflake>=4.0.0",
    "apache-airflow-providers-amazon>=8.0.0",
]

[tool.semantic_release]
# Auto-bump version based on conventional commits
# feat: → minor bump, fix: → patch bump, BREAKING: → major bump
```

**CI/CD for the library:**

```yaml
# .github/workflows/ci.yml
on: [push, pull_request]
jobs:
  test:
    steps:
      - run: pytest tests/unit/ --cov=company_airflow --cov-fail-under=90
      - run: mypy company_airflow/   # Type checking
      - run: ruff check company_airflow/  # Linting
  
  integration:
    if: github.ref == 'refs/heads/main'
    steps:
      - run: pytest tests/integration/ --airflow-home=/tmp/airflow-test
  
  publish:
    if: github.event_name == 'release'
    steps:
      - run: pip install build && python -m build
      - run: twine upload --repository company-pypi dist/*
```

**Consumption by teams:**

```python
# Any team's DAG — simple, consistent, validated
from company_airflow.operators.data_quality import DataQualityOperator
from company_airflow.operators.snowflake_load import IdempotentSnowflakeLoadOperator
from company_airflow.sensors.s3_sensor import S3KeyWithMinSizeSensor

with DAG('team_abc_pipeline', ...) as dag:
    wait  = S3KeyWithMinSizeSensor(...)
    load  = IdempotentSnowflakeLoadOperator(...)
    check = DataQualityOperator(...)
    wait >> load >> check
```

**Key architectural decisions to explain in interviews:**
1. **Hooks separate from operators** — hooks are independently testable
2. **Fail-fast validation in `__init__`** — catch misconfiguration at DAG parse time, not task runtime
3. **Semantic versioning + changelog** — teams can pin versions and migrate deliberately
4. **90%+ test coverage enforced in CI** — operators are production infrastructure
5. **Type hints throughout** — IDEs provide autocomplete for operator parameters

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: When should you write a custom operator vs. using a PythonOperator?**
A: Write a custom operator when the logic is reused across multiple DAGs, when you need clean separation of concerns (template fields, connection management), or when you want to encapsulate retries and error handling in a reusable component. Use PythonOperator for one-off logic that doesn't need to be shared.

**Q: What method must every custom operator implement?**
A: Every custom operator must implement the `execute(self, context)` method. This is called by the Airflow executor when the task runs. The `context` dict contains DagRun metadata (execution_date, task_instance, etc.) useful for dynamic behavior.

**Q: What are `template_fields` in a custom operator and why do they matter?**
A: `template_fields` is a tuple of attribute names that Airflow will render as Jinja2 templates before `execute()` is called. Declaring them allows users to pass dynamic values like `{{ ds }}` or `{{ var.value.my_var }}` to operator parameters, making operators more reusable without code changes.

**Q: How do you inherit from `BaseOperator` vs. from an existing operator like `PythonOperator`?**
A: Inherit from `BaseOperator` when building a completely new integration (custom API, service, or protocol). Inherit from an existing operator when extending its behavior — but be careful about coupling to parent class internals. Prefer composition (using a Hook inside a `BaseOperator` subclass) over deep inheritance chains.

**Q: How do you write unit tests for a custom operator?**
A: Instantiate the operator with test parameters, mock external calls (HTTP requests, database connections) using `unittest.mock`, and call `operator.execute(context={})` directly. Test that `execute()` calls the right methods with the right arguments and handles exceptions as expected.

**Q: What is the `ui_color` attribute in a custom operator used for?**
A: `ui_color` sets the background color of the task box in the Airflow UI. It's purely cosmetic but useful for visually distinguishing task types in complex DAGs — e.g., using orange for data extraction tasks and blue for transformation tasks.

**Q: How do you pass output from a custom operator to downstream tasks?**
A: Return a value from the `execute()` method — Airflow automatically pushes it to XCom under the key `return_value`. Downstream tasks can retrieve it with `ti.xcom_pull(task_ids='upstream_task')`. For large data, avoid XCom and instead write to external storage, pushing only a reference (path/URI).

**Q: What is a sensor operator and how does it differ from a standard custom operator?**
A: A sensor inherits from `BaseSensorOperator` and implements `poke(self, context)` instead of `execute()`. Airflow calls `poke()` repeatedly on an interval until it returns `True`. Sensors are designed for waiting on external conditions (file arrival, API availability, partition existence) before triggering downstream tasks.

---

## 💼 Interview Tips

- Show you understand when NOT to write a custom operator — overengineering simple one-off logic as a custom operator is a common antipattern that senior reviewers spot immediately.
- Always mention `template_fields` when describing custom operators — it's a critical feature that enables reuse and distinguishes purpose-built operators from hardcoded Python callables.
- Testing operators is a common interview follow-up — be ready to describe exactly how you'd mock the Hook or external service inside an operator's `execute()` method.
- Senior interviewers care about operator design principles: single responsibility, composing with Hooks for connection management, and clear error handling with meaningful exception messages.
- Discuss the operator contribution pattern — well-designed operators can be contributed back to official Airflow providers, which signals community awareness and code quality standards.
- Mention the `context` dict and what's available in it (`dag_run`, `task_instance`, `execution_date`, `conf`) — using it well shows you've written operators used in real production DAGs.
