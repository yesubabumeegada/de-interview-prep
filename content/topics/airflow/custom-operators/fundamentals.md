---
title: "Airflow Custom Operators - Fundamentals"
topic: airflow
subtopic: custom-operators
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [airflow, custom-operators, baseoperator, hooks, extensibility]
---

# Airflow Custom Operators — Fundamentals

## Why Build Custom Operators?

Built-in and provider operators cover most common tasks, but you build a custom operator when:

- No provider operator exists for your internal system
- You want to encapsulate business logic shared across multiple DAGs
- You need custom retry logic, error handling, or callbacks
- You want to create a reusable abstraction your team can use without knowing the implementation details

> **Analogy:** If `PythonOperator` is a general-purpose screwdriver, a custom operator is a specialty bit designed exactly for your specific screw type. It's more effort to make, but far easier to use repeatedly.

---

## Anatomy of a Custom Operator

Every custom operator extends `BaseOperator`:

```python
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class MyFirstOperator(BaseOperator):
    """
    A simple operator that logs a greeting.
    
    :param name: The name to greet.
    :param greeting: The greeting word (default: 'Hello').
    """

    # Defines which __init__ parameters are Jinja-templated before execute()
    template_fields = ('name', 'greeting')

    def __init__(
        self,
        name: str,
        greeting: str = 'Hello',
        **kwargs,          # Always pass **kwargs to BaseOperator
    ):
        super().__init__(**kwargs)   # Required — sets task_id, retries, etc.
        self.name = name
        self.greeting = greeting

    def execute(self, context: dict):
        """
        Called by Airflow when the task runs.
        context contains: ds, execution_date, ti, dag, conf, etc.
        """
        ds = context['ds']
        self.log.info(f"{self.greeting}, {self.name}! Running for {ds}")
        return f"{self.greeting}, {self.name}"   # Return value goes to XCom
```

**Key elements:**

| Element | Purpose |
|---------|---------|
| `BaseOperator` | Provides all Airflow plumbing (retries, callbacks, pool slots, etc.) |
| `template_fields` | List of attributes that get Jinja-rendered before `execute()` |
| `**kwargs` in `__init__` | Must be passed to `super().__init__(**kwargs)` so `task_id`, `retries`, etc. work |
| `execute(context)` | The only method you must implement — contains the task logic |
| `self.log` | Airflow logger — use this instead of `print()` or `logging` |

---

## Using Your Custom Operator in a DAG

```python
from airflow import DAG
from datetime import datetime
from my_operators import MyFirstOperator   # import from wherever you store it

with DAG(
    dag_id='greeting_dag',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
) as dag:

    greet = MyFirstOperator(
        task_id='greet_team',
        name='Data Engineering Team',
        greeting='Good morning',
        retries=1,      # All BaseOperator params work automatically
    )
```

---

## Where to Put Custom Operators

```
project/
├── dags/
│   ├── my_dag.py
│   └── another_dag.py
├── plugins/               # ← Airflow auto-discovers from this folder
│   ├── __init__.py
│   └── operators/
│       ├── __init__.py
│       └── my_operator.py
└── requirements.txt
```

Airflow automatically adds the `plugins/` folder to the Python path. You can also package operators as a Python package and install with pip.

---

## The Hook + Operator Pattern

The recommended pattern separates **connection management** (Hook) from **orchestration logic** (Operator):

```python
# hooks/my_system_hook.py
from airflow.hooks.base import BaseHook
import requests

class MyApiHook(BaseHook):
    """Manages connection to My API using Airflow connection store."""

    def __init__(self, conn_id: str = 'my_api_default'):
        super().__init__()
        self.conn_id = conn_id

    def get_conn(self):
        conn = self.get_connection(self.conn_id)   # reads from Airflow Connections
        return requests.Session(), conn.host, conn.password

    def post_data(self, endpoint: str, payload: dict) -> dict:
        session, host, token = self.get_conn()
        session.headers['Authorization'] = f'Bearer {token}'
        response = session.post(f'https://{host}{endpoint}', json=payload)
        response.raise_for_status()
        return response.json()
```

```python
# operators/my_api_operator.py
from airflow.models import BaseOperator
from hooks.my_system_hook import MyApiHook

class PostToMyApiOperator(BaseOperator):
    """Posts data to My API."""

    template_fields = ('endpoint', 'payload')

    def __init__(self, endpoint: str, payload: dict, conn_id: str = 'my_api_default', **kwargs):
        super().__init__(**kwargs)
        self.endpoint = endpoint
        self.payload = payload
        self.conn_id = conn_id

    def execute(self, context):
        hook = MyApiHook(conn_id=self.conn_id)    # Hook handles all auth/connection
        result = hook.post_data(self.endpoint, self.payload)
        self.log.info(f"API response: {result}")
        return result
```

**Why this separation?**
- Hook is independently testable without Airflow context
- Hook can be reused by multiple different operators
- Operator stays thin and focused on orchestration

---

## Interview Tips

> **Tip 1:** "When would you build a custom operator vs using a PythonOperator?" — Use `PythonOperator` for one-off task logic. Build a custom operator when the same pattern needs to be reused across multiple DAGs or teams. Custom operators are the Airflow equivalent of writing a library function instead of copy-pasting code.

> **Tip 2:** Always pass `**kwargs` to `super().__init__(**kwargs)`. Forgetting this means parameters like `task_id`, `retries`, `pool`, and `on_failure_callback` silently don't work.

> **Tip 3:** Use `self.log` (Airflow's built-in logger), not `print()`. The Airflow logger writes to the task log file visible in the UI, and includes contextual information like task ID and execution date.
