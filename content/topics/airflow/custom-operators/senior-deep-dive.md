---
title: "Airflow Custom Operators - Senior Deep Dive"
topic: airflow
subtopic: custom-operators
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [airflow, custom-operators, deferrable, triggers, lineage, plugin-system]
---

# Airflow Custom Operators — Senior Deep Dive

## Deferrable Custom Operators

Standard operators hold a worker slot for their entire duration. **Deferrable operators** suspend themselves to free the worker while waiting for a long-running external process:

```python
from airflow.models import BaseOperator
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.exceptions import TaskDeferred
from airflow.utils.context import Context
import asyncio

# Step 1: The Trigger — runs in the triggerer process (asyncio)
class SnowflakeQueryTrigger(BaseTrigger):
    """Polls Snowflake query status asynchronously."""

    def __init__(self, query_id: str, conn_id: str):
        super().__init__()
        self.query_id = query_id
        self.conn_id = conn_id

    def serialize(self) -> tuple:
        # Must be serialisable — stored in DB between poll cycles
        return (
            'my_operators.triggers.SnowflakeQueryTrigger',
            {'query_id': self.query_id, 'conn_id': self.conn_id},
        )

    async def run(self):
        """Called by the triggerer event loop — must be async."""
        while True:
            status = await self._poll_query_status()
            if status == 'SUCCESS':
                yield TriggerEvent({'status': 'success', 'query_id': self.query_id})
                return
            elif status == 'FAILED':
                yield TriggerEvent({'status': 'failed', 'query_id': self.query_id})
                return
            await asyncio.sleep(15)   # Non-blocking sleep

    async def _poll_query_status(self) -> str:
        # Use async Snowflake client or run sync client in thread pool
        import asyncio
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._sync_poll)

    def _sync_poll(self) -> str:
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id=self.conn_id)
        result = hook.get_first(
            f"SELECT STATUS FROM TABLE(RESULT_SCAN('{self.query_id}'))"
        )
        return result[0] if result else 'RUNNING'


# Step 2: The Deferrable Operator
class DeferrableSnowflakeOperator(BaseOperator):
    """
    Submits a Snowflake query and defers until it completes.
    Frees the worker slot while the query runs (which may take hours).
    """

    template_fields = ('sql',)

    def __init__(self, sql: str, conn_id: str = 'snowflake_default', **kwargs):
        super().__init__(**kwargs)
        self.sql = sql
        self.conn_id = conn_id

    def execute(self, context: Context):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id=self.conn_id)

        # Submit query asynchronously — get a query ID immediately
        query_id = hook.run(self.sql, handler=lambda cursor: cursor.sfqid)
        self.log.info(f"Submitted query: {query_id} — deferring")

        # Suspend this task and free the worker slot
        raise TaskDeferred(
            trigger=SnowflakeQueryTrigger(query_id=query_id, conn_id=self.conn_id),
            method_name='execute_complete',
            timeout=timedelta(hours=4),   # Maximum defer time
        )

    def execute_complete(self, context: Context, event: dict):
        """Called when the trigger fires — worker is re-acquired here."""
        if event['status'] == 'failed':
            raise AirflowException(f"Snowflake query {event['query_id']} failed")
        self.log.info(f"Query {event['query_id']} completed successfully")
        return event['query_id']
```

**Resource comparison:**

| Scenario | Pattern | Worker slots used |
|----------|---------|------------------|
| 100 Snowflake queries, each 30 min | Standard operator | 100 slots × 30 min |
| 100 Snowflake queries, each 30 min | Deferrable operator | ~0 slots while running (re-acquired on completion) |

---

## Data Lineage Integration (OpenLineage)

Enterprise operators emit lineage events to track data flow across systems:

```python
from openlineage.airflow.extractors.base import OperatorLineage
from openlineage.client.run import Dataset

class LineageAwareOperator(BaseOperator):
    """Operator that reports data lineage to OpenLineage/Marquez."""

    def __init__(self, source_table: str, target_table: str, **kwargs):
        super().__init__(**kwargs)
        self.source_table = source_table
        self.target_table = target_table

    def execute(self, context):
        # Actual work
        rows = transform_and_load(self.source_table, self.target_table, context['ds'])
        self.log.info(f"Loaded {rows} rows from {self.source_table} → {self.target_table}")
        return rows

    def get_openlineage_facets_on_start(self):
        """Called by OpenLineage Airflow integration before execute()."""
        from openlineage.client.run import Dataset
        return OperatorLineage(
            inputs=[Dataset(namespace='postgresql://prod', name=self.source_table)],
            outputs=[Dataset(namespace='snowflake://analytics', name=self.target_table)],
        )

    def get_openlineage_facets_on_complete(self, ti):
        """Called after successful execute() — can include row counts."""
        row_count = ti.xcom_pull(task_ids=ti.task_id) or 0
        from openlineage.client.facet import OutputStatisticsOutputDatasetFacet
        return OperatorLineage(
            outputs=[
                Dataset(
                    namespace='snowflake://analytics',
                    name=self.target_table,
                    facets={'stats': OutputStatisticsOutputDatasetFacet(rowCount=row_count)},
                )
            ]
        )
```

---

## Operator with Connection UI Support

Custom operators using custom connection types need a connection form in the Airflow UI:

```python
# plugins/my_provider_plugin.py
from airflow.plugins_manager import AirflowPlugin
from airflow.hooks.base import BaseHook
from flask import Blueprint

class MySystemHook(BaseHook):
    conn_name_attr = 'my_system_conn_id'
    default_conn_name = 'my_system_default'
    conn_type = 'my_system'
    hook_name = 'My Custom System'

    @staticmethod
    def get_connection_form_widgets():
        """Extra fields shown in the Airflow Connections UI."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from wtforms import StringField
        return {
            'extra__my_system__region': StringField(
                'Region', widget=BS3TextFieldWidget()
            ),
            'extra__my_system__account_id': StringField(
                'Account ID', widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour():
        return {
            'hidden_fields': ['port', 'schema'],
            'relabeling': {'host': 'Endpoint URL', 'login': 'API Key'},
        }

    def get_conn(self):
        conn = self.get_connection(self.my_system_conn_id)
        region = conn.extra_dejson.get('extra__my_system__region', 'us-east-1')
        return MySystemClient(
            endpoint=conn.host,
            api_key=conn.password,
            region=region,
        )


class MySystemPlugin(AirflowPlugin):
    name = 'my_system_plugin'
    hooks = [MySystemHook]
```

---

## Packaging as a Provider Package

For sharing operators across teams/organisations, package them as an Airflow provider:

```
apache-airflow-providers-mycompany/
├── pyproject.toml
├── airflow/
│   └── providers/
│       └── mycompany/
│           ├── __init__.py
│           ├── hooks/
│           │   ├── __init__.py
│           │   └── myapi_hook.py
│           ├── operators/
│           │   ├── __init__.py
│           │   └── data_load_operator.py
│           └── sensors/
│               ├── __init__.py
│               └── file_ready_sensor.py
└── provider.yaml
```

```yaml
# provider.yaml
package-name: apache-airflow-providers-mycompany
name: MyCompany
description: Airflow provider for MyCompany internal systems
versions:
  - 1.0.0
connection-types:
  - hook-class-name: airflow.providers.mycompany.hooks.myapi_hook.MyApiHook
    connection-type: mycompany_api
operators:
  - integration-name: MyCompany Data Platform
    python-modules:
      - airflow.providers.mycompany.operators.data_load_operator.DataLoadOperator
```

```bash
pip install apache-airflow-providers-mycompany
# Now teams can use:
from airflow.providers.mycompany.operators.data_load_operator import DataLoadOperator
```

---

## Interview Tips

> **Tip 1:** Deferrable operators are the answer to "how do you run 500 concurrent Snowflake queries without 500 worker slots?" The triggerer process is a single asyncio event loop that can handle thousands of triggers with minimal resources. This is a clear architectural differentiator for senior engineers.

> **Tip 2:** The Hook + Operator + Plugin architecture is the right answer to "how do you scale custom operator development across teams." Hooks encapsulate connection logic, operators encapsulate task logic, and plugins register them into the Airflow system. Teams share hooks via internal Python packages.

> **Tip 3:** When discussing custom operators in interviews, always mention `template_fields`. It's a subtle but important detail — if you forget to list a field, users can't use Jinja templating in it, which silently breaks expected behaviour. Senior engineers understand this and always document which fields support templating.

## ⚡ Cheat Sheet

**Operator Architecture — Three-Layer Rule**
- **Hook**: connection/auth logic, independently testable, reusable across operators
- **Operator**: orchestration logic, calls hook, defines `template_fields`
- **Plugin**: registers hook + operator into Airflow UI/system

**Deferrable Operator — How It Works**
1. `execute()` submits job → gets async ID → `raise TaskDeferred(trigger=..., method_name='execute_complete')`
2. Worker slot freed; trigger serialized to metadata DB
3. Triggerer process (asyncio loop) polls asynchronously
4. On completion: `yield TriggerEvent(...)` → scheduler re-queues task
5. Worker slot re-acquired; `execute_complete(context, event)` called

**Worker Slot Comparison**
| Pattern | Slots During Wait |
|---|---|
| Standard operator (blocking) | 1 slot held entire duration |
| Sensor poke mode | 1 slot held entire duration |
| Sensor reschedule mode | Released between polls |
| Deferrable operator | ~0 slots (re-acquired on completion) |

**Trigger Serialization Requirement**
```python
def serialize(self) -> tuple:
    return ('mymodule.MyTrigger', {'query_id': self.query_id, 'conn_id': self.conn_id})
    # Must be JSON-serializable — stored in DB between polls
```

**`template_fields` — Critical Detail**
- Every field that should support Jinja must be listed in `template_fields`
- Forgetting a field: `{{ ds }}` in that field renders as a literal string (silent bug)
- Files with `.sql` extension also rendered if listed in `template_ext`

**Provider Package Structure**
```
apache-airflow-providers-mycompany/
├── airflow/providers/mycompany/
│   ├── hooks/myapi_hook.py
│   ├── operators/data_load_operator.py
│   └── sensors/file_ready_sensor.py
└── provider.yaml  # connection-types, operators, sensors declared here
```

**OpenLineage Integration**
- Implement `get_openlineage_facets_on_start()` → declare inputs/outputs
- Implement `get_openlineage_facets_on_complete(ti)` → add row counts, stats
- Enables automatic data lineage tracking without code changes in consumers
