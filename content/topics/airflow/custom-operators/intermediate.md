---
title: "Airflow Custom Operators - Intermediate"
topic: airflow
subtopic: custom-operators
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [airflow, custom-operators, template-fields, sensors, hooks, testing]
---

# Airflow Custom Operators — Intermediate

## Advanced `template_fields` Usage

Template fields allow Jinja expressions in operator parameters:

```python
class DataQualityOperator(BaseOperator):
    template_fields = ('sql', 'conn_id', 'table')
    template_ext = ('.sql',)   # Files with this extension are also rendered as Jinja

    def __init__(self, sql: str, conn_id: str, table: str, **kwargs):
        super().__init__(**kwargs)
        self.sql = sql
        self.conn_id = conn_id
        self.table = table

    def execute(self, context):
        # By this point, self.sql is already rendered
        # e.g. "SELECT COUNT(*) FROM orders WHERE dt = '2024-03-15'"
        self.log.info(f"Running quality check: {self.sql}")
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        result = hook.get_first(self.sql)
        count = result[0]
        if count == 0:
            raise ValueError(f"Quality check failed: {self.table} has 0 rows for {{{{ ds }}}}")
        return count
```

```python
# Usage with Jinja templates
quality_check = DataQualityOperator(
    task_id='check_orders',
    sql="SELECT COUNT(*) FROM {{ params.schema }}.orders WHERE dt = '{{ ds }}'",
    conn_id='postgres_prod',
    table='orders',
    params={'schema': 'warehouse'},
)

# Or reference a .sql file (rendered as Jinja)
quality_check_v2 = DataQualityOperator(
    task_id='check_orders_v2',
    sql='sql/check_orders.sql',   # file content is Jinja-rendered
    template_searchpath='/opt/airflow/dags/sql/',
    conn_id='postgres_prod',
    table='orders',
)
```

---

## Building a Custom Sensor

Custom sensors extend `BaseSensorOperator` and implement `poke()`:

```python
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

class S3KeyWithMinSizeSensor(BaseSensorOperator):
    """
    Waits until an S3 key exists AND is larger than min_size bytes.
    Useful when upstream writes files incrementally — you need to wait
    until the file is "done", not just present.
    """

    template_fields = ('bucket_name', 's3_key')

    def __init__(
        self,
        bucket_name: str,
        s3_key: str,
        min_size_bytes: int = 1024,
        aws_conn_id: str = 'aws_default',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.s3_key = s3_key
        self.min_size_bytes = min_size_bytes
        self.aws_conn_id = aws_conn_id

    def poke(self, context: dict) -> bool:
        """
        Called every poke_interval seconds until it returns True.
        Return True = condition met, continue. False = keep waiting.
        """
        hook = S3Hook(aws_conn_id=self.aws_conn_id)

        if not hook.check_for_key(self.s3_key, bucket_name=self.bucket_name):
            self.log.info(f"Key s3://{self.bucket_name}/{self.s3_key} does not exist yet")
            return False

        obj = hook.get_key(self.s3_key, bucket_name=self.bucket_name)
        size = obj.content_length
        self.log.info(f"Key exists, size={size} bytes (min={self.min_size_bytes})")

        if size < self.min_size_bytes:
            self.log.info(f"File too small ({size} < {self.min_size_bytes}), waiting...")
            return False

        self.log.info("Condition met — proceeding")
        return True
```

```python
# Usage
wait_for_file = S3KeyWithMinSizeSensor(
    task_id='wait_for_daily_export',
    bucket_name='data-lake',
    s3_key='exports/{{ ds }}/transactions.parquet',
    min_size_bytes=1_000_000,   # At least 1MB
    mode='reschedule',          # Release worker slot between pokes
    poke_interval=60,           # Check every 60 seconds
    timeout=3600,               # Give up after 1 hour
)
```

**`mode='reschedule'` vs `mode='poke'`:**

| Mode | Worker slot | Use when |
|------|------------|---------|
| `poke` | Held between checks | Short waits (< 5 min) |
| `reschedule` | Released between checks | Long waits (> 5 min) — use this by default |

---

## Passing Data Between Custom Operators via XCom

```python
class ExtractMetadataOperator(BaseOperator):
    """Extracts file metadata and pushes to XCom for downstream use."""

    def execute(self, context):
        # Discover files for today
        files = list_files_for_date(context['ds'])
        metadata = {
            'file_count': len(files),
            'file_paths': files,
            'total_size_mb': sum(f.size for f in files) / 1024**2,
        }
        # Pushing to XCom is automatic via return value
        return metadata


class ValidateAndLoadOperator(BaseOperator):
    """Reads XCom from ExtractMetadataOperator before loading."""

    def __init__(self, extract_task_id: str, **kwargs):
        super().__init__(**kwargs)
        self.extract_task_id = extract_task_id

    def execute(self, context):
        ti = context['ti']
        metadata = ti.xcom_pull(task_ids=self.extract_task_id)

        if metadata['file_count'] == 0:
            raise ValueError(f"No files found for {context['ds']}")

        self.log.info(f"Loading {metadata['file_count']} files, "
                      f"{metadata['total_size_mb']:.1f} MB total")

        for path in metadata['file_paths']:
            load_file(path)
```

---

## Error Handling and Custom Exceptions

```python
from airflow.exceptions import AirflowException, AirflowSkipException

class ConditionalLoadOperator(BaseOperator):
    def __init__(self, source_table: str, **kwargs):
        super().__init__(**kwargs)
        self.source_table = source_table

    def execute(self, context):
        row_count = get_row_count(self.source_table, context['ds'])

        if row_count == 0:
            # Skip silently — task turns green with "skipped" state
            raise AirflowSkipException(
                f"No data in {self.source_table} for {context['ds']} — skipping"
            )

        if row_count < 0:
            # Hard fail — task turns red
            raise AirflowException(
                f"Invalid row count {row_count} from {self.source_table}"
            )

        self.log.info(f"Loading {row_count} rows from {self.source_table}")
        do_load(self.source_table, context['ds'])
        return row_count
```

---

## Unit Testing Custom Operators

```python
# tests/test_my_operator.py
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from airflow.models import DagBag
from my_operators.data_quality import DataQualityOperator

def make_context(ds='2024-03-15'):
    return {
        'ds': ds,
        'execution_date': datetime(2024, 3, 15),
        'ti': MagicMock(),
        'dag': MagicMock(),
    }

class TestDataQualityOperator:
    def test_passes_when_count_positive(self):
        op = DataQualityOperator(
            task_id='test',
            sql='SELECT COUNT(*) FROM orders WHERE dt = "{{ ds }}"',
            conn_id='test_conn',
            table='orders',
        )
        with patch('my_operators.data_quality.PostgresHook') as mock_hook_cls:
            mock_hook = MagicMock()
            mock_hook.get_first.return_value = (1500,)
            mock_hook_cls.return_value = mock_hook

            result = op.execute(make_context())

            assert result == 1500
            mock_hook.get_first.assert_called_once()

    def test_raises_when_count_zero(self):
        op = DataQualityOperator(
            task_id='test',
            sql='SELECT COUNT(*) FROM empty_table WHERE dt = "{{ ds }}"',
            conn_id='test_conn',
            table='empty_table',
        )
        with patch('my_operators.data_quality.PostgresHook') as mock_hook_cls:
            mock_hook = MagicMock()
            mock_hook.get_first.return_value = (0,)
            mock_hook_cls.return_value = mock_hook

            with pytest.raises(ValueError, match="0 rows"):
                op.execute(make_context())
```

---

## Interview Tips

> **Tip 1:** When explaining template fields, be concrete: `template_fields = ('sql',)` means the string `"SELECT * FROM orders WHERE dt = '{{ ds }}'"` is evaluated by Jinja just before `execute()` is called, replacing `{{ ds }}` with the actual execution date. Without this, the string is passed verbatim as `{{ ds }}` and your SQL will fail.

> **Tip 2:** Use `AirflowSkipException` when "no data" is a valid non-error state. This makes the task yellow/skipped in the UI instead of red/failed, and doesn't trigger failure callbacks. It's the right pattern for optional steps and conditional branches.

> **Tip 3:** Test the `execute()` method directly with a mock context dictionary — you don't need a live Airflow environment to unit test operators. This makes operator tests fast and runnable in CI without a real Airflow installation.
