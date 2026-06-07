---
title: "Airflow Custom Operators - Real-World Scenarios"
topic: airflow
subtopic: custom-operators
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [airflow, custom-operators, production, hooks, deferrable, data-quality]
---

# Airflow Custom Operators — Real-World Scenarios

## Scenario 1: Data Quality Operator Library

A data platform team standardises data quality checks across 50 pipelines. Instead of duplicating validation logic in every DAG, they build a reusable operator library.

```python
# plugins/operators/data_quality.py
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.exceptions import AirflowException, AirflowSkipException
from typing import Optional, List
import json

class DataQualityOperator(BaseOperator):
    """
    Runs a suite of data quality checks against a table.
    Fails the task if any check returns 0 rows or violates a threshold.
    """

    template_fields = ('sql_checks', 'table', 'ds')

    def __init__(
        self,
        table: str,
        conn_id: str,
        sql_checks: List[dict],
        # sql_checks format:
        # [{'name': 'row_count', 'sql': 'SELECT COUNT(*) FROM ...', 'min': 1},
        #  {'name': 'null_check', 'sql': 'SELECT COUNT(*) FROM ... WHERE col IS NULL', 'max': 0}]
        fail_on_empty: bool = True,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.table = table
        self.conn_id = conn_id
        self.sql_checks = sql_checks
        self.fail_on_empty = fail_on_empty
        self.ds = '{{ ds }}'   # Template field — resolved before execute()

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        results = {}
        failures = []

        for check in self.sql_checks:
            name = check['name']
            sql = check['sql']
            result = hook.get_first(sql)
            value = result[0] if result else None
            results[name] = value

            self.log.info(f"Check '{name}': value={value}")

            if value is None:
                if self.fail_on_empty:
                    failures.append(f"Check '{name}' returned NULL")
            elif 'min' in check and value < check['min']:
                failures.append(
                    f"Check '{name}' failed: {value} < min={check['min']}"
                )
            elif 'max' in check and value > check['max']:
                failures.append(
                    f"Check '{name}' failed: {value} > max={check['max']}"
                )

        if failures:
            self.log.error(f"Data quality failures: {failures}")
            raise AirflowException(
                f"{len(failures)} quality check(s) failed for {self.table}:\n"
                + "\n".join(failures)
            )

        self.log.info(f"All {len(self.sql_checks)} quality checks passed for {self.table}")
        return results
```

```python
# Usage in any DAG — consistent quality checks across all pipelines
from plugins.operators.data_quality import DataQualityOperator

quality_check = DataQualityOperator(
    task_id='check_orders_quality',
    table='warehouse.orders',
    conn_id='postgres_prod',
    sql_checks=[
        {
            'name': 'row_count',
            'sql': "SELECT COUNT(*) FROM warehouse.orders WHERE order_date = '{{ ds }}'",
            'min': 100,    # Must have at least 100 rows
        },
        {
            'name': 'null_customer_ids',
            'sql': "SELECT COUNT(*) FROM warehouse.orders WHERE customer_id IS NULL AND order_date = '{{ ds }}'",
            'max': 0,      # No nulls allowed
        },
        {
            'name': 'negative_amounts',
            'sql': "SELECT COUNT(*) FROM warehouse.orders WHERE amount < 0 AND order_date = '{{ ds }}'",
            'max': 0,
        },
    ],
)
```

---

## Scenario 2: Multi-Cloud Data Transfer Operator

A platform engineering team builds a unified operator that handles S3→Snowflake, GCS→BigQuery, and ADLS→Synapse — same interface regardless of cloud.

```python
# plugins/operators/cloud_transfer.py
from airflow.models import BaseOperator
from enum import Enum

class CloudProvider(str, Enum):
    AWS = 'aws'
    GCP = 'gcp'
    AZURE = 'azure'

class CloudDataTransferOperator(BaseOperator):
    """
    Transfers data from a cloud storage bucket to a data warehouse.
    Abstracts away provider-specific details behind a unified interface.
    """

    template_fields = ('source_path', 'target_table')

    def __init__(
        self,
        source_provider: CloudProvider,
        source_path: str,           # e.g. "s3://bucket/prefix/{{ ds }}/", "gs://bucket/..."
        target_provider: CloudProvider,
        target_table: str,          # e.g. "ANALYTICS.WAREHOUSE.ORDERS"
        source_conn_id: str,
        target_conn_id: str,
        file_format: str = 'parquet',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.source_provider = source_provider
        self.source_path = source_path
        self.target_provider = target_provider
        self.target_table = target_table
        self.source_conn_id = source_conn_id
        self.target_conn_id = target_conn_id
        self.file_format = file_format

    def execute(self, context):
        # Route to the correct transfer implementation
        if self.source_provider == CloudProvider.AWS and self.target_provider == CloudProvider.AWS:
            return self._s3_to_redshift(context)
        elif self.source_provider == CloudProvider.AWS and self.target_table.startswith('SNOWFLAKE'):
            return self._s3_to_snowflake(context)
        elif self.source_provider == CloudProvider.GCP:
            return self._gcs_to_bigquery(context)
        else:
            raise ValueError(f"Unsupported transfer: {self.source_provider} → {self.target_provider}")

    def _s3_to_snowflake(self, context):
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
        hook = SnowflakeHook(snowflake_conn_id=self.target_conn_id)
        sql = f"""
            COPY INTO {self.target_table}
            FROM '{self.source_path}'
            STORAGE_INTEGRATION = s3_integration
            FILE_FORMAT = (TYPE='{self.file_format.upper()}')
            PURGE = FALSE
            ON_ERROR = 'ABORT_STATEMENT'
        """
        hook.run(sql)
        result = hook.get_first(f"SELECT COUNT(*) FROM {self.target_table} WHERE _loaded_at = CURRENT_DATE()")
        self.log.info(f"Loaded {result[0]} rows into {self.target_table}")
        return result[0]

    def _gcs_to_bigquery(self, context):
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        hook = BigQueryHook(gcp_conn_id=self.target_conn_id)
        job_config = {
            'load': {
                'destinationTable': {'projectId': 'my-project', 'datasetId': 'warehouse',
                                     'tableId': self.target_table},
                'sourceUris': [self.source_path],
                'sourceFormat': self.file_format.upper(),
                'writeDisposition': 'WRITE_APPEND',
            }
        }
        job_id = hook.insert_job(configuration=job_config, location='US')
        self.log.info(f"BigQuery load job: {job_id}")
        return job_id
```

---

## Scenario 3: Notification Operator with Fallback Chain

A reliability team builds an alert operator that tries Slack → PagerDuty → Email in sequence:

```python
class MultiChannelNotifyOperator(BaseOperator):
    """
    Sends a notification via multiple channels with automatic fallback.
    Uses the first channel that succeeds.
    """

    CHANNELS = ['slack', 'pagerduty', 'email']

    def __init__(
        self,
        message: str,
        severity: str = 'info',   # 'info', 'warning', 'critical'
        channels: list = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.message = message
        self.severity = severity
        self.channels = channels or self.CHANNELS

    def execute(self, context):
        dag_id = context['dag'].dag_id
        ds = context['ds']
        full_message = f"[{self.severity.upper()}] {dag_id} ({ds}): {self.message}"

        for channel in self.channels:
            try:
                self.log.info(f"Attempting to send via {channel}")
                if channel == 'slack':
                    self._send_slack(full_message)
                elif channel == 'pagerduty':
                    self._send_pagerduty(full_message, self.severity)
                elif channel == 'email':
                    self._send_email(full_message, context)

                self.log.info(f"Notification sent via {channel}")
                return {'channel': channel, 'message': full_message}

            except Exception as e:
                self.log.warning(f"Failed to send via {channel}: {e}. Trying next channel.")

        raise AirflowException(f"All notification channels failed: {self.channels}")

    def _send_slack(self, message: str):
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
        hook = SlackWebhookHook(slack_webhook_conn_id='slack_alerts')
        hook.send(text=message)

    def _send_pagerduty(self, message: str, severity: str):
        import requests
        from airflow.hooks.base import BaseHook
        conn = BaseHook.get_connection('pagerduty_default')
        severity_map = {'info': 'info', 'warning': 'warning', 'critical': 'critical'}
        requests.post(
            'https://events.pagerduty.com/v2/enqueue',
            json={
                'routing_key': conn.password,
                'event_action': 'trigger',
                'payload': {
                    'summary': message,
                    'severity': severity_map.get(severity, 'error'),
                    'source': 'airflow',
                },
            },
        ).raise_for_status()

    def _send_email(self, message: str, context):
        from airflow.utils.email import send_email
        send_email(
            to=['oncall@company.com'],
            subject=f"Airflow Alert: {context['dag'].dag_id}",
            html_content=f"<pre>{message}</pre>",
        )
```

```python
# Usage — same operator for both info and critical alerts
notify_success = MultiChannelNotifyOperator(
    task_id='notify_done',
    message='Pipeline completed successfully — {{ ti.xcom_pull("load") }} rows loaded',
    severity='info',
    channels=['slack'],   # Only Slack for success
    trigger_rule='all_success',
)

notify_failure = MultiChannelNotifyOperator(
    task_id='notify_failure',
    message='Pipeline failed on task {{ context.exception }}',
    severity='critical',
    channels=['slack', 'pagerduty', 'email'],   # All channels for failures
    trigger_rule='one_failed',
)
```
