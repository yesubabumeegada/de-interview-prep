---
title: "Airflow XCom - Real World Scenarios"
topic: airflow
subtopic: xcom
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [airflow, xcom, S3-backend, row-counts, DataFrames, anti-pattern, production]
---

# Airflow XCom — Real World Scenarios

## Scenario 1: Passing Row Counts and Metadata Between Tasks

### Context

A daily ETL pipeline extracts data from a PostgreSQL source, validates it, transforms it, and loads it to Snowflake. The team wants end-to-end visibility: how many rows were extracted, how many passed validation, how many were loaded. They also want the notification task to include these metrics in the Slack alert.

### Solution

```python
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


@dag(
    dag_id='daily_sales_etl_with_metrics',
    schedule_interval='0 5 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        'owner': 'data-engineering',
        'retries': 2,
        'retry_delay': timedelta(minutes=10),
        'email_on_failure': True,
        'email': ['de-team@company.com'],
    },
    tags=['sales', 'etl', 'snowflake'],
)
def daily_sales_etl():

    @task
    def extract(ds: str) -> dict:
        """
        Extract daily sales from PostgreSQL.
        Returns metadata dict — actual data written to S3.
        """
        hook = PostgresHook(postgres_conn_id='postgres_sales')

        # Get count without loading all data
        count_result = hook.get_first(
            f"SELECT COUNT(*) FROM sales WHERE sale_date = '{ds}'"
        )
        row_count = count_result[0] if count_result else 0

        if row_count == 0:
            raise ValueError(f"No sales data for {ds} — possible upstream issue")

        # Export to S3 via COPY (for large datasets)
        output_path = f"s3://pipeline-data/raw/sales/dt={ds}/sales_export.csv"
        hook.run(f"""
            SELECT aws_s3.query_export_to_s3(
                'SELECT * FROM sales WHERE sale_date = ''{ds}''',
                aws_commons.create_s3_uri(
                    'pipeline-data',
                    'raw/sales/dt={ds}/sales_export.csv',
                    'us-east-1'
                )
            )
        """)

        logger.info("Extracted %d rows from PostgreSQL → %s", row_count, output_path)

        return {
            'row_count': row_count,
            'output_path': output_path,
            'source': 'postgres_sales',
            'date': ds,
        }

    @task
    def validate(extract_result: dict) -> dict:
        """
        Validate the extracted data quality.
        Returns validation summary with pass/fail counts.
        """
        import pandas as pd
        import boto3

        output_path = extract_result['output_path']
        ds = extract_result['date']

        # Read for validation
        df = pd.read_csv(output_path.replace('s3://', 's3a://'))

        total = len(df)
        issues = []

        # Null checks
        null_ids = df['sale_id'].isna().sum()
        if null_ids > 0:
            issues.append(f"{null_ids} rows with null sale_id")

        # Amount range check
        bad_amounts = ((df['amount'] < 0) | (df['amount'] > 1_000_000)).sum()
        if bad_amounts > 0:
            issues.append(f"{bad_amounts} rows with suspicious amount values")

        # Duplicate check
        dupes = df.duplicated('sale_id').sum()
        if dupes > 0:
            issues.append(f"{dupes} duplicate sale_ids")

        invalid_count = null_ids + bad_amounts + dupes
        valid_count = total - invalid_count
        validity_pct = round(valid_count / total * 100, 2) if total > 0 else 0

        if validity_pct < 95.0:
            raise ValueError(
                f"Data quality below threshold: {validity_pct}% valid "
                f"(threshold: 95%). Issues: {'; '.join(issues)}"
            )

        # Write valid rows only
        validated_path = output_path.replace('/raw/', '/validated/')
        df[df['sale_id'].notna()].to_parquet(validated_path, index=False)

        logger.info(
            "Validation: %d/%d rows valid (%.1f%%). Validated data at: %s",
            valid_count, total, validity_pct, validated_path
        )

        return {
            'total_rows': total,
            'valid_rows': valid_count,
            'invalid_rows': invalid_count,
            'validity_pct': validity_pct,
            'issues': issues,
            'validated_path': validated_path,
            'date': ds,
        }

    @task
    def load(validation_result: dict) -> dict:
        """
        Load validated data to Snowflake.
        Returns load metrics.
        """
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        validated_path = validation_result['validated_path']
        ds = validation_result['date']

        # COPY INTO from S3
        hook.run(f"""
            COPY INTO analytics.fact_sales
            FROM '{validated_path}'
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            ON_ERROR = 'ABORT_STATEMENT'
        """)

        # Get loaded count
        result = hook.get_first(
            f"SELECT COUNT(*) FROM analytics.fact_sales WHERE sale_date = '{ds}'"
        )
        loaded_count = result[0] if result else 0

        logger.info("Loaded %d rows to Snowflake for %s", loaded_count, ds)

        return {
            'loaded_rows': loaded_count,
            'target_table': 'analytics.fact_sales',
            'date': ds,
        }

    @task
    def notify(extract_result: dict, validation_result: dict, load_result: dict):
        """Send pipeline summary to Slack."""
        import json

        message = {
            "text": "Daily Sales ETL Complete ✓",
            "blocks": [
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": (
                            f"*Daily Sales ETL — {extract_result['date']}*\n"
                            f"• Extracted: {extract_result['row_count']:,} rows\n"
                            f"• Validated: {validation_result['valid_rows']:,} rows "
                            f"({validation_result['validity_pct']}% valid)\n"
                            f"• Loaded to Snowflake: {load_result['loaded_rows']:,} rows\n"
                        )
                    }
                }
            ]
        }

        if validation_result['issues']:
            message['blocks'][0]['text']['text'] += (
                f"• Data issues (non-blocking): {', '.join(validation_result['issues'])}\n"
            )

        # Post to Slack
        from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
        # In practice, use a hook here rather than an operator
        print(f"Slack message: {json.dumps(message, indent=2)}")

    # Wire the pipeline
    extracted = extract(ds="{{ ds }}")
    validated = validate(extracted)
    loaded = load(validated)
    notify(extracted, validated, loaded)


dag_instance = daily_sales_etl()
```

**Key decisions:**
- All tasks return small metadata dicts — actual data always moves through S3
- `validate()` accepts the full extract result dict and unpacks what it needs
- `notify()` receives results from ALL upstream tasks, not just the immediate predecessor — TaskFlow makes this clean
- XCom values are JSON-safe dicts with string/int/float values only

---

## Scenario 2: Using the S3 XCom Backend for Large Payloads

### Context

A ML feature engineering pipeline generates feature matrices that can be 50–500 MB. The team initially passed them through XCom (using pickle), which caused the Airflow metadata DB to slow down and the scheduler to miss heartbeats. The fix: switch to S3 XCom backend and ensure all large objects are transparently stored in S3.

### Migration Strategy

```python
# Step 1: Install the S3 backend
# pip install apache-airflow-providers-amazon

# Step 2: Configure airflow.cfg (or environment variables)
# AIRFLOW__CORE__XCOM_BACKEND=airflow.providers.amazon.aws.xcom_backends.s3.S3XComBackend
# AIRFLOW__AWS__XCOM_BUCKET=company-airflow-xcom
# AIRFLOW__AWS__XCOM_KEY_PREFIX=xcom/production/

# Step 3: Update DAGs to use proper return types (no behavioral change needed)
from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd
import numpy as np

@dag(
    dag_id='ml_feature_engineering',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ml', 'features'],
)
def feature_pipeline():

    @task
    def extract_raw_events(ds: str) -> str:
        """
        Extract raw events from warehouse.
        IMPORTANT: Returns S3 path, not the data.
        The S3 XCom backend would handle this transparently if we returned
        a DataFrame, but we explicitly use paths for clarity and control.
        """
        import boto3
        hook = SnowflakeHook(snowflake_conn_id='snowflake_default')

        # Use Snowflake UNLOAD for large datasets
        output_path = f"s3://ml-data/raw_events/dt={ds}/"
        hook.run(f"""
            COPY INTO '{output_path}'
            FROM (
                SELECT user_id, event_type, event_ts, properties
                FROM events.raw
                WHERE event_date = '{ds}'
            )
            FILE_FORMAT = (TYPE = 'PARQUET')
            OVERWRITE = TRUE
            HEADER = TRUE
        """)

        return output_path   # ~60 bytes in XCom

    @task
    def compute_user_features(raw_events_path: str, ds: str) -> str:
        """
        Compute user-level features from raw events.
        Writes 50-200 MB feature matrix to S3.
        Returns path to feature matrix.
        """
        df = pd.read_parquet(raw_events_path)

        features = (
            df.groupby('user_id')
            .agg(
                event_count=('event_type', 'count'),
                unique_event_types=('event_type', 'nunique'),
                last_event_ts=('event_ts', 'max'),
                first_event_ts=('event_ts', 'min'),
            )
            .reset_index()
        )

        # Feature engineering
        features['days_active'] = (
            pd.to_datetime(features['last_event_ts']) -
            pd.to_datetime(features['first_event_ts'])
        ).dt.days + 1

        output_path = f"s3://ml-data/user_features/dt={ds}/features.parquet"
        features.to_parquet(output_path, index=False)

        logger.info(
            "Computed features for %d users, wrote %.1f MB to %s",
            len(features),
            features.memory_usage(deep=True).sum() / 1024 / 1024,
            output_path,
        )
        return output_path

    @task
    def validate_features(feature_path: str) -> dict:
        """Validate feature matrix quality."""
        df = pd.read_parquet(feature_path)

        metrics = {
            'user_count': len(df),
            'feature_count': len(df.columns),
            'null_pct': float(df.isnull().mean().mean()),
            'avg_event_count': float(df['event_count'].mean()),
        }

        if metrics['null_pct'] > 0.05:
            raise ValueError(f"Too many nulls in feature matrix: {metrics['null_pct']:.1%}")

        return metrics   # Small dict — safe for default XCom backend

    @task
    def register_feature_set(feature_path: str, metrics: dict, ds: str):
        """Register the feature set in the ML feature store."""
        # Register in feature store
        hook = PostgresHook(postgres_conn_id='feature_store')
        hook.run("""
            INSERT INTO feature_registry
                (feature_date, feature_path, user_count, null_pct, created_at)
            VALUES (%s, %s, %s, %s, NOW())
        """, parameters=(ds, feature_path, metrics['user_count'], metrics['null_pct']))
        logger.info("Registered feature set for %s in feature store", ds)

    raw_path = extract_raw_events(ds="{{ ds }}")
    feature_path = compute_user_features(raw_path, ds="{{ ds }}")
    metrics = validate_features(feature_path)
    register_feature_set(feature_path, metrics, ds="{{ ds }}")


dag_instance = feature_pipeline()
```

---

## Scenario 3: The Anti-Pattern — Passing DataFrames Through XCom

### Context

A junior engineer joined the team and wrote the following pipeline. The code works fine in development (small datasets), but causes production incidents. Identify the anti-patterns and refactor.

### The Problematic Code

```python
# ANTI-PATTERN — Do not use this in production

from airflow.decorators import dag, task
import pandas as pd

@dag(schedule_interval='@daily', start_date=datetime(2024, 1, 1))
def broken_pipeline():

    @task
    def extract() -> pd.DataFrame:
        # PROBLEM 1: Returning a DataFrame from an @task pushes it to XCom
        # In production this table has 5M rows = ~800 MB DataFrame
        # Airflow will try to pickle 800 MB into the metadata DB BYTEA column
        return pd.read_sql("SELECT * FROM transactions", engine)

    @task
    def transform(df: pd.DataFrame) -> pd.DataFrame:
        # PROBLEM 2: Receiving a DataFrame via XCom
        # Airflow unpickles 800 MB from DB into memory
        # Then the transform creates another 800+ MB DataFrame
        # PROBLEM 3: Returning another DataFrame to XCom
        return df.assign(amount_usd=df['amount'] / df['exchange_rate'])

    @task
    def load(df: pd.DataFrame):
        # PROBLEM 4: Third 800 MB read from XCom
        df.to_sql('fact_transactions', engine, if_exists='append', index=False)

    raw = extract()
    transformed = transform(raw)
    load(transformed)
```

**What happens in production:**
1. `extract()` → tries to write 800 MB to metadata DB → OOM on worker, DB write timeout
2. Scheduler DB is slow → 10-minute delays on all other DAGs
3. If it somehow succeeds: every downstream task reads 800 MB from DB on start
4. `xcom` table grows 2.4 GB per day (3 tasks × 800 MB)

### The Correct Approach

```python
from airflow.decorators import dag, task
import pandas as pd
import boto3

BUCKET = 'pipeline-data'

@dag(schedule_interval='@daily', start_date=datetime(2024, 1, 1))
def fixed_pipeline():

    @task
    def extract(ds: str) -> str:
        """Write data to S3, return path."""
        df = pd.read_sql(
            f"SELECT * FROM transactions WHERE txn_date = '{ds}'",
            engine
        )
        path = f"s3://{BUCKET}/raw/transactions/dt={ds}/data.parquet"
        df.to_parquet(path, index=False)
        logger.info("Extracted %d rows → %s", len(df), path)
        return path   # 60 bytes in XCom, not 800 MB

    @task
    def transform(input_path: str, ds: str) -> str:
        """Read from S3, transform, write to S3, return new path."""
        df = pd.read_parquet(input_path)
        transformed = df.assign(amount_usd=df['amount'] / df['exchange_rate'])
        output_path = f"s3://{BUCKET}/transformed/transactions/dt={ds}/data.parquet"
        transformed.to_parquet(output_path, index=False)
        logger.info("Transformed %d rows → %s", len(transformed), output_path)
        return output_path   # 60 bytes in XCom

    @task
    def load(transformed_path: str):
        """Read from S3 and load to DB."""
        df = pd.read_parquet(transformed_path)
        df.to_sql('fact_transactions', engine, if_exists='append', index=False)
        logger.info("Loaded %d rows", len(df))
        return len(df)   # Small int — fine for XCom

    raw_path = extract(ds="{{ ds }}")
    transformed_path = transform(raw_path, ds="{{ ds }}")
    load(transformed_path)
```

**Comparison:**

| | Anti-Pattern | Fixed |
|--|--|--|
| XCom per task | 800 MB | ~60 bytes |
| DB impact | Catastrophic | Negligible |
| S3 reads per run | 0 | 2 |
| Debuggable? | Hard (data in DB blob) | Easy (check S3 path) |
| Scalable? | No | Yes |
| Works if data grows to 5 GB? | No (OOM) | Yes |
