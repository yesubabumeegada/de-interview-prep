---
title: "Airflow Connections and Hooks - Fundamentals"
topic: airflow
subtopic: connections-and-hooks
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [airflow, connections, hooks, credentials, conn_id, get_connection, secret-management]
---

# Airflow Connections and Hooks — Fundamentals


## 🎯 Analogy

Think of connections like saved passwords in a password manager: you store credentials once in Airflow's secret store, and hooks use the connection ID to look them up — no hardcoded secrets in DAG code.

---
## What Is an Airflow Connection?

An Airflow **Connection** is a stored set of credentials and endpoint information that tells Airflow how to connect to an external system — a database, API, cloud provider, or file system. Instead of hardcoding passwords and hostnames in your DAG code, you store them in Airflow Connections and reference them by a **connection ID (conn_id)**.

> **Analogy:** Think of Connections like saved contacts in your phone. Instead of memorizing and typing a phone number every time you call someone, you store it once under a name ("Mom's Cell") and just say "call Mom's Cell." If the number changes, you update it in one place — not in every message you've ever sent. In Airflow, `conn_id` is the name, and the connection stores the actual credentials.

This separation of **configuration from code** is essential for:
- Security — credentials never appear in source control
- Maintainability — rotate credentials without changing DAG files
- Portability — same DAG code works across dev/staging/prod with different connections

---

## Connection Fields

Every Airflow Connection is structured around a standard set of fields:

| Field | Description | Example |
|-------|-------------|---------|
| `conn_id` | Unique identifier (the "name" you reference) | `snowflake_prod`, `aws_default` |
| `conn_type` | The type of system | `snowflake`, `aws`, `http`, `postgres` |
| `host` | Hostname or endpoint | `account.snowflakecomputing.com` |
| `port` | TCP port | `5432` (Postgres), `443` (HTTPS) |
| `login` | Username or access key ID | `etl_user`, `AKIAIOSFODNN7EXAMPLE` |
| `password` | Password or secret key | `s3cr3t`, `wJalrXUtnFEMI/K7...` |
| `schema` | Database name or schema | `analytics`, `prod_db` |
| `extra` | JSON string for additional settings | `{"role": "TRANSFORMER", "warehouse": "ETL_WH"}` |

```mermaid
flowchart LR
    subgraph DAG["DAG Code"]
        T1["SnowflakeOperator\nconn_id='snowflake_prod'"]
    end
    subgraph CS["Connection Store"]
        C1["Connection: snowflake_prod\nhost: acct.snowflakecomputing.com\nlogin: etl_user\npassword: [encrypted]\nextra: {role: TRANSFORMER}"]
    end
    subgraph SF["Snowflake"]
        DB["Snowflake DB"]
    end
    T1 -->|"looks up conn_id"| C1
    C1 -->|"establishes connection"| DB
```

**What this shows:** DAG code references only the `conn_id` string. The actual credentials are resolved at runtime by Airflow's connection store. Credentials never appear in DAG files.

---

## Creating Connections

### Method 1: Airflow UI

Navigate to **Admin → Connections → Add Connection (+)**. Fill in the fields and save. This stores the connection in the metadata DB (encrypted with Fernet key).

Best for: Development and one-off connections. **Not recommended for production** — credentials in the metadata DB are only as secure as your DB access controls.

### Method 2: Environment Variables

Airflow reads environment variables in the format `AIRFLOW_CONN_{CONN_ID_UPPERCASE}`:

```bash
# PostgreSQL connection
export AIRFLOW_CONN_POSTGRES_DEFAULT='postgresql://etl_user:s3cr3t@db.example.com:5432/analytics'

# Generic URI format: conn_type://login:password@host:port/schema?extra_params

# Snowflake with extra parameters (JSON format also works)
export AIRFLOW_CONN_SNOWFLAKE_PROD='{
  "conn_type": "snowflake",
  "host": "account.snowflakecomputing.com",
  "login": "etl_user",
  "password": "secret_password",
  "schema": "analytics",
  "extra": {
    "account": "myaccount",
    "warehouse": "ETL_WH",
    "role": "TRANSFORMER",
    "database": "PROD_DB"
  }
}'
```

**Why environment variables are better than UI for production:**
- Injected by your secrets management system (AWS Secrets Manager, Vault, Kubernetes secrets)
- Never stored in the Airflow metadata DB
- Automatically available to all Airflow components

### Method 3: Airflow CLI

```bash
airflow connections add 'postgres_default' \
    --conn-type 'postgres' \
    --conn-host 'db.example.com' \
    --conn-port '5432' \
    --conn-login 'etl_user' \
    --conn-password 's3cr3t' \
    --conn-schema 'analytics'
```

### Method 4: Secrets Backends (Recommended for Production)

Configure Airflow to fetch connections from AWS Secrets Manager, HashiCorp Vault, or GCP Secret Manager. This is covered in the Intermediate section.

---

## Standard Connection IDs

Airflow providers have default connection IDs they look for:

| Provider | Default conn_id | Notes |
|----------|----------------|-------|
| AWS | `aws_default` | Used by S3, EMR, Glue, Athena operators |
| GCP | `google_cloud_default` | Used by BigQuery, GCS operators |
| Postgres | `postgres_default` | Used by PostgresOperator |
| Snowflake | `snowflake_default` | Used by SnowflakeOperator |
| HTTP | `http_default` | Used by HttpOperator, HttpSensor |
| SFTP | `sftp_default` | Used by SFTPOperator |

**Best practice:** Don't use default conn_ids in production. Instead, use descriptive names like `postgres_analytics_prod` or `snowflake_reporting_user`. This makes it clear which system and environment the connection targets, and you can have multiple connections to the same system type.

---

## What Is a Hook?

A **Hook** is a Python class that wraps the connection to an external system and provides a clean interface for interacting with it. Hooks handle:
- Looking up the connection credentials by `conn_id`
- Establishing and managing the connection (sessions, pooling)
- Providing system-specific methods (query, upload, download)

> **Analogy:** If a Connection is the stored phone number, a Hook is the act of picking up the phone and dialing. The Hook knows how to use the Connection information to actually establish communication. Operators use Hooks; you can also use Hooks directly in `PythonOperator` callables.

```python
# Hook example: PostgresHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

def run_query(**context):
    # The Hook looks up 'postgres_analytics' connection credentials
    hook = PostgresHook(postgres_conn_id='postgres_analytics')
    
    # Execute a query using the hook's methods
    records = hook.get_records("SELECT id, name FROM customers LIMIT 10")
    
    for record in records:
        print(f"Customer: {record[0]} - {record[1]}")
    
    return len(records)
```

---

## Using get_connection()

You can retrieve a Connection object directly to access its fields:

```python
from airflow.hooks.base import BaseHook

def use_connection_directly(**context):
    # Retrieve the full Connection object
    conn = BaseHook.get_connection('my_api')
    
    # Access individual fields
    base_url = f"https://{conn.host}"
    api_key = conn.password
    timeout = conn.extra_dejson.get('timeout', 30)   # Parse extra JSON
    
    # Use the credentials
    response = requests.get(
        f"{base_url}/api/v1/data",
        headers={'Authorization': f'Bearer {api_key}'},
        timeout=timeout,
    )
    return response.json()
```

**`extra_dejson`:** The `extra` field is stored as a JSON string. `.extra_dejson` automatically parses it to a Python dict — much easier than calling `json.loads(conn.extra)` yourself.

---

## Common Hooks and Their Methods

### PostgresHook

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id='postgres_default')

# Execute a query (no results)
hook.run("INSERT INTO audit_log VALUES (%s, %s)", parameters=('event', 'complete'))

# Fetch all results as list of tuples
records = hook.get_records("SELECT id, name FROM customers WHERE active = true")

# Fetch a single value
count = hook.get_first("SELECT COUNT(*) FROM orders WHERE date = '2024-01-15'")[0]

# Get a pandas DataFrame
df = hook.get_pandas_df("SELECT * FROM fact_sales LIMIT 1000")

# Get a SQLAlchemy connection for advanced use
with hook.get_conn() as conn:
    conn.execute("CREATE TABLE IF NOT EXISTS temp_staging (...)")
```

### S3Hook

```python
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

hook = S3Hook(aws_conn_id='aws_default')

# Check if a key exists
exists = hook.check_for_key(key='data/export.csv', bucket_name='my-bucket')

# Download a file
hook.download_file(key='data/export.csv', bucket_name='my-bucket', local_path='/tmp/')

# Upload a file
hook.load_file('/tmp/result.csv', key='output/result.csv', bucket_name='my-bucket')

# List keys with a prefix
keys = hook.list_keys(bucket_name='my-bucket', prefix='data/dt=2024-01-15/')

# Read file content directly
content = hook.read_key(key='config/settings.json', bucket_name='my-bucket')
```

### HttpHook

```python
from airflow.providers.http.hooks.http import HttpHook

hook = HttpHook(method='GET', http_conn_id='external_api')

# GET request
response = hook.run('/api/v1/status', headers={'Accept': 'application/json'})
data = response.json()

# POST request
hook_post = HttpHook(method='POST', http_conn_id='external_api')
response = hook_post.run(
    '/api/v1/jobs',
    data='{"type": "export"}',
    headers={'Content-Type': 'application/json'},
)
```

---

## Hook vs Operator

Understanding the relationship:

| | Operator | Hook |
|--|----------|------|
| What it is | A task unit (appears in DAG) | A connection wrapper (utility class) |
| Configured in | DAG task definition | Used inside operators or Python callables |
| Encapsulates | Business logic + hook usage | Low-level connection management |
| Used by | Airflow scheduler | Operators and PythonOperator callables |

```python
# Under the hood, SnowflakeOperator uses SnowflakeHook
class SnowflakeOperator(BaseOperator):
    def execute(self, context):
        hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)  # Uses hook
        hook.run(self.sql)

# You can use the hook directly in a PythonOperator callable
def custom_snowflake_logic(**context):
    hook = SnowflakeHook(snowflake_conn_id='snowflake_prod')
    hook.run("MERGE INTO ...")   # Full SQL flexibility
```

---

## Complete Example: End-to-End with Hooks

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import csv
import io

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def extract_to_s3(ds: str, **context):
    """Extract from PostgreSQL, upload to S3."""
    pg_hook = PostgresHook(postgres_conn_id='postgres_sales')
    s3_hook = S3Hook(aws_conn_id='aws_default')

    # Query data
    records = pg_hook.get_records(
        f"SELECT sale_id, amount, region FROM sales WHERE sale_date = '{ds}'"
    )

    # Format as CSV
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['sale_id', 'amount', 'region'])
    writer.writerows(records)

    # Upload to S3
    s3_hook.load_string(
        string_data=output.getvalue(),
        key=f'exports/sales/dt={ds}/sales.csv',
        bucket_name='company-data-lake',
        replace=True,
    )

    context['ti'].xcom_push(key='row_count', value=len(records))
    return f's3://company-data-lake/exports/sales/dt={ds}/sales.csv'


with DAG(
    dag_id='extract_sales_to_s3',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sales', 'extract'],
) as dag:

    PythonOperator(
        task_id='extract_to_s3',
        python_callable=extract_to_s3,
        op_kwargs={'ds': '{{ ds }}'},
    )
```

---


## ▶️ Try It Yourself

```python
from airflow.hooks.base import BaseHook
# In practice, connection is stored in Airflow UI or environment variable
# AIRFLOW_CONN_MY_POSTGRES=postgresql://user:pass@host:5432/db

# Use a hook to get the connection
# conn = BaseHook.get_connection("my_postgres")
# print(conn.host, conn.schema)

# Example: PostgresHook uses the connection automatically
from airflow.providers.postgres.hooks.postgres import PostgresHook
# hook = PostgresHook(postgres_conn_id="my_postgres")
# df = hook.get_pandas_df("SELECT * FROM orders LIMIT 10")
print("Connection ID abstracts credentials from DAG code — no secrets in code!")
```

> **Run it:** Copy the snippet into a REPL or file and run it — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What is the difference between a Connection and a Hook in Airflow?" — "A Connection is the stored credential record — the hostname, username, password, and extra config. A Hook is the Python class that uses those credentials to establish and manage an actual connection to the external system. Hooks provide the interface (methods like get_records, upload, run); Connections provide the credentials. Operators use Hooks internally; you can also use Hooks directly in PythonOperator callables."

> **Tip 2:** "Where should you store Airflow connection credentials in production?" — "Never hardcode them in DAGs or store them only in the metadata DB UI. Best practice is a secrets backend — AWS Secrets Manager, HashiCorp Vault, or GCP Secret Manager. Airflow can be configured to automatically fetch connections from these backends, so credentials never touch the metadata DB and can be rotated without restarting Airflow."

> **Tip 3:** "What is `conn.extra_dejson`?" — "It automatically parses the extra JSON field of a Connection into a Python dict. The extra field is stored as a raw JSON string, so `extra_dejson` is a convenience property that calls json.loads for you. Very useful for provider-specific settings like Snowflake warehouse, GCP project ID, or HTTP auth headers."
