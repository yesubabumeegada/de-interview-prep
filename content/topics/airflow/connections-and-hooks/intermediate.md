---
title: "Airflow Connections and Hooks - Intermediate"
topic: airflow
subtopic: connections-and-hooks
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [airflow, connections, hooks, custom-hook, secrets-manager, vault, connection-testing, pooling]
---

# Airflow Connections and Hooks — Intermediate

## Building a Custom Hook

When Airflow doesn't have a built-in provider for your system, you build a custom Hook by subclassing `BaseHook`. A good custom hook:
1. Accepts a `conn_id` parameter
2. Implements `get_conn()` to establish the connection
3. Provides system-specific methods
4. Caches the connection to avoid reconnecting on every call

```python
# hooks/rest_api_hook.py
from typing import Any, Optional
from airflow.hooks.base import BaseHook
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


class RestApiHook(BaseHook):
    """
    Hook for interacting with a generic REST API.
    Reads base_url, auth token, and timeout from an Airflow Connection.

    Connection configuration:
    - conn_type: http
    - host: api.myservice.com (no https://)
    - schema: https (used as protocol)
    - password: Bearer token or API key
    - extra: {"timeout": 30, "max_retries": 3, "verify_ssl": true}
    """

    conn_name_attr = 'http_conn_id'
    default_conn_name = 'rest_api_default'
    conn_type = 'http'
    hook_name = 'REST API'

    def __init__(self, http_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.http_conn_id = http_conn_id
        self._session: Optional[requests.Session] = None

    def get_conn(self) -> requests.Session:
        """
        Return a requests.Session configured with credentials from the Connection.
        Caches the session — called once per hook lifecycle.
        """
        if self._session is not None:
            return self._session

        conn = self.get_connection(self.http_conn_id)

        # Build base URL from connection fields
        protocol = conn.schema or 'https'
        host = conn.host
        port = f":{conn.port}" if conn.port else ''
        self.base_url = f"{protocol}://{host}{port}"

        # Extra configuration
        extra = conn.extra_dejson
        timeout = extra.get('timeout', 30)
        max_retries = extra.get('max_retries', 3)
        verify_ssl = extra.get('verify_ssl', True)

        # Configure session with retry logic
        session = requests.Session()
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount('https://', adapter)
        session.mount('http://', adapter)

        # Set authentication header
        if conn.password:
            session.headers.update({'Authorization': f'Bearer {conn.password}'})

        session.verify = verify_ssl
        session.timeout = timeout   # Default timeout for all requests via this session

        self._session = session
        return session

    def get(self, endpoint: str, params: dict = None) -> dict:
        """GET request to the API."""
        session = self.get_conn()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = session.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def post(self, endpoint: str, payload: dict) -> dict:
        """POST request to the API."""
        session = self.get_conn()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response = session.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def paginate(self, endpoint: str, params: dict = None, page_size: int = 100):
        """Paginate through all results, yielding pages."""
        params = params or {}
        params['page_size'] = page_size
        page = 1

        while True:
            params['page'] = page
            data = self.get(endpoint, params=params)

            results = data.get('results', data if isinstance(data, list) else [])
            if not results:
                break

            yield results
            page += 1

            if not data.get('next'):   # Standard pagination: no 'next' = done
                break
```

**Using the custom hook in a DAG:**

```python
from airflow.operators.python import PythonOperator
from hooks.rest_api_hook import RestApiHook

def fetch_all_users(**context):
    hook = RestApiHook(http_conn_id='crm_api')
    all_users = []

    for page in hook.paginate('/api/v2/users', params={'active': 'true'}):
        all_users.extend(page)

    context['ti'].xcom_push(key='user_count', value=len(all_users))
    return len(all_users)
```

---

## Secret Backends: AWS Secrets Manager

Instead of storing connections in the Airflow metadata DB, configure a secrets backend so Airflow fetches them from a secure secrets manager.

### AWS Secrets Manager Setup

```bash
# Install the provider
pip install apache-airflow-providers-amazon
```

```ini
# airflow.cfg
[secrets]
backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
backend_kwargs = {
    "connections_prefix": "airflow/connections",
    "variables_prefix": "airflow/variables",
    "profile_name": null
}
```

**Create the secret in AWS:**

```bash
# AWS CLI — create a Snowflake connection secret
aws secretsmanager create-secret \
    --name "airflow/connections/snowflake_prod" \
    --secret-string '{
        "conn_type": "snowflake",
        "host": "myaccount.snowflakecomputing.com",
        "login": "etl_user",
        "password": "super_secret_password",
        "schema": "ANALYTICS",
        "extra": {
            "account": "myaccount",
            "warehouse": "ETL_WH",
            "role": "TRANSFORMER",
            "database": "PROD_DB"
        }
    }'
```

**How Airflow resolves connections with the backend:**

```
1. Code requests conn_id='snowflake_prod'
2. Airflow checks secrets backend first:
   GET secretsmanager: "airflow/connections/snowflake_prod"
   → Found → deserialize → return Connection object
3. If not found in secrets backend:
   → Check environment variable AIRFLOW_CONN_SNOWFLAKE_PROD
4. If not found:
   → Check metadata DB (last resort)
```

The resolution order allows gradual migration and fallback.

---

## Secret Backends: HashiCorp Vault

```bash
pip install apache-airflow-providers-hashicorp
```

```ini
[secrets]
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
backend_kwargs = {
    "connections_path": "airflow/connections",
    "variables_path": "airflow/variables",
    "mount_point": "secret",
    "url": "http://vault.internal:8200",
    "auth_type": "kubernetes",
    "kubernetes_role": "airflow"
}
```

**Storing a connection in Vault:**

```bash
# Vault CLI
vault kv put secret/airflow/connections/postgres_prod \
    conn_type="postgres" \
    host="pg.internal.example.com" \
    port="5432" \
    login="airflow_etl" \
    password="db_password_here" \
    schema="analytics"
```

**IAM/Kubernetes auth for Vault** (preferred in K8s clusters):
- Airflow workers authenticate to Vault using their K8s service account token
- Vault verifies the token with the K8s API server
- No static credentials needed for Vault itself — authentication is identity-based

---

## Connection Testing

Test connections before deploying to production.

### UI Testing

In the Airflow UI, navigate to **Admin → Connections**, open any connection, and click **Test**. This calls the hook's `test_connection()` method (if implemented by the provider).

### Programmatic Testing

```python
# Test a connection in code (useful for CI)
from airflow.hooks.base import BaseHook

def test_connection_works(conn_id: str) -> bool:
    """Test that a connection is valid and reachable."""
    try:
        conn = BaseHook.get_connection(conn_id)
        success, message = conn.test_connection()
        if success:
            print(f"✓ Connection '{conn_id}' is valid: {message}")
            return True
        else:
            print(f"✗ Connection '{conn_id}' failed: {message}")
            return False
    except Exception as e:
        print(f"✗ Connection '{conn_id}' error: {e}")
        return False
```

### Custom `test_connection()` in Your Hook

```python
class RestApiHook(BaseHook):
    # ...
    def test_connection(self):
        """Test the connection by hitting a health endpoint."""
        try:
            response = self.get('/health')
            if response.get('status') == 'ok':
                return True, "Connection successful"
            return False, f"Unexpected health response: {response}"
        except requests.ConnectionError as e:
            return False, f"Cannot reach API: {e}"
        except requests.HTTPError as e:
            return False, f"HTTP error: {e.response.status_code} {e.response.text}"
```

---

## Multiple Connections Per System

In production, you often need multiple connections to the same system type with different privileges:

```
snowflake_admin       — admin user for schema creation, GRANT statements
snowflake_etl         — ETL user for read/write on raw + staging schemas
snowflake_transform   — dbt user for read on raw, write on analytics
snowflake_reporting   — read-only user for BI tools
```

```python
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

# Admin tasks use the admin connection
create_schema = SnowflakeOperator(
    task_id='create_schema',
    sql="CREATE SCHEMA IF NOT EXISTS analytics",
    snowflake_conn_id='snowflake_admin',    # High-privilege connection
    dag=dag,
)

# ETL tasks use limited ETL connection
load_data = SnowflakeOperator(
    task_id='load_data',
    sql="INSERT INTO analytics.fact_sales ...",
    snowflake_conn_id='snowflake_etl',      # Limited to ETL operations
    dag=dag,
)
```

**Benefits:**
- Least-privilege principle — each task uses the minimum necessary permissions
- Auditing — Snowflake query history shows which user ran each query
- Security isolation — a DAG bug with the ETL user can't drop schemas

---

## Connection Pools

For high-concurrency DAGs where many tasks connect to the same system simultaneously, connection pooling prevents overwhelming the target system.

### Airflow's Built-in Pool for DB Connections

Hooks that use SQLAlchemy (PostgresHook, MySqlHook, etc.) use SQLAlchemy connection pools internally:

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

# The hook manages a connection pool per connection string
hook = PostgresHook(postgres_conn_id='postgres_analytics')

# This uses a pooled connection — doesn't reconnect every call
with hook.get_conn() as conn:
    conn.execute("INSERT INTO ...")
```

### Custom Pool for Non-SQLAlchemy Hooks

```python
from queue import Queue
from threading import Lock
import requests

class PooledApiHook(BaseHook):
    """Hook with a session pool for high-concurrency use."""

    _pool: Queue = None
    _pool_lock = Lock()
    POOL_SIZE = 10

    def __init__(self, http_conn_id: str, pool_size: int = POOL_SIZE):
        super().__init__()
        self.http_conn_id = http_conn_id
        self.pool_size = pool_size

    def _get_pool(self) -> Queue:
        with PooledApiHook._pool_lock:
            if PooledApiHook._pool is None:
                conn = self.get_connection(self.http_conn_id)
                PooledApiHook._pool = Queue(maxsize=self.pool_size)
                for _ in range(self.pool_size):
                    session = requests.Session()
                    session.headers['Authorization'] = f'Bearer {conn.password}'
                    PooledApiHook._pool.put(session)
        return PooledApiHook._pool

    def run_request(self, endpoint: str) -> dict:
        pool = self._get_pool()
        session = pool.get(timeout=30)   # Wait up to 30s for a session
        try:
            response = session.get(f"https://{self.get_connection(self.http_conn_id).host}/{endpoint}")
            response.raise_for_status()
            return response.json()
        finally:
            pool.put(session)    # Return session to pool
```

### Airflow Pools (Worker Slot Limiting)

While connection pools manage client-side connections, Airflow's **Pool** feature limits how many tasks can run concurrently for a particular resource:

```python
# In the Airflow UI: Admin → Pools → Create Pool
# Name: "snowflake_pool", Slots: 5

# In DAG: assign tasks to the pool
load_task = SnowflakeOperator(
    task_id='load',
    sql="INSERT INTO ...",
    snowflake_conn_id='snowflake_etl',
    pool='snowflake_pool',    # Max 5 concurrent Snowflake tasks cluster-wide
    pool_slots=1,
    dag=dag,
)
```

---

## Interview Tips

> **Tip 1:** "How do you implement a custom Hook?" — "Subclass BaseHook, define `conn_name_attr` pointing to the conn_id parameter name, implement `get_conn()` to return the established connection using credentials from `self.get_connection(self.conn_id)`, cache the connection object to avoid reconnecting, and provide system-specific methods. Implement `test_connection()` so the UI Test button works."

> **Tip 2:** "What's the priority order when Airflow resolves a connection?" — "First the configured secrets backend (Vault, AWS Secrets Manager). Second, environment variables in the format AIRFLOW_CONN_{CONN_ID_UPPERCASE}. Third, the metadata database. This resolution order lets you override connections at any level without changing code."

> **Tip 3:** "Why would you have multiple connections to the same Snowflake account?" — "Least-privilege security: different tasks need different permissions. An admin connection for DDL operations, an ETL connection for data loads, a read-only connection for reporting queries. Each connection maps to a different Snowflake user/role, ensuring tasks can't accidentally perform actions outside their scope, and queries are auditable by user."
