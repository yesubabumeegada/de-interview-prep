---
title: "Airflow Connections and Hooks - Senior Deep Dive"
topic: airflow
subtopic: connections-and-hooks
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [airflow, connections, hooks, secrets-backend, credential-rotation, connection-pooling, testing, CI]
---

# Airflow Connections and Hooks — Senior Deep Dive

## Secrets Backend Architecture

Understanding how secrets backends integrate with Airflow's connection resolution is critical for designing secure production deployments.

### The Resolution Chain

```mermaid
flowchart TD
    A["Code: BaseHook.get_connection('snowflake_prod')"] --> B["SecretsBackend.get_conn_uri()"]
    B --> C{Found in\nSecrets Manager?}
    C -->|Yes| D["Deserialize → Connection object"]
    C -->|No| E["Check AIRFLOW_CONN_SNOWFLAKE_PROD env var"]
    E --> F{Found in\nenv?}
    F -->|Yes| G["Parse URI/JSON → Connection object"]
    F -->|No| H["Query metadata DB: SELECT * FROM connection WHERE conn_id='snowflake_prod'"]
    H --> I{Found in DB?}
    I -->|Yes| J["Decrypt (Fernet) → Connection object"]
    I -->|No| K["Raise AirflowNotFoundException"]
    D --> L["Return to calling Hook"]
    G --> L
    J --> L
```

**Why this layered approach matters:**
- Secrets backend is checked first → most secure credentials never touch the metadata DB
- Environment variable fallback → works in dev environments without a secrets backend
- DB fallback → backward compatibility with older Airflow deployments
- No fallback → fail loudly rather than silently use wrong credentials

### Implementing a Custom Secrets Backend

For proprietary secrets managers or internal PKI systems:

```python
# plugins/custom_secrets_backend.py
from __future__ import annotations
from typing import Optional

from airflow.secrets import BaseSecretsBackend
from airflow.models.connection import Connection


class InternalSecretsBackend(BaseSecretsBackend):
    """
    Fetches connections from an internal secrets API.
    Implements the BaseSecretsBackend interface.
    """

    def __init__(
        self,
        api_url: str,
        service_account_token_path: str = '/var/run/secrets/token',
        connections_path: str = 'airflow-connections',
        variables_path: str = 'airflow-variables',
    ):
        super().__init__()
        self.api_url = api_url
        self.token_path = service_account_token_path
        self.connections_path = connections_path
        self.variables_path = variables_path
        self._token_cache: Optional[str] = None

    def _get_auth_token(self) -> str:
        """Read service account token from mounted file."""
        with open(self.token_path) as f:
            return f.read().strip()

    def get_conn_value(self, conn_id: str) -> Optional[str]:
        """
        Return the raw connection string/JSON for a given conn_id.
        Airflow calls this method; it handles deserialization.
        Returns None if not found (triggers fallback to next backend).
        """
        import requests

        token = self._get_auth_token()
        secret_path = f"{self.connections_path}/{conn_id}"

        try:
            response = requests.get(
                f"{self.api_url}/v1/secrets/{secret_path}",
                headers={'Authorization': f'Bearer {token}'},
                timeout=5,
            )

            if response.status_code == 404:
                return None   # Not found — fall through to next backend

            response.raise_for_status()
            data = response.json()

            # Return as JSON string — Airflow will deserialize it
            import json
            return json.dumps(data['secret_value'])

        except requests.Timeout:
            self.log.warning(
                "Timeout fetching secret '%s' from internal backend. "
                "Falling back to environment/DB.", conn_id
            )
            return None
        except Exception as e:
            self.log.error("Error fetching secret '%s': %s", conn_id, e)
            return None

    def get_variable(self, key: str) -> Optional[str]:
        """Fetch an Airflow Variable from the secrets backend."""
        import requests

        token = self._get_auth_token()
        try:
            response = requests.get(
                f"{self.api_url}/v1/secrets/{self.variables_path}/{key}",
                headers={'Authorization': f'Bearer {token}'},
                timeout=5,
            )
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()['secret_value']
        except Exception:
            return None
```

**Registration in airflow.cfg:**
```ini
[secrets]
backend = plugins.custom_secrets_backend.InternalSecretsBackend
backend_kwargs = {
    "api_url": "https://secrets.internal.example.com",
    "connections_path": "prod/airflow/connections",
    "variables_path": "prod/airflow/variables"
}
```

---

## Rotating Credentials Without DAG Restarts

A critical production requirement: when API keys expire or database passwords rotate, you should be able to update credentials without restarting Airflow or modifying DAG files.

### How Secrets Backends Enable Zero-Downtime Rotation

```
Rotation workflow (AWS Secrets Manager example):

1. Security team creates a new database password
2. Update the secret in AWS Secrets Manager:
   aws secretsmanager update-secret \
     --secret-id "airflow/connections/postgres_prod" \
     --secret-string '{"password": "new_password_here", ...}'

3. New DAG runs fetch the updated secret automatically
   — no Airflow restart needed
   — no DAG code changes
   — in-flight tasks using old credentials complete normally

4. Database team disables old password after rotation grace period
```

### Hook-Level Credential Refresh

For hooks that cache connections, ensure the cache is invalidated on credential rotation:

```python
class RotatableApiHook(BaseHook):
    """
    Hook that supports credential rotation without restart.
    Checks token expiry before each request.
    """

    def __init__(self, http_conn_id: str):
        super().__init__()
        self.http_conn_id = http_conn_id
        self._session: Optional[requests.Session] = None
        self._token_fetched_at: Optional[datetime] = None
        self._token_ttl_seconds = 3600   # Refresh token hourly

    def _is_token_expired(self) -> bool:
        if self._token_fetched_at is None:
            return True
        age = (datetime.utcnow() - self._token_fetched_at).total_seconds()
        return age >= self._token_ttl_seconds

    def get_conn(self) -> requests.Session:
        if self._session is None or self._is_token_expired():
            # Re-fetch credentials from secrets backend
            conn = self.get_connection(self.http_conn_id)
            # Note: get_connection() always calls the secrets backend —
            # it does not cache. So this picks up rotated credentials.

            session = requests.Session()
            session.headers['Authorization'] = f'Bearer {conn.password}'
            self._session = session
            self._token_fetched_at = datetime.utcnow()
            self.log.info("Refreshed API credentials for %s", self.http_conn_id)

        return self._session
```

### Database Connection Rotation with Connection Pooling

```python
# For SQLAlchemy-based hooks (PostgresHook etc.)
# The hook creates a new engine per connection string
# To force credential refresh: clear the cached engine

from airflow.providers.postgres.hooks.postgres import PostgresHook

class RotatablePostgresHook(PostgresHook):
    """PostgresHook that drops the connection pool on credential rotation."""

    def get_conn(self):
        """Override to detect credential changes and rebuild pool."""
        conn = self.get_connection(self.postgres_conn_id)
        current_password = conn.password

        if hasattr(self, '_last_password') and self._last_password != current_password:
            # Password changed — dispose old pool
            if hasattr(self, '_engine'):
                self._engine.dispose()
                del self._engine
            self.log.info("Detected credential rotation — rebuilt connection pool")

        self._last_password = current_password
        return super().get_conn()
```

---

## Hook Connection Pooling Internals

For high-concurrency DAGs, understanding how hooks manage connections prevents both resource exhaustion and under-utilization.

### SQLAlchemy Pool (Used by DB Hooks)

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import create_engine, pool

# PostgresHook.get_sqlalchemy_engine() builds an engine with a pool
def inspect_pool_config():
    hook = PostgresHook(postgres_conn_id='postgres_analytics')
    engine = hook.get_sqlalchemy_engine()

    pool_obj = engine.pool
    print(f"Pool class: {type(pool_obj).__name__}")
    print(f"Pool size: {pool_obj.size()}")
    print(f"Checked out: {pool_obj.checkedout()}")
    print(f"Overflow: {pool_obj.overflow()}")

# Customize pool settings via extra connection parameters
# In connection extra field:
# {"pool_size": 5, "max_overflow": 10, "pool_recycle": 1800}
```

**Pool types used by SQLAlchemy in Airflow:**
- `QueuePool` (default): maintains `pool_size` permanent connections plus `max_overflow` overflow connections
- `NullPool`: no pooling — creates a new connection for every request; safer for distributed workers that don't share memory (Celery, KubernetesExecutor)
- `StaticPool`: single connection shared (SQLite/testing only)

For **KubernetesExecutor and CeleryExecutor**, each task runs in an isolated process or pod. Connection pools are **per-process** — they don't share connections across tasks. This means `pool_size=5` means up to 5 connections per worker process, which can be many more connections total.

### Monitoring Connection Usage

```python
def audit_db_connections():
    """Check active connections to prevent DB exhaustion."""
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    hook = PostgresHook(postgres_conn_id='postgres_analytics')
    active = hook.get_records("""
        SELECT client_addr, usename, application_name, state, wait_event_type, wait_event
        FROM pg_stat_activity
        WHERE datname = 'analytics'
          AND state != 'idle'
        ORDER BY query_start
    """)

    print(f"Active DB connections: {len(active)}")
    for conn in active:
        print(f"  {conn[0]} - {conn[1]} - {conn[3]} - waiting: {conn[4]}/{conn[5]}")
```

---

## Testing Hooks in CI

Hooks that make real network calls need to be tested with mocks. Here's a comprehensive testing pattern:

```python
# tests/test_rest_api_hook.py
import pytest
import json
from unittest.mock import patch, MagicMock, PropertyMock
from airflow.models import Connection
from hooks.rest_api_hook import RestApiHook


@pytest.fixture
def mock_connection():
    """Create a test Connection object without touching the DB."""
    return Connection(
        conn_id='test_api',
        conn_type='http',
        host='api.test.example.com',
        schema='https',
        password='test_token',
        extra=json.dumps({'timeout': 10, 'max_retries': 1}),
    )


class TestRestApiHook:

    @patch('hooks.rest_api_hook.RestApiHook.get_connection')
    def test_get_conn_sets_auth_header(self, mock_get_conn, mock_connection):
        """Test that get_conn() correctly reads credentials and sets headers."""
        mock_get_conn.return_value = mock_connection

        hook = RestApiHook(http_conn_id='test_api')
        session = hook.get_conn()

        assert 'Authorization' in session.headers
        assert session.headers['Authorization'] == 'Bearer test_token'
        assert hook.base_url == 'https://api.test.example.com'

    @patch('hooks.rest_api_hook.RestApiHook.get_connection')
    @patch('hooks.rest_api_hook.requests.Session.get')
    def test_get_returns_parsed_json(self, mock_get, mock_get_conn, mock_connection):
        """Test that get() makes the right request and returns parsed JSON."""
        mock_get_conn.return_value = mock_connection

        mock_response = MagicMock()
        mock_response.json.return_value = {'status': 'ok', 'count': 42}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        hook = RestApiHook(http_conn_id='test_api')
        result = hook.get('/api/v1/status')

        assert result == {'status': 'ok', 'count': 42}
        mock_get.assert_called_once()

    @patch('hooks.rest_api_hook.RestApiHook.get_connection')
    @patch('hooks.rest_api_hook.requests.Session.get')
    def test_get_raises_on_http_error(self, mock_get, mock_get_conn, mock_connection):
        """Test that HTTP errors are surfaced rather than swallowed."""
        mock_get_conn.return_value = mock_connection

        import requests
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("403 Forbidden")
        mock_get.return_value = mock_response

        hook = RestApiHook(http_conn_id='test_api')
        with pytest.raises(requests.HTTPError):
            hook.get('/api/v1/protected')

    @patch('hooks.rest_api_hook.RestApiHook.get_connection')
    def test_connection_caching(self, mock_get_conn, mock_connection):
        """Test that get_conn() is called once and session is reused."""
        mock_get_conn.return_value = mock_connection

        hook = RestApiHook(http_conn_id='test_api')
        session_1 = hook.get_conn()
        session_2 = hook.get_conn()

        assert session_1 is session_2   # Same object — cached
        assert mock_get_conn.call_count == 1   # get_connection called only once

    @patch('hooks.rest_api_hook.RestApiHook.get_connection')
    def test_paginate_yields_all_pages(self, mock_get_conn, mock_connection):
        """Test pagination returns all pages and stops correctly."""
        mock_get_conn.return_value = mock_connection

        hook = RestApiHook(http_conn_id='test_api')

        # Mock get() to return two pages then stop
        page_responses = [
            {'results': [{'id': 1}, {'id': 2}], 'next': '/api?page=2'},
            {'results': [{'id': 3}], 'next': None},
        ]

        with patch.object(hook, 'get', side_effect=page_responses):
            all_results = list(hook.paginate('/api/v1/items'))

        assert all_results == [[{'id': 1}, {'id': 2}], [{'id': 3}]]


# Integration test (requires real connection — skip in CI unless env is available)
@pytest.mark.integration
class TestRestApiHookIntegration:

    def test_real_api_health(self):
        """Test against a real API endpoint (requires 'test_api' connection)."""
        hook = RestApiHook(http_conn_id='test_api_staging')
        result = hook.get('/health')
        assert result.get('status') == 'ok'
```

**CI Pipeline Integration:**

```yaml
# .github/workflows/test.yml
- name: Run unit tests (no network)
  run: pytest tests/ -m "not integration" --cov=hooks

- name: Run integration tests (staging only)
  if: github.ref == 'refs/heads/main'
  env:
    AIRFLOW_CONN_TEST_API_STAGING: '{"conn_type": "http", "host": "staging-api.example.com", ...}'
  run: pytest tests/ -m "integration"
```

---

## Anti-Patterns in Connections and Hooks

### Anti-Pattern 1: Hardcoded Credentials in DAG Files

```python
# NEVER DO THIS
def extract(**context):
    conn = psycopg2.connect(
        host='prod-db.internal.example.com',
        user='etl_user',
        password='actual_password_123',   # In source control!
        database='analytics',
    )
```

### Anti-Pattern 2: Creating New Connections on Every Call

```python
# BAD: Creates a new HTTP session on every poke (N sessions per sensor lifetime)
class BadSensor(BaseSensorOperator):
    def poke(self, context):
        session = requests.Session()   # New session every poke!
        session.headers['Authorization'] = 'Bearer token'
        return session.get('https://api.example.com/status').ok

# GOOD: Use a hook that caches the session
class GoodSensor(BaseSensorOperator):
    def poke(self, context):
        hook = RestApiHook(http_conn_id='my_api')   # Hook caches session
        result = hook.get('/status')
        return result.get('ready', False)
```

### Anti-Pattern 3: Storing Secrets in Connection Extra as Plaintext JSON

```python
# This is in the metadata DB as plaintext (only Fernet-encrypted at rest)
# If DB is compromised, all extra fields are readable
extra = json.dumps({
    "private_key": "-----BEGIN RSA PRIVATE KEY-----\n...",   # DON'T store full keys here
    "service_account_json": "{...full service account...}",  # DON'T store full credentials
})

# BETTER: Store only references
extra = json.dumps({
    "key_vault_secret_name": "my-service-account",  # Reference to Vault secret
})
```

---

## Interview Tips

> **Tip 1:** "How would you rotate database credentials without restarting Airflow?" — "Use a secrets backend like AWS Secrets Manager. When you update the secret, Airflow's BaseHook.get_connection() fetches fresh credentials on the next call — it doesn't cache. For hooks that do cache connections (like custom REST hooks), implement TTL-based cache invalidation. For SQLAlchemy hooks, disposing the engine pool forces new connections with the new password."

> **Tip 2:** "Walk me through a complete secrets backend setup." — "Install the provider (e.g., amazon). Configure airflow.cfg with the backend class and prefix. Create secrets in Secrets Manager with the naming convention (e.g., 'airflow/connections/my_conn'). Airflow resolves the connection chain: secrets backend first, then env vars, then metadata DB. Grant the Airflow worker IAM role read access to the specific secret ARNs. Zero credentials in the metadata DB or DAG files."

> **Tip 3:** "How do you test hooks in CI without real external services?" — "Mock at the get_connection() level to inject test Connection objects without a database. Mock at the session/client level to control API responses. Use pytest.mark.integration to separate tests that require real connectivity — run those only in staging environments. The key is that your hook's get_conn() accepts the Connection object from get_connection(), so you can inject any credentials in tests."

## ⚡ Cheat Sheet

**Connection Resolution Chain (Priority Order)**
1. Secrets backend (AWS Secrets Manager, Vault, custom) — checked first
2. Environment variable `AIRFLOW_CONN_<CONN_ID>` (uppercase)
3. Metadata DB (Fernet-encrypted at rest)
4. `AirflowNotFoundException` — fails loudly, never silent fallback

**Secrets Backend Setup (AWS)**
```ini
[secrets]
backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
backend_kwargs = {"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables"}
```
- Secret name format: `airflow/connections/<conn_id>`
- Zero-downtime rotation: update secret in SM → new DAG runs fetch fresh creds automatically

**Pool Types for SQLAlchemy Hooks**
| Pool | When to Use |
|---|---|
| `QueuePool` (default) | Persistent workers sharing memory |
| `NullPool` | KubernetesExecutor / CeleryExecutor (per-process, no sharing) |
| `StaticPool` | SQLite / tests only |

**Hook Testing Pattern**
```python
@patch('hooks.my_hook.MyHook.get_connection')
def test_auth_header(self, mock_get_conn, mock_connection):
    mock_get_conn.return_value = mock_connection  # inject test Connection
    hook = MyHook(http_conn_id='test_api')
    session = hook.get_conn()
    assert session.headers['Authorization'] == 'Bearer test_token'
```
- Mock at `get_connection()` level — no DB needed
- Separate `@pytest.mark.integration` tests for real network calls
- Run integration tests only on `main` branch in staging env

**Credential Rotation Without Restart**
- `BaseHook.get_connection()` never caches — always calls secrets backend
- For caching hooks: implement TTL check (`_token_fetched_at + TTL > now`)
- For SQLAlchemy hooks: detect password change → `engine.dispose()` → rebuild pool

**Anti-Patterns**
- ❌ Hardcoded credentials in DAG files (in source control)
- ❌ Creating new `requests.Session()` in `poke()` (N sessions per sensor lifetime)
- ❌ Storing full private keys in Connection `extra` field (only store references/names)
- ✅ Use hook's `get_conn()` → caches session, reads from secrets backend
