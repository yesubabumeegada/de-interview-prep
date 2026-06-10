---
title: "Airflow Connections and Hooks - Scenario Questions"
topic: airflow
subtopic: connections-and-hooks
content_type: scenario_question
tags: [airflow, connections, hooks, secrets, vault, credential-rotation, custom-hook, security]
---

# Airflow Connections and Hooks — Scenario Questions

<article data-difficulty="junior">

## 🟢 Scenario 1: Connection vs Hardcoded Credentials

A new data engineer on your team wrote their first DAG. It works fine locally. You're reviewing it before deployment to production:

```python
import psycopg2
from airflow.operators.python import PythonOperator

def load_data(**context):
    conn = psycopg2.connect(
        host='analytics-db.prod.company.com',
        user='airflow_etl',
        password='Tr0ub4dor&3',
        database='analytics_db',
        port=5432,
    )
    cursor = conn.cursor()
    cursor.execute("INSERT INTO fact_sales SELECT * FROM staging.sales_raw")
    conn.commit()
    conn.close()
```

What are the problems with this code, and how would you fix it?

<details>
<summary>💡 Hint</summary>
Think about what happens to this code when it's committed to git. What happens when the password needs to rotate? What Airflow mechanism is designed to solve exactly this problem?
</details>

<details>
<summary>✅ Solution</summary>

**Problems:**

1. **Credentials in source code:** The password `Tr0ub4dor&3` is in the DAG file. If this file is committed to git (it will be), the password is now in git history forever — even if removed later. Any developer with repo access sees the production password.

2. **No rotation path:** To change the password, you must edit the DAG file, commit it, and redeploy — a disruptive process. If the password is compromised and must be rotated urgently, this is a slow response.

3. **Hardcoded hostname:** The production DB hostname is in the code. The same code can't be tested in dev without modification.

4. **Not using Airflow Hooks:** Re-implementing a database connection ignores the tested, retry-handling, pool-managing infrastructure Airflow provides.

**Fix:**

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator

def load_data(**context):
    # Hook looks up credentials from the Connection store — no passwords in code
    hook = PostgresHook(postgres_conn_id='postgres_analytics')

    hook.run("""
        INSERT INTO fact_sales
        SELECT * FROM staging.sales_raw
        WHERE load_date = %s
    """, parameters=(context['ds'],))

# In Airflow UI (Admin → Connections) or via environment variable:
# AIRFLOW_CONN_POSTGRES_ANALYTICS=postgresql://airflow_etl:password@analytics-db.prod.company.com:5432/analytics_db
```

**Benefits of the fix:**
- Password never appears in DAG code or git history
- To rotate credentials: update the Connection (UI, env var, or secrets manager) — no DAG changes
- Dev, staging, prod each use the same conn_id pointing to different DBs
- PostgresHook handles connection pooling and error handling
</details>

</article>

---

<article data-difficulty="junior">

## 🟢 Scenario 2: Reading Connection Extra Fields

You're building a task that calls an internal REST API. The API requires both an API key (password) and a custom header `X-Tenant-ID`. You need to store the tenant ID in the Airflow Connection. A colleague suggests putting it in the Connection's `login` field. Is this correct? How should the Connection be configured and the credentials accessed?

<details>
<summary>💡 Hint</summary>
The `extra` field of an Airflow Connection is a JSON string designed for provider-specific settings that don't fit into standard fields. Look at `conn.extra_dejson` for easy access.
</details>

<details>
<summary>✅ Solution</summary>

**The colleague's suggestion is incorrect.** `login` is the username for authentication. Tenant ID is additional configuration, not a username.

**Correct approach: Use the `extra` field**

Configure the Connection (in Airflow UI or environment variable):
```json
{
    "conn_type": "http",
    "host": "api.internal.example.com",
    "schema": "https",
    "password": "api_key_secret_value",
    "extra": "{\"tenant_id\": \"company-prod\", \"api_version\": \"v3\", \"timeout\": 30}"
}
```

Or as an environment variable:
```bash
export AIRFLOW_CONN_INTERNAL_API='{
  "conn_type": "http",
  "host": "api.internal.example.com",
  "schema": "https",
  "password": "api_key_secret_value",
  "extra": {"tenant_id": "company-prod", "api_version": "v3", "timeout": 30}
}'
```

**Reading it in code:**

```python
from airflow.hooks.base import BaseHook
import requests

def call_internal_api(**context):
    conn = BaseHook.get_connection('internal_api')

    # Standard fields
    base_url = f"{conn.schema}://{conn.host}"
    api_key = conn.password

    # Extra fields — extra_dejson parses the JSON string automatically
    extra = conn.extra_dejson
    tenant_id = extra.get('tenant_id')
    api_version = extra.get('api_version', 'v2')
    timeout = extra.get('timeout', 30)

    response = requests.get(
        f"{base_url}/api/{api_version}/data",
        headers={
            'Authorization': f'Bearer {api_key}',
            'X-Tenant-ID': tenant_id,
        },
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()
```

**Key point:** `extra_dejson` is a property that calls `json.loads(self.extra)` for you. Use it instead of manually parsing. The `extra` field is the correct place for provider-specific or custom configuration that doesn't fit the standard fields.
</details>

</article>

---

<article data-difficulty="mid-level">

## 🟡 Scenario 3: Building a Custom Hook for a New System

Your company signs a contract with a new data vendor. They provide a REST API with the following characteristics:
- Authentication: API key in `X-API-Key` header
- Rate limiting: 60 requests/minute; returns 429 with `Retry-After` header
- Pagination: returns `{"data": [...], "cursor": "abc123"}` — pass cursor as `?cursor=abc123` for the next page; no cursor means last page
- Endpoint: `GET /v1/events?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD`

Design and implement a reusable Hook for this vendor.

<details>
<summary>💡 Hint</summary>
Subclass BaseHook. Handle rate limiting by checking the 429 response and using the Retry-After header. Implement cursor-based pagination as a generator. Store the API key in the Connection password field.
</details>

<details>
<summary>✅ Solution</summary>

```python
# hooks/vendor_events_hook.py
import time
import logging
from typing import Iterator, Optional
from airflow.hooks.base import BaseHook
import requests

logger = logging.getLogger(__name__)


class VendorEventsHook(BaseHook):
    """
    Hook for the Vendor Events API.

    Connection (conn_type='http'):
        host: api.vendor.com
        schema: https
        password: <your_api_key>
    """

    conn_name_attr = 'vendor_conn_id'
    default_conn_name = 'vendor_events_default'
    conn_type = 'http'
    hook_name = 'Vendor Events API'

    def __init__(self, vendor_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.vendor_conn_id = vendor_conn_id
        self._session: Optional[requests.Session] = None
        self._base_url: Optional[str] = None

    def get_conn(self) -> requests.Session:
        if self._session is not None:
            return self._session

        conn = self.get_connection(self.vendor_conn_id)
        protocol = conn.schema or 'https'
        self._base_url = f"{protocol}://{conn.host}"

        session = requests.Session()
        session.headers['X-API-Key'] = conn.password
        session.headers['Accept'] = 'application/json'

        self._session = session
        return session

    def _get_with_rate_limit(self, url: str, params: dict) -> dict:
        """
        Make a GET request, handling 429 rate limit responses automatically.
        Respects the Retry-After header.
        """
        session = self.get_conn()

        while True:
            response = session.get(url, params=params, timeout=30)

            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                logger.warning(
                    "Rate limited. Waiting %d seconds before retry.", retry_after
                )
                time.sleep(retry_after)
                continue   # Retry the request

            response.raise_for_status()
            return response.json()

    def get_events(
        self,
        start_date: str,
        end_date: str,
    ) -> Iterator[dict]:
        """
        Yield all events for the date range.
        Handles cursor-based pagination automatically.

        :param start_date: Start date in YYYY-MM-DD format
        :param end_date: End date in YYYY-MM-DD format
        """
        url = f"{self._base_url}/v1/events"
        params = {'start_date': start_date, 'end_date': end_date}
        cursor = None
        page_count = 0

        while True:
            if cursor:
                params['cursor'] = cursor

            data = self._get_with_rate_limit(url, params)
            events = data.get('data', [])
            cursor = data.get('cursor')   # None if last page
            page_count += 1

            logger.info(
                "Fetched page %d: %d events (cursor: %s)",
                page_count, len(events), cursor or 'END'
            )

            for event in events:
                yield event

            if not cursor:
                break   # No cursor = last page

    def test_connection(self):
        try:
            session = self.get_conn()
            response = session.get(f"{self._base_url}/v1/health", timeout=10)
            if response.status_code == 200:
                return True, "Vendor API connection successful"
            return False, f"Unexpected status: {response.status_code}"
        except Exception as e:
            return False, f"Connection failed: {e}"
```

**Usage in a DAG:**
```python
def fetch_vendor_events(start_date: str, end_date: str, **context):
    hook = VendorEventsHook(vendor_conn_id='vendor_events_prod')
    events = list(hook.get_events(start_date=start_date, end_date=end_date))
    logger.info("Fetched %d events from %s to %s", len(events), start_date, end_date)

    output_path = f"s3://data-lake/vendor_events/dt={start_date}/events.json"
    write_to_s3(events, output_path)
    return output_path
```
</details>

</article>

---

<article data-difficulty="mid-level">

## 🟡 Scenario 4: Connection Not Found After Deploying to Kubernetes

A DAG works perfectly in local development (Docker Compose with Airflow metadata DB), but when deployed to the production Kubernetes cluster, all tasks fail with `AirflowNotFoundException: The conn_id 'snowflake_prod' isn't defined`. The connection definitely exists in the Airflow UI. What are the likely causes and how do you debug them?

<details>
<summary>💡 Hint</summary>
In Kubernetes, Airflow components (scheduler, workers, webserver) may be configured differently. Think about where the connection is stored and whether all components have access to the same store. Also consider if a secrets backend is configured on some components but not others.
</details>

<details>
<summary>✅ Solution</summary>

**Possible Causes (most to least likely):**

**Cause 1: Secrets backend configured on webserver but not workers**

The UI shows connections from the secrets backend (e.g., Vault). But worker pods may have different environment variables or ConfigMaps that don't include the secrets backend config:

```bash
# Check: What secrets backend config do the worker pods have?
kubectl exec -n airflow <worker-pod-name> -- env | grep -i airflow__secrets

# Should match:
# AIRFLOW__SECRETS__BACKEND=airflow.providers.hashicorp.secrets.vault.VaultBackend
# AIRFLOW__SECRETS__BACKEND_KWARGS={"url": "...", "connections_path": "..."}
```

**Fix:** Ensure the secrets backend environment variables are in the worker pod spec (Helm values, not just the webserver deployment):

```yaml
# values.yaml for Airflow Helm chart
workers:
  env:
    - name: AIRFLOW__SECRETS__BACKEND
      value: "airflow.providers.hashicorp.secrets.vault.VaultBackend"
    - name: AIRFLOW__SECRETS__BACKEND_KWARGS
      valueFrom:
        secretKeyRef:
          name: airflow-vault-config
          key: backend-kwargs
```

**Cause 2: Connection stored in metadata DB, but workers point to a different DB**

Workers use `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` to connect to the metadata DB. If this is misconfigured in the worker deployment, they see a different (empty) DB:

```bash
kubectl exec -n airflow <worker-pod> -- airflow connections list
# If empty, the worker is not reading from the right metadata DB
```

**Cause 3: Fernet key mismatch**

Connections in the metadata DB are encrypted with a Fernet key. If worker pods have a different `AIRFLOW__CORE__FERNET_KEY` than the one used to encrypt the connection, decryption fails:

```bash
# Check Fernet key consistency
kubectl get secret airflow-fernet-key -o jsonpath='{.data.fernet-key}' | base64 -d
# Should match across all components
```

**Cause 4: Wrong connection ID (typo)**

The UI shows connections — but the DAG might reference a slightly different `conn_id`:

```python
# Check the exact conn_id in the DAG
SnowflakeOperator(snowflake_conn_id='snowflake_prod')   # Matches exactly?
# vs what's in the UI: "snowflake-prod" (dash vs underscore)?
```

**Debugging steps:**
```bash
# 1. List connections visible to the workers
kubectl exec -n airflow <worker-pod> -- airflow connections list

# 2. Get a specific connection
kubectl exec -n airflow <worker-pod> -- airflow connections get snowflake_prod

# 3. Check secrets backend resolution
kubectl exec -n airflow <worker-pod> -- python -c "
from airflow.hooks.base import BaseHook
try:
    conn = BaseHook.get_connection('snowflake_prod')
    print('Found:', conn.conn_type, conn.host)
except Exception as e:
    print('Error:', e)
"
```
</details>

</article>

---

<article data-difficulty="senior">

## 🔴 Scenario 5: Designing a Zero-Trust Secrets Architecture for a Multi-Team Airflow Platform

Your company is building a shared Airflow platform for 20 teams with 300 total DAGs. Security audit requires: (1) no credentials in git or metadata DB, (2) each team can only see their own connections, (3) credential rotation must happen without any DAG deployments, (4) every credential access must be auditable. Design the complete architecture.

<details>
<summary>💡 Hint</summary>
Think about Vault namespaces or path-based isolation, Kubernetes service account identity per team, audit logging in Vault, and how Airflow's secrets backend can be configured with team-scoped prefixes. Also consider the deployment model: per-team Airflow, or one Airflow with connection scoping.
</details>

<details>
<summary>✅ Solution</summary>

**Architecture Overview**

```
Option A: One shared Airflow, team-scoped Vault paths + RBAC
Option B: Per-team Airflow namespace (stronger isolation, higher ops cost)
```

For 20 teams, Option A is typically right. Here's the full design:

**1. Vault Namespace Structure**

```
vault/
├── secret/                              # KV v2 engine
│   ├── platform/                        # Platform team connections
│   │   └── airflow/connections/
│   │       ├── metadata_db              # Shared infra connections
│   │       └── internal_monitoring
│   ├── team-sales/
│   │   └── airflow/connections/
│   │       ├── snowflake_sales_prod
│   │       ├── salesforce_api
│   │       └── s3_sales_bucket
│   ├── team-ml/
│   │   └── airflow/connections/
│   │       ├── mlflow_tracking
│   │       └── sagemaker_prod
│   └── team-finance/
│       └── airflow/connections/
│           ├── netsuite_api
│           └── snowflake_finance_prod
```

**2. Vault Policies (team-scoped)**

```hcl
# policy: team-sales-airflow
path "secret/data/team-sales/airflow/*" {
    capabilities = ["read", "list"]
}
# Cannot read team-ml or team-finance paths

# policy: team-ml-airflow
path "secret/data/team-ml/airflow/*" {
    capabilities = ["read", "list"]
}
```

**3. Kubernetes Service Accounts (one per team)**

```yaml
# K8s: create SA per team
apiVersion: v1
kind: ServiceAccount
metadata:
  name: airflow-team-sales
  namespace: airflow

---
# Vault Kubernetes auth role per team
# vault write auth/kubernetes/role/team-sales
#   bound_service_account_names=airflow-team-sales
#   bound_service_account_namespaces=airflow
#   policies=team-sales-airflow
#   ttl=1h
```

**4. Per-DAG Vault Authentication (KubernetesExecutor)**

```yaml
# Helm values: each team's DAGs run with their team's service account
# airflow.cfg or environment per team

# Pod template override in DAG (for KubernetesExecutor)
from kubernetes.client import models as k8s

sales_task = PythonOperator(
    task_id='load_sales',
    python_callable=my_func,
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                service_account_name="airflow-team-sales",   # Team-scoped SA
                containers=[...]
            )
        )
    }
)
```

**5. Custom Secrets Backend with Team Isolation**

```python
class TeamScopedVaultBackend(BaseSecretsBackend):
    """
    Vault backend that scopes connection lookup to the DAG's owning team.
    Team is inferred from the DAG's 'owner' tag or an env variable.
    """

    def __init__(self, vault_url: str, team_prefix_env_var: str = 'AIRFLOW_TEAM'):
        self.vault_url = vault_url
        self.team_prefix_env_var = team_prefix_env_var

    def _get_team(self) -> str:
        team = os.environ.get(self.team_prefix_env_var)
        if not team:
            raise ValueError(f"Environment variable {self.team_prefix_env_var} not set")
        return team

    def get_conn_value(self, conn_id: str) -> Optional[str]:
        team = self._get_team()
        secret_path = f"secret/data/{team}/airflow/connections/{conn_id}"

        # Authenticate with the team's Kubernetes service account token
        token = open('/var/run/secrets/kubernetes.io/serviceaccount/token').read()
        vault_client = self._authenticate_vault(token)

        try:
            secret = vault_client.secrets.kv.v2.read_secret_version(
                path=f"{team}/airflow/connections/{conn_id}",
                mount_point='secret',
            )
            return json.dumps(secret['data']['data'])
        except Exception:
            return None   # Fall through to env/DB

    def _authenticate_vault(self, k8s_token: str):
        import hvac
        client = hvac.Client(url=self.vault_url)
        team = self._get_team()
        client.auth.kubernetes.login(
            role=f"team-{team}",
            jwt=k8s_token,
        )
        return client
```

**6. Audit Logging**

```bash
# Enable Vault audit logging — every secret access is logged
vault audit enable file file_path=/vault/audit/audit.log

# Log format includes: timestamp, operation, path, client_token, request_id
# Forward to SIEM (Splunk, Elastic) for compliance reporting

# Sample audit query for access report:
# "Who accessed snowflake_sales_prod in the last 30 days?"
jq 'select(.request.path == "secret/data/team-sales/airflow/connections/snowflake_sales_prod")' \
    /vault/audit/audit.log
```

**7. Rotation Without Deployments**

```python
# Rotation is entirely in Vault — zero Airflow involvement
# Vault Dynamic Secrets (for databases) or Lambda rotation functions

# For Snowflake: Lambda rotates password in Snowflake AND Vault
# For APIs: security team updates the secret in Vault after vendor sends new key
# Airflow workers fetch new credentials on the next get_connection() call

# Validation: trigger the rotation-verify DAG
airflow dags trigger api_key_rotation_verify \
    --conf '{"conn_id": "snowflake_sales_prod", "team": "team-sales"}'
```

**Summary: What each requirement maps to**

| Requirement | Solution |
|-------------|----------|
| No credentials in git | Vault secrets backend — connections never in DAG files |
| No credentials in metadata DB | Vault backend takes priority; metadata DB is bypass path (disabled) |
| Team isolation | Vault policies scoped to team paths; K8s SA per team |
| Credential rotation without deploys | Vault is the source; Airflow fetches fresh on each run |
| Audit trail | Vault audit log: every read logged with timestamp, path, identity |
</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is a Connection in Airflow and where are connection credentials stored?**
A: A Connection is a named object storing connection parameters (host, login, password, port, schema, extras). By default credentials are stored in Airflow's metadata database, but production setups use secrets backends (AWS Secrets Manager, HashiCorp Vault, GCP Secret Manager) to avoid storing credentials in the database.

**Q: What is a Hook in Airflow and how does it relate to a Connection?**
A: A Hook is a Python class that abstracts the interface to an external system, using a Connection's credentials to establish the session. For example, `S3Hook` wraps boto3 and uses an `aws_default` connection. Hooks are reusable across operators and can be used directly in custom Python callables.

**Q: How do you use a secrets backend in Airflow for connection credentials?**
A: Configure `[secrets] backend` in `airflow.cfg` (e.g., `airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend`). Airflow will look up connection URIs in the secrets backend before falling back to the metadata database. No code changes are needed — operators and hooks resolve connections transparently.

**Q: What is the difference between defining a connection in the UI vs. in an environment variable?**
A: UI-defined connections are stored in the metadata database — convenient for development but a security concern in production. Environment variable connections use the format `AIRFLOW_CONN_<CONN_ID>=<connection-uri>` — useful for containerized deployments and secrets injection without UI access.

**Q: How do you test a custom Hook in isolation without a running Airflow environment?**
A: Mock the connection using `unittest.mock.patch` on `BaseHook.get_connection` to return a fake `Connection` object with test credentials. This lets you test Hook logic, connection parsing, and error handling without a live database or external service.

**Q: What is the `get_hook` pattern and how do Operators use it?**
A: Operators accept a `conn_id` parameter and call `MyHook(conn_id=self.conn_id)` internally to retrieve the hook. The hook then calls `get_connection(conn_id)` to fetch credentials. This pattern decouples credential management from operator logic, enabling connection swapping without code changes.

**Q: How do you handle connection pooling for high-throughput operators hitting the same database?**
A: Use Airflow pools to limit concurrent tasks hitting the same connection endpoint. For database connections, configure the hook's underlying connection pool size. For Postgres/MySQL, use pgBouncer or ProxySQL as a connection pooler between Airflow workers and the database to avoid connection exhaustion.

---

## 💼 Interview Tips

- Always distinguish between Connections (credential storage) and Hooks (connection interface) — conflating them is a red flag that suggests limited Airflow depth.
- In any production context, proactively mention secrets backends. Storing database passwords in the Airflow metadata database is a security anti-pattern that senior interviewers will probe for.
- When describing custom hooks, mention inheriting from `BaseHook` and implementing `get_conn()` — showing you've actually built one is more convincing than describing the pattern abstractly.
- Discuss connection testing strategies — production engineers know that connection misconfiguration is a common failure mode and write integration tests for their hooks.
- Senior interviewers appreciate hearing about connection reuse and pooling: creating a new database connection per task execution can overwhelm downstream systems at scale.
- Show awareness of provider packages (`apache-airflow-providers-*`) — most production hooks come from providers, and knowing how to extend them or override behavior signals Airflow maturity.
