---
title: "Airflow Security - Scenario Questions"
topic: airflow
subtopic: airflow-security
content_type: scenario_question
tags: [airflow, security, fernet, rbac, secrets-backend, multi-tenant, interview, scenarios]
---

# Scenario Questions — Airflow Security

<article data-difficulty="junior">

## 🟢 Junior: Fernet Encryption and Key Loss

**Scenario:** Your manager asks you to explain what the Fernet key does in Airflow and what happens if it's lost. You also notice the current Airflow setup has no `fernet_key` set in `airflow.cfg`. What do you tell them, and what's the risk?

<details>
<summary>✅ Solution</summary>

**What the Fernet key does:**

The Fernet key encrypts connection passwords and variable values before storing them in Airflow's metadata database. Without it, anyone who can read the database (a DBA, a stolen backup file, a SQL injection attack) can see all your connection passwords in plaintext.

```python
# Without Fernet key — connection in DB looks like:
{"conn_type": "postgres", "host": "db.example.com", "password": "supersecret123"}

# With Fernet key — connection in DB looks like:
{"conn_type": "postgres", "host": "db.example.com", "password": "gAAAAABk7Xm..."}
```

**Generating and configuring a Fernet key:**

```bash
# Step 1: Generate a key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Output: ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=

# Step 2: Set it via environment variable (preferred over airflow.cfg)
export AIRFLOW__CORE__FERNET_KEY="ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg="
```

**What happens if the key is lost:**

- All existing encrypted connection passwords and variable values become permanently unreadable
- Airflow throws `AirflowException: Could not deserialize connection password`
- Every connection must be manually re-created from another source (password manager, IaC)
- There is no recovery — Fernet is symmetric encryption, not hashed. Without the key, the ciphertext is useless

**Immediate action for the current setup:**

1. Generate a Fernet key now
2. Set it in all Airflow components (webserver, scheduler, workers) — they all need the same key
3. Store the key in a secure location (AWS Secrets Manager, HashiCorp Vault, or at minimum a password manager)
4. Document the key rotation procedure before rotation is ever needed

</details>

</article>

<article data-difficulty="mid">

## 🟡 Mid: Configure AWS Secrets Manager and Migrate Existing Connections

**Scenario:** Your team wants to migrate away from storing connections in Airflow's metadata DB to AWS Secrets Manager. You have 12 existing connections. Walk through the full migration plan with no downtime.

<details>
<summary>✅ Solution</summary>

**Step 1: Create secrets in AWS Secrets Manager for all existing connections**

```python
import boto3
import json
from airflow.models import Connection
from airflow import settings

# Export existing connections
session = settings.Session()
connections = session.query(Connection).all()

sm_client = boto3.client("secretsmanager", region_name="us-east-1")

for conn in connections:
    secret_name = f"airflow/connections/{conn.conn_id}"
    secret_value = {
        "conn_type": conn.conn_type,
        "host": conn.host or "",
        "login": conn.login or "",
        "password": conn.get_password() or "",  # Decrypts Fernet-encrypted value
        "schema": conn.schema or "",
        "port": conn.port,
        "extra": conn.extra or "{}",
    }
    
    try:
        sm_client.create_secret(
            Name=secret_name,
            SecretString=json.dumps(secret_value),
        )
        print(f"Created: {secret_name}")
    except sm_client.exceptions.ResourceExistsException:
        sm_client.update_secret(
            SecretId=secret_name,
            SecretString=json.dumps(secret_value),
        )
        print(f"Updated: {secret_name}")
```

**Step 2: Configure the Secrets Manager backend (test with fallback first)**

```ini
# airflow.cfg — Enable SM backend; falls back to env vars, then metadata DB
[secrets]
backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
backend_kwargs = {
  "connections_prefix": "airflow/connections",
  "variables_prefix": "airflow/variables",
  "profile_name": null
}
```

The lookup order with this config:
1. AWS Secrets Manager (`airflow/connections/<conn_id>`)
2. Environment variables (`AIRFLOW_CONN_<CONN_ID>`)
3. Metadata DB

**Step 3: Verify all connections resolve correctly before removing from DB**

```bash
# Test each connection resolves via backend
airflow connections get my_postgres
# Should show connection details pulled from Secrets Manager

# Test actual connectivity
airflow tasks test my_dag my_postgres_task 2024-01-01
```

**Step 4: Remove connections from metadata DB (after validation)**

```bash
# Only after confirming SM backend works for all connections
for conn_id in my_postgres my_s3 my_redshift; do
  airflow connections delete "$conn_id"
done
```

**IAM policy for Airflow pods:**

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["secretsmanager:GetSecretValue"],
    "Resource": "arn:aws:secretsmanager:us-east-1:123456789:secret:airflow/*"
  }]
}
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Multi-Tenant Airflow Security for 10 Teams with DAG Isolation and Audit Compliance

**Scenario:** Your organization has 10 data teams (Payments, Marketing, Logistics, etc.) sharing one Airflow instance. Security requirements: (1) each team can only see and trigger their own DAGs, (2) all actions must be audit-logged for SOC2 compliance, (3) the platform team can view all DAGs but cannot trigger them, (4) no team can access another team's connections or variables.

Design the complete security architecture.

<details>
<summary>✅ Solution</summary>

**Architecture Overview:**

```
                    [Airflow Instance]
                           │
              ┌────────────┼────────────┐
              │            │            │
      [Team Roles]  [Connection NS]  [Audit Log]
      per-team RBAC   per-team prefix   SIEM export
```

**1. Role structure — one role per team per permission level**

```bash
# Create roles for each team
for team in payments marketing logistics; do
  airflow roles create "role_${team}_engineer"
  airflow roles create "role_${team}_viewer"
done
airflow roles create "role_platform_viewer"
```

**2. DAG-level access control — enforced in every DAG**

```python
# payments/dag_payments_reconcile.py
TEAM = "payments"

dag = DAG(
    dag_id=f"{TEAM}_reconcile",
    access_control={
        f"role_{TEAM}_engineer": {"can_read", "can_edit", "can_dag_run", "can_delete"},
        "role_platform_viewer": {"can_read"},
        # No other roles listed = no other teams can see this DAG
    },
    default_args={"owner": TEAM},
)
```

Use a DAG factory to enforce this consistently:

```python
def create_team_dag(dag_id: str, team: str, **kwargs) -> DAG:
    """Every team DAG must go through this factory to enforce RBAC."""
    return DAG(
        dag_id=f"{team}_{dag_id}",
        access_control={
            f"role_{team}_engineer": {"can_read", "can_edit", "can_dag_run"},
            "role_platform_viewer": {"can_read"},
        },
        tags=[team],
        **kwargs,
    )
```

**3. Connection and variable namespacing**

```bash
# Naming convention enforced by policy:
# connections: <team>_<name>  e.g., payments_postgres, marketing_bigquery
# variables: <team>/<name>    e.g., payments/batch_size

# In AWS Secrets Manager:
# airflow/connections/payments_postgres
# airflow/connections/marketing_bigquery
# Team IAM policies restrict which secrets each team's CI/CD can write
```

**4. Audit logging for SOC2**

```python
# custom_security_manager.py
from airflow.www.security import AirflowSecurityManager
import logging

audit_logger = logging.getLogger("airflow.audit")

class AuditingSecurityManager(AirflowSecurityManager):
    def log_action(self, action: str, dag_id: str = None, **kwargs):
        from flask_login import current_user
        audit_logger.info(
            "AUDIT",
            extra={
                "user": current_user.username if current_user else "system",
                "action": action,
                "dag_id": dag_id,
                "timestamp": time.time(),
                "ip": request.remote_addr if request else None,
            }
        )
    
    def trigger_dag(self, dag_id: str, run_id: str, **kwargs):
        self.log_action("TRIGGER_DAG", dag_id=dag_id)
        return super().trigger_dag(dag_id, run_id, **kwargs)
    
    def delete_dag(self, dag_id: str):
        self.log_action("DELETE_DAG", dag_id=dag_id)
        return super().delete_dag(dag_id)
```

```python
# webserver_config.py
from myproject.security import AuditingSecurityManager
SECURITY_MANAGER_CLASS = AuditingSecurityManager
```

**5. Ship audit logs to SIEM (CloudWatch / Splunk)**

```python
# logging_config.py
LOGGING_CONFIG = {
    "version": 1,
    "handlers": {
        "cloudwatch": {
            "class": "watchtower.CloudWatchLogHandler",
            "log_group": "/airflow/audit",
            "stream_name": "airflow-audit-{strftime:%Y-%m-%d}",
        }
    },
    "loggers": {
        "airflow.audit": {
            "handlers": ["cloudwatch"],
            "level": "INFO",
            "propagate": False,
        }
    }
}
```

**6. Kubernetes namespace isolation (for KubernetesExecutor)**

```yaml
# Each team's tasks run in their own namespace with dedicated service account
apiVersion: v1
kind: Namespace
metadata:
  name: airflow-team-payments
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: team-isolation
  namespace: airflow-team-payments
spec:
  podSelector: {}
  policyTypes: [Ingress, Egress]
  egress:
    - to:  # Only allow access to payments-specific resources
        - namespaceSelector:
            matchLabels:
              team: payments
```

**SOC2 compliance checklist this architecture satisfies:**
- CC6.1: Logical access restricted to authorized personnel (per-team RBAC)
- CC6.2: New user access provisioned through defined process (role creation via IaC)
- CC6.3: Access removed when no longer needed (LDAP sync removes roles on group removal)
- CC7.2: System activity monitored (audit log → CloudWatch → Splunk alerts)

</details>

</article>
