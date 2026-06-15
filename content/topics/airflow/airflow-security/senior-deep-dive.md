---
title: "Airflow Security - Senior Deep Dive"
topic: airflow
subtopic: airflow-security
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [airflow, security, zero-trust, kubernetes, audit-logging, multi-tenant, fernet-rotation]
---

# Airflow Security — Senior Deep Dive

## Zero-Trust Airflow Architecture

A zero-trust model assumes no component is inherently trusted — every request is verified, every connection is authenticated.

```
Internet
    │
    ▼
[VPN/Bastion Host] ──── only entry point
    │
    ▼
[Ingress + mTLS] ──── all internal traffic encrypted
    │
    ├─ [Webserver]  (private subnet, no public IP)
    ├─ [Scheduler]  (no inbound connections)
    └─ [Workers]    (egress-only to task targets)
         │
         ▼
    [Metadata DB]  (no direct developer access)
```

**Key controls:**
- Webserver behind VPN — only corporate network can reach the UI
- All Airflow components communicate over TLS, no plain HTTP
- Kubernetes NetworkPolicies restrict pod-to-pod traffic
- Workers have no access to the webserver or scheduler network segments

```yaml
# NetworkPolicy — workers can only reach the metadata DB and external task targets
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: airflow-worker-policy
spec:
  podSelector:
    matchLabels:
      component: worker
  policyTypes:
    - Egress
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              name: postgres
      ports:
        - port: 5432
    - to: []  # Allow external (internet) egress for task targets
      ports:
        - port: 443
```

---

## KubernetesExecutor Pod Security Contexts

When using `KubernetesExecutor`, each task runs in its own pod. Harden these pods with security contexts:

```python
# In the DAG — per-task pod override
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

task = KubernetesPodOperator(
    task_id="process_data",
    security_context={
        "runAsNonRoot": True,
        "runAsUser": 50000,          # Non-root UID
        "fsGroup": 50000,
        "seccompProfile": {"type": "RuntimeDefault"},
    },
    container_security_context={
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": True,
        "capabilities": {"drop": ["ALL"]},
    },
    volumes=[{"name": "tmp", "emptyDir": {}}],
    volume_mounts=[{"name": "tmp", "mountPath": "/tmp"}],  # Writable /tmp
)
```

Set these as defaults in `airflow.cfg` for all KubernetesExecutor pods:

```ini
[kubernetes]
worker_pods_pending_timeout = 300
pod_template_file = /opt/airflow/pod_template.yaml
```

---

## Audit Logging

Airflow's FAB provides a built-in audit log for user actions (login, DAG trigger, connection edit):

```python
# Access audit log in the UI: Security → Audit Logs
# Or query directly:
from airflow.www.fab_security_manager import AirflowSecurityManager

# Programmatic access via FAB's log model
from flask_appbuilder.models.sqla import Model
# FAB logs are in the `ab_view_menu` and action log tables
```

For custom audit logging, write a custom log handler that ships audit events to your SIEM:

```python
import logging
from airflow.utils.log.logging_mixin import LoggingMixin

class SIEMAuditHandler(logging.Handler):
    def emit(self, record):
        if "dag_id" in record.__dict__:
            event = {
                "timestamp": record.created,
                "user": getattr(record, "user", "unknown"),
                "dag_id": record.dag_id,
                "action": record.getMessage(),
                "ip": getattr(record, "remote_addr", ""),
            }
            # Ship to Splunk / Datadog / CloudWatch
            self._send_to_siem(event)

# Register in logging config
LOGGING_CONFIG = {
    "handlers": {
        "siem": {
            "class": "myproject.audit.SIEMAuditHandler",
        }
    },
    "loggers": {
        "airflow.task": {"handlers": ["siem"], "propagate": True},
    }
}
```

---

## Fernet Key Rotation Without Downtime

You cannot simply swap the Fernet key — all existing encrypted values become unreadable. The rotation procedure:

**Step 1:** Generate a new key, set both keys (old first, new second — Airflow 2.x supports key rotation with `MultiFernet`):

```python
from cryptography.fernet import MultiFernet, Fernet

old_key = "ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg="
new_key = Fernet.generate_key().decode()

# MultiFernet: encrypts with new key, decrypts with any key in list
multi_key = f"{new_key},{old_key}"
```

```ini
[core]
fernet_key = <new_key>,<old_key>
```

**Step 2:** Re-encrypt all existing values with the new key:

```bash
airflow db rotate-fernet-key
# This re-encrypts all connections and variables using the new primary key
```

**Step 3:** Remove the old key from the config:

```ini
[core]
fernet_key = <new_key>
```

> **Zero downtime:** During step 1-2, both keys work. No service restart needed between steps.

---

## Multi-Tenant Airflow

For organizations with 10+ teams, give each team isolation:

**Option A — Namespace-per-team (KubernetesExecutor):**

```yaml
# Team A's tasks run in namespace "team-payments"
# Team B's tasks run in namespace "team-marketing"
# Each namespace has its own RBAC, network policies, resource quotas
```

```python
# In DAG — specify team namespace
dag = DAG(
    dag_id="payments_reconcile",
    default_args={"executor_config": {
        "KubernetesExecutor": {
            "namespace": "team-payments",
        }
    }},
)
```

**Option B — Separate Airflow instances per team** (stronger isolation, higher ops overhead).

**DAG-level permissions for team isolation:**

```python
# Each DAG specifies which roles can access it
dag = DAG(
    dag_id="payments_reconcile",
    access_control={
        "role_payments_engineer": {"can_read", "can_edit", "can_dag_run"},
        "role_data_platform": {"can_read"},  # Platform team can view but not modify
    },
)
```

---

## Secrets Backend Comparison

| | HashiCorp Vault | AWS Secrets Manager | GCP Secret Manager |
|---|---|---|---|
| **Latency** | ~5-20ms (internal) | ~20-50ms (API call) | ~20-50ms (API call) |
| **Cost** | Self-hosted infra cost | $0.40/secret/month + $0.05/10k API calls | $0.06/version/month |
| **Auto-rotation** | Built-in with dynamic secrets | Built-in with Lambda rotation | Manual or via Cloud Functions |
| **Auth methods** | Kubernetes, AppRole, IAM, LDAP, many more | IAM roles only | Workload Identity / SA keys |
| **Best for** | Multi-cloud, complex rotation needs | AWS-native shops | GCP-native shops |
| **DAG access pattern** | Vault agent sidecar or direct API | Boto3 / IAM role on pod | ADC on pod |

For latency-sensitive pipelines with thousands of tasks/hour, the extra 30-50ms per secret lookup from cloud SM backends adds up — consider caching with a TTL:

```python
import functools
import time

@functools.lru_cache(maxsize=None)
def _cached_secret(secret_name: str, _cache_buster: int) -> str:
    return boto3.client("secretsmanager").get_secret_value(SecretId=secret_name)["SecretString"]

def get_secret(secret_name: str, ttl_seconds: int = 300) -> str:
    cache_buster = int(time.time() / ttl_seconds)
    return _cached_secret(secret_name, cache_buster)
```

---

## Key Takeaways

- Zero-trust means no public webserver, all traffic over VPN/mTLS, NetworkPolicies restricting lateral movement
- KubernetesExecutor tasks should run as non-root with read-only filesystems and dropped Linux capabilities
- Fernet key rotation uses `MultiFernet` with comma-separated keys, followed by `airflow db rotate-fernet-key`
- Multi-tenant Airflow uses DAG-level `access_control` + Kubernetes namespace isolation per team
- Secrets backend choice depends on cloud provider, latency budget, and rotation requirements
