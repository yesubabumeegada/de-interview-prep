---
title: "Airflow Security - Real World"
topic: airflow
subtopic: airflow-security
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [airflow, security, incidents, xcom, fernet, ldap, production]
---

# Airflow Security — Real World

## Production Incidents

### Incident 1: Credential Leak via XCom

**What happened:** A data engineer wrote a task that fetched an API token and returned it from the Python function. Airflow automatically stored the return value in XCom. The token was then visible in the UI to anyone with Viewer access.

```python
# DANGEROUS — Do NOT do this
def fetch_api_token(**context):
    token = requests.post("https://api.example.com/auth", 
                         json={"client_id": "x", "client_secret": "y"}).json()["access_token"]
    return token  # ← This goes into XCom, visible in UI to all users

# Later task reads the token from XCom
def call_api(**context):
    token = context["ti"].xcom_pull(task_ids="fetch_api_token")  # Now token is in plain XCom
```

**Resolution:**

```python
# SAFE — Store in a Variable with masking, or pass via encrypted channel
def fetch_and_use_api_token(**context):
    """Fetch token and use it within the same task — never store in XCom."""
    token = requests.post("https://api.example.com/auth",
                         json={"client_id": Variable.get("api_client_id"),
                               "client_secret": Variable.get("api_client_secret")}).json()["access_token"]
    
    from airflow.utils.log.secrets_masker import mask_secret
    mask_secret(token)
    
    # Use token immediately — never return it
    result = requests.get("https://api.example.com/data",
                         headers={"Authorization": f"Bearer {token}"})
    return result.json()["record_count"]  # Return only safe metadata
```

**Lesson:** XCom is not a secrets store. Only return non-sensitive metadata from tasks. Never return tokens, passwords, or PII.

---

### Incident 2: OAuth Token Expiry Breaking All DAGs at 2am

**What happened:** Airflow was configured with Google OAuth2. The service account token used by Airflow's scheduler to sync DAGs from GCS expired after 1 hour. At 2am, the token expired mid-run and all DAG runs failed with `401 Unauthorized`.

**Root cause:** A short-lived OAuth token was baked into `AIRFLOW__CORE__REMOTE_LOG_CONN_ID` and hardcoded in a ConfigMap instead of using Workload Identity.

**Resolution:**

```yaml
# Use Workload Identity (GKE) — no token to expire
apiVersion: v1
kind: ServiceAccount
metadata:
  name: airflow-worker
  namespace: airflow
  annotations:
    iam.gke.io/gcp-service-account: airflow-worker@my-project.iam.gserviceaccount.com
```

```python
# In airflow.cfg — use the connection backed by Workload Identity, not a static key
# The GCP connection with no key_path uses ADC (Application Default Credentials)
# which auto-refreshes via the metadata server — no expiry
```

**Lesson:** In GCP/AWS/Azure, use Workload Identity / IAM roles / Managed Identities. Never use static tokens or keys that expire.

---

### Incident 3: Fernet Key Loss Scenario

**What happened:** An engineer rotated the Kubernetes secret containing the Fernet key without following the two-step rotation procedure. The old key was deleted before `airflow db rotate-fernet-key` was run. All 47 connections in the metadata DB became permanently unreadable.

```
airflow.exceptions.AirflowException: 
    Could not deserialize connection password. 
    Fernet key may have changed or the value is corrupt.
```

**Recovery procedure:**

```bash
# Step 1: Identify all affected connections
airflow connections list

# Step 2: Manually re-create each connection from a secure source
# (password manager, team vault, original infrastructure-as-code)
airflow connections add 'my_postgres' \
  --conn-type postgres \
  --conn-host db.example.com \
  --conn-login airflow \
  --conn-password 'recovered_password' \
  --conn-schema warehouse \
  --conn-port 5432

# Step 3: Audit which DAGs were affected and re-test
```

**Prevention:** Store all connection credentials in infrastructure-as-code (Terraform, or an Airflow connections YAML loaded at startup). The metadata DB should be reproducible, not the source of truth.

---

### Incident 4: LDAP Group Sync Misconfiguration Locking Out All Users

**What happened:** An IT team renamed an AD group from `airflow-admins` to `airflow-platform-admins`. With `AUTH_ROLES_SYNC_AT_LOGIN = True`, on the next login, all Admins found their Admin role stripped and replaced with the default `Viewer` role. No one could administer Airflow.

**Recovery via CLI (even without UI access):**

```bash
# CLI still works when UI is locked out
airflow users add-role -u admin@example.com -r Admin

# Or create a new admin user
airflow users create \
  --username recovery_admin \
  --firstname Recovery \
  --lastname Admin \
  --role Admin \
  --email recovery@example.com \
  --password "$(openssl rand -base64 32)"
```

**Prevention:**

```python
# webserver_config.py — map BOTH old and new group names during transition
AUTH_ROLES_MAPPING = {
    "cn=airflow-admins,...": ["Admin"],
    "cn=airflow-platform-admins,...": ["Admin"],  # Add new group before renaming old one
}
```

---

## Security Hardening Checklist for Production Airflow

```
Authentication & Authorization
[ ] Fernet key set and stored in secrets manager (not plaintext config)
[ ] WEBSERVER_SECRET_KEY set to a random 32-byte value
[ ] AUTH_TYPE is not AUTH_DB in production (use LDAP/OAuth2)
[ ] AUTH_USER_REGISTRATION = False (no self-registration)
[ ] AUTH_ROLES_SYNC_AT_LOGIN = True (prevent role accumulation)
[ ] DAG-level access_control set for sensitive DAGs
[ ] Public role has no permissions

Network
[ ] Webserver not exposed to public internet (VPN or bastion required)
[ ] TLS enabled on webserver (redirect HTTP to HTTPS)
[ ] Metadata DB not accessible directly by users
[ ] NetworkPolicies in place for Kubernetes deployments

Secrets Management
[ ] No plaintext passwords in DAG code
[ ] No passwords in XCom
[ ] Secrets backend configured (Vault / AWS SM / GCP SM)
[ ] sensitive_var_conn_names configured to mask all secret patterns
[ ] Connection passwords verified to be encrypted (check raw DB row)

Operational
[ ] Fernet key rotation procedure documented and tested
[ ] Audit log enabled and shipping to SIEM
[ ] Recovery admin account documented for lockout scenarios
[ ] All connections stored in infrastructure-as-code
[ ] Regular access review (quarterly) of user roles
```

---

## Key Takeaways

- XCom is not a secrets store — never return tokens or passwords from Python callables
- Use cloud Workload Identity instead of static tokens to prevent expiry incidents
- Fernet key loss is catastrophic — always follow two-step rotation and back up credentials in IaC
- LDAP group renames can lock out all admins — maintain CLI access procedures
- Security hardening requires both technical controls and operational procedures
