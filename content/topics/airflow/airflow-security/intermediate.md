---
title: "Airflow Security - Intermediate"
topic: airflow
subtopic: airflow-security
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [airflow, security, ldap, oauth2, rbac, vault, secrets-backend]
---

# Airflow Security — Intermediate

## LDAP / Active Directory Integration

Most enterprises manage user identities in Active Directory. Airflow can delegate authentication to LDAP so employees use their corporate credentials.

```python
# webserver_config.py — LDAP auth
from flask_appbuilder.security.manager import AUTH_LDAP

AUTH_TYPE = AUTH_LDAP
AUTH_LDAP_SERVER = "ldap://corp.example.com:389"
AUTH_LDAP_USE_TLS = True
AUTH_LDAP_BIND_USER = "cn=airflow-svc,ou=ServiceAccounts,dc=corp,dc=example,dc=com"
AUTH_LDAP_BIND_PASSWORD = os.environ.get("LDAP_BIND_PASSWORD")

# Where to find users
AUTH_LDAP_SEARCH = "ou=Users,dc=corp,dc=example,dc=com"
AUTH_LDAP_UID_FIELD = "sAMAccountName"  # or "uid" for OpenLDAP

# Map LDAP groups to Airflow roles
AUTH_ROLES_MAPPING = {
    "cn=data-engineers,ou=Groups,dc=corp,dc=example,dc=com": ["Op"],
    "cn=data-viewers,ou=Groups,dc=corp,dc=example,dc=com": ["Viewer"],
    "cn=airflow-admins,ou=Groups,dc=corp,dc=example,dc=com": ["Admin"],
}
AUTH_ROLES_SYNC_AT_LOGIN = True  # Sync roles on every login
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Viewer"
```

> **Common Pitfall:** `AUTH_ROLES_SYNC_AT_LOGIN = True` removes roles a user has if they're removed from the LDAP group. Without it, roles accumulate and are never revoked.

---

## OAuth2 with Google / GitHub / Okta

OAuth2 lets users log in with their existing identity provider. FAB supports it via `flask-oauthlib`.

```python
# webserver_config.py — Google OAuth2
from flask_appbuilder.security.manager import AUTH_OAUTH

AUTH_TYPE = AUTH_OAUTH
OAUTH_PROVIDERS = [
    {
        "name": "google",
        "icon": "fa-google",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.environ.get("GOOGLE_CLIENT_ID"),
            "client_secret": os.environ.get("GOOGLE_CLIENT_SECRET"),
            "api_base_url": "https://www.googleapis.com/oauth2/v2/",
            "client_kwargs": {"scope": "email profile"},
            "access_token_url": "https://accounts.google.com/o/oauth2/token",
            "authorize_url": "https://accounts.google.com/o/oauth2/auth",
            "jwks_uri": "https://www.googleapis.com/oauth2/v3/certs",
        },
    }
]

# Map email domains to roles
AUTH_ROLES_MAPPING = {
    "data_engineers": ["Op"],
    "viewers": ["Viewer"],
}
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Viewer"
```

For Okta, replace the URLs with your Okta domain's discovery endpoints.

---

## Custom Roles and DAG-Level Permissions

Beyond built-in roles, you can create fine-grained roles that limit access to specific DAGs.

```bash
# CLI — create a custom role
airflow roles create team_payments_op

# Grant DAG-specific permissions
airflow roles add-perms -r team_payments_op -a can_read -r "DAG:payments_daily"
airflow roles add-perms -r team_payments_op -a can_edit -r "DAG:payments_daily"
airflow roles add-perms -r team_payments_op -a can_dag_run -r "DAG:payments_daily"

# Assign role to user
airflow users add-role -u john.doe@example.com -r team_payments_op
```

Via Python (for automation):

```python
from airflow.www.security import AirflowSecurityManager

# In a custom security manager subclass
def sync_dag_permissions(self, dag_id: str, access_control: dict):
    """Create per-DAG role and assign permissions."""
    for role_name, perms in access_control.items():
        role = self.find_role(role_name) or self.add_role(role_name)
        for perm in perms:
            self.add_permission_to_role(role, perm, f"DAG:{dag_id}")
```

DAG-level access control in the DAG file:

```python
dag = DAG(
    dag_id="payments_daily",
    access_control={
        "team_payments_op": {"can_read", "can_edit"},
        "finance_viewer": {"can_read"},
    },
    schedule_interval="@daily",
)
```

---

## Kubernetes Secrets for Fernet Key Rotation

In Kubernetes deployments, store the Fernet key in a K8s secret, not in a ConfigMap or environment variable baked into the image.

```yaml
# Create the secret
apiVersion: v1
kind: Secret
metadata:
  name: airflow-fernet-key
  namespace: airflow
type: Opaque
data:
  fernet-key: WmZEZmNURjdfNjBHcnJZMTY3enNpUGQ2N3BFdnMwYUdPdjJvYXNPTTFQZz0=
  # base64-encoded value of: ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=
```

```yaml
# Reference in Airflow pod spec
env:
  - name: AIRFLOW__CORE__FERNET_KEY
    valueFrom:
      secretKeyRef:
        name: airflow-fernet-key
        key: fernet-key
```

---

## Secret Backends — HashiCorp Vault, AWS Secrets Manager, GCP Secret Manager

Instead of storing secrets in Airflow's metadata DB, delegate to an external secrets backend. Airflow looks up connections and variables from the backend at runtime.

### AWS Secrets Manager

```ini
# airflow.cfg
[secrets]
backend = airflow.providers.amazon.aws.secrets.secrets_manager.SecretsManagerBackend
backend_kwargs = {"connections_prefix": "airflow/connections", "variables_prefix": "airflow/variables", "profile_name": null}
```

Store a connection:
```bash
aws secretsmanager create-secret \
  --name "airflow/connections/my_postgres" \
  --secret-string '{"conn_type": "postgres", "host": "db.example.com", "login": "airflow", "password": "secret123", "schema": "warehouse", "port": 5432}'
```

### HashiCorp Vault

```ini
[secrets]
backend = airflow.providers.hashicorp.secrets.vault.VaultBackend
backend_kwargs = {
  "connections_path": "connections",
  "variables_path": "variables",
  "mount_point": "airflow",
  "url": "http://vault.internal:8200",
  "auth_type": "kubernetes"
}
```

### GCP Secret Manager

```ini
[secrets]
backend = airflow.providers.google.cloud.secrets.secret_manager.CloudSecretManagerBackend
backend_kwargs = {"connections_prefix": "airflow-connections", "variables_prefix": "airflow-variables", "project_id": "my-gcp-project"}
```

With a secrets backend configured, `Connection.get_connection_from_secrets("my_postgres")` first checks the backend, then falls back to environment variables, then the metadata DB.

---

## Masking Sensitive Values in Logs

Airflow automatically masks values of connections and variables in task logs, but only if it knows they're sensitive.

```python
from airflow.utils.log.secrets_masker import mask_secret

def my_task(**context):
    api_key = Variable.get("stripe_api_key")
    mask_secret(api_key)  # Ensure this value is masked in any future log output
    
    # Now even if api_key appears in an exception traceback, it's replaced with ***
    response = requests.get(f"https://api.stripe.com/charges", 
                           headers={"Authorization": f"Bearer {api_key}"})
```

Configure additional sensitive variable name patterns:

```ini
# airflow.cfg
[core]
sensitive_var_conn_names = password,secret,passwd,authorization,api_key,token,access_key
```

---

## Key Takeaways

- LDAP integration uses `AUTH_LDAP_*` configs and maps groups to roles; `AUTH_ROLES_SYNC_AT_LOGIN=True` is critical for permission revocation
- OAuth2 delegates auth to Google/GitHub/Okta via FAB's OAuth provider config
- DAG-level permissions let you restrict which roles can see/run specific DAGs using `access_control` in the DAG definition
- Store Fernet key in Kubernetes secrets, not plaintext config files
- Secret backends (Vault, AWS SM, GCP SM) externalize credential storage so secrets never land in the metadata DB
