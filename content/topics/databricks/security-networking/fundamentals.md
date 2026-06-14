---
title: "Security & Networking - Fundamentals"
topic: databricks
subtopic: security-networking
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [databricks, security, networking, authentication, rbac, secrets]
---

# Security & Networking — Fundamentals

## 🎯 Analogy

Databricks security is like a secure office building. Authentication (who are you?) is your ID badge. Network controls (VPC, private link) are the locked doors and building perimeter. RBAC (who can do what?) is the keycard access system — some doors open for everyone, others only for executives. Secrets management is the secure vault for sensitive items like API keys.

---

## Authentication Methods

```python
# 1. Personal Access Tokens (PAT) — most common for API/CLI access
# Created in: User Settings → Developer → Access Tokens
# Use in requests:
import requests

headers = {"Authorization": "Bearer dapi12345abcde..."}
response = requests.get(
    "https://workspace.azuredatabricks.net/api/2.0/clusters/list",
    headers=headers
)

# 2. Service Principal — for automated/CI-CD access (preferred for production)
# Never use a human account for automated pipelines
# Configure with OAuth2 client credentials

# 3. Databricks CLI
databricks configure --token
# Enter host: https://your-workspace.azuredatabricks.net
# Enter token: dapi12345...

# 4. Azure AD / Google / Okta SSO — for browser/UI access
# Configured at account level, not user-managed
```

---

## Secrets Management

Never hardcode credentials. Use Databricks Secrets:

```python
# Create a secret scope (done once via CLI or UI)
databricks secrets create-scope --scope prod-secrets

# Add secrets
databricks secrets put --scope prod-secrets --key db-password
databricks secrets put --scope prod-secrets --key api-key

# Read in notebooks — NEVER prints the value
api_key = dbutils.secrets.get(scope="prod-secrets", key="api-key")

# Use in JDBC connection
jdbc_password = dbutils.secrets.get(scope="prod-secrets", key="mysql-password")
df = spark.read.format("jdbc") \
    .option("url", "jdbc:mysql://db.host:3306/mydb") \
    .option("user", "etl_user") \
    .option("password", jdbc_password) \
    .option("dbtable", "orders") \
    .load()

# List secrets (shows keys, not values)
dbutils.secrets.list("prod-secrets")
```

**Secret scopes:**
- **Databricks-backed** — stored in Databricks internal storage
- **Azure Key Vault-backed** — reads from Azure Key Vault directly (preferred for Azure)

---

## Unity Catalog RBAC

```sql
-- Three privilege levels
GRANT SELECT ON TABLE prod.sales.orders TO `analyst-group`;
GRANT MODIFY ON TABLE prod.sales.orders TO `data-engineers`;
GRANT ALL PRIVILEGES ON SCHEMA prod.sales TO `schema-owners`;

-- Revoke access
REVOKE SELECT ON TABLE prod.sales.customers FROM `contractor-group`;

-- Check what access a user has
SHOW GRANTS ON TABLE prod.sales.orders;
SHOW GRANTS ON SCHEMA prod.sales;
```

**Unity Catalog privilege hierarchy:**

```
Account Admin
    ↓
Metastore Admin (manages catalogs)
    ↓
Catalog Owner (GRANT on catalog)
    ↓
Schema Owner (GRANT on schema)
    ↓
Table Owner (GRANT on table)
    ↓
Reader (SELECT only)
```

---

## Cluster Access Control

```python
# Cluster-level permissions (set in cluster UI or via API)
# - Can Attach To: users who can submit jobs to this cluster
# - Can Restart: users who can restart the cluster
# - Can Manage: users who can edit cluster config and terminate

# Best practices:
# - Prod clusters: only service principals can attach
# - Shared clusters: analysts can attach, not manage
# - Engineers: can create their own clusters within policy limits

# Cluster policies limit what users can configure
# (Set in Admin Console → Compute → Cluster Policies)
```

---

## IP Access Lists

Restrict workspace access to specific IP ranges:

```bash
# Add IP allowlist (Admin Console → Settings → Security)
# Or via API:
curl -X POST https://workspace.databricks.net/api/2.0/ip-access-lists \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "label": "corporate-vpn",
    "list_type": "ALLOW",
    "ip_addresses": ["203.0.113.0/24", "198.51.100.10"]
  }'
```

**Effect:** Anyone not on the allowlist gets a 403 when accessing the workspace — even with valid credentials.

---

## Key Concepts

| Concept | Purpose |
|---------|---------|
| **PAT** | Personal Access Token — human user auth for API/CLI |
| **Service Principal** | Non-human identity for automated jobs |
| **Secret scope** | Named container for secrets |
| **Cluster policy** | Template that limits what cluster configs users can set |
| **IP allowlist** | Restrict workspace access by source IP |
| **Unity Catalog RBAC** | Fine-grained table/schema/catalog permissions |

---

## Interview Tips

> **Tip 1:** "How do you handle credentials in Databricks?" — "Use Databricks Secrets — never hardcode. Create a secret scope (Databricks-backed or Key Vault-backed), store credentials there, and read them in notebooks with `dbutils.secrets.get(scope, key)`. The secret value is never logged or displayed, even in notebook output."

> **Tip 2:** "What's the difference between a personal access token and a service principal?" — "PATs are tied to a human user account — if the user leaves, the PAT stops working. Service principals are non-human identities (like an application account) — they have their own credentials, don't expire with personnel changes, and can be scoped to specific permissions. Always use service principals for production pipelines."

> **Tip 3:** "What is a cluster policy and why is it useful?" — "A cluster policy is a template that restricts what cluster configurations users can set — e.g., force auto-termination after 60 minutes, limit max workers to 10, require a specific instance type. It prevents runaway costs from users spinning up oversized clusters and ensures compliance with org standards."
