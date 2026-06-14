---
title: "Security & Networking - Intermediate"
topic: databricks
subtopic: security-networking
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, security, vpc, private-link, network-isolation, encryption]
---

# Security & Networking — Intermediate Concepts

## VPC Architecture for Databricks

Databricks runs in your cloud account's VPC (Virtual Private Cloud) — data stays in your network:

```
┌──────────────────────────────────────────────────────┐
│                  Customer VPC                         │
│                                                      │
│  ┌───────────────────┐  ┌───────────────────────┐   │
│  │  Public Subnet    │  │   Private Subnet       │   │
│  │  (Databricks      │  │   (Cluster nodes)      │   │
│  │   NAT Gateway)    │  │   - Driver node        │   │
│  └───────────────────┘  │   - Worker nodes       │   │
│           ↓             │   - No public IPs      │   │
│     Internet            └───────────────────────┘   │
│    (for updates)                  ↑                  │
│                           Private Link               │
│                        (Databricks control plane)    │
└──────────────────────────────────────────────────────┘
```

**Customer-Managed VPC (bring your own VPC):**
- You control subnets, security groups, and routing
- Required for: strict network isolation, on-prem connectivity, custom DNS
- Two subnets required: cluster nodes subnet + Databricks private link subnet

---

## Private Link (No Public Internet Exposure)

Private Link ensures cluster traffic never traverses public internet:

```bash
# AWS: Databricks Private Link setup (done by admin)
# 1. Create VPC endpoint for Databricks control plane
aws ec2 create-vpc-endpoint \
  --vpc-id vpc-12345 \
  --service-name com.amazonaws.vpce.us-east-1.vpce-svc-xxxxx \
  --subnet-ids subnet-abc subnet-def \
  --security-group-ids sg-012345

# 2. Enable Private Link on Databricks workspace (Account Console)
# Settings → Network → Private Connectivity

# Azure: Private Endpoint for Databricks
az network private-endpoint create \
  --name databricks-private-endpoint \
  --resource-group myRG \
  --vnet-name myVNet \
  --subnet mySubnet \
  --private-connection-resource-id /subscriptions/.../Databricks/myWorkspace \
  --connection-name databricks-connection \
  --group-ids databricks_ui_api
```

**What Private Link protects:**
- Web UI traffic (browser → workspace)
- REST API calls (SDK, CLI, pipelines)
- Cluster → control plane communication

---

## Encryption

**At-rest encryption:** Databricks encrypts Delta files on S3/ADLS by default. For extra security:

```python
# Customer-managed keys (CMK) — you control the encryption key
# Configured at workspace level in AWS KMS / Azure Key Vault

# In notebooks — DBFS data is always encrypted at rest
# Direct S3/ADLS: encryption handled by cloud provider

# Encrypt secrets at rest: always done automatically by Databricks
```

**In-transit encryption:**
```python
# All data in transit is TLS 1.2+ by default
# Cluster-to-cluster (inter-node): TLS encrypted by default
# JDBC connections: always use SSL
df = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://db.host:5432/mydb?ssl=true&sslmode=require") \
    .option("user", "etl_user") \
    .option("password", dbutils.secrets.get("prod", "pg-password")) \
    .load()
```

---

## Service Principal Best Practices

```python
# Provision a service principal for a pipeline (done once via SDK)
from databricks.sdk import WorkspaceClient, AccountClient

account_client = AccountClient()

# Create SP at account level
sp = account_client.service_principals.create(
    display_name="etl-pipeline-prod",
    application_id="<azure-app-id>"  # or auto-generated
)

# Grant workspace-level permissions
workspace_client = WorkspaceClient()
workspace_client.permissions.update(
    request_object_type="authorization",
    request_object_id="tokens",
    access_control_list=[{
        "service_principal_name": sp.application_id,
        "permission_level": "CAN_USE"  # can create PATs
    }]
)

# Grant Unity Catalog permissions
spark.sql(f"""
    GRANT USE CATALOG ON CATALOG prod TO `{sp.application_id}`;
    GRANT SELECT ON ALL TABLES IN SCHEMA prod.sales TO `{sp.application_id}`;
    GRANT MODIFY ON SCHEMA prod.etl_output TO `{sp.application_id}`;
""")
```

---

## Audit Logging for Security

```sql
-- All authentication events
SELECT
    event_time,
    user_identity.email AS user,
    source_ip_address,
    action_name,
    response.status_code
FROM system.access.audit
WHERE action_name IN ('login', 'tokenLogin', 'aadBrowserLogin')
  AND event_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY event_time DESC;

-- Failed authentication attempts (potential brute force)
SELECT
    user_identity.email AS user,
    source_ip_address,
    COUNT(*) AS failed_attempts
FROM system.access.audit
WHERE action_name IN ('login', 'tokenLogin')
  AND response.status_code = 401
  AND event_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
GROUP BY 1, 2
HAVING COUNT(*) > 5
ORDER BY failed_attempts DESC;

-- Permission escalation events
SELECT event_time, user_identity.email, action_name, request_params
FROM system.access.audit
WHERE action_name IN ('grantPrivilege', 'addAdminUser', 'createRole')
  AND event_time >= DATEADD(day, -1, CURRENT_TIMESTAMP());
```

---

## Network Security Groups / Security Groups

```bash
# AWS Security Group rules for Databricks clusters
# Inbound: allow cluster-to-cluster within SG (required for Spark)
aws ec2 authorize-security-group-ingress \
  --group-id sg-cluster-nodes \
  --protocol all \
  --source-group sg-cluster-nodes

# Outbound: allow HTTPS to Databricks control plane
aws ec2 authorize-security-group-egress \
  --group-id sg-cluster-nodes \
  --protocol tcp \
  --port 443 \
  --cidr 0.0.0.0/0   # or restrict to Databricks IPs

# Outbound: block all other internet traffic (no exfiltration)
# (achieved via no-default-route + NAT Gateway with restricted egress)
```

---

## Interview Tips

> **Tip 1:** "Why would a company use a customer-managed VPC instead of the default Databricks VPC?" — "Three reasons: (1) Connect clusters to on-premises data sources via VPN/Direct Connect/ExpressRoute. (2) Apply org-standard network security policies (security groups, NACLs, firewall rules). (3) Meet compliance requirements that mandate full network control. The trade-off is increased setup complexity — you must configure subnets, routing, and NAT correctly."

> **Tip 2:** "What is Private Link in the Databricks context?" — "Private Link creates a private network endpoint in your VPC that routes Databricks control plane traffic over AWS/Azure private backbone — not public internet. Cluster nodes, web UI users, and API clients all communicate through this endpoint. Prevents data exfiltration via network interception and meets strict compliance requirements like HIPAA and PCI-DSS."

> **Tip 3:** "Why should you use service principals instead of PATs for production pipelines?" — "PATs are tied to a human account — if the person leaves, all pipelines using their PAT break immediately. Service principals are application identities with separate lifecycle management, can be locked down to specific permissions, and are auditable separately. Also, service principals can use OAuth2 client credentials (short-lived tokens) instead of long-lived PATs."
