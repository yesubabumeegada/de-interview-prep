---
title: "Security & Networking - Senior Deep Dive"
topic: databricks
subtopic: security-networking
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [databricks, security, zero-trust, compliance, network-architecture, exfiltration-prevention]
---

# Security & Networking — Senior Deep Dive

## Zero-Trust Network Architecture

A hardened Databricks deployment prevents both external attacks and insider threats:

```
Zero-trust principles applied to Databricks:
1. Verify explicitly — all access authenticated + authorized per request
2. Least privilege — minimum access needed for each identity
3. Assume breach — monitor and alert on all access, even internal
```

```
┌──────────────────────────────────────────────────────────────┐
│  Zero-Trust Databricks Architecture                           │
│                                                              │
│  Identity Layer:                                             │
│    Users → SSO (Okta/AAD) → MFA required                    │
│    Services → Service Principals + OAuth2 (no long-lived PAT)│
│                                                              │
│  Network Layer:                                              │
│    Cluster nodes → Private subnet (no public IP)             │
│    Control plane → Private Link only                         │
│    Egress → Restricted NAT (allowlist: S3, UC, approved APIs)│
│                                                              │
│  Data Layer:                                                 │
│    All PHI/PCI tables → Column masks + Row filters           │
│    Customer-managed encryption keys (KMS/Key Vault)          │
│                                                              │
│  Monitoring Layer:                                           │
│    All access → system.access.audit (SIEM integration)       │
│    Anomaly detection → alert on bulk access, new IPs         │
└──────────────────────────────────────────────────────────────┘
```

---

## Data Exfiltration Prevention

Prevent employees from copying sensitive data out:

```bash
# 1. Outbound network restriction via firewall rules
# Block all outbound except:
# - Databricks control plane (Private Link)
# - Cloud storage (S3/ADLS — via VPC endpoint, no public internet)
# - Approved external APIs (explicitly allowlisted)

# AWS: VPC endpoint for S3 (traffic stays in AWS network)
aws ec2 create-vpc-endpoint \
  --vpc-id vpc-12345 \
  --service-name com.amazonaws.us-east-1.s3 \
  --route-table-ids rtb-12345

# Then restrict S3 access to specific buckets via VPC endpoint policy
```

```python
# 2. Unity Catalog prevents data export by disabling COPY INTO to external stages
# Only allow writing to approved catalogs

# 3. Output table restrictions via IP allowlist on downstream services

# 4. Monitor bulk data access (>100K rows) from unusual IPs
spark.sql("""
    SELECT
        user_identity.email,
        source_ip_address,
        request_params.full_name_arg AS table_name,
        response.numRows AS rows_returned,
        event_time
    FROM system.access.audit
    WHERE response.numRows > 100000
      AND event_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    ORDER BY rows_returned DESC
""")
```

---

## Compliance Frameworks Implementation

**SOC 2 Type II controls in Databricks:**

```python
# Control 1: Access reviews (quarterly)
# List all users with Production access
current_prod_access = spark.sql("""
    SELECT
        grantee AS user_or_group,
        privilege_type,
        object_type,
        object_name
    FROM system.information_schema.object_privileges
    WHERE object_name LIKE 'prod.%'
      AND privilege_type IN ('SELECT', 'MODIFY', 'ALL PRIVILEGES')
    ORDER BY object_name, privilege_type
""")
current_prod_access.write.mode("overwrite").saveAsTable("compliance.access_reviews.q4_2024")

# Control 2: Audit trail retention (min 1 year for SOC2)
# system.access.audit retains 365 days — export to long-term storage
spark.sql("""
    INSERT INTO compliance.audit_archive.databricks_access_log
    SELECT * FROM system.access.audit
    WHERE event_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
      AND event_time < CURRENT_DATE()
""")

# Control 3: Privileged access monitoring
# Alert when admin privileges are granted
def monitor_privilege_escalation():
    new_admins = spark.sql("""
        SELECT event_time, user_identity.email AS granted_by,
               request_params.user_name AS granted_to,
               action_name
        FROM system.access.audit
        WHERE action_name IN ('addAdminUser', 'updatePermissions')
          AND event_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
    """).collect()

    if new_admins:
        for row in new_admins:
            send_alert(f"PRIVILEGE ESCALATION: {row['granted_to']} granted admin by {row['granted_by']}")
```

**PCI-DSS specific controls:**

```sql
-- Cardholder data must be masked
-- Verify no raw card numbers accessible to non-PCI-authorized users
SELECT
    table_catalog, table_schema, table_name, column_name
FROM system.information_schema.columns
WHERE lower(column_name) LIKE '%card%'
   OR lower(column_name) LIKE '%pan%'
   OR lower(column_name) LIKE '%credit%';

-- For each found: verify column mask is applied
SHOW COLUMN MASK ON TABLE prod.payments.transactions COLUMN card_number;
-- If no mask exists → security finding
```

---

## Multi-Workspace Security Model

Large enterprises run multiple workspaces (dev/staging/prod) with strict separation:

```python
# Service principal per workspace (not shared across envs)
# Dev SP: read/write dev catalog only
# Prod SP: read/write prod catalog only
# No cross-workspace service principals

# Unity Catalog metastore separation:
# Option A: One metastore, three catalogs (dev/staging/prod)
#   - Simpler setup, same governance for all
#   - Risk: accidental cross-environment access
#   - Mitigated by: strict RBAC between catalogs

# Option B: Three metastores (one per environment)
#   - Complete isolation — prod data invisible from dev workspace
#   - Higher complexity: separate governance, no shared data
#   - Used by: regulated industries (finance, healthcare)

# Cross-workspace data sharing via Delta Sharing (not direct access)
# Prod workspace shares specific tables → dev workspace can read published shares
# No direct access to prod S3/ADLS from dev clusters
```

---

## Security Incident Response Playbook

```python
# When a security event is detected:
def security_incident_response(event_type: str, details: dict):

    if event_type == "bulk_data_access":
        # Immediate: revoke token
        user = details["user_email"]
        # Via admin API:
        # DELETE /api/2.0/token-management/tokens?token_id=<id>

        # Investigate: what did they access?
        access_log = spark.sql(f"""
            SELECT action_name, request_params.full_name_arg, response.numRows, event_time
            FROM system.access.audit
            WHERE user_identity.email = '{user}'
              AND event_time >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
            ORDER BY event_time
        """)
        access_log.write.mode("overwrite").saveAsTable(f"compliance.incidents.{user.replace('@','_').replace('.','_')}_investigation")

    elif event_type == "unauthorized_privilege_grant":
        # Immediately revoke the granted privilege
        # Audit who granted it and whether they had authority to do so
        # Preserve all audit evidence before taking any remediation

    elif event_type == "failed_login_spike":
        # Add IP to temporary blocklist
        # Check if it's a legitimate user being locked out or an attack
        # Enable adaptive MFA challenge for affected account
```

---

## Interview Tips

> **Tip 1:** "How would you prevent data exfiltration from a Databricks cluster?" — "Three layers: (1) Network: restrict outbound traffic via VPC routing — clusters can only reach approved services (S3/ADLS via VPC endpoint, Databricks control plane via Private Link). (2) Data access: Unity Catalog row filters and column masks ensure sensitive data can't be bulk-exported by unauthorized users. (3) Detection: query `system.access.audit` for bulk reads (numRows > 100K) from non-standard IPs and alert the security team."

> **Tip 2:** "What compliance challenges are specific to Databricks vs a traditional data warehouse?" — "Databricks stores raw data on cloud object storage (S3/ADLS) that's outside the query engine. The storage can be accessed directly if IAM policies aren't tight — bypass Unity Catalog entirely. Mitigation: S3 bucket policy should require access through VPC endpoint only, and Unity Catalog service principals should be the only principals with bucket-level access. Also: cluster logs, job output, and notebook results can contain PII — ensure these are written to governed storage."

> **Tip 3:** "How do you implement least-privilege for a data engineering team?" — "Per-role grants via Unity Catalog: ingestion service accounts get MODIFY on raw schemas only; transformation jobs get SELECT on raw + MODIFY on silver; analysts get SELECT on gold/reporting only. Use service principals (not human accounts) for all automated jobs. Enforce via GRANT statements and review quarterly with `system.information_schema.object_privileges`. Block schema-level CREATE TABLE for anyone except the data platform team — prevent shadow tables in production catalogs."
