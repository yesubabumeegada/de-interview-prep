---
title: "Security & Networking - Real-World Examples"
topic: databricks
subtopic: security-networking
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, security, production, compliance, incident-response, network]
---

# Security & Networking — Real-World Production Examples

## Production Pattern: HIPAA-Compliant Databricks Deployment

A healthcare analytics company deployed Databricks to meet HIPAA requirements:

**Network isolation:**
```bash
# Customer-managed VPC with Private Link
# Clusters in private subnets — no public IPs on any node
# All control plane traffic via Private Link (not public internet)
# S3 access via VPC endpoint (data never leaves AWS network)

# Security group rules (no inbound from internet)
Inbound: cluster-to-cluster only (same SG)
Outbound: 
  - S3 (VPC endpoint)
  - Databricks control plane (Private Link)
  - BLOCKED: all other internet traffic
```

**PHI data access:**
```sql
-- Only clinical data stewards see real PHI
-- All others see de-identified data via column masks
ALTER TABLE prod.clinical.patient_records
    ALTER COLUMN ssn SET MASK prod.security.phi_mask_ssn;
ALTER TABLE prod.clinical.patient_records
    ALTER COLUMN mrn SET MASK prod.security.phi_mask_mrn;
ALTER TABLE prod.clinical.patient_records
    ALTER COLUMN full_name SET MASK prod.security.phi_mask_name;

-- Row filter: researchers see only their approved study cohort
ALTER TABLE prod.clinical.patient_records
    SET ROW FILTER prod.security.study_cohort_filter ON (patient_id);

-- Audit: every PHI table access logged and reviewed weekly
SELECT user_identity.email, COUNT(*) AS access_count
FROM system.access.audit
WHERE request_params.full_name_arg LIKE 'prod.clinical.%'
  AND event_time >= DATEADD(week, -1, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY 2 DESC;
```

**Result:** SOC2 Type II + HIPAA attestation obtained. PHI never leaves the VPC. Access audited monthly.

---

## Production Pattern: Security Incident — Compromised PAT

A data engineer's laptop was stolen with a PAT stored in a `.env` file:

**Incident timeline:**
- T+0: Laptop reported stolen
- T+15min: Security team identifies the PAT
- T+20min: PAT revoked via admin API
- T+1hr: Audit investigation of all activity from that PAT

```python
# T+20min: Revoke the compromised token immediately
import requests

# Find the token ID from audit logs
tokens = requests.get(
    "https://workspace.databricks.net/api/2.0/token-management/tokens",
    headers={"Authorization": f"Bearer {admin_token}"}
).json()

compromised_token_id = [
    t["token_id"] for t in tokens["token_infos"]
    if t.get("comment") == "alice-dev-laptop"
][0]

# Revoke it
requests.delete(
    f"https://workspace.databricks.net/api/2.0/token-management/tokens/{compromised_token_id}",
    headers={"Authorization": f"Bearer {admin_token}"}
)
print("Token revoked")

# T+1hr: Audit what was accessed
accessed = spark.sql(f"""
    SELECT
        action_name,
        request_params.full_name_arg AS resource,
        response.numRows AS rows_returned,
        event_time,
        source_ip_address
    FROM system.access.audit
    WHERE user_identity.email = 'alice@company.com'
      AND event_time >= DATEADD(day, -3, CURRENT_TIMESTAMP())  -- 3 days before theft reported
    ORDER BY event_time
""")
display(accessed)
```

**What the audit revealed:**
- The PAT had accessed only development tables (no production data)
- Source IPs were all from Alice's known work locations
- No evidence of unauthorized use before theft

**Post-incident improvements:**
1. Require all PATs to expire after 90 days (previously: no expiry)
2. Mandate password manager for storing PATs — no `.env` files
3. Implement IP allowlist so tokens can only be used from corporate IPs
4. Move to service principal + OAuth2 for all automated pipelines

---

## Production Pattern: Multi-Environment Network Isolation

A financial services company isolates dev/staging/prod completely:

```
Environment → AWS Account → VPC → Databricks Workspace → Unity Catalog
dev         → dev-account  → dev-vpc  → dev-ws   → dev catalog (dev.*)
staging     → stage-account → stage-vpc → stage-ws → staging catalog (staging.*)
prod        → prod-account  → prod-vpc  → prod-ws  → prod catalog (prod.*)

Cross-environment rules:
- NO service principals shared across environments
- NO direct VPC peering between prod and dev
- Data promotion: Delta Sharing (not IAM access)
- Developers: can access dev+staging, NEVER prod
- Data engineers: read-only prod access for debugging only
```

```python
# Data promotion: dev → staging → prod via Delta Sharing (not direct access)
# In prod workspace: share specific tables
from databricks.sdk import WorkspaceClient

prod_ws = WorkspaceClient(host=prod_host, token=prod_sp_token)

# Create a share for staging to consume
prod_ws.shares.create(name="staging-promotion-share")
prod_ws.shares.update(
    name="staging-promotion-share",
    added_tables=[{
        "name": "prod.ml.validated_features",
        "table_reference": {"catalog_name": "prod", "schema_name": "ml",
                            "table_name": "validated_features"}
    }]
)

# Grant staging workspace read access via Delta Sharing recipient
prod_ws.recipients.create(
    name="staging-workspace",
    comment="Staging environment - read-only access to approved production tables"
)
```

---

## Compliance Monitoring Dashboard

```sql
-- Weekly security dashboard (run in compliance notebook)

-- 1. Access review: production table access by user
SELECT
    user_identity.email AS user,
    COUNT(DISTINCT request_params.full_name_arg) AS distinct_tables_accessed,
    COUNT(*) AS total_access_events,
    MAX(event_time) AS last_access
FROM system.access.audit
WHERE request_params.full_name_arg LIKE 'prod.%'
  AND action_name = 'select'
  AND event_time >= DATEADD(week, -1, CURRENT_TIMESTAMP())
GROUP BY 1
ORDER BY total_access_events DESC;

-- 2. New grants this week (privileged changes)
SELECT event_time, user_identity.email AS granted_by,
       action_name, request_params
FROM system.access.audit
WHERE action_name IN ('grantPrivilege', 'revokePrivilege', 'addAdminUser')
  AND event_time >= DATEADD(week, -1, CURRENT_TIMESTAMP())
ORDER BY event_time DESC;

-- 3. Token management: tokens expiring soon (potential disruption risk)
SELECT token_id, comment, expiry_time
FROM (
    SELECT *, DATEDIFF(day, CURRENT_TIMESTAMP(), expiry_time) AS days_until_expiry
    FROM system.access.tokens
    WHERE expiry_time IS NOT NULL
)
WHERE days_until_expiry <= 14
ORDER BY expiry_time;

-- 4. Failed login attempts (potential attack)
SELECT source_ip_address, COUNT(*) AS failure_count
FROM system.access.audit
WHERE action_name IN ('login', 'tokenLogin')
  AND response.status_code = 401
  AND event_time >= DATEADD(day, -1, CURRENT_TIMESTAMP())
GROUP BY 1
HAVING COUNT(*) > 10
ORDER BY failure_count DESC;
```
