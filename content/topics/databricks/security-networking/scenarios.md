---
title: "Security & Networking - Scenario Questions"
topic: databricks
subtopic: security-networking
content_type: scenario_question
tags: [databricks, security, networking, scenarios, interview, compliance]
---

# Scenario Questions — Security & Networking

<article data-difficulty="junior">

## 🟢 Junior: Secure a Data Pipeline's Credentials

**Scenario:** A new data engineer wrote a pipeline that connects to a PostgreSQL database and an external API. Currently the connection string and API key are hardcoded in the notebook. Fix this using Databricks Secrets.

<details>
<summary>✅ Solution</summary>

**Current (insecure) code:**
```python
# ❌ NEVER DO THIS
jdbc_url = "jdbc:postgresql://prod-db.company.com:5432/analytics?user=etl_user&password=SuperSecret123"
api_key = "sk-abc123xyz456"

df = spark.read.format("jdbc").option("url", jdbc_url).load()
```

**Step 1: Create a secret scope**
```bash
# Via Databricks CLI (one-time setup by admin or engineer)
databricks secrets create-scope --scope prod-pipeline-secrets
```

**Step 2: Store secrets (via CLI, never in notebooks)**
```bash
# Store each secret separately (principle of separation)
databricks secrets put --scope prod-pipeline-secrets --key pg-host
# Enter: prod-db.company.com

databricks secrets put --scope prod-pipeline-secrets --key pg-user
# Enter: etl_user

databricks secrets put --scope prod-pipeline-secrets --key pg-password
# Enter: SuperSecret123

databricks secrets put --scope prod-pipeline-secrets --key external-api-key
# Enter: sk-abc123xyz456
```

**Step 3: Use secrets in the notebook**
```python
# ✅ Secure version
pg_host = dbutils.secrets.get(scope="prod-pipeline-secrets", key="pg-host")
pg_user = dbutils.secrets.get(scope="prod-pipeline-secrets", key="pg-user")
pg_password = dbutils.secrets.get(scope="prod-pipeline-secrets", key="pg-password")
api_key = dbutils.secrets.get(scope="prod-pipeline-secrets", key="external-api-key")

# The JDBC URL is constructed safely — password not visible in logs
jdbc_url = f"jdbc:postgresql://{pg_host}:5432/analytics"

df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("user", pg_user) \
    .option("password", pg_password) \   # secret value — never printed
    .option("dbtable", "orders") \
    .option("ssl", "true") \
    .load()

# API call with secret key
import requests
response = requests.get(
    "https://api.external.com/data",
    headers={"Authorization": f"Bearer {api_key}"}
)

print(f"Loaded {df.count()} rows")  # This prints, NOT the credentials
```

**Key points:**
- Secrets are never displayed in notebook output — Databricks automatically redacts them
- Store each credential as a separate secret — allows individual rotation without touching all credentials
- Use `dbutils.secrets.list("scope-name")` to audit what keys exist (values not shown)
- Never `print()` a secret value — even though it would be redacted, it's bad practice

</details>
</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Set Up Least-Privilege Access for a Multi-Team Project

**Scenario:** You have a new Databricks project with three groups: (1) data-ingestion team (write raw data), (2) analytics team (read cleaned data, write reports), (3) external-auditors (read-only access to reports, no PII). Design the Unity Catalog permission model and implement it.

<details>
<summary>✅ Solution</summary>

```sql
-- Step 1: Create the catalog structure
CREATE CATALOG IF NOT EXISTS prod;
USE CATALOG prod;

CREATE SCHEMA IF NOT EXISTS prod.raw;        -- raw ingested data (PII may exist)
CREATE SCHEMA IF NOT EXISTS prod.analytics;  -- cleaned, transformed data
CREATE SCHEMA IF NOT EXISTS prod.reports;    -- aggregated, no PII

-- Step 2: Tag schemas with data classification
ALTER SCHEMA prod.raw SET TAGS ('data_classification' = 'confidential', 'contains_pii' = 'true');
ALTER SCHEMA prod.analytics SET TAGS ('data_classification' = 'internal');
ALTER SCHEMA prod.reports SET TAGS ('data_classification' = 'internal', 'external_shareable' = 'true');

-- Step 3: Data ingestion team — write to raw, no access to downstream
GRANT USE CATALOG ON CATALOG prod TO `data-ingestion-team`;
GRANT USE SCHEMA ON SCHEMA prod.raw TO `data-ingestion-team`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA prod.raw TO `data-ingestion-team`;
GRANT SELECT, MODIFY, CREATE TABLE ON FUTURE TABLES IN SCHEMA prod.raw TO `data-ingestion-team`;
-- Explicitly deny analytics access (default is deny, so this is just documentation):
-- data-ingestion-team cannot access prod.analytics or prod.reports

-- Step 4: Analytics team — read raw, write analytics and reports
GRANT USE CATALOG ON CATALOG prod TO `analytics-team`;

-- Read access to raw (for transformations)
GRANT USE SCHEMA ON SCHEMA prod.raw TO `analytics-team`;
GRANT SELECT ON ALL TABLES IN SCHEMA prod.raw TO `analytics-team`;
GRANT SELECT ON FUTURE TABLES IN SCHEMA prod.raw TO `analytics-team`;

-- Write access to analytics and reports
GRANT USE SCHEMA ON SCHEMA prod.analytics TO `analytics-team`;
GRANT ALL PRIVILEGES ON SCHEMA prod.analytics TO `analytics-team`;
GRANT USE SCHEMA ON SCHEMA prod.reports TO `analytics-team`;
GRANT ALL PRIVILEGES ON SCHEMA prod.reports TO `analytics-team`;

-- Step 5: External auditors — read reports ONLY, no PII
GRANT USE CATALOG ON CATALOG prod TO `external-auditors`;
GRANT USE SCHEMA ON SCHEMA prod.reports TO `external-auditors`;
GRANT SELECT ON ALL TABLES IN SCHEMA prod.reports TO `external-auditors`;
GRANT SELECT ON FUTURE TABLES IN SCHEMA prod.reports TO `external-auditors`;

-- Step 6: Add column mask on any PII that might appear in analytics schema
-- Even though auditors can't access analytics, defense in depth
CREATE OR REPLACE FUNCTION prod.security.mask_pii_for_external(value STRING)
RETURNS STRING
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('analytics-team')
      OR IS_ACCOUNT_GROUP_MEMBER('data-ingestion-team')
    THEN value
    ELSE '***'
END;

-- Apply to any PII column that might leak through
ALTER TABLE prod.analytics.customer_events
    ALTER COLUMN user_email SET MASK prod.security.mask_pii_for_external;

-- Step 7: Verify
SHOW GRANTS ON SCHEMA prod.reports;
SHOW GRANTS ON SCHEMA prod.raw;
```

**Access matrix:**

| Team | prod.raw | prod.analytics | prod.reports |
|------|---------|----------------|--------------|
| data-ingestion | READ+WRITE | ❌ None | ❌ None |
| analytics | READ only | READ+WRITE | READ+WRITE |
| external-auditors | ❌ None | ❌ None | READ only |

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design a Network Architecture for PCI-DSS Compliance

**Scenario:** Your company processes credit card payments and needs to achieve PCI-DSS Level 1 compliance for Databricks. Requirements: (1) Cardholder data (CHD) must never traverse public internet, (2) all CHD access must be auditable, (3) cardholder data must be masked for anyone not PCI-authorized, (4) the system must support least-privilege access for a 50-person team. Design the architecture.

<details>
<summary>✅ Solution</summary>

**Network Architecture:**

```
PCI-DSS Network Design:
┌──────────────────────────────────────────────────────────┐
│  AWS Account: payments-prod (dedicated, no shared infra) │
│                                                          │
│  VPC: 10.0.0.0/16                                       │
│  ┌──────────────────┐  ┌──────────────────────────────┐ │
│  │  Private Subnet  │  │  Private Subnet (clusters)   │ │
│  │  (Databricks     │  │  10.0.2.0/24                 │ │
│  │   Private Link)  │  │  - No public IPs             │ │
│  │  10.0.1.0/24     │  │  - SG: cluster-to-cluster    │ │
│  └──────────────────┘  │    only + PrivateLink         │ │
│                        └──────────────────────────────┘ │
│                                                          │
│  VPC Endpoints:                                          │
│    com.amazonaws.*.s3 (S3 access stays in AWS)          │
│    Databricks control plane (Private Link)               │
│                                                          │
│  No VPC Peering with dev/staging (complete isolation)    │
└──────────────────────────────────────────────────────────┘
```

**Data access controls:**

```sql
-- PCI scope: only these tables contain CHD
-- All others are out of scope
ALTER TABLE prod.payments.transactions
    SET TAGS ('pci_scope' = 'true',
              'chd_type' = 'pan_partial',  -- last 4 only stored
              'data_classification' = 'pci_confidential');

-- Column masking: PAN (card number) visible only to PCI-authorized roles
CREATE OR REPLACE FUNCTION prod.security.mask_pan(pan_last4 STRING)
RETURNS STRING
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('pci-authorized-analysts')
      OR IS_ACCOUNT_GROUP_MEMBER('security-team')
    THEN pan_last4  -- last 4 digits only (never full PAN stored)
    ELSE '****'
END;

ALTER TABLE prod.payments.transactions
    ALTER COLUMN card_last4 SET MASK prod.security.mask_pan;

-- Row filter: PCI analysts see only approved merchant categories
CREATE OR REPLACE FUNCTION prod.security.merchant_category_filter(mcc STRING)
RETURNS BOOLEAN
RETURN (
    IS_ACCOUNT_GROUP_MEMBER('pci-authorized-analysts')
    OR mcc IN (
        SELECT mcc FROM prod.security.analyst_approved_categories
        WHERE analyst_email = CURRENT_USER()
    )
);

ALTER TABLE prod.payments.transactions
    SET ROW FILTER prod.security.merchant_category_filter ON (merchant_category_code);
```

**Audit and monitoring:**

```python
# PCI Requirement 10: Log all access to CHD
# Monitor continuously — alert on anomalies

def pci_access_monitor():
    """Run every hour — alert on suspicious CHD access."""

    # Bulk access (> 10K transactions at once — unusual)
    bulk_access = spark.sql("""
        SELECT user_identity.email, response.numRows, event_time, source_ip_address
        FROM system.access.audit
        WHERE request_params.full_name_arg LIKE 'prod.payments.%'
          AND response.numRows > 10000
          AND event_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
    """).collect()

    if bulk_access:
        for row in bulk_access:
            send_security_alert(
                f"PCI ALERT: Bulk CHD access\n"
                f"User: {row['user_identity.email']}\n"
                f"Rows: {row['response.numRows']:,}\n"
                f"IP: {row['source_ip_address']}"
            )

    # Access from new/unexpected IPs
    new_ip_access = spark.sql("""
        SELECT user_identity.email, source_ip_address, COUNT(*) AS queries
        FROM system.access.audit
        WHERE request_params.full_name_arg LIKE 'prod.payments.%'
          AND event_time >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
          AND source_ip_address NOT IN (
              SELECT DISTINCT source_ip_address
              FROM system.access.audit
              WHERE event_time BETWEEN DATEADD(day, -30, CURRENT_TIMESTAMP())
                                   AND DATEADD(hour, -1, CURRENT_TIMESTAMP())
          )
        GROUP BY 1, 2
    """).collect()

    for row in new_ip_access:
        send_security_alert(f"PCI ALERT: CHD access from new IP {row['source_ip_address']}")
```

**PCI compliance checklist for this design:**

| PCI Requirement | Implementation |
|----------------|---------------|
| Req 1: Network security | Private subnet, no public IPs, VPC endpoints |
| Req 2: Secure defaults | Cluster policies enforce TLS, no default passwords |
| Req 3: Protect stored CHD | Full PAN never stored; last 4 only; column masks |
| Req 4: Protect transmitted CHD | TLS 1.2+ for all data in transit; Private Link |
| Req 7: Access by business need | Unity Catalog RBAC with MCC row filter |
| Req 8: User authentication | SSO + MFA, no shared accounts |
| Req 10: Track all access | system.access.audit + real-time monitoring |
| Req 11: Regular security testing | Quarterly access reviews + automated anomaly detection |

</details>
</article>
