---
title: "GCP IAM & Security — Intermediate"
topic: gcp
subtopic: gcp-iam-security
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [gcp, iam, vpc-service-controls, secret-manager, audit-logging, interview]
---

# GCP IAM & Security — Intermediate

At the mid-level, you're expected to design security controls that go beyond basic IAM role assignments: VPC Service Controls for data exfiltration prevention, Secret Manager for credential management, IAM Conditions for fine-grained access, audit logging for compliance, and organization policies for governance. These are the controls that appear in security reviews, SOC 2 audits, and regulated industry compliance frameworks.

---

## VPC Service Controls: Preventing Data Exfiltration

VPC Service Controls (VPC-SC) create a **security perimeter** around GCP resources — a boundary that prevents data from flowing out of your controlled environment, even if an attacker compromises credentials.

### The Problem It Solves

Without VPC-SC:
- A malicious insider with BigQuery read access could: `bq extract` data to a personal GCS bucket in another project → download from there.
- A compromised service account could exfiltrate data to an attacker's GCP project.
- `bq cp` can copy tables across projects freely.

With VPC-SC:
- A perimeter defines which projects, services, and identities are "inside."
- API calls that would move data across the perimeter boundary are blocked, even with valid IAM credentials.

### Architecture

```
VPC Service Controls Perimeter: "data-perimeter"
├─ Protected Projects:
│   ├─ data-warehouse-prod
│   └─ ml-platform-prod
├─ Protected Services:
│   ├─ bigquery.googleapis.com
│   ├─ storage.googleapis.com
│   └─ aiplatform.googleapis.com
└─ Access Levels (who can cross the boundary):
    ├─ "corporate-network": requests from IP 203.0.113.0/24
    └─ "approved-service-accounts": specific SA identities
```

```bash
# Create an access policy (organization-level)
gcloud access-context-manager policies create \
  --organization=123456789 \
  --title="Data Security Policy"

# Create an access level (corporate network)
gcloud access-context-manager levels create corporate_network \
  --policy=accessPolicies/123456789 \
  --title="Corporate Network" \
  --basic-level-spec=levels.yaml

# levels.yaml:
# conditions:
#   - ipSubnetworks:
#     - "203.0.113.0/24"

# Create the service perimeter
gcloud access-context-manager perimeters create data_perimeter \
  --policy=accessPolicies/123456789 \
  --title="Data Perimeter" \
  --resources="projects/111111111,projects/222222222" \
  --restricted-services="bigquery.googleapis.com,storage.googleapis.com" \
  --access-levels="accessPolicies/123456789/accessLevels/corporate_network"
```

### Perimeter Bridges

When two separate perimeters need to exchange data legitimately (e.g., prod data warehouse → analytics project outside the perimeter), you configure a **perimeter bridge**:

```bash
gcloud access-context-manager perimeters update data_perimeter \
  --set-perimeter-bridges="accessPolicies/123456789/servicePerimeters/analytics_perimeter"
```

**Interview point**: VPC-SC operates at the Google API level, not the network level. It's about controlling API calls, not network packets. A VM inside a VPC can still be blocked from accessing BigQuery if it's outside the VPC-SC perimeter.

---

## Secret Manager: Credential Management

Secret Manager stores, versions, and audits access to sensitive values: API keys, database passwords, OAuth tokens, TLS certificates. For data engineers, it's the correct place to store:

- Database connection strings for pipeline sources (MySQL, PostgreSQL, Oracle).
- Third-party API keys (Salesforce, Stripe, Snowflake).
- Service account keys (when Workload Identity isn't available).
- Encryption keys (though Cloud KMS is preferred for key management).

```bash
# Create a secret
echo -n "postgresql://user:password@host:5432/db" | \
gcloud secrets create postgres-prod-connection \
  --data-file=- \
  --replication-policy=user-managed \
  --locations=us-central1,us-east1

# Access a secret in shell
gcloud secrets versions access latest --secret="postgres-prod-connection"

# Rotate: add a new version
echo -n "postgresql://user:newpassword@host:5432/db" | \
gcloud secrets versions add postgres-prod-connection --data-file=-
# Disable old version after verifying new one works
gcloud secrets versions disable 1 --secret="postgres-prod-connection"
```

### Accessing Secrets in Python Pipelines

```python
from google.cloud import secretmanager

def get_secret(project_id: str, secret_id: str, version: str = "latest") -> str:
    """Retrieve a secret value from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version}"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

# In your pipeline:
db_connection = get_secret("my-project", "postgres-prod-connection")
```

### IAM for Secrets

```bash
# Grant a service account access to a specific secret only
gcloud secrets add-iam-policy-binding postgres-prod-connection \
  --member="serviceAccount:etl-pipeline@my-project.iam.gserviceaccount.com" \
  --role="roles/secretmanager.secretAccessor"
```

**Principle of least privilege**: the service account gets `secretAccessor` on the specific secret it needs — not `roles/secretmanager.admin` on all secrets.

### Secret Rotation with Cloud Functions

```python
# Cloud Function triggered by Secret Manager rotation event
import functions_framework
from google.cloud import secretmanager
import psycopg2

@functions_framework.cloud_event
def rotate_db_password(cloud_event):
    """Auto-rotate the PostgreSQL password and update Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    secret_name = cloud_event.data["name"]

    # Generate new password
    import secrets
    import string
    new_password = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(32))

    # Update in PostgreSQL
    conn = psycopg2.connect(get_secret("my-project", "postgres-admin-connection"))
    with conn.cursor() as cur:
        cur.execute("ALTER USER etl_user WITH PASSWORD %s", (new_password,))
    conn.commit()

    # Store new version in Secret Manager
    parent = f"projects/my-project/secrets/postgres-prod-connection"
    client.add_secret_version(
        request={"parent": parent, "payload": {"data": new_password.encode("UTF-8")}}
    )
```

---

## IAM Conditions: Fine-Grained Temporal and Resource Access

IAM Conditions add attribute-based access control (ABAC) on top of role-based IAM. Conditions can gate access on:
- Resource attributes (name, type, service).
- Request attributes (time of day, origin IP).
- Custom claims from workload identity providers.

```bash
# Grant access to a specific BigQuery table only
gcloud projects add-iam-policy-binding my-project \
  --member="serviceAccount:reporting-sa@my-project.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer" \
  --condition='expression=resource.name.startsWith("projects/my-project/datasets/reporting"),title=Reporting datasets only'

# Grant access only during business hours (Mon-Fri 9am-5pm UTC)
gcloud projects add-iam-policy-binding my-project \
  --member="user:contractor@external.com" \
  --role="roles/bigquery.jobUser" \
  --condition='expression=request.time.getHours("UTC") >= 9 && request.time.getHours("UTC") < 17 && request.time.getDayOfWeek("UTC") >= 1 && request.time.getDayOfWeek("UTC") <= 5,title=Business hours only'
```

**Use cases in data engineering**:
- Time-limited contractor access (expires automatically).
- Restrict service accounts to specific datasets by resource name pattern.
- Allow external audit access only from specific IP ranges.

---

## Audit Logging: Data Access Logs

GCP's **Cloud Audit Logs** record admin activity and data access across GCP services. For data engineering and compliance, **Data Access audit logs** are critical — they record every BigQuery query, read, and write operation.

### Log Types

| Log Type | What it records | Default On? |
|---|---|---|
| Admin Activity | API calls that modify resources (create table, modify IAM) | Yes, always |
| Data Access - DATA_READ | Reading resource data (SELECT queries) | No — must enable |
| Data Access - DATA_WRITE | Modifying data (INSERT, UPDATE, DML) | No — must enable |
| System Event | GCP-automated actions | Yes, always |

```bash
# Enable Data Access audit logs for BigQuery (org-level, applies to all projects)
gcloud organizations set-iam-policy 123456789 policy.json

# policy.json:
# {
#   "auditConfigs": [{
#     "service": "bigquery.googleapis.com",
#     "auditLogConfigs": [
#       {"logType": "DATA_READ"},
#       {"logType": "DATA_WRITE"}
#     ]
#   }]
# }
```

### Querying Audit Logs in BigQuery

For long-term retention and analysis, export Cloud Audit Logs to BigQuery via Log Sink:

```bash
# Create a log sink to export BigQuery audit logs to a BQ dataset
gcloud logging sinks create bq-audit-sink \
  bigquery.googleapis.com/projects/my-project/datasets/audit_logs \
  --log-filter='resource.type="bigquery_resource" AND protoPayload.serviceName="bigquery.googleapis.com"' \
  --project=my-project
```

```sql
-- Who accessed the PII table in the last 30 days?
SELECT
  protopayload_auditlog.authenticationInfo.principalEmail AS accessor,
  protopayload_auditlog.methodName AS method,
  protopayload_auditlog.resourceName AS resource,
  timestamp
FROM `my-project.audit_logs.cloudaudit_googleapis_com_data_access_*`
WHERE
  timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND protopayload_auditlog.resourceName LIKE '%datasets/customers%'
ORDER BY timestamp DESC;

-- Detect unusual access: users accessing datasets they haven't accessed before
WITH user_history AS (
  SELECT
    protopayload_auditlog.authenticationInfo.principalEmail AS user_email,
    REGEXP_EXTRACT(protopayload_auditlog.resourceName, r'datasets/([^/]+)') AS dataset,
    MIN(DATE(timestamp)) AS first_access_date
  FROM `my-project.audit_logs.cloudaudit_googleapis_com_data_access_*`
  GROUP BY 1, 2
)
SELECT *
FROM user_history
WHERE first_access_date = CURRENT_DATE()
ORDER BY user_email;
```

---

## Org Policies for Data Governance

Organization policies enforce guardrails at the organization or folder level — constraints that apply regardless of IAM permissions.

```bash
# Prevent creation of service account keys (force Workload Identity)
gcloud resource-manager org-policies set-policy \
  --organization=123456789 policy.yaml

# policy.yaml:
# constraint: constraints/iam.disableServiceAccountKeyCreation
# booleanPolicy:
#   enforced: true

# Restrict which regions BigQuery datasets can be created in (data residency)
gcloud resource-manager org-policies set-policy \
  --organization=123456789 location-policy.yaml

# location-policy.yaml:
# constraint: constraints/gcp.resourceLocations
# listPolicy:
#   allowedValues:
#     - in:us-locations
#     - in:europe-locations
```

**Common org policies for data engineering:**

| Policy | What it prevents |
|---|---|
| `iam.disableServiceAccountKeyCreation` | Prevents downloading SA keys; forces Workload Identity |
| `gcp.resourceLocations` | Restricts which regions resources can be created |
| `bigquery.disableAllIamPoliciesOnUnencryptedTables` | Forces CMEK encryption |
| `storage.uniformBucketLevelAccess` | Forces bucket-level IAM (disables legacy ACLs) |
| `compute.requireShieldedVm` | Forces Shielded VM for Compute instances |
