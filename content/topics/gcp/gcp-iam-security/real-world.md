---
title: "GCP IAM & Security — Real-World Patterns"
topic: gcp
subtopic: gcp-iam-security
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [gcp, iam, security, production, patterns, compliance, interview]
---

# GCP IAM & Security — Real-World Patterns

These are the security incidents, architectural decisions, and production patterns that data engineers encounter in real regulated environments. Each section describes a realistic scenario, the failure mode, and the correct engineering response.

---

## Pattern 1: The Service Account Key Leak

**Incident**: A data engineer committed a service account JSON key to a public GitHub repo. The key had `roles/bigquery.dataEditor` on the entire production dataset. The repo was indexed by automated scanners within minutes; the key was used to exfiltrate 2M customer records within 2 hours.

**Immediate response**:

```bash
# Step 1: Immediately disable the compromised key
gcloud iam service-accounts keys disable KEY_ID \
  --iam-account=etl@my-project.iam.gserviceaccount.com

# Step 2: Delete the key
gcloud iam service-accounts keys delete KEY_ID \
  --iam-account=etl@my-project.iam.gserviceaccount.com

# Step 3: Audit what the key was used for
# Query Cloud Audit Logs for all API calls using this service account in last 24h
# (search in Log Explorer for authenticationInfo.serviceAccountKeyName=KEY_ID)
```

**Root cause analysis**: the engineer was using a local `.env` file to store the key path, which was gitignored locally. After a machine change, they temporarily put the key in the project directory without adding it to `.gitignore`, then committed it in a batch commit.

**Systemic fixes implemented**:

1. **Org policy to disable key creation**: `constraints/iam.disableServiceAccountKeyCreation` enforced at org level. New pipelines must use Workload Identity or Application Default Credentials.
2. **GitHub secret scanning**: enabled GitHub Advanced Security with custom patterns for GCP SA key JSON structure.
3. **Pre-commit hook**: added `detect-secrets` pre-commit hook that scans staged files for key patterns.
4. **IAM scope reduction**: even if a key leaks, the SA only has `dataEditor` on specific datasets — not the entire project. Data-level policy tags on PII columns add a second layer.

```bash
# Pre-commit hook setup
pip install detect-secrets
detect-secrets scan > .secrets.baseline
cat >> .pre-commit-config.yaml << 'EOF'
- repo: https://github.com/Yelp/detect-secrets
  rev: v1.4.0
  hooks:
    - id: detect-secrets
      args: ['--baseline', '.secrets.baseline']
EOF
pre-commit install
```

---

## Pattern 2: VPC-SC Misconfiguration Breaking Production

**Incident**: A team enabled VPC Service Controls perimeter in enforce mode (after dry-run) on a Friday afternoon. By Sunday, a Dataflow job that copies data from Cloud Storage to BigQuery started failing with `VPC_SERVICE_CONTROLS` errors. The pipeline had been running for 18 months.

**Root cause**: the Dataflow job ran in project A, but the destination BigQuery dataset was in project B (inside the VPC-SC perimeter). The Dataflow SA was not included in the perimeter's access levels. During dry-run, the violations were logged but the job wasn't blocked. The Friday enablement blocked the cross-project API call.

**Investigation**:

```bash
# Check VPC-SC violation logs
gcloud logging read \
  'protoPayload.metadata."@type"="type.googleapis.com/google.cloud.audit.BigQueryAuditMetadata" AND
   protoPayload.status.code=7 AND
   protoPayload.status.message:"VPC_SERVICE_CONTROLS"' \
  --project=my-project \
  --format=json | jq '.[].protoPayload.authenticationInfo.principalEmail'
```

**Fix**:

```bash
# Add the Dataflow SA to an access level that allows cross-perimeter access
gcloud access-context-manager levels update etl_service_accounts \
  --policy=accessPolicies/123456789 \
  --add-basic-level-condition='{
    "principals": ["serviceAccount:dataflow-sa@etl-project.iam.gserviceaccount.com"]
  }'
```

**Lessons**:
1. Always run dry-run mode for at least 2 weeks before enforcing. Review violation logs daily.
2. Test perimeter enforcement in a staging environment before prod.
3. Keep a documented "break-glass" procedure to temporarily disable the perimeter if production is impacted.
4. Use the VPC-SC troubleshooter in the Console to simulate API calls before enforcement.

---

## Pattern 3: Least-Privilege Audit Reveals Over-Permission

**Scenario**: A quarterly IAM audit at a fintech reveals that 15 service accounts have `roles/bigquery.admin` on the production project. Tracing back, a data engineer 3 years ago needed to create datasets programmatically and was told "just give it admin, it'll work" — and the habit spread.

**Audit query**:

```bash
# Find all SA bindings on the project
gcloud projects get-iam-policy my-project \
  --format=json | \
jq -r '.bindings[] | select(.role | test("admin|owner|editor")) | {role: .role, members: .members[]}' | \
grep serviceAccount
```

**Remediation approach** (cannot just remove access — would break pipelines):

**Phase 1**: Shadow test — run each pipeline with a least-privilege SA in parallel for 2 weeks. Log any `PERMISSION_DENIED` errors to identify what permissions are actually needed.

```python
# Tool to capture actual permissions used
# Monitor Cloud Audit Logs for each SA over 2 weeks, collect unique methodNames
from google.cloud import logging as cloud_logging

client = cloud_logging.Client()
filter_str = """
protoPayload.authenticationInfo.principalEmail="etl-pipeline@my-project.iam.gserviceaccount.com"
AND protoPayload.serviceName="bigquery.googleapis.com"
AND timestamp > "2024-01-01T00:00:00Z"
"""

for entry in client.list_entries(filter_=filter_str):
    method = entry.payload.get("methodName", "")
    print(method)
# Output: unique set of methods this SA actually calls
```

**Phase 2**: Create custom roles with only the needed permissions:

```hcl
resource "google_project_iam_custom_role" "etl_pipeline_role" {
  role_id     = "etlPipelineRole"
  title       = "ETL Pipeline Role"
  description = "Minimum permissions for ETL pipeline SA"
  permissions = [
    "bigquery.datasets.get",
    "bigquery.tables.create",
    "bigquery.tables.updateData",
    "bigquery.tables.get",
    "bigquery.tables.list",
    "bigquery.jobs.create"
  ]
}
```

**Phase 3**: Swap the admin role for the custom role, monitor for failures for 1 week, then remove admin binding.

---

## Pattern 4: Multi-Environment IAM Strategy

**Scenario**: A data platform team manages dev, staging, and prod environments. Engineers need full access to dev, read-only to staging, and no direct access to prod (production runs via CI/CD service accounts only).

```
Project: data-warehouse-dev
  Engineers: roles/bigquery.admin (full access to dev, experiment freely)

Project: data-warehouse-staging
  Engineers: roles/bigquery.dataViewer + roles/bigquery.jobUser (read-only)
  CI/CD SA: roles/bigquery.dataEditor + roles/bigquery.jobUser

Project: data-warehouse-prod
  Engineers: NO direct IAM binding (zero standing access)
  CI/CD SA: roles/bigquery.dataEditor (only for ETL writes)
  Reporting SA: roles/bigquery.dataViewer (only for BI tool reads)
  Break-glass group: roles/bigquery.admin (emergency access, requires MFA + justification)
```

### Break-Glass Access Pattern

For the rare case where an engineer needs prod access for an incident:

```bash
# Break-glass: time-limited IAM binding with justification
# Engineers are in a group that can grant themselves temporary access

# Policy (enforced via JIT access tool or manual process):
# 1. Engineer requests access in ticketing system with justification
# 2. Security team approves
# 3. Temporary binding created with 4-hour expiry

gcloud projects add-iam-policy-binding data-warehouse-prod \
  --member="user:engineer@company.com" \
  --role="roles/bigquery.dataViewer" \
  --condition='expression=request.time < timestamp("2024-11-01T20:00:00Z"),title=Break-glass until 8pm'
```

All break-glass access is logged in audit logs AND in the ticketing system — creating a complete trail for security reviews.

---

## Pattern 5: GDPR Right to Erasure Implementation

**Scenario**: GDPR requires the ability to delete all data associated with a specific EU user upon request ("right to erasure"). The data is spread across multiple BigQuery tables.

**Implementation options evaluated**:

Option A: **DML DELETE statements** — delete specific rows from each table.

```sql
-- Run across all tables containing user data
DELETE FROM `project.dataset.events` WHERE user_id = 'EU-USER-123';
DELETE FROM `project.dataset.orders` WHERE user_id = 'EU-USER-123';
DELETE FROM `project.dataset.user_profile` WHERE user_id = 'EU-USER-123';
-- ...repeat for all tables
```

Problem: BigQuery is optimized for append-only workloads. DML deletes trigger full partition rewrites. For a table with 1TB/partition, deleting one row rewrites 1TB. Expensive and slow.

Option B: **CMEK key revocation** — if user data is in a CMEK-encrypted dataset, revoke the key. **All** data in the dataset becomes inaccessible. But this is all-or-nothing — you can't revoke for one user.

Option C: **Logical deletion + anonymization view** (chosen):

```sql
-- Table: erasure_requests (maintained by the compliance team)
CREATE TABLE `project.compliance.erasure_requests` (
  user_id STRING,
  requested_at TIMESTAMP,
  completed_at TIMESTAMP
);

-- All downstream consumers query through an authorized view that masks erased users
CREATE OR REPLACE VIEW `project.analytics.events_gdpr_safe` AS
SELECT
  e.*,
  CASE
    WHEN er.user_id IS NOT NULL THEN NULL  -- mask user_id
    ELSE e.user_id
  END AS user_id_masked,
  CASE
    WHEN er.user_id IS NOT NULL THEN '[ERASED]'
    ELSE e.email
  END AS email_masked
FROM `project.dataset.events` e
LEFT JOIN `project.compliance.erasure_requests` er ON e.user_id = er.user_id;
```

Option D: **Pseudonymization with tokenization** (best practice for large-scale):

Store user_id as a token; the mapping table (token → real user_id) is in a CMEK-encrypted dataset. For erasure, delete the mapping row — the token still appears in event data but is now meaningless.

The right answer depends on regulatory interpretation: some regulators accept logical deletion (anonymization); others require physical deletion. Always involve legal counsel.

---

## Audit Trail for SOC 2

A SOC 2 Type II audit requires evidence of access controls enforced continuously over 6-12 months. Key evidence collection:

```sql
-- SOC 2 evidence: IAM changes audit
SELECT
  timestamp,
  protopayload_auditlog.authenticationInfo.principalEmail AS changed_by,
  protopayload_auditlog.request AS change_request,
  protopayload_auditlog.methodName
FROM `my-project.audit_logs.cloudaudit_googleapis_com_activity_*`
WHERE
  timestamp > TIMESTAMP("2024-01-01")
  AND protopayload_auditlog.serviceName = "iam.googleapis.com"
  AND protopayload_auditlog.methodName IN (
    "google.iam.admin.v1.SetIamPolicy",
    "google.iam.v1.IAMPolicy.SetIamPolicy"
  )
ORDER BY timestamp;
```

This query produces the evidence artifact showing who changed IAM policies and when — required for SOC 2 CC6.1 (logical access control changes) and CC6.3 (access removal for terminated personnel).
