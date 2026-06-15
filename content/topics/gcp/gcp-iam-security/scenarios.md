---
title: "GCP IAM & Security — Scenario Questions"
topic: gcp
subtopic: gcp-iam-security
content_type: scenario_question
tags: [gcp, iam, security, vpc-service-controls, audit-logging, interview]
---

# GCP IAM & Security — Scenario Questions

<article data-difficulty="junior">

## 🟢 Junior: Configuring Least-Privilege BigQuery Access

**Scenario:** You're onboarding a new analyst, Jordan, to the data team. Jordan needs to:
1. Run SQL queries against the `sales` and `marketing` BigQuery datasets in project `data-warehouse-prod`.
2. NOT be able to modify or delete any tables.
3. NOT be able to see the `hr` dataset (which contains salary data).
4. NOT be able to create tables or load data.

The project currently uses these roles for existing analysts: `roles/bigquery.user` granted at the project level. Your manager says "just add Jordan to that binding." Explain why that's wrong and implement the correct access.

<details>
<summary>✅ Solution</summary>

**Why `roles/bigquery.user` at project level is wrong:**

`roles/bigquery.user` includes `bigquery.datasets.get` which allows the user to **list and access all datasets in the project** — including the `hr` dataset containing salary data. It also includes permissions that go slightly beyond read-only for certain metadata operations.

**Correct implementation: dataset-level bindings, not project-level**

```bash
# Step 1: Grant jobUser at the PROJECT level
# (jobUser = permission to run queries; must be at project level)
gcloud projects add-iam-policy-binding data-warehouse-prod \
  --member="user:jordan@company.com" \
  --role="roles/bigquery.jobUser"

# Step 2: Grant dataViewer on specific datasets (NOT at project level)
bq add-iam-policy-binding \
  --member="user:jordan@company.com" \
  --role="roles/bigquery.dataViewer" \
  data-warehouse-prod:sales

bq add-iam-policy-binding \
  --member="user:jordan@company.com" \
  --role="roles/bigquery.dataViewer" \
  data-warehouse-prod:marketing

# Jordan is NOT added to hr dataset - so they cannot access it at all
```

Or in Terraform (preferred for production):

```hcl
# Project-level: allow running jobs
resource "google_project_iam_member" "jordan_job_user" {
  project = "data-warehouse-prod"
  role    = "roles/bigquery.jobUser"
  member  = "user:jordan@company.com"
}

# Dataset-level: read-only on sales
resource "google_bigquery_dataset_iam_member" "jordan_sales_viewer" {
  project    = "data-warehouse-prod"
  dataset_id = "sales"
  role       = "roles/bigquery.dataViewer"
  member     = "user:jordan@company.com"
}

# Dataset-level: read-only on marketing
resource "google_bigquery_dataset_iam_member" "jordan_marketing_viewer" {
  project    = "data-warehouse-prod"
  dataset_id = "marketing"
  role       = "roles/bigquery.dataViewer"
  member     = "user:jordan@company.com"
}
# No binding for "hr" dataset — Jordan gets no access
```

**Verification**: Jordan attempts `SELECT * FROM hr.salaries` → "Access Denied: BigQuery BigQuery: Permission denied while getting Drive credentials."

**Key distinctions:**
- `bigquery.jobUser`: must be at PROJECT level (controls the ability to submit jobs, billed to that project).
- `bigquery.dataViewer`: should be at DATASET level when you want to restrict to specific datasets.
- Never grant `bigquery.user` at the project level if there are sensitive datasets in the same project — it grants visibility into all datasets.

**For groups (better practice for real teams):**
```bash
# Better: use Google Groups to manage team access
gcloud projects add-iam-policy-binding data-warehouse-prod \
  --member="group:data-analysts@company.com" \
  --role="roles/bigquery.jobUser"
# Add Jordan to the group in Google Workspace admin, not in IAM directly
# Dataset-level bindings use the group, not individual users
```

</details>

</article>

<article data-difficulty="mid">

## 🟡 Mid-Level: Investigating a Potential Data Exfiltration

**Scenario:** Your security team notifies you that a service account `reporting-sa@data-warehouse-prod.iam.gserviceaccount.com` — which normally only runs SELECT queries for a Looker dashboard — has been observed running `bq extract` commands copying data from the `customers` table to a Cloud Storage bucket in a different project (`gs://external-bucket-unknown-project/dump/`). The extract jobs were run at 2am for the past 3 nights.

The SA has `roles/bigquery.dataViewer` on the `customers` dataset and `roles/bigquery.jobUser` on the project. You need to investigate, contain the incident, and propose controls to prevent recurrence.

<details>
<summary>✅ Solution</summary>

**Immediate Containment (first 15 minutes):**

```bash
# Step 1: Disable the service account immediately
gcloud iam service-accounts disable \
  reporting-sa@data-warehouse-prod.iam.gserviceaccount.com \
  --project=data-warehouse-prod

# Step 2: Revoke all active access tokens (force re-authentication)
# Disabling the SA immediately invalidates future API calls; existing short-lived tokens
# (~1 hour) expire naturally. No way to revoke them retroactively.

# Step 3: Preserve evidence - export relevant audit logs
gcloud logging read \
  'protoPayload.authenticationInfo.principalEmail="reporting-sa@data-warehouse-prod.iam.gserviceaccount.com"' \
  --freshness=7d \
  --format=json > incident-evidence.json
```

**Investigation:**

```sql
-- Query audit logs in BigQuery (if already exported)
SELECT
  timestamp,
  protopayload_auditlog.methodName AS method,
  protopayload_auditlog.resourceName AS resource,
  JSON_EXTRACT_SCALAR(protopayload_auditlog.request, '$.destinationUri') AS destination,
  protopayload_auditlog.authenticationInfo.principalEmail,
  protopayload_auditlog.requestMetadata.callerIp AS source_ip
FROM `data-warehouse-prod.audit_logs.cloudaudit_googleapis_com_data_access_*`
WHERE
  timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND protopayload_auditlog.authenticationInfo.principalEmail
      = 'reporting-sa@data-warehouse-prod.iam.gserviceaccount.com'
ORDER BY timestamp DESC;
```

**Key questions from audit logs:**
- What source IP called the `bq extract` API? (A Looker server? An unknown external IP?)
- Were there any SA key downloads (service account key management events) recently?
- Was the SA's usage pattern normal before these 3 nights?

**Likely root causes to investigate:**
1. SA key was compromised (downloaded and stolen).
2. An application that uses the SA was compromised (code injection, supply chain attack).
3. A rogue insider with access to the SA key.
4. Misconfigured automation (unlikely to target an unknown project's GCS bucket).

**Root cause finding**: the `callerIp` in audit logs shows an IP not belonging to Looker's known IP range → confirms external compromise of the SA key.

**Remediation:**

```bash
# List all keys for the SA
gcloud iam service-accounts keys list \
  --iam-account=reporting-sa@data-warehouse-prod.iam.gserviceaccount.com

# Delete all user-managed keys (GCP-managed key stays)
gcloud iam service-accounts keys delete KEY_ID \
  --iam-account=reporting-sa@data-warehouse-prod.iam.gserviceaccount.com

# Re-enable the SA after key deletion
gcloud iam service-accounts enable \
  reporting-sa@data-warehouse-prod.iam.gserviceaccount.com
```

**Preventive Controls:**

1. **VPC Service Controls**: create a perimeter around `data-warehouse-prod`. The `bq extract` API call to a bucket in a different project would be blocked because the destination project is outside the perimeter.

2. **Org Policy — restrict GCS export destinations**: 
```bash
# Org policy: BigQuery data exports only to approved GCS buckets
# (via VPC-SC egress rules or custom org constraints)
```

3. **Eliminate SA keys**: migrate Looker to use Workload Identity Federation or an authorized BigQuery connection (native Looker → BQ OAuth connection). No downloadable keys → nothing to steal.

4. **Anomaly detection**: alert when a reporting SA runs anything other than `BigQueryStorage.ReadRows` and `Jobs.List` methods:
```sql
-- Alert: reporting-sa running unexpected methods
SELECT timestamp, methodName, callerIp
FROM audit_logs
WHERE principalEmail = 'reporting-sa@...'
  AND methodName NOT IN ('google.cloud.bigquery.storage.v1.BigQueryRead.ReadRows',
                         'bigquery.jobs.get', 'bigquery.tables.getData')
```

5. **Minimum scope**: `bigquery.dataViewer` only on the specific tables the dashboard needs — not the entire dataset. If the customers table wasn't in scope for the dashboard, this exfiltration wouldn't have been possible even with the SA key.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing a Security Architecture for a Healthcare Data Platform

**Scenario:** You're the data platform architect at a healthcare company that just acquired a small analytics startup. You need to design a GCP security architecture for a unified data platform that will hold:
- **PHI (Protected Health Information)**: patient records, diagnoses, prescription data — HIPAA regulated.
- **PII**: employee data, customer contact information — GDPR regulated (EU employees).
- **Non-sensitive**: anonymized research datasets, marketing analytics.

The platform will serve:
- Clinical analysts (read PHI within their hospital system only).
- Data scientists (access to anonymized data for model training).
- External researchers (read-only access to approved research datasets, no PHI).
- ETL pipelines (write raw data, read transformed data).
- BI tools (aggregate dashboards with no row-level PHI).

Design the complete security architecture. Address: project structure, IAM model, BigQuery security (column and row-level), VPC Service Controls, audit logging, encryption, and data residency.

<details>
<summary>✅ Solution</summary>

**Project Structure (Separation of Concerns by Sensitivity):**

```
Organization: healthco.com
└─ Folder: data-platform
    ├─ Project: data-raw-phi          # PHI landing zone
    ├─ Project: data-raw-pii          # PII landing zone  
    ├─ Project: data-warehouse-phi    # PHI transformed/analytics
    ├─ Project: data-warehouse-nonphi # Non-PHI analytics
    └─ Project: data-research         # Anonymized research datasets
```

Rationale: separate projects per sensitivity level ensures that even a project-level IAM misconfiguration cannot grant cross-sensitivity access. PHI and PII never co-exist in projects with external researcher access.

**VPC Service Controls:**

```
Perimeter A: "phi-perimeter" (ENFORCE mode)
  Projects: data-raw-phi, data-warehouse-phi
  Services: bigquery.googleapis.com, storage.googleapis.com, healthcare.googleapis.com
  Access levels:
    - "phi-users": clinical analysts on corporate VPN (IP range) + PHI ETL SA
    - "phi-etl": ETL pipeline SAs for PHI ingestion

Perimeter B: "pii-perimeter" (ENFORCE mode)
  Projects: data-raw-pii
  Services: bigquery.googleapis.com, storage.googleapis.com
  Access levels:
    - "hr-systems": HR system SA + on-prem IP range

Perimeter C: "research-perimeter" (ENFORCE mode, lower restrictions)
  Projects: data-research
  Services: bigquery.googleapis.com
  Access levels:
    - "authenticated-researchers": approved researcher Google accounts + institutional IPs
```

PHI and PII perimeters have NO ingress rule for researcher identities — they are physically blocked from accessing PHI/PII projects even if they somehow get IAM bindings.

**BigQuery Security Layers:**

```sql
-- Data Catalog Taxonomy for PHI/PII tagging
Taxonomy: "Health Data Classification"
├─ "PHI"
│   ├─ "patient_id"
│   ├─ "diagnosis_code"
│   ├─ "prescription_info"
│   └─ "provider_id"
└─ "PII"
    ├─ "email_address"
    └─ "employee_ssn"
```

```sql
-- PHI table: column security
CREATE TABLE `data-warehouse-phi.clinical.patient_encounters`
(
  encounter_id STRING,
  patient_id STRING OPTIONS (policy_tags='["...phi/patient_id"]'),
  diagnosis_code STRING OPTIONS (policy_tags='["...phi/diagnosis_code"]'),
  encounter_date DATE,
  hospital_system_id STRING,
  -- anonymized fields (no policy tag)
  age_group STRING,
  region STRING
);

-- Row-level security: clinical analysts see only their hospital system
CREATE OR REPLACE ROW ACCESS POLICY hospital_system_rls
ON `data-warehouse-phi.clinical.patient_encounters`
GRANT TO ("domain:healthco.com")
FILTER USING (
  hospital_system_id IN (
    SELECT hospital_system_id
    FROM `data-warehouse-phi.clinical.user_hospital_assignments`
    WHERE user_email = SESSION_USER()
  )
);

-- Data scientists and researchers: separate table (view over anonymized subset)
CREATE OR REPLACE VIEW `data-research.public.anonymized_encounters` AS
SELECT
  encounter_id,
  age_group,
  region,
  diagnosis_category,  -- generalized from ICD code
  encounter_date
  -- NO patient_id, NO diagnosis_code (PHI columns excluded)
FROM `data-warehouse-phi.clinical.patient_encounters`;
-- This view is an authorized view: data-research project is granted access
```

**Encryption:**

```hcl
# CMEK for PHI datasets (HIPAA requires encryption key control)
resource "google_kms_key_ring" "phi_keyring" {
  name     = "phi-keyring"
  location = "us-central1"  # data residency: US only for PHI
}

resource "google_kms_crypto_key" "phi_key" {
  name            = "phi-bq-key"
  key_ring        = google_kms_key_ring.phi_keyring.id
  rotation_period = "7776000s"  # 90-day automatic rotation
}

resource "google_bigquery_dataset" "clinical" {
  project    = "data-warehouse-phi"
  dataset_id = "clinical"
  location   = "US"

  default_encryption_configuration {
    kms_key_name = google_kms_crypto_key.phi_key.id
  }
}
```

**IAM Model Summary:**

| Role | Clinical Analysts | Data Scientists | External Researchers | ETL SAs | BI Tools |
|---|---|---|---|---|---|
| PHI raw data | None | None | None | dataEditor | None |
| PHI warehouse | dataViewer (RLS) + Fine-Grained Reader on PHI tags | None | None | dataEditor | None |
| Anonymized/research | None | dataViewer | dataViewer (read-only) | None | dataViewer |
| jobUser | In data-warehouse-phi project | In data-warehouse-nonphi | In data-research project | All projects | In relevant project |

**Audit Logging for HIPAA:**

HIPAA requires audit logs of who accessed PHI, when, from where. All Data Access audit logs must be retained for 6 years.

```bash
# Enable Data Access logs for ALL services in PHI projects
gcloud projects set-iam-policy data-warehouse-phi phi-audit-policy.json
# phi-audit-policy.json: enables DATA_READ and DATA_WRITE for all services

# Create log sink with 6-year retention
gcloud logging sinks create phi-audit-sink \
  bigquery.googleapis.com/projects/data-warehouse-phi/datasets/audit_logs \
  --log-filter="resource.type=bigquery_resource" \
  --project=data-warehouse-phi

# Set audit_logs dataset retention to 6 years (2190 days)
bq update --default_table_expiration=189302400 data-warehouse-phi:audit_logs
```

**Data Residency:**

EU employee PII (GDPR): `data-raw-pii` dataset in `europe-west4`. No cross-region copies.
PHI (HIPAA): `data-warehouse-phi` in `US` (multi-region). Cross-region copies require BAA (Business Associate Agreement) review.
Research data: no residency restriction; `US` multi-region for analyst performance.

Org policy enforces this:
```bash
# Per-project resource location constraint (set on folders)
gcloud resource-manager org-policies set-policy \
  --folder=folders/phi-folder \
  --policy=us-only-locations.yaml  # only in:us-locations

gcloud resource-manager org-policies set-policy \
  --folder=folders/pii-eu-folder \
  --policy=eu-only-locations.yaml  # only in:europe-locations
```

**External Researcher Access:**

External researchers never get GCP accounts with project-level IAM. Instead:
1. They authenticate via Google Workspace accounts on an approved list.
2. Access is time-limited via IAM Conditions (3-month research agreement period).
3. They query through BigQuery Studio with `maximum_bytes_billed` set to 1TB/day.
4. VPC-SC research perimeter allows their IPs/identities but blocks any access to PHI/PII perimeters.
5. All queries are logged and reviewed monthly for compliance.

**Compliance Summary:**

| Requirement | Implementation |
|---|---|
| HIPAA access controls | IAM + RLS; no PHI access for data scientists or researchers |
| HIPAA encryption | CMEK on all PHI datasets |
| HIPAA audit logs | Data Access logs, 6-year retention |
| GDPR data residency | EU-location org policy for PII projects |
| GDPR right to access | Audit logs show all accesses per user |
| GDPR right to erasure | Pseudonymization; token → identity mapping deletable |
| Data exfiltration prevention | VPC-SC perimeters block cross-project API calls |

</details>

</article>
