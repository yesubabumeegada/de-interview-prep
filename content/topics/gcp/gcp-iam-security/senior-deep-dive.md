---
title: "GCP IAM & Security — Senior Deep Dive"
topic: gcp
subtopic: gcp-iam-security
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [gcp, iam, security, vpc-service-controls, data-governance, interview]
---

# GCP IAM & Security — Senior Deep Dive

At the senior level, you design the security architecture for a data platform: the layered defense model, service account governance at scale, the VPC Service Controls configuration for regulated environments, and the audit framework that satisfies SOC 2, HIPAA, or GDPR requirements. Interviewers at this level ask "how would you design it?" not "what does it do?"

---

## Defense in Depth for GCP Data Platforms

A mature data platform security design has multiple independent layers. Breaching any single layer should not expose sensitive data.

```
Layer 1: Identity (IAM)
    Who can access what resources?
    → Least-privilege roles, no broad predefined roles

Layer 2: Network (VPC Service Controls)
    Which API calls cross project/network boundaries?
    → Perimeter blocks data exfiltration even with valid credentials

Layer 3: Data (Column/Row Security)
    Which columns and rows can a user see?
    → BigQuery Policy Tags, Row Access Policies

Layer 4: Encryption (CMEK)
    Who controls the encryption keys?
    → Customer-managed encryption keys for regulated data

Layer 5: Audit (Cloud Audit Logs)
    Who accessed what, when?
    → Data Access logs exported to BigQuery, 7-year retention
```

**Senior interview insight**: defense in depth means an attacker who compromises a Google account cannot exfiltrate PII even if:
- IAM appears to grant access (VPC-SC blocks cross-project API calls).
- VPC-SC is misconfigured (BigQuery Policy Tags block column access).
- Policy Tags are absent (CMEK keys are revoked for terminated employees).

---

## Service Account Governance at Scale

In a 50-project GCP organization, service account sprawl is a common security risk: hundreds of service accounts, many with excessive permissions, some created for one-time tasks and never deleted.

### Service Account Lifecycle Management

```bash
# Audit: find service accounts unused in the last 90 days
gcloud asset search-all-iam-policies \
  --scope="organizations/123456789" \
  --query="policy.bindings.members:serviceAccount" \
  --format=json | jq -r '.[].policy.bindings[].members[]' | \
  grep serviceAccount | sort -u > all_sa.txt

# Check last authentication time for each SA
# (use Policy Troubleshooter or Cloud Asset Inventory)
gcloud asset search-all-resources \
  --asset-types="iam.googleapis.com/ServiceAccount" \
  --scope="organizations/123456789" \
  --format="csv(name,additionalAttributes.lastAuthenticatedTime)"
```

### Workload Identity Federation: No Keys

The gold standard for service account authentication from external systems (GitHub Actions, on-prem servers, other clouds) is Workload Identity Federation — exchange an external OIDC/SAML token for a short-lived GCP access token.

```bash
# Set up WIF for GitHub Actions
gcloud iam workload-identity-pools create github-pool \
  --project=my-project \
  --location=global \
  --display-name="GitHub Actions Pool"

gcloud iam workload-identity-pools providers create-oidc github-provider \
  --project=my-project \
  --location=global \
  --workload-identity-pool=github-pool \
  --display-name="GitHub Provider" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" \
  --issuer-uri="https://token.actions.githubusercontent.com"

# Allow the specific GitHub repo to impersonate the SA
gcloud iam service-accounts add-iam-policy-binding \
  etl-pipeline@my-project.iam.gserviceaccount.com \
  --project=my-project \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/123/locations/global/workloadIdentityPools/github-pool/attribute.repository/my-org/my-repo"
```

```yaml
# GitHub Actions workflow using WIF
jobs:
  deploy:
    permissions:
      id-token: write  # Required for WIF
      contents: read
    steps:
      - uses: google-github-actions/auth@v2
        with:
          workload_identity_provider: "projects/123/locations/global/workloadIdentityPools/github-pool/providers/github-provider"
          service_account: "etl-pipeline@my-project.iam.gserviceaccount.com"
      - run: bq query --use_legacy_sql=false "SELECT COUNT(*) FROM my_table"
```

No JSON key files. The GitHub Actions OIDC token is exchanged for a 1-hour GCP access token automatically.

### Service Account Impersonation

Instead of granting a service account direct permissions everywhere, impersonation allows a trusted SA to temporarily act as another SA — useful for separation of concerns:

```bash
# Allow a data engineer's personal account to impersonate the ETL SA (for testing)
gcloud iam service-accounts add-iam-policy-binding \
  etl-pipeline@my-project.iam.gserviceaccount.com \
  --member="user:engineer@company.com" \
  --role="roles/iam.serviceAccountTokenCreator"

# Engineer can now generate tokens for the SA:
gcloud auth print-access-token \
  --impersonate-service-account=etl-pipeline@my-project.iam.gserviceaccount.com
```

**Security benefit**: the engineer's personal account doesn't need BigQuery access — they impersonate the SA for testing, and all audit logs show the original user AND the impersonated SA.

---

## VPC Service Controls: Advanced Configuration

### Dry-Run Mode

Before enforcing a VPC-SC perimeter, run in dry-run mode — violations are logged but not blocked. Essential for discovering legitimate traffic patterns before enforcement breaks production.

```bash
# Update perimeter to dry-run mode
gcloud access-context-manager perimeters update data_perimeter \
  --policy=accessPolicies/123456789 \
  --dry-run-resources="projects/111111111,projects/222222222"

# After testing, promote dry-run config to enforced:
gcloud access-context-manager perimeters update data_perimeter \
  --policy=accessPolicies/123456789 \
  --apply-dry-run
```

### Access Levels for Service Accounts

When a pipeline service account needs to call BigQuery APIs from outside the perimeter (e.g., from a Dataflow job in a different project), you create an Access Level that includes the SA identity:

```yaml
# access_level.yaml
name: accessPolicies/123456789/accessLevels/etl_service_accounts
title: ETL Service Accounts
basic:
  conditions:
    - principals:
        - "serviceAccount:etl-pipeline@my-project.iam.gserviceaccount.com"
        - "serviceAccount:dataflow-sa@etl-project.iam.gserviceaccount.com"
```

### Ingress/Egress Rules

The modern VPC-SC model (v2) uses explicit ingress/egress rules instead of perimeter bridges — more granular and auditable:

```json
// Ingress rule: allow Looker Studio to read from BigQuery inside perimeter
{
  "ingressFrom": {
    "identities": ["serviceAccount:looker-sa@looker-project.iam.gserviceaccount.com"],
    "sources": [{"resource": "projects/looker-project"}]
  },
  "ingressTo": {
    "operations": [{
      "serviceName": "bigquery.googleapis.com",
      "methodSelectors": [{"method": "BigQueryStorage.ReadRows"}, {"method": "TableService.ListTables"}]
    }],
    "resources": ["projects/data-warehouse-prod"]
  }
}
```

This is more precise than perimeter bridges — only Looker's SA can call specific BigQuery read methods, not the full BigQuery API surface.

---

## CMEK: Customer-Managed Encryption Keys

By default, GCP encrypts data at rest with Google-managed keys. For regulated industries (healthcare, finance, government), **Customer-Managed Encryption Keys (CMEK)** give you control over the encryption key lifecycle.

```bash
# Create a Cloud KMS key ring and key
gcloud kms keyrings create bq-keyring --location=us

gcloud kms keys create bq-encryption-key \
  --keyring=bq-keyring \
  --location=us \
  --purpose=encryption

# Grant BigQuery service account permission to use the key
gcloud kms keys add-iam-policy-binding bq-encryption-key \
  --keyring=bq-keyring \
  --location=us \
  --member="serviceAccount:bq-sa@my-project.iam.gserviceaccount.com" \
  --role="roles/cloudkms.cryptoKeyEncrypterDecrypter"
```

```sql
-- Create a BigQuery dataset with CMEK
-- (set via Console or bq CLI at dataset creation; SQL has limited support)
bq mk --dataset \
  --default_kms_key=projects/my-project/locations/us/keyRings/bq-keyring/cryptoKeys/bq-encryption-key \
  my-project:sensitive_data
```

**Key revocation**: if you revoke access to the KMS key (e.g., by destroying the key version), all BigQuery data encrypted with that key becomes permanently inaccessible — including to Google. This is the nuclear option for data deletion in regulated environments, and is documented as a GDPR "right to erasure" mechanism.

---

## Terraform: Complete Security Baseline

```hcl
# Complete IAM/security baseline for a data engineering project

# Org policy: no SA key creation
resource "google_organization_policy" "disable_sa_keys" {
  org_id     = var.org_id
  constraint = "constraints/iam.disableServiceAccountKeyCreation"
  boolean_policy { enforced = true }
}

# Org policy: restrict resource locations
resource "google_organization_policy" "resource_locations" {
  org_id     = var.org_id
  constraint = "constraints/gcp.resourceLocations"
  list_policy {
    allow {
      values = ["in:us-locations"]
    }
  }
}

# Data pipeline service account
resource "google_service_account" "etl_pipeline" {
  account_id   = "etl-pipeline"
  display_name = "ETL Pipeline Service Account"
  project      = var.project_id
}

# Least-privilege BigQuery access
resource "google_bigquery_dataset_iam_member" "etl_editor" {
  project    = var.project_id
  dataset_id = "data_warehouse"
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.etl_pipeline.email}"
}

resource "google_project_iam_member" "etl_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.etl_pipeline.email}"
}

# Secret Manager access for database credentials
resource "google_secret_manager_secret_iam_member" "etl_secret_access" {
  project   = var.project_id
  secret_id = "postgres-prod-connection"
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.etl_pipeline.email}"
}

# Audit log export to BigQuery
resource "google_logging_project_sink" "bq_audit_sink" {
  name    = "bq-audit-sink"
  project = var.project_id
  destination = "bigquery.googleapis.com/projects/${var.project_id}/datasets/audit_logs"
  filter  = "resource.type=\"bigquery_resource\""

  bigquery_options {
    use_partitioned_tables = true
  }
}

# Grant the sink's SA writer access to the audit dataset
resource "google_bigquery_dataset_iam_member" "audit_sink_writer" {
  project    = var.project_id
  dataset_id = "audit_logs"
  role       = "roles/bigquery.dataEditor"
  member     = google_logging_project_sink.bq_audit_sink.writer_identity
}
```

---

## Compliance Frameworks Mapping

| Control | GCP Mechanism | Compliance Relevance |
|---|---|---|
| Access control | IAM roles + resource hierarchy | SOC 2 CC6.1, HIPAA 164.312(a) |
| PII protection | BigQuery Policy Tags + RLS | GDPR Art. 25, CCPA |
| Data exfiltration prevention | VPC Service Controls | SOC 2 CC6.7, PCI DSS 12.3 |
| Encryption at rest | CMEK (Cloud KMS) | HIPAA 164.312(a)(2)(iv), FedRAMP |
| Audit logging | Data Access audit logs | SOC 2 CC7.2, HIPAA 164.312(b) |
| Secret management | Secret Manager | SOC 2 CC6.1 |
| Org policies | Org policy constraints | SOC 2 CC5.2 |
| Data deletion | CMEK key revocation | GDPR Art. 17 (right to erasure) |

**Senior interview synthesis**: the difference between a junior and senior security design is that the senior can articulate which control addresses which threat model. VPC-SC addresses the threat of credential compromise enabling exfiltration; CMEK addresses the threat of storage layer breach or employee departure; Policy Tags address the threat of over-privileged data access within the organization.
