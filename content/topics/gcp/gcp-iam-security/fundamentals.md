---
title: "GCP IAM & Security — Fundamentals"
topic: gcp
subtopic: gcp-iam-security
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [gcp, iam, security, service-accounts, bigquery, interview]
---

# GCP IAM & Security — Fundamentals

Identity and Access Management (IAM) is the foundation of GCP security. For data engineers, understanding IAM means knowing who can read your data, who can run your pipelines, and how to build systems where code runs with least-privilege credentials — never a human's credentials. Interviewers regularly test IAM fundamentals because security mistakes in data engineering are catastrophically expensive (data breaches, compliance violations).

---

## The IAM Mental Model

Think of GCP IAM as a three-part system:

- **Who**: a principal (user, group, service account, or domain)
- **Can do what**: a role (a collection of permissions)
- **On which resource**: a resource (project, dataset, table, bucket)

An IAM **policy** binds principals to roles on a specific resource:

```
Policy on resource "bigquery.googleapis.com/projects/my-project/datasets/sales":
  Binding 1: role=bigquery.dataViewer  → members=[user:alice@company.com, group:analysts@company.com]
  Binding 2: role=bigquery.dataEditor  → members=[serviceAccount:etl-sa@my-project.iam.gserviceaccount.com]
```

---

## Resource Hierarchy

GCP resources are organized in a strict hierarchy. IAM policies are **inherited downward** — a policy set at the organization level applies to all projects, folders, and resources within it.

```
Organization (company.com)
    └─ Folder: production
        ├─ Project: data-warehouse-prod
        │   ├─ BigQuery dataset: sales
        │   │   └─ Table: orders
        │   └─ Cloud Storage bucket: raw-data-prod
        └─ Project: ml-platform-prod
    └─ Folder: development
        └─ Project: data-warehouse-dev
```

**Key inheritance rule**: IAM bindings are additive down the hierarchy. If Alice has `roles/viewer` at the Organization level, she has viewer access to every resource in the organization — you cannot take it away at the project or resource level (there's no "deny" in classic IAM, though IAM Deny policies were added in 2022).

**Data engineering implication**: be careful about granting broad roles at the organization or folder level. Grant at the lowest level that makes practical sense.

---

## Principals: Who Gets Access

| Principal Type | Identifier | Use Case |
|---|---|---|
| Google Account | `user:alice@gmail.com` | Individual human users |
| Google Workspace Group | `group:analysts@company.com` | Teams of humans |
| Service Account | `serviceAccount:etl@project.iam.gserviceaccount.com` | Code/automation |
| Google Workspace Domain | `domain:company.com` | All users in a domain |
| `allAuthenticatedUsers` | special | Any Google-authenticated user |
| `allUsers` | special | Anyone (including unauthenticated) — avoid for data |

**Data engineering principle**: production pipelines should always run as **service accounts**, never as user accounts. User accounts require OAuth tokens that expire and depend on the human being employed.

---

## Roles: What Can Be Done

### Predefined Roles

Google-managed roles that bundle permissions for common use cases. You cannot modify predefined roles.

```bash
# List predefined BigQuery roles
gcloud iam roles list --filter="name:roles/bigquery"
```

**Critical BigQuery roles for data engineering:**

| Role | Key Permissions | Typical Assignee |
|---|---|---|
| `roles/bigquery.dataViewer` | Read tables and datasets | BI tools, analysts (read-only) |
| `roles/bigquery.dataEditor` | Read + write tables | ETL service accounts |
| `roles/bigquery.dataOwner` | Read + write + manage metadata | Dataset owners |
| `roles/bigquery.jobUser` | Run jobs (queries) in the project | Any user who runs queries |
| `roles/bigquery.user` | `jobUser` + ability to view datasets | General analyst role |
| `roles/bigquery.admin` | Full control of BigQuery | Data platform team only |

**Common mistake**: granting `roles/bigquery.dataViewer` WITHOUT `roles/bigquery.jobUser`. The user can see tables but cannot run queries — they'll get "Permission denied: missing bigquery.jobs.create" errors.

```bash
# Correct: grant both for read-only analyst access
gcloud projects add-iam-policy-binding my-project \
  --member="user:analyst@company.com" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding my-project \
  --member="user:analyst@company.com" \
  --role="roles/bigquery.dataViewer"
```

### Custom Roles

When predefined roles are too broad, you create custom roles with exactly the permissions needed.

```bash
# Create a custom role: can read BigQuery data but NOT create jobs (for billing control)
gcloud iam roles create BigQueryReadOnlyNoJobs \
  --project=my-project \
  --title="BigQuery Read Only (No Jobs)" \
  --description="Read BigQuery data via authorized views only" \
  --permissions=bigquery.datasets.get,bigquery.tables.get,bigquery.tables.list,bigquery.tables.getData

# Create from YAML:
cat > custom_role.yaml <<EOF
title: "BigQuery Read Only No Jobs"
description: "Read-only BigQuery access without job creation"
stage: GA
includedPermissions:
  - bigquery.datasets.get
  - bigquery.tables.get
  - bigquery.tables.list
  - bigquery.tables.getData
EOF

gcloud iam roles create BigQueryReadNoJobs --project=my-project --file=custom_role.yaml
```

---

## Service Accounts: Identity for Code

Service accounts are non-human identities that applications and automated pipelines use to authenticate to GCP APIs.

### Service Account Types

| Type | Where Keys Live | Use Case |
|---|---|---|
| User-managed service accounts | Created by you, keys managed separately | Most production workloads |
| Default service accounts | Auto-created by GCP for Compute, App Engine | Default for VMs (avoid using directly) |
| Google-managed service accounts | Managed by GCP services | Internal GCP service-to-service |

### Workload Identity (Preferred)

Instead of downloading service account keys (JSON files), Workload Identity Federation allows GKE pods or external workloads to impersonate a service account using short-lived tokens. No key files to rotate or leak.

```bash
# Create a service account for a data pipeline
gcloud iam service-accounts create etl-pipeline-sa \
  --display-name="ETL Pipeline Service Account" \
  --project=my-project

# Grant it BigQuery Editor on a specific dataset
gcloud projects add-iam-policy-binding my-project \
  --member="serviceAccount:etl-pipeline-sa@my-project.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

# For GKE: bind a Kubernetes service account to this GCP service account (Workload Identity)
gcloud iam service-accounts add-iam-policy-binding \
  etl-pipeline-sa@my-project.iam.gserviceaccount.com \
  --role="roles/iam.workloadIdentityUser" \
  --member="serviceAccount:my-project.svc.id.goog[my-namespace/my-k8s-sa]"
```

**Key management rule**: if you must use service account keys (JSON), treat them like passwords:
- Never commit to git.
- Rotate every 90 days.
- Store in Secret Manager, not environment variables or config files.
- Prefer Workload Identity or Application Default Credentials (ADC) over downloaded keys.

---

## IAM Policy Binding: Terraform

In production, IAM policies are managed as code:

```hcl
# Grant analysts read access to the sales dataset
resource "google_bigquery_dataset_iam_binding" "analyst_viewer" {
  project    = "my-project"
  dataset_id = "sales"
  role       = "roles/bigquery.dataViewer"

  members = [
    "group:analysts@company.com",
    "serviceAccount:looker-sa@my-project.iam.gserviceaccount.com"
  ]
}

# Grant ETL service account data editor
resource "google_bigquery_dataset_iam_member" "etl_editor" {
  project    = "my-project"
  dataset_id = "sales"
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:etl-pipeline-sa@my-project.iam.gserviceaccount.com"
}
```

**`iam_binding` vs. `iam_member`**: `iam_binding` is **authoritative** — it replaces ALL members for that role. `iam_member` is **additive** — it adds one member without touching others. Use `iam_member` when you don't want Terraform to manage the full list.

---

## Common Junior Mistakes

1. **Granting `roles/owner` to a service account** — owners have full control including billing and IAM modifications. ETL pipelines only need data access roles.
2. **Committing service account key JSON to git** — this is a career-limiting security incident. Use Secret Manager or Workload Identity.
3. **Using your personal Google account in production pipelines** — if you leave the company, the pipeline breaks. Always use service accounts.
4. **Granting `roles/bigquery.admin` to all data engineers** — admin can delete datasets, modify access controls. Use `dataOwner` on specific datasets instead.
5. **Forgetting `roles/bigquery.jobUser`** — always pair with data access roles for users who need to run queries.
