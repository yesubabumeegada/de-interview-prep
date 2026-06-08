---
title: "ADLS Gen2 — Scenarios"
topic: azure
subtopic: adls
content_type: study_material
difficulty_level: mid-level
layer: scenarios
tags: [azure, adls, scenarios, interview, storage-design, security]
---

# ADLS Gen2 — Interview Scenarios

## Scenario 1: Design Storage Architecture for a Healthcare Company

**Question:** A healthcare company needs to store patient data including lab results, imaging metadata, and billing records. Requirements: HIPAA compliance, 7-year retention, different access for clinical vs billing teams, no cross-team data visibility.

**Answer:**

```
Architecture: separate storage accounts per data domain

Storage accounts:
  healthco-clinical   → patient lab results, imaging metadata, clinical notes
  healthco-billing    → billing records, insurance data, payment info
  healthco-analytics  → anonymized/de-identified analytics data

Why separate accounts (not containers):
  1. Customer-managed keys per account (each domain has its own encryption key)
  2. Independent audit logs (clinical vs billing audited separately for HIPAA)
  3. Network rules: clinical accessible from clinical app VNet only; billing from finance VNet
  4. No risk of accidental cross-read via misconfigured role at account level

Security configuration:
  All accounts:
    ✓ Private endpoints only (no public access)
    ✓ CMK via Azure Key Vault (HIPAA BAA requirement)
    ✓ Infrastructure (double) encryption enabled at creation
    ✓ Diagnostic logs → dedicated Log Analytics workspace
    ✓ Defender for Storage enabled (malware scan on uploads)
    ✓ Immutable storage for audit logs (WORM — Write Once Read Many)

Lifecycle / retention:
  HIPAA requires 6-year retention; keep 7 years to be safe
  Bronze (raw): ingest_date → Hot 90d → Cool 1y → Cold 2y → Archive 7y
  Archive: use Azure Blob Lifecycle with Archive tier ($0.00099/GB/month)
  
  Immutable backup:
    Enable time-based retention policy on container: 7 years
    Files cannot be deleted or overwritten until retention period expires
    Satisfies: HIPAA audit log immutability requirement

Access control:
  Clinical team service principal:
    Storage Blob Data Reader on healthco-clinical (RBAC)
    ACL: r-x on /patients/{their_facility_id}/ only
    
  Billing team service principal:
    Storage Blob Data Reader on healthco-billing
    ACL: r-x on /billing/{their_org_id}/ only
    
  ETL service principal:
    Storage Blob Data Contributor on all accounts (write-only from designated Databricks)
    
  Audit query example (Log Analytics):
    StorageBlobLogs
    | where TimeGenerated > ago(90d)
    | where OperationType in ("GetBlob", "PutBlob", "DeleteBlob")
    | where CallerIpAddress !startswith "10.0."  -- flag non-VNet access
    | project TimeGenerated, CallerIpAddress, AuthenticationType, Uri

Cost estimate (1PB total):
  Clinical (500TB): 200TB Hot + 300TB Archive = $3,900 + $300 = $4,200/month
  Billing (300TB): all Archive = $297/month
  Analytics (200TB): Hot for active = $3,600/month
  Total: ~$8,100/month
```

---

## Scenario 2: Accidental Delete Recovery

**Question:** A data engineer accidentally ran `az storage fs directory delete --recursive` on the `/silver/customers/` directory, deleting 500GB of production data. What do you do?

**Answer:**

```
Immediate response:

Step 1 (< 5 minutes): Check if soft delete is enabled
  az storage account blob-service-properties show \
    --account-name mycompany-silver \
    --query deleteRetentionPolicy

  If soft delete enabled (7-day retention): data is recoverable
  If soft delete NOT enabled: data is gone → escalate to incident commander

Step 2 (< 15 minutes): Stop any running pipelines writing to /silver/customers/
  Pause ADF triggers: az datafactory trigger stop ...
  Stop Databricks jobs via API

Step 3: Restore soft-deleted blobs
  # List soft-deleted files
  az storage blob list \
    --container-name silver \
    --account-name mycompany-silver \
    --include d \  # include deleted
    --prefix "customers/" \
    --query "[?deleted].name" \
    --output tsv

  # Restore via Python SDK (no single CLI command for bulk restore):
  from azure.storage.blob import BlobServiceClient
  
  client = BlobServiceClient.from_connection_string(conn_str)
  container = client.get_container_client("silver")
  
  # List deleted blobs under customers/
  deleted_blobs = container.list_blobs(name_starts_with="customers/", include=["deleted"])
  
  restored = 0
  for blob in deleted_blobs:
      if blob.deleted:
          container.get_blob_client(blob.name).undelete_blob()
          restored += 1
  
  print(f"Restored {restored} blobs")

Step 4: Validate
  # Count restored files
  file_count = sum(1 for _ in container.list_blobs(name_starts_with="customers/"))
  total_size = sum(b.size for b in container.list_blobs(name_starts_with="customers/"))
  print(f"Restored: {file_count} files, {total_size / 1e9:.1f} GB")

Step 5: Resume pipelines with validation run
  Run quality check: SELECT COUNT(*) from silver.customers
  Compare to yesterday's count

Post-incident:
  Enable soft delete on all accounts if not already (should have been on)
  Add delete confirmation prompt in runbooks
  Enable Azure RBAC with "Storage Blob Data Contributor" instead of "Storage Blob Data Owner"
    (Contributor: no delete; Owner: can delete)
  Terraform: enforce soft delete via Azure Policy
```

---

## Scenario 3: Storage Cost Spike Investigation

**Question:** Azure bill shows ADLS Gen2 storage costs went from $5,000/month to $20,000/month in one week. Investigate.

**Answer:**

```
Step 1: Azure Cost Management breakdown
  Cost Management → Analyze costs → Group by: Meter Category, Resource
  Identify: which storage account and which meter (storage, transactions, bandwidth)?
  
  If "Data Stored" meter spiked: volume grew unexpectedly
  If "Operations" meter spiked: transaction rate exploded
  If "Data Transfer" meter: egress billing

Step 2: Volume spike (Data Stored)
  Get storage by container via Azure Monitor:
  az monitor metrics list \
    --resource /subscriptions/.../storageAccounts/myaccount \
    --metric "UsedCapacity" \
    --interval PT1H \
    --start-time 2024-01-08 \
    --end-time 2024-01-15
  
  Identify day of spike → check ADF pipeline runs on that day
  Likely causes:
    a) Backfill job wrote 5 years of historical data → 15TB added in 1 day
    b) Bug: wrote to wrong directory (double-writing)
    c) Delta/Iceberg logs not vacuumed (transaction log files accumulated)
    d) Checkpoint files not expiring (Spark/Flink checkpoints grew unbounded)

Step 3: Operations spike
  Azure Monitor → Storage → Transactions metric → split by API operation type
  Common culprit: GetBlob operations exploded
  
  If "LIST" operations spiked:
    Spark job listing millions of small files (e.g., 50M partition problem)
    Fix: compact files (OPTIMIZE), fix partition strategy
  
  If "GetBlob" requests from unknown IP:
    Security incident — check diagnostic logs for unauthorized access

Step 4: Fix
  Volume issue: identify offending prefix → delete extra data → configure lifecycle
  Transaction issue: fix small files problem → OPTIMIZE tables
  Checkpoint issue: add lifecycle rule deleting checkpoints older than 30 days
  
  Implement prevention:
    Azure Monitor alert: StorageCapacity > threshold → notify data team
    Storage Quota: Azure Policy deny if storage > X TB (prevents runaway growth)
    Cost alert: Azure Budget alert at $8,000/month (fires before doubling)
```

---

## Interview Tips

> **Tip 1:** "What ADLS Gen2 features would you use to prevent data loss?" — Defense in depth: (1) Soft delete: 7-day retention, recover from accidental delete. (2) Blob versioning: keep previous version on every overwrite, restore to any previous state. (3) Immutable storage (WORM policy): locked blobs cannot be deleted until retention period — for compliance data. (4) Azure Backup for Storage: vault-based backup with point-in-time recovery. (5) Cross-region replication (RA-GRS): protect against regional outage. For most teams: soft delete + versioning on Silver/Gold covers 99% of scenarios. Immutable storage for regulated data.

> **Tip 2:** "Can ADLS Gen2 support atomic writes for multiple files simultaneously?" — No — ADLS Gen2 has atomic operations per file (single-file atomic rename/overwrite) but not multi-file atomic transactions. For multi-file atomic commits (e.g., Spark job writes 100 files to a partition, all must be visible together or none), the convention is the "rename commit protocol": Spark writes to a temporary `_temporary/` directory, then renames each file to the final path atomically. Delta Lake and Iceberg handle this automatically — they write data files first, then update the transaction log (single file atomic write) making all new files visible simultaneously.

> **Tip 3:** "What's the security implication of disabling public network access on ADLS Gen2?" — When public network access is disabled, only requests from approved networks (private endpoints or VNet service endpoints) are accepted. All Azure services that access the storage account (ADF, Databricks, Synapse) must either: (a) use private endpoints in the same or peered VNet, or (b) be on the approved VNet subnet. This prevents accidental data exposure but requires careful network design. Key risk: if the private endpoint is misconfigured, all jobs fail. Always test connectivity in dev before enabling in production. Use "Allow trusted Microsoft services" exception for Azure Backup, Azure Monitor, and other Azure-native services.
