---
title: "ADLS Gen2 — Senior Deep Dive"
topic: azure
subtopic: adls
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [azure, adls, security, encryption, performance, multi-region, access-patterns]
---

# ADLS Gen2 — Senior Deep Dive

## ADLS Gen2 Internal Architecture

```
Physical storage model:
  ADLS Gen2 built on Azure Blob Storage (FlatNamespace or HNS)
  Data split into blocks: up to 4,000 blocks × 4,000 MB = 190.7 TB max per blob
  
  HNS implementation:
  - Directory entries stored in Azure's distributed metadata store
  - File rename = metadata-only update (single distributed transaction)
  - Without HNS: rename = copy blocks + delete original (O(n) blocks)
  - With HNS: rename = O(1) metadata update
  
  Write modes:
    Block blob (default): write data in blocks, commit via Put Block List
    Append blob: sequential append only (not ADLS Gen2 compatible)
    Page blob: random read/write (VHD disks, not analytics)
  
  Concurrency:
  - Last-writer-wins for same path (no built-in optimistic concurrency)
  - Atomic HNS rename used by Spark for "atomic commit" pattern
  - Multiple writers to different paths: fully parallel, no locking
  - Multiple writers to same file: use ETags for conditional writes

  Storage account redundancy options:
    LRS:  3 replicas, same datacenter (99.999999999% durability)
    ZRS:  3 replicas, 3 availability zones (99.9999999999% durability)
    GRS:  6 replicas, 2 regions (LRS primary + LRS secondary) — async replication
    GZRS: ZRS primary + GRS secondary (highest durability + availability)
    
  For analytics: ZRS (production) or LRS (cost-optimized non-production)
  For disaster recovery: GZRS (adds ~40% cost vs LRS)
```

---

## Encryption and Key Management

```python
# ADLS Gen2 encryption: all data encrypted at rest by default
# AES-256 encryption, transparent to clients

# Encryption key options:

# Option 1: Microsoft-managed keys (default)
# Microsoft rotates keys automatically
# Zero configuration, free
# Compliance: meets most standard requirements

# Option 2: Customer-managed keys (CMK) with Azure Key Vault
# You own and control the encryption key
# Required for: FedRAMP High, HIPAA BAA, financial regulations
# Key rotation: manual (in Key Vault, then update storage account reference)

# Enable CMK via Azure CLI:
# az storage account update \
#   --name myaccount \
#   --resource-group rg-data \
#   --encryption-key-source Microsoft.Keyvault \
#   --encryption-key-vault https://myvault.vault.azure.net \
#   --encryption-key-name storage-encryption-key \
#   --encryption-key-version <key-version>

# Option 3: Customer-provided keys (CPK)
# Client provides the key per-request (key never stored in Azure)
# Extreme security requirement — key must be sent with each API call
# Complex to implement, rarely used

# Infrastructure encryption (double encryption):
# AES-256 at storage layer + AES-256 at infrastructure layer
# For ultra-high compliance requirements
# Enable: storage account creation only (cannot enable later)

# Key rotation best practice:
# CMK rotation: Key Vault key rotation policy (90-day or 180-day)
# When rotated: Azure automatically re-wraps the data encryption key
# Data: NOT re-encrypted (only the key-wrapping key changes, fast operation)
# Account keys: rotate via portal or:
# az storage account keys renew --account-name myaccount --key primary
```

---

## Advanced Access Pattern Design

```python
# Multi-zone architecture: separate storage accounts per zone (not containers)
# Advantages: independent lifecycle policies, separate firewall rules, independent keys

# Bronze account: mycompany-bronze  (HNS enabled, LRS, Cool default tier)
# Silver account: mycompany-silver  (HNS enabled, ZRS, Hot default tier)
# Gold account:   mycompany-gold    (HNS enabled, ZRS, Hot default tier)

# Cross-account access pattern:
# ADF, Databricks, Synapse: each service needs Storage Blob Data Contributor
# on each account separately (or at resource group level)

# Why separate accounts (not just separate containers):
# 1. Lifecycle policies: different rules per zone (Bronze → Archive, Gold → never archive)
# 2. Network access: Gold might allow Power BI IPs; Bronze more restricted
# 3. CMK: separate encryption keys per zone (compliance requirement)
# 4. Billing: separate storage accounts = clear cost attribution per zone
# 5. Quota: no cross-zone interference (storage account has 5 PiB soft limit)

# Directory naming conventions (example):
# silver/
#   orders/
#     year=2024/month=01/day=15/
#       _delta_log/          # Delta transaction log (auto-created)
#       part-00000-uuid.parquet
#   customers/
#     current/               # SCD Type 1 (overwrite)
#     history/               # SCD Type 2 (append versions)
# gold/
#   reporting/
#     daily_revenue/year=2024/month=01/day=15/
#   ml_features/
#     customer_features/snapshot_date=2024-01-15/

# Access patterns by consumer:
def configure_spark_adls_access(spark, account_name: str, client_id: str, client_secret: str, tenant_id: str):
    """Configure Spark/Databricks to access ADLS Gen2 via Service Principal."""
    spark.conf.set(
        f"fs.azure.account.auth.type.{account_name}.dfs.core.windows.net",
        "OAuth"
    )
    spark.conf.set(
        f"fs.azure.account.oauth.provider.type.{account_name}.dfs.core.windows.net",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.id.{account_name}.dfs.core.windows.net",
        client_id
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.secret.{account_name}.dfs.core.windows.net",
        client_secret
    )
    spark.conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{account_name}.dfs.core.windows.net",
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    )
```

---

## Multi-Region and Disaster Recovery

```
ADLS Gen2 DR patterns:

Active-Passive (most common):
  Primary region: all writes
  Secondary region: GRS replica (read-only, async replication ~15 min RPO)
  Failover: Microsoft-initiated (outage) or customer-initiated (planned)
  After failover: secondary becomes primary, data up to RPO point recovered
  
  Limitation: failover is not instant — takes 1 hour
  During failover: storage inaccessible (RPO ~15 min, RTO ~1 hour)

Active-Active (for zero-downtime requirement):
  Two storage accounts in two regions
  Application writes to both via custom code or EventGrid
  No native Azure solution — requires application-level dual-write
  
  Pattern:
    Write path: app → Event Hub → Flink/Functions → write to both regions
    Read path: read from nearest region
    Consistency: eventual (slight drift between regions)

Cross-region replication via ADF:
  Schedule daily: ADF Copy Activity → bronze/silver → secondary region account
  RTO: secondary is always 24h behind primary
  Simpler but coarser RPO than GRS

Key design decisions:
  Analytics data (can be rebuilt from source): LRS sufficient, GZRS overkill
  Raw data (cannot be rebuilt): ZRS or GRS
  Regulatory (data sovereignty): LRS only (stays in single datacenter)
```

---

## Interview Tips

> **Tip 1:** "How would you design storage access for a multi-tenant SaaS company using ADLS Gen2?" — Create a separate directory per tenant (`silver/tenant_a/orders/`, `silver/tenant_b/orders/`). Assign each tenant's service principal exactly the ACLs needed for their directory — `tenant_a_sp` gets `rwx` on `silver/tenant_a/` only. At the parent `silver/` level: all tenant SPs get `--x` (execute = directory traversal) but not `r--` (cannot list all tenants). At container level: grant `--x` traverse. This means tenant A can reach its own data but cannot list or read tenant B's directory. Audit all access via diagnostic logs to Log Analytics.

> **Tip 2:** "What happens to data writes during an Azure region outage if using GRS?" — GRS asynchronously replicates to a secondary region with ~15-minute RPO (data written in the last 15 minutes before outage may be lost). During the outage: the secondary region is read-only (RA-GRS) or inaccessible (GRS without RA). Microsoft must initiate failover; self-initiated failover available in preview. For analytics: most teams accept the RPO for cold/warm data. For raw Bronze data that cannot be replicated from source: use RA-GRS and immediately fail over reads to secondary during outage. For zero RPO: active-active dual-write (custom application logic required).

> **Tip 3:** "How do you audit ADLS Gen2 access for compliance?" — Enable diagnostic settings: Storage → Diagnostic Settings → add Log Analytics workspace, select "StorageRead", "StorageWrite", "StorageDelete" categories. This logs every blob operation with: identity (who), operation, time, status, bytes. KQL query in Log Analytics: `StorageBlobLogs | where OperationType == "GetBlob" | where AuthenticationType == "SAS" | project TimeGenerated, CallerIpAddress, Uri`. For Purview: register ADLS Gen2 as a data source and run classification scans (auto-detects PII columns). Combine: Log Analytics for access patterns, Purview for data classification.

## ⚡ Cheat Sheet

**ADLS Gen2 vs Blob Storage**
- ADLS Gen2 = Blob Storage + hierarchical namespace (HNS) enabled
- HNS enables: atomic directory rename/delete (O(1)); POSIX-compatible ACLs
- Never use Blob Storage for data lake — always enable HNS

**Storage tiers**
| Tier | Access | Cost | Min retention |
|---|---|---|---|
| Hot | Frequent | Highest storage | None |
| Cool | Infrequent | Lower storage, retrieval fee | 30 days |
| Cold | Rare | Very low storage | 90 days |
| Archive | Near-zero | Lowest storage, high retrieval | 180 days |

**Access control (dual model)**
- RBAC: Azure roles (Storage Blob Data Reader/Contributor/Owner) — coarse-grained
- POSIX ACLs: fine-grained on files/dirs; `getfacl` / `setfacl` pattern
- Recommended: RBAC for broad access + ACLs for fine-grained per-directory control
- Managed identities: always prefer over access keys/SAS for service access

**Performance**
- Partitioning: `container/year=YYYY/month=MM/day=DD/` — enables partition pruning in Synapse/Databricks
- File size: target 256 MB–1 GB for Parquet/ORC; small files (<1 MB) hurt performance
- Premium tier: SSD-backed; 10× lower latency; use for streaming ingestion

**Lifecycle management**
```json
{"rules": [{"name": "cool-after-30", "type": "Lifecycle",
  "definition": {"filters": {"blobTypes": ["blockBlob"], "prefixMatch": ["raw/"]},
    "actions": {"baseBlob": {"tierToCool": {"daysAfterModificationGreaterThan": 30},
                              "tierToArchive": {"daysAfterModificationGreaterThan": 90}}}}}]}
```

**Key integration patterns**
- Databricks: mount with service principal + OAuth; or use direct ABFS path `abfss://container@account.dfs.core.windows.net/`
- Synapse Analytics: built-in managed private endpoint; linked service with managed identity
- ADF: ADLS Gen2 connector; copy activity; Data Flow for transform
