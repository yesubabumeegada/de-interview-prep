---
title: "ADLS Gen2 — Fundamentals"
topic: azure
subtopic: adls
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [azure, adls, data-lake, storage, hierarchical-namespace]
---

# ADLS Gen2 — Fundamentals

## What Is ADLS Gen2?

Azure Data Lake Storage Gen2 (ADLS Gen2) is Azure's **enterprise-grade cloud object storage** optimized for big data analytics. It combines Azure Blob Storage with a **Hierarchical Namespace (HNS)** to provide true directory semantics and atomic operations.

```
ADLS Gen2 = Azure Blob Storage + Hierarchical Namespace

Why Gen2 over regular Blob Storage:
  Regular Blob:  flat namespace (directories are virtual, emulated via key prefixes)
                 Renaming a directory = copy all objects + delete originals (slow + non-atomic)
                 
  ADLS Gen2:    true directory tree (like POSIX filesystem)
                Rename a directory = metadata-only operation (milliseconds, atomic)
                ACL permissions per file/directory (POSIX-style)
                Required for: Databricks, Spark, HDInsight, big data analytics

Key specs:
  Storage tiers: Hot, Cool, Cold, Archive
  Durability: 11 nines (LRS: 3 copies same datacenter, ZRS: 3 AZs, GRS: 2 regions)
  Max file size: 190.7 TB (single object)
  Max storage account size: effectively unlimited (exabytes)
  Throughput: up to 60 Gbps ingress per account (LRS)
  API compatibility: Azure Blob API + ADLS Gen2 API (both work on same data)
```

---

## Storage Account Structure

```
Hierarchy:

Storage Account (account.dfs.core.windows.net)
  └── Container (like a top-level directory)
        ├── bronze/
        │     ├── orders/
        │     │     ├── ingest_date=2024-01-15/
        │     │     │     └── part-00000.parquet
        │     │     └── ingest_date=2024-01-16/
        │     └── customers/
        ├── silver/
        └── gold/

URL formats:
  Blob API:   https://account.blob.core.windows.net/container/path/file
  ADLS API:   https://account.dfs.core.windows.net/container/path/file
  ABFS URI:   abfss://container@account.dfs.core.windows.net/path/file
  
ABFS (Azure Blob File System):
  Hadoop-compatible filesystem driver for Spark/HDInsight
  abfss:// = TLS-encrypted ABFS
  Used in: Spark jobs, Databricks notebooks, ADF, HDInsight
  Example: spark.read.parquet("abfss://silver@myaccount.dfs.core.windows.net/orders/")
```

---

## Authentication and Access Control

```
Authentication options (choose one per connection):

1. Storage Account Key
   Full admin access to entire storage account
   Use only for: emergency access, service-to-service in trusted VNet
   Risk: leaked key = full data access
   Never use in application code

2. Shared Access Signature (SAS token)
   Time-limited, scope-limited URL token
   Parameters: permissions (r/w/d), start/expiry time, allowed IPs
   Example: read-only access to a specific container for 24 hours
   Use for: temporary external sharing, CDN, signed downloads

3. Azure Active Directory (Azure AD / Entra ID)
   Preferred for: all service-to-service access, user access
   
   a. Managed Identity (System-Assigned or User-Assigned)
      Azure services (ADF, Databricks, Functions) get an identity automatically
      No credentials stored — Azure handles token rotation
      Grant: Storage Blob Data Contributor role to the managed identity
      
   b. Service Principal
      App registration in Azure AD with client_id + client_secret or certificate
      Use for: applications, CI/CD pipelines
      
   c. User identity (interactive)
      Azure CLI: az login → use user's token
      Azure Storage Explorer: OAuth browser login

4. Access Control Lists (ACLs)
   Only available with Hierarchical Namespace (ADLS Gen2)
   POSIX-style: owner/group/other permissions (rwx)
   Set at: container, directory, or file level
   Recommended: use RBAC (roles) at storage account level, ACLs for fine-grained path control
```

---

## Storage Tiers

```
Tier selection by access frequency:

Hot tier:
  Cost: $0.018/GB/month storage (higher)
  Access: $0.0004 per 10K operations (low)
  Latency: milliseconds
  Use for: Active data (Bronze recent, Silver all, Gold all)

Cool tier:
  Cost: $0.01/GB/month storage (lower — 44% cheaper)
  Access: $0.01 per 10K operations (higher)
  Latency: milliseconds
  Minimum retention: 30 days (early deletion charged)
  Use for: Bronze data > 30 days old, infrequently accessed ML features

Cold tier (new):
  Cost: $0.0045/GB/month (75% cheaper than Hot)
  Minimum retention: 90 days
  Use for: Bronze data > 90 days, data archives accessed quarterly

Archive tier:
  Cost: $0.00099/GB/month (cheapest — 94% cheaper than Hot)
  Access: rehydration required (hours)
  Use for: compliance archives, raw data > 1 year old

Lifecycle management: automate tier transitions
  Rule: Bronze files older than 90 days → Cool
  Rule: Bronze files older than 365 days → Archive
  Applied at: blob level (uses Last Modified time)
```

---

## Interview Tips

> **Tip 1:** "Why does ADLS Gen2 require a Hierarchical Namespace for big data analytics?" — Without HNS, directories are virtual (just object key prefixes like `bronze/orders/`). A rename or delete on a "folder" requires listing all objects under that prefix and copying/deleting each one individually — for a million files, this is millions of API calls and is non-atomic (failure halfway = corrupted state). With HNS, rename is a single metadata operation on the directory entry — milliseconds, atomic. Spark workflows depend on atomic renames for commit protocols (file-based commits use rename to make output visible).

> **Tip 2:** "What's the difference between RBAC and ACLs in ADLS Gen2?" — RBAC (Role-Based Access Control) is at the storage account or container level: `Storage Blob Data Reader`, `Storage Blob Data Contributor`, `Storage Blob Data Owner`. It grants access to all data in the scope. ACLs are POSIX-style permissions on individual files and directories — you can grant read access to `/silver/finance/` only, without access to `/silver/hr/`. Best practice: use RBAC for coarse-grained access (service accounts), ACLs for fine-grained path-level access (specific team directories).

> **Tip 3:** "What is an ABFS URI and when do you use it?" — ABFS (Azure Blob File System) is the Hadoop-compatible filesystem driver for Azure storage. The URI format `abfss://container@account.dfs.core.windows.net/path` is used in Spark, Databricks, and HDInsight to read/write ADLS Gen2 data. The `s` in `abfss://` means TLS-encrypted (always use this in production). Without this driver, Spark would use the older WASB driver (Azure Blob-based, no atomic rename, slower). Always configure Spark to use ABFS for ADLS Gen2 access.
