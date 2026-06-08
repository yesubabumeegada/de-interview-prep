---
title: "ADLS Gen2 — Intermediate"
topic: azure
subtopic: adls
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [azure, adls, security, lifecycle, performance, networking]
---

# ADLS Gen2 — Intermediate

## Configuring ADLS Gen2 with Python SDK

```python
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential

# Authenticate with Managed Identity (preferred in Azure)
credential = DefaultAzureCredential()
service_client = DataLakeServiceClient(
    account_url="https://myaccount.dfs.core.windows.net",
    credential=credential
)

# Create containers (file systems)
def setup_lakehouse_containers(service_client):
    for container in ["bronze", "silver", "gold", "checkpoints"]:
        try:
            service_client.create_file_system(file_system=container)
            print(f"Created container: {container}")
        except Exception as e:
            if "ContainerAlreadyExists" in str(e):
                print(f"Container exists: {container}")
            else:
                raise

# Create directory
fs_client = service_client.get_file_system_client("silver")
dir_client = fs_client.create_directory("orders/year=2024/month=01")

# Upload file
with open("orders_2024_01.parquet", "rb") as f:
    file_client = dir_client.create_file("orders_2024_01.parquet")
    file_contents = f.read()
    file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
    file_client.flush_data(len(file_contents))

# Set ACLs on directory
acl = "user::rwx,group::r-x,other::---,user:service-principal-id:rwx"
dir_client.set_access_control(acl=acl)

# List files in directory
paths = fs_client.get_paths(path="orders/year=2024/month=01")
for path in paths:
    print(f"  {path.name} ({path.content_length} bytes, modified: {path.last_modified})")
```

---

## Lifecycle Management Automation

```python
import json
from azure.mgmt.storage import StorageManagementClient
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
storage_client = StorageManagementClient(credential, subscription_id)

def configure_lakehouse_lifecycle(resource_group: str, account_name: str):
    lifecycle_policy = {
        "rules": [
            {
                "name": "bronze-tiering",
                "enabled": True,
                "type": "Lifecycle",
                "definition": {
                    "filters": {
                        "blobTypes": ["blockBlob"],
                        "prefixMatch": ["bronze/"]
                    },
                    "actions": {
                        "baseBlob": {
                            "tierToCool": {
                                "daysAfterModificationGreaterThan": 90
                            },
                            "tierToCold": {
                                "daysAfterModificationGreaterThan": 365
                            },
                            "tierToArchive": {
                                "daysAfterModificationGreaterThan": 1825  # 5 years
                            }
                        }
                    }
                }
            },
            {
                "name": "silver-tiering",
                "enabled": True,
                "type": "Lifecycle",
                "definition": {
                    "filters": {
                        "blobTypes": ["blockBlob"],
                        "prefixMatch": ["silver/"]
                    },
                    "actions": {
                        "baseBlob": {
                            "tierToCool": {
                                "daysAfterModificationGreaterThan": 180
                            }
                        }
                    }
                }
            },
            {
                "name": "checkpoints-expiry",
                "enabled": True,
                "type": "Lifecycle",
                "definition": {
                    "filters": {
                        "blobTypes": ["blockBlob"],
                        "prefixMatch": ["checkpoints/"]
                    },
                    "actions": {
                        "baseBlob": {
                            "delete": {
                                "daysAfterModificationGreaterThan": 30
                            }
                        }
                    }
                }
            }
        ]
    }

    storage_client.management_policies.create_or_update(
        resource_group_name=resource_group,
        account_name=account_name,
        management_policy_name="default",
        properties={"policy": lifecycle_policy}
    )
    print("Lifecycle policy applied")
```

---

## Network Security Configuration

```
ADLS Gen2 network security layers:

1. Firewall rules (IP-based):
   Allow specific IP ranges or Azure service bypass
   Example: allow only 10.0.0.0/16 (internal VNet) + Azure services
   Portal: Storage Account → Networking → Firewalls and virtual networks

2. Private Endpoints (recommended for production):
   Create private IP in your VNet for the storage account
   Storage account accessible only via private IP (not public internet)
   DNS: myaccount.dfs.core.windows.net → resolves to 10.0.1.5 (private IP) via Private DNS Zone
   
   Setup with Azure CLI:
   az network private-endpoint create \
     --name pe-adls-silver \
     --resource-group rg-data \
     --vnet-name vnet-data \
     --subnet subnet-private \
     --private-connection-resource-id /subscriptions/.../storageAccounts/myaccount \
     --group-id dfs \
     --connection-name adls-connection

3. Service Endpoints (lighter alternative):
   Traffic from VNet to storage stays on Azure backbone (not internet)
   Storage account allows traffic from specific subnet
   Simpler than private endpoints but storage still has public IP

4. Defender for Storage:
   Microsoft Defender scans for: unusual access patterns, malware uploads, sensitive data
   Alert: "Unusual number of file deletes" → potential ransomware
   Malware scanning: scans new blobs for malicious content
   Cost: ~$0.15/GB scanned

Security checklist for production ADLS:
  ✓ Private endpoint or at minimum firewall rules
  ✓ All access via Managed Identity (no account keys in code)
  ✓ Key rotation policy (even if using MI, rotate account keys annually)
  ✓ Soft delete enabled (undelete files up to 30 days after deletion)
  ✓ Versioning for Silver/Gold (keep previous version on overwrite)
  ✓ Diagnostic logs → Log Analytics (track all access)
  ✓ Defender for Storage enabled
```

---

## Performance Optimization

```python
# Performance tuning for large-scale ADLS access from Spark

# Spark configuration for optimal ADLS Gen2 throughput:
spark_config = {
    # Use ABFS driver (not WASB)
    "fs.azure.account.auth.type.account.dfs.core.windows.net": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    
    # Parallel reads (multiple connections per file)
    "fs.azure.read.alwaysReadBufferSize": "4194304",   # 4MB read buffer
    "fs.azure.io.retry.max.retries": "5",
    
    # Write optimization
    "fs.azure.enable.small.write.optimization": "true",  # batch small writes
    "fs.azure.output.stream.buffer.size": "8388608",     # 8MB write buffer
}

# File size recommendations:
# Small files problem: same as S3 (1M × 1MB = bad)
# Target: 128MB–1GB per Parquet file
# Spark: df.repartition(target_partitions).write.format("parquet").save(path)
# Calculate: total_data_gb * 1024 / 128 = target file count
# Example: 100GB → 100*1024/128 = 800 files → .repartition(800)

# Avoid small files from streaming:
# Flink: set checkpoint interval = 5 min (not 1 min)
# Spark Structured Streaming: trigger(processingTime="5 minutes")
# Then compact daily: OPTIMIZE in Delta or rewrite_data_files in Iceberg

# Parallel listing performance:
# HNS enables O(1) directory rename and efficient listing
# For 1M files: HNS listing = O(1000) API calls
# For blob storage: O(1M) API calls (prefix-based listing)
# Always enable HNS when creating storage account (cannot enable after creation)
```

---

## Interview Tips

> **Tip 1:** "Can you enable Hierarchical Namespace on an existing Blob Storage account?" — No — HNS must be enabled at storage account creation time. Enabling it later requires data migration to a new account. This is a critical architectural decision upfront. If you already have data in a non-HNS account, use AzCopy or ADF to migrate files to a new HNS-enabled account. Always enable HNS for any storage account intended for big data analytics (Spark, Databricks, Synapse).

> **Tip 2:** "How does soft delete help with accidental deletions in ADLS?" — Soft delete retains deleted blobs/directories for a configurable retention period (1–365 days). An accidental DELETE shows the file as deleted but keeps the data. Recovery: use `undelete` API call to restore. Enable via: Storage Account → Data Management → Data Protection → Enable soft delete for blobs (set retention to 30 days for Bronze, 7 days for temp). Cost: you pay storage for soft-deleted blobs during retention period. Pair with blob versioning for `/silver` and `/gold` zones (keeps previous version on every overwrite).

> **Tip 3:** "What's the difference between ADLS Gen2 and Azure Blob Storage for analytics?" — Technically the same underlying storage (ADLS Gen2 is a Blob Storage account with HNS enabled), but HNS enables: (1) atomic rename/move (critical for Spark commit protocols), (2) true directory ACLs (per-path POSIX permissions), (3) efficient directory listing (not prefix enumeration), (4) consistent performance at scale (no metadata bottleneck). For analytics workloads, always use ADLS Gen2 (HNS enabled). Use regular Blob Storage only for web static files, CDN content, or non-analytics workloads.
