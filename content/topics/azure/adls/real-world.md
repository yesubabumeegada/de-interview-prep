---
title: "ADLS Gen2 — Real World"
topic: azure
subtopic: adls
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [azure, adls, production, access-control, monitoring, cost]
---

# ADLS Gen2 — Real World

## Pattern 1: Lakehouse Storage Account Setup

```python
# Production ADLS Gen2 setup for medallion lakehouse

import subprocess

def setup_adls_lakehouse(subscription_id: str, resource_group: str, location: str, account_name: str):
    """Create and configure ADLS Gen2 storage account for medallion lakehouse."""
    
    cmds = [
        # Create storage account with HNS enabled
        ["az", "storage", "account", "create",
         "--name", account_name,
         "--resource-group", resource_group,
         "--location", location,
         "--sku", "Standard_ZRS",        # Zone redundant
         "--kind", "StorageV2",
         "--hierarchical-namespace", "true",  # CRITICAL: enable HNS
         "--min-tls-version", "TLS1_2",
         "--allow-blob-public-access", "false",  # disable public access
         "--default-action", "Deny",     # deny by default (firewall enabled)
         ],
        
        # Create containers (one per zone)
        *[["az", "storage", "fs", "create",
           "--name", zone,
           "--account-name", account_name,
           "--auth-mode", "login"]
          for zone in ["bronze", "silver", "gold", "checkpoints", "tmp"]],
        
        # Enable soft delete (7 days)
        ["az", "storage", "account", "blob-service-properties", "update",
         "--account-name", account_name,
         "--resource-group", resource_group,
         "--enable-delete-retention", "true",
         "--delete-retention-days", "7"],
        
        # Enable versioning for Silver and Gold
        ["az", "storage", "account", "blob-service-properties", "update",
         "--account-name", account_name,
         "--resource-group", resource_group,
         "--enable-versioning", "true"],
    ]
    
    for cmd in cmds:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            print(f"Error: {result.stderr}")
        else:
            print(f"Done: {' '.join(cmd[2:4])}")

# Grant access to Databricks service principal
def grant_databricks_access(account_name: str, resource_group: str, sp_object_id: str):
    """Grant Databricks SP Storage Blob Data Contributor on the account."""
    scope = f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Storage/storageAccounts/{account_name}"
    subprocess.run([
        "az", "role", "assignment", "create",
        "--assignee", sp_object_id,
        "--role", "Storage Blob Data Contributor",
        "--scope", scope
    ])
    print(f"Granted Storage Blob Data Contributor to {sp_object_id}")
```

---

## Pattern 2: ACL-Based Fine-Grained Access

```bash
# Scenario: separate read access for Finance and Marketing teams
# Finance: read access to silver/finance/ only
# Marketing: read access to silver/marketing/ only
# ETL service: write access to all of silver/

ACCOUNT="mycompany-data"
FINANCE_SP="finance-reader-sp-object-id"
MARKETING_SP="marketing-reader-sp-object-id"
ETL_SP="etl-service-sp-object-id"

# 1. ETL service: full access to silver container
az role assignment create \
  --assignee $ETL_SP \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/.../storageAccounts/$ACCOUNT/blobServices/default/containers/silver"

# 2. Finance SP: traverse access on container + read on finance/ directory
# Container level: --x (execute = can enter)
az storage fs access set \
  --acl "user:$FINANCE_SP:--x" \
  --path "/" \
  --file-system silver \
  --account-name $ACCOUNT \
  --auth-mode login

# Finance directory: r-x (read and list)
az storage fs access set \
  --acl "user:$FINANCE_SP:r-x" \
  --path "/finance" \
  --file-system silver \
  --account-name $ACCOUNT \
  --auth-mode login

# 3. Marketing SP: same pattern for /marketing
az storage fs access set \
  --acl "user:$MARKETING_SP:--x" \
  --path "/" \
  --file-system silver \
  --account-name $ACCOUNT \
  --auth-mode login

az storage fs access set \
  --acl "user:$MARKETING_SP:r-x" \
  --path "/marketing" \
  --file-system silver \
  --account-name $ACCOUNT \
  --auth-mode login

# Test: Finance SP should fail on /marketing, succeed on /finance
az storage fs file list \
  --file-system silver \
  --path "/marketing" \
  --account-name $ACCOUNT \
  --auth-mode login \
  --account-tenant-id $TENANT \
  # Should return: authorization failure
```

---

## Pattern 3: Cost Monitoring and Optimization

```python
# Monitor ADLS Gen2 storage costs via Azure Monitor

import pandas as pd
from azure.mgmt.monitor import MonitorManagementClient
from datetime import datetime, timedelta

def get_storage_cost_breakdown(subscription_id: str, resource_group: str, account_name: str):
    """Get storage size by container using Azure Monitor metrics."""
    monitor_client = MonitorManagementClient(DefaultAzureCredential(), subscription_id)
    
    resource_id = f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Storage/storageAccounts/{account_name}"
    
    # Get capacity metric
    metrics = monitor_client.metrics.list(
        resource_uri=resource_id,
        timespan=f"{(datetime.utcnow() - timedelta(days=1)).isoformat()}/{datetime.utcnow().isoformat()}",
        interval="PT1H",
        metricnames="UsedCapacity",
        aggregation="Average"
    )
    
    total_gb = 0
    for metric in metrics.value:
        for ts in metric.timeseries:
            for dp in ts.data:
                if dp.average:
                    total_gb = dp.average / (1024**3)
    
    print(f"Total storage: {total_gb:.1f} GB")
    
    # Cost estimate by tier (assuming Hot tier)
    cost_by_tier = {
        "Hot":     total_gb * 0.018,
        "Cool":    total_gb * 0.01,
        "Archive": total_gb * 0.00099
    }
    print("If all data at each tier:")
    for tier, cost in cost_by_tier.items():
        print(f"  {tier}: ${cost:.2f}/month")
    
    return total_gb

# S3-equivalent cost comparison:
# ADLS Hot: $0.018/GB vs S3 Standard: $0.023/GB → ADLS 22% cheaper
# ADLS Archive: $0.00099/GB vs S3 Glacier Deep Archive: $0.00099/GB → same
# Transaction costs: ADLS $0.065 per 10K write ops vs S3 $0.005 → ADLS much cheaper
```

---

## Interview Tips

> **Tip 1:** "How do you manage ADLS Gen2 access when the data team grows from 5 to 50 engineers?" — Scale from individual ACLs to group-based access. Create Azure AD security groups: `DataEngineers-Bronze-Write`, `DataAnalysts-Silver-Read`, `MLTeam-Gold-Read`. Assign RBAC roles to groups, not individuals. When an engineer joins: add them to the appropriate groups — no storage ACL changes needed. Use Privileged Identity Management (PIM) for just-in-time access to sensitive containers. Maintain an access request process with Terraform-managed role assignments (infrastructure-as-code for access control).

> **Tip 2:** "A Spark job fails with 403 Forbidden on ADLS. How do you diagnose?" — Check in order: (1) Is the Managed Identity / Service Principal correct? (`spark.conf.get("fs.azure.account.auth.type....")`) (2) Does the identity have Storage Blob Data Contributor (or Reader) RBAC role on the storage account? Check via `az role assignment list`. (3) Is the storage account firewall blocking the Spark cluster IP range? Check in Storage → Networking — add the VNet subnet or allow Azure services. (4) Are ACLs denying at directory level? Even with RBAC, ACLs can override. Check parent directory has `--x` traverse permission.

> **Tip 3:** "What's the difference between Storage Account Key access and Managed Identity access?" — Account Key: the equivalent of root password — whoever has it can read/write/delete any blob in the account. It's stored as a long-term secret (rotation is manual, often neglected). Managed Identity: Azure assigns a cryptographic identity to the service (ADF, Databricks, Functions). The token is short-lived (1 hour), auto-renewed by Azure, and scoped to specific RBAC roles. No secret to store, no rotation needed. In 2024+, Microsoft enforces "use Managed Identity" via Azure Policy — many enterprises disable shared key authentication entirely.
