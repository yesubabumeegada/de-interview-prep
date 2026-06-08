---
title: "Microsoft Purview — Real World"
topic: azure
subtopic: purview
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [azure, purview, production, compliance, scanning, lineage, data-catalog]
---

# Microsoft Purview — Real World

## Pattern 1: Enterprise-Wide Data Catalog Setup

```python
# Automated Purview setup: register all Azure resources in a subscription

import subprocess
import json

def register_all_azure_sources(subscription_id: str, purview_account: str):
    """
    Auto-register all data sources in a subscription with Purview.
    Run during initial setup or when new resources are created.
    """
    
    # Get all ADLS Gen2 accounts in subscription
    result = subprocess.run([
        "az", "storage", "account", "list",
        "--subscription", subscription_id,
        "--query", "[?kind=='StorageV2'].{name:name, rg:resourceGroup, location:location, sku:sku.name}",
        "--output", "json"
    ], capture_output=True, text=True)
    
    storage_accounts = json.loads(result.stdout)
    
    for acct in storage_accounts:
        # Check if HNS-enabled (ADLS Gen2)
        hns_result = subprocess.run([
            "az", "storage", "account", "show",
            "--name", acct["name"],
            "--resource-group", acct["rg"],
            "--query", "isHnsEnabled"
        ], capture_output=True, text=True)
        
        if hns_result.stdout.strip() == "true":
            print(f"Registering ADLS Gen2: {acct['name']}")
            register_adls_source(acct["name"], acct["rg"], subscription_id, purview_account)
    
    # Get all Azure SQL Servers
    result = subprocess.run([
        "az", "sql", "server", "list",
        "--subscription", subscription_id,
        "--query", "[].{name:name, rg:resourceGroup, fqdn:fullyQualifiedDomainName}",
        "--output", "json"
    ], capture_output=True, text=True)
    
    sql_servers = json.loads(result.stdout)
    for server in sql_servers:
        print(f"Registering SQL Server: {server['name']}")
        register_sql_source(server, subscription_id, purview_account)

def grant_purview_access():
    """
    Grant Purview Managed Identity read access to all registered sources.
    Required before scanning.
    """
    # Get Purview MSI object ID
    result = subprocess.run([
        "az", "purview", "account", "show",
        "--name", PURVIEW_ACCOUNT,
        "--resource-group", PURVIEW_RG,
        "--query", "identity.principalId",
        "--output", "tsv"
    ], capture_output=True, text=True)
    
    purview_msi = result.stdout.strip()
    
    # Grant Storage Blob Data Reader on each storage account
    for acct in storage_accounts:
        subprocess.run([
            "az", "role", "assignment", "create",
            "--assignee", purview_msi,
            "--role", "Storage Blob Data Reader",
            "--scope", f"/subscriptions/{subscription_id}/resourceGroups/{acct['rg']}/providers/Microsoft.Storage/storageAccounts/{acct['name']}"
        ])
    
    print(f"Granted Storage Blob Data Reader to Purview MSI: {purview_msi}")

# Schedule: run daily via Azure Automation Runbook to pick up new resources
```

---

## Pattern 2: Compliance Dashboard with Purview Insights

```python
# Extract Purview scan + classification data for compliance reporting

import requests
from azure.identity import DefaultAzureCredential
from datetime import datetime

def generate_compliance_report(purview_account: str) -> dict:
    """Generate weekly data governance compliance report."""
    
    credential = DefaultAzureCredential()
    token = credential.get_token("https://purview.azure.com/.default").token
    headers = {"Authorization": f"Bearer {token}"}
    BASE_URL = f"https://{purview_account}.purview.azure.com"
    
    report = {
        "generated_at": datetime.utcnow().isoformat(),
        "scan_summary": {},
        "classification_summary": {},
        "pii_exposure": {}
    }
    
    # 1. Total assets by type
    facet_search = {
        "keywords": "",
        "facets": [
            {"facet": "assetType", "count": 20},
            {"facet": "classification", "count": 20},
            {"facet": "sensitivityLabel", "count": 10}
        ],
        "limit": 1  # only need counts, not actual results
    }
    r = requests.post(f"{BASE_URL}/catalog/api/search/query", headers=headers, json=facet_search)
    facets = r.json().get("@search.facets", {})
    
    report["scan_summary"]["total_assets"] = r.json().get("@search.count", 0)
    report["scan_summary"]["by_type"] = {
        f["value"]: f["count"] for f in facets.get("assetType", [])
    }
    
    # 2. PII classifications
    report["classification_summary"] = {
        f["value"]: f["count"] for f in facets.get("classification", [])
    }
    
    # 3. PII exposure: assets with PII but no owner
    pii_search = {
        "keywords": "",
        "filter": {
            "and": [
                {"attributeName": "classificationNames", "operator": "contains", "attributeValue": "EMAIL_ADDRESS"},
                {"attributeName": "owner", "operator": "eq", "attributeValue": None}
            ]
        },
        "limit": 100
    }
    r = requests.post(f"{BASE_URL}/catalog/api/search/query", headers=headers, json=pii_search)
    report["pii_exposure"]["unowned_pii_assets"] = r.json().get("@search.count", 0)
    report["pii_exposure"]["samples"] = [a["qualifiedName"] for a in r.json().get("value", [])[:5]]
    
    # 4. Stale scans (sources not scanned in 7+ days)
    report["scan_freshness"] = check_scan_freshness(BASE_URL, headers)
    
    return report

def check_scan_freshness(base_url: str, headers: dict) -> dict:
    """Identify data sources with stale scans."""
    r = requests.get(f"{base_url}/scan/datasources", headers=headers)
    sources = r.json().get("value", [])
    
    stale = []
    from datetime import timedelta
    threshold = datetime.utcnow() - timedelta(days=7)
    
    for source in sources:
        # Get latest scan run
        scans_r = requests.get(f"{base_url}/scan/datasources/{source['name']}/scans", headers=headers)
        scans = scans_r.json().get("value", [])
        
        if not scans:
            stale.append({"source": source["name"], "reason": "never_scanned"})
            continue
        
        # Check last scan time (simplified)
        last_scan_time = scans[0].get("properties", {}).get("lastModifiedAt")
        if last_scan_time and datetime.fromisoformat(last_scan_time.replace("Z","")) < threshold:
            stale.append({"source": source["name"], "last_scan": last_scan_time})
    
    return {"stale_count": len(stale), "stale_sources": stale}
```

---

## Pattern 3: Automated PII Detection Pipeline

```python
# Workflow: new table scanned → PII detected → notify data owner → apply sensitivity label

import azure.functions as func
from azure.eventgrid import EventGridConsumerClient

# Azure Event Grid sends events when Purview scan completes with new PII classifications

def on_purview_scan_complete(event: func.EventGridEvent):
    """React to Purview scan completion events via Event Grid."""
    
    scan_result = event.get_json()
    if scan_result.get("eventType") != "Microsoft.Purview.ScanCompletedWithNewClassifications":
        return
    
    asset_guid   = scan_result["data"]["assetGuid"]
    asset_name   = scan_result["data"]["assetQualifiedName"]
    new_classifs = scan_result["data"]["newClassifications"]  # ["EMAIL_ADDRESS", "SSN"]
    
    # 1. Fetch asset details including owner
    asset = get_asset_by_guid(asset_guid)
    owner_email = asset.get("attributes", {}).get("ownerEmail", "data-governance@company.com")
    
    # 2. Notify owner via Teams/email
    if any(c in new_classifs for c in ["SSN", "CREDIT_CARD_NUMBER", "PASSPORT_NUMBER"]):
        send_sensitive_data_alert(
            to=owner_email,
            asset_name=asset_name,
            classifications=new_classifs,
            message=f"High-sensitivity data detected in {asset_name}. "
                    f"Please review and apply appropriate access controls."
        )
    
    # 3. Auto-apply sensitivity label based on classifications
    if "SSN" in new_classifs or "CREDIT_CARD_NUMBER" in new_classifs:
        sensitivity_label = "Highly Confidential"
    elif "EMAIL_ADDRESS" in new_classifs or "DATE_OF_BIRTH" in new_classifs:
        sensitivity_label = "Confidential"
    else:
        sensitivity_label = "Internal"
    
    apply_sensitivity_label(asset_guid, sensitivity_label)
    
    # 4. Log to compliance audit trail
    log_compliance_event(
        event_type="PII_DETECTED",
        asset=asset_name,
        classifications=new_classifs,
        sensitivity_label=sensitivity_label,
        notified=owner_email
    )
    
    print(f"PII workflow complete for {asset_name}: {new_classifs} → {sensitivity_label}")
```

---

## Interview Tips

> **Tip 1:** "How do you handle a situation where Purview scanning impacts production database performance?" — Purview uses sampling-based scanning (doesn't read all rows), but for large databases it can still generate I/O load. Mitigations: (a) Schedule scans during off-peak hours (Purview supports cron-style scheduling — run at 2 AM), (b) For SQL sources: limit the scan to specific schemas/tables rather than full database, (c) Create a read replica (Azure SQL AG readable secondary, Synapse named replica) and point the Purview scan at the replica — zero impact on production, (d) For Cosmos DB: scans use the analytical store (zero OLTP RU consumption), (e) For ADLS: reads are against Blob Storage (not a live transactional system — no performance concern).

> **Tip 2:** "What's the difference between a Purview collection and an Azure resource group?" — Resource group: Azure resource management boundary (billing, deployment, RBAC for resource management). Purview collection: logical grouping of data assets WITHIN the Purview catalog (for catalog discovery, catalog-level RBAC, and organization). They don't have to match. Example: Finance Azure SQL DB and Finance ADLS are in separate resource groups (managed by different infrastructure teams) but belong to the same "Finance" Purview collection (managed by the Finance data team). Purview collection structure should reflect your organizational data domains, not infrastructure topology.

> **Tip 3:** "How does Purview integrate with Power BI for lineage and governance?" — Purview natively connects to Power BI tenant via Power BI Admin API. When you register the Power BI tenant as a data source in Purview and scan it: Purview discovers all workspaces, datasets, reports, and dashboards. Lineage is automatically captured: SQL DB table → Power BI dataset → Power BI report → Power BI dashboard (end-to-end). Sensitivity labels applied in Purview flow to Power BI (e.g., "Confidential" dataset → Power BI marks the report as Confidential → DLP prevents external sharing). This gives complete lineage: raw ADLS → Databricks Gold → Azure SQL → Power BI dashboard.
