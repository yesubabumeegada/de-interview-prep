---
title: "Microsoft Purview — Intermediate"
topic: azure
subtopic: purview
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [azure, purview, scanning, api, glossary, lineage-api, classification]
---

# Microsoft Purview — Intermediate

## Registering and Scanning Data Sources

```python
# Purview REST API: register a source and create a scan programmatically
import requests
from azure.identity import DefaultAzureCredential
import json

PURVIEW_ACCOUNT = "mypurviewaccount"
BASE_URL = f"https://{PURVIEW_ACCOUNT}.purview.azure.com"

credential = DefaultAzureCredential()
token = credential.get_token("https://purview.azure.com/.default").token
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# 1. Register ADLS Gen2 data source
source_body = {
    "name": "adls-silver-account",
    "kind": "AdlsGen2",
    "properties": {
        "endpoint": "https://mysilver.dfs.core.windows.net/",
        "resourceGroup": "rg-data",
        "subscriptionId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
        "resourceName": "mysilver",
        "collection": {
            "referenceName": "data-engineering",  # Purview collection
            "type": "CollectionReference"
        }
    }
}
r = requests.put(
    f"{BASE_URL}/scan/datasources/adls-silver-account",
    headers=headers, json=source_body
)
print(f"Register ADLS: {r.status_code}")

# 2. Create scan (with managed identity auth + custom scan rules)
scan_body = {
    "name": "daily-scan-silver",
    "kind": "AdlsGen2Msi",          # auth: Purview managed identity
    "properties": {
        "scanRulesetName": "AdlsGen2",
        "scanRulesetType": "System",
        "collection": {"referenceName": "data-engineering", "type": "CollectionReference"},
        # Scope: specific containers/paths
        "resourceTypes": {
            "Azure.DataLake.Storage.Gen2": {
                "credential": {
                    "referenceName": "purview-msi",
                    "credentialType": "ManagedIdentity"
                },
                "resourceNameFilter": {
                    "includePatterns": ["silver/orders/*", "silver/customers/*"]
                }
            }
        }
    }
}
r = requests.put(
    f"{BASE_URL}/scan/datasources/adls-silver-account/scans/daily-scan-silver",
    headers=headers, json=scan_body
)

# 3. Schedule scan (daily at 2 AM UTC)
trigger_body = {
    "name": "daily-trigger",
    "properties": {
        "recurrence": {
            "frequency": "Day",
            "interval": 1,
            "startTime": "2024-01-15T02:00:00Z",
            "timezone": "UTC"
        },
        "recurrenceInterval": None,
        "scanLevel": "Incremental"   # Incremental: only scan new/changed assets
    }
}
r = requests.put(
    f"{BASE_URL}/scan/datasources/adls-silver-account/scans/daily-scan-silver/triggers/daily-trigger",
    headers=headers, json=trigger_body
)
print(f"Trigger scheduled: {r.status_code}")
```

---

## Business Glossary Management

```python
# Business Glossary: standard business terms with definitions, owners, related terms
# Reduces ambiguity: what does "Revenue" mean? Gross or net? Including refunds?

# Create glossary term via API
term_body = {
    "name": "Net Revenue",
    "longDescription": "Total sales amount after deducting refunds, discounts, and returns. "
                       "Calculated at the order completion date. Excludes shipping costs.",
    "abbreviation": "NRev",
    "status": "Approved",
    "resources": [
        {
            "displayName": "Revenue Calculation Runbook",
            "url": "https://wiki.company.com/revenue-calc"
        }
    ],
    "contacts": {
        "Expert": [{"id": "finance-team-group-object-id", "info": "Finance Analytics Team"}],
        "Owner":  [{"id": "cfo-user-object-id", "info": "CFO Office"}]
    },
    "attributes": {
        "DataDomain": "Finance",
        "Sensitivity": "Confidential",
        "Regulatory": "SOX"
    }
}

r = requests.post(
    f"{BASE_URL}/catalog/api/atlas/v2/glossary/term",
    headers=headers, json=term_body
)
term_guid = r.json()["guid"]
print(f"Created term: {term_guid}")

# Link glossary term to a column in the catalog
# First: find the asset (column) GUID by searching
search_body = {
    "keywords": "orders totalAmount",
    "filter": {"and": [{"attributeName": "objectType", "operator": "eq", "attributeValue": "Column"}]},
    "limit": 10
}
search_result = requests.post(f"{BASE_URL}/catalog/api/search/query", headers=headers, json=search_body)
column_guid = search_result.json()["value"][0]["id"]

# Assign term to column
assign_body = [{"termGuid": term_guid, "relationshipType": "AtlasGlossarySemanticAssignment"}]
requests.post(
    f"{BASE_URL}/catalog/api/atlas/v2/entity/guid/{column_guid}/classifications",
    headers=headers, json=assign_body
)
print("Term assigned to column")
```

---

## Custom Classification Rules

```python
# Create custom classifier for proprietary data patterns

# Example: Internal Project Code classifier (PRJ-ABC-1234 format)
custom_classifier = {
    "name": "InternalProjectCode",
    "description": "Matches internal project codes in format PRJ-{3 letters}-{4 digits}",
    "kind": "Custom",
    "classificationAction": "Keep",
    "ruleStatus": "Enabled",
    "datumPatterns": [
        {
            "kind": "Regex",
            "pattern": "PRJ-[A-Z]{3}-\\d{4}"
        }
    ],
    "columnPatterns": [
        {
            "kind": "Regex",
            "pattern": "(?i)(project_code|proj_id|project_id)"  # match column names
        }
    ],
    "minimumPercentageMatch": 0.6   # 60% of sampled rows must match
}

r = requests.put(
    f"{BASE_URL}/scan/classificationrules/InternalProjectCode",
    headers=headers, json=custom_classifier
)
print(f"Created classifier: {r.status_code}")

# Apply custom classifier in a scan rule set:
scan_ruleset = {
    "name": "CustomScanRuleSet",
    "kind": "AdlsGen2",
    "properties": {
        "scanningRule": {
            "customClassificationRuleNames": ["InternalProjectCode"],
            "builtInScanRulesetName": "AdlsGen2"   # inherit all built-in classifiers too
        }
    }
}
r = requests.put(
    f"{BASE_URL}/scan/scanrulesets/CustomScanRuleSet",
    headers=headers, json=scan_ruleset
)
```

---

## Querying the Data Catalog

```python
# Search and discover assets programmatically

def search_assets(keywords: str, classifications: list = None, asset_type: str = None):
    """Search Purview catalog for data assets."""
    
    filters = []
    if classifications:
        filters.append({
            "and": [
                {"or": [{"attributeName": "classificationNames", "operator": "contains", "attributeValue": c}
                        for c in classifications]}
            ]
        })
    if asset_type:
        filters.append({"attributeName": "objectType", "operator": "eq", "attributeValue": asset_type})
    
    body = {
        "keywords": keywords,
        "filter": {"and": filters} if filters else None,
        "facets": [
            {"facet": "assetType", "count": 10},
            {"facet": "classification", "count": 10},
            {"facet": "glossaryType", "count": 10}
        ],
        "limit": 20,
        "offset": 0
    }
    
    r = requests.post(f"{BASE_URL}/catalog/api/search/query", headers=headers, json=body)
    results = r.json()
    
    print(f"Total results: {results['@search.count']}")
    for asset in results.get("value", []):
        print(f"  {asset.get('name')} | {asset.get('assetType')} | {asset.get('qualifiedName')}")
        print(f"  Classifications: {asset.get('classificationNames', [])}")
    
    return results

# Examples:
search_assets("customer email", classifications=["EMAIL_ADDRESS"])
# → Finds all columns containing email addresses across all sources

search_assets("revenue", asset_type="Column")  
# → Finds all columns named "revenue" or containing revenue data

# Get asset lineage:
def get_asset_lineage(asset_guid: str, direction: str = "BOTH", depth: int = 3):
    r = requests.get(
        f"{BASE_URL}/catalog/api/atlas/v2/lineage/{asset_guid}",
        headers=headers,
        params={"direction": direction, "depth": depth}
    )
    return r.json()

lineage = get_asset_lineage(column_guid, direction="OUTPUT")
print(json.dumps(lineage, indent=2))
```

---

## Interview Tips

> **Tip 1:** "How does Purview handle scanning large data lakes with millions of files?" — Purview uses incremental scanning: after the initial full scan, subsequent scans only process new or modified files (based on modification timestamp). The scan divides work across multiple scanner threads. For ADLS Gen2 with millions of Parquet files: Purview reads Parquet schema metadata (not full file content) and samples ~1,000 rows per file for classification. You can limit the scan scope by configuring specific prefixes/containers in the scan filter. Initial scan of a 500TB lake with 1M files: typically 4-24 hours. Subsequent incremental scans: 30-60 minutes for daily changes.

> **Tip 2:** "What's the difference between a scan ruleset and a classification rule?" — Classification rule: the individual pattern or regex that identifies a specific data type (e.g., the rule that detects Social Security Numbers by matching `\d{3}-\d{2}-\d{4}`). Scan ruleset: a collection of classification rules that are applied together during a scan. You create a scan ruleset that includes: all built-in system classifiers (200+ rules) + your custom classifiers. When scanning a source, you assign one scan ruleset to it. A scan ruleset is reusable across multiple scan configurations.

> **Tip 3:** "How do you ensure data engineers can discover new datasets but not access sensitive data?" — Purview separates discovery from access. In Purview Data Catalog: all authenticated Azure AD users can search for and see asset metadata (names, schemas, classifications). Whether they can actually READ the underlying data is controlled by Azure RBAC/ACLs on the storage account or database — independent of Purview. Purview's role: show that `customers.ssn` exists and is classified as SSN. Azure RBAC's role: allow only the compliance team to read that column. Data engineers see the catalog entry (with sensitivity label "Highly Confidential"), but query attempts are rejected by ADLS ACLs.
