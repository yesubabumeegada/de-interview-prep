---
title: "Microsoft Purview — Senior Deep Dive"
topic: azure
subtopic: purview
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [azure, purview, governance, data-mesh, atlas-api, lineage, compliance, architecture]
---

# Microsoft Purview — Senior Deep Dive

## Purview Architecture and Atlas API

```
Purview is built on Apache Atlas (open-source metadata and governance framework)
All data stored in Atlas — accessed via Atlas REST API

Atlas data model:
  TypeDef:     defines a type (like a class) — e.g., "azure_sql_column", "adls_gen2_path"
  Entity:      an instance of a TypeDef — the actual table or column asset
  Relationship:  connection between entities (e.g., column_lineage, dataset_process)
  Classification: label applied to an entity (e.g., EMAIL_ADDRESS, SSN)
  Glossary Term: business vocabulary term linked to entities

Entity example (Azure SQL column):
{
  "typeName": "azure_sql_column",
  "guid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
  "attributes": {
    "name": "customer_email",
    "qualifiedName": "mssql://myserver.database.windows.net/orders/dbo/customers#customer_email",
    "dataType": "varchar",
    "length": 200,
    "isNullable": true,
    "table": {"guid": "parent-table-guid"},
    "isPrimaryKey": false,
    "classifications": ["EMAIL_ADDRESS"],
    "meanings": [{"guid": "glossary-term-guid", "displayText": "Customer Email"}]
  }
}

Custom TypeDef (extend Purview's model for custom assets):
# Register a custom entity type for "MLModel" assets:
custom_type = {
    "entityDefs": [{
        "name": "MLModel",
        "superTypes": ["DataSet"],
        "typeVersion": "1.0",
        "attributeDefs": [
            {"name": "modelFramework", "typeName": "string", "isOptional": True},
            {"name": "accuracy", "typeName": "float", "isOptional": True},
            {"name": "trainingDataset", "typeName": "string", "isOptional": True},
            {"name": "modelVersion", "typeName": "string", "isOptional": False}
        ]
    }]
}
# POST to /catalog/api/atlas/v2/types/typedefs
```

---

## Enterprise Lineage: OpenLineage Integration

```python
# OpenLineage: open standard for lineage metadata emission
# Any pipeline can emit lineage events to Purview without native integration

# OpenLineage event structure:
lineage_event = {
    "eventType": "COMPLETE",
    "eventTime": "2024-01-15T14:30:00.000Z",
    "run": {
        "runId": "3c5d5a0b-4a5c-4b8a-9c3e-2a8b5c6d7e8f",
        "facets": {
            "nominalTime": {
                "nominalStartTime": "2024-01-15T02:00:00Z",
                "nominalEndTime":   "2024-01-15T04:00:00Z"
            },
            "parent": {
                "run": {"runId": "adf-pipeline-run-id"},
                "job": {"namespace": "adf", "name": "daily-silver-pipeline"}
            }
        }
    },
    "job": {
        "namespace": "databricks",
        "name": "transform_silver_orders"
    },
    "inputs": [
        {
            "namespace": "adls://myaccount.dfs.core.windows.net",
            "name": "bronze/orders",
            "facets": {
                "schema": {
                    "fields": [
                        {"name": "order_id",    "type": "long"},
                        {"name": "customer_id", "type": "int"},
                        {"name": "amount",      "type": "double"}
                    ]
                }
            }
        }
    ],
    "outputs": [
        {
            "namespace": "adls://myaccount.dfs.core.windows.net",
            "name": "silver/orders",
            "facets": {
                "schema": {
                    "fields": [
                        {"name": "order_id",    "type": "long"},
                        {"name": "customer_id", "type": "int"},
                        {"name": "amount",      "type": "decimal"},
                        {"name": "processed_at","type": "timestamp"}
                    ]
                }
            }
        }
    ]
}

# Emit to Purview via Kafka endpoint:
# Purview provides a Kafka-compatible endpoint for OpenLineage events
# Configure producer:
from confluent_kafka import Producer

kafka_conf = {
    'bootstrap.servers': f'{PURVIEW_ACCOUNT}.servicebus.windows.net:9093',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': '$ConnectionString',
    'sasl.password': PURVIEW_KAFKA_CONNECTION_STRING
}

producer = Producer(kafka_conf)
producer.produce(
    topic="purview-datamap-openlineage",
    key="databricks-job",
    value=json.dumps(lineage_event)
)
producer.flush()
print("Lineage event emitted to Purview")

# Alternatively: emit directly to Purview REST Atlas API
# POST /catalog/api/atlas/v2/entity/bulk
```

---

## Data Governance at Scale: Collections and Policies

```
Collections: hierarchical organization of assets in Purview

Collection tree:
  Root collection (Purview account)
  ├── Shared Platform
  │    ├── ADLS Gen2 (all accounts)
  │    └── Event Hubs (all namespaces)
  ├── Finance Domain
  │    ├── Finance ADLS
  │    └── Finance SQL DB
  ├── Marketing Domain
  └── HR Domain (restricted)

Collection-level RBAC roles:
  Collection Admin:       manage the collection and sub-collections
  Data Source Admin:     register and scan data sources
  Data Curator:          create/edit glossary terms, apply classifications
  Data Reader:           search and view catalog (read-only)
  
  HR Domain → Data Reader: only HR group members (sensitive PII)
  Finance Domain → Data Reader: Finance team + Audit team
  Shared Platform → Data Reader: all authenticated employees

Data governance maturity with Purview:

Level 1 (Ad Hoc): No catalog, engineers know where data is informally
Level 2 (Reactive, Purview starting): Register Azure sources, auto-scan, basic catalog
Level 3 (Proactive): Custom classifiers, business glossary populated, scan all sources
Level 4 (Managed): Lineage captured from all pipelines, sensitivity labels enforced, DLP active
Level 5 (Optimized): 
  - Self-service data discovery (engineers find data without asking)
  - Data contracts enforced (schema violations rejected at pipeline)
  - Automated GDPR erasure tracking (Purview tracks all PII copies for erasure)
  - Data product ownership clear (domain teams own their data in mesh)

Data Mesh with Purview:
  Each domain team registers and manages their own collection in Purview
  Central Purview (federated): shared catalog infrastructure, each domain self-manages
  Cross-domain lineage: Purview tracks data flowing between domain collections
  Central policies: classification rules and sensitivity labels defined centrally, applied globally
```

---

## GDPR Compliance with Purview

```python
# GDPR Right to Erasure: find all copies of a specific customer's data

# Step 1: Search catalog for all tables containing customer_id column
def find_customer_data_locations(customer_id_column_classifier: str = "CUSTOMER_ID") -> list:
    """Find all data assets containing customer personal data."""
    
    search_body = {
        "keywords": "",
        "filter": {
            "and": [
                {
                    "attributeName": "classificationNames",
                    "operator": "contains",
                    "attributeValue": customer_id_column_classifier
                }
            ]
        },
        "limit": 100
    }
    
    r = requests.post(f"{BASE_URL}/catalog/api/search/query", headers=headers, json=search_body)
    assets = r.json().get("value", [])
    
    locations = []
    for asset in assets:
        locations.append({
            "asset_name": asset["name"],
            "source_type": asset.get("assetType"),
            "qualified_name": asset.get("qualifiedName"),
            "collection": asset.get("collectionId"),
            "sensitivity": asset.get("sensitivityLabel")
        })
    
    print(f"Found {len(locations)} data assets containing customer data")
    return locations

# Step 2: Trace lineage to find downstream copies
def trace_customer_data_lineage(source_asset_guid: str) -> list:
    """Trace all downstream copies of customer data via lineage."""
    lineage = get_asset_lineage(source_asset_guid, direction="OUTPUT", depth=5)
    
    downstream = []
    def traverse(node):
        if node:
            downstream.append(node.get("qualifiedName"))
            for child in lineage.get("guidEntityMap", {}).values():
                traverse(child)
    
    traverse(lineage.get("baseEntityGuid"))
    return downstream

# Step 3: For each location, apply erasure
# (Purview doesn't delete data — it tells you WHERE to delete)
def generate_erasure_plan(customer_id: str) -> dict:
    locations = find_customer_data_locations()
    plan = {
        "customer_id": customer_id,
        "identified_at": datetime.utcnow().isoformat(),
        "erasure_actions": []
    }
    
    for loc in locations:
        if "adls" in loc["source_type"].lower():
            plan["erasure_actions"].append({
                "type": "delta_delete",
                "location": loc["qualified_name"],
                "action": f"DELETE FROM table WHERE customer_id = '{customer_id}'",
                "follow_up": "VACUUM + expire_snapshots after delete"
            })
        elif "sql" in loc["source_type"].lower():
            plan["erasure_actions"].append({
                "type": "sql_delete",
                "location": loc["qualified_name"],
                "action": f"DELETE FROM ... WHERE customer_id = '{customer_id}'"
            })
    
    return plan

# Audit: log erasure completion
def record_erasure_completion(customer_id: str, locations_erased: list):
    audit_record = {
        "id": f"gdpr_erasure_{customer_id}_{int(datetime.utcnow().timestamp())}",
        "customerId": customer_id,
        "requestDate": "2024-01-15",
        "completionDate": datetime.utcnow().isoformat(),
        "locationsErased": locations_erased,
        "operator": "data_compliance_team",
        "status": "COMPLETED"
    }
    # Store in compliance Cosmos DB container (immutable records)
    compliance_container.upsert_item(audit_record)
```

---

## Interview Tips

> **Tip 1:** "How does Purview handle lineage for custom pipelines not natively supported?" — Use the Apache Atlas REST API to programmatically create lineage: create entities for the input and output datasets, create a "Process" entity representing the pipeline/job, then create relationships (INPUT, OUTPUT) linking the process to its datasets. For standardized emission: use the OpenLineage spec (open standard) and Purview's Kafka endpoint — your pipeline emits a structured JSON event describing inputs, outputs, and job metadata. Any Python, Java, or Scala pipeline can emit OpenLineage events. This is how you get lineage for custom Databricks notebooks, in-house ETL tools, and third-party data systems.

> **Tip 2:** "What is the Atlas TypeDef system and why does it matter for enterprise governance?" — Atlas TypeDef is the metadata schema system — it defines what types of assets Purview understands and what attributes those assets have. Built-in types: `azure_sql_table`, `adls_gen2_path`, `azure_databricks_job`, etc. You can extend Purview's model with custom TypeDefs for proprietary assets (ML models, data contracts, feature store entries, business metrics). This matters because: (a) everything in Purview is an entity of a TypeDef — custom types give first-class catalog entries for your proprietary systems, (b) relationship TypeDefs let you define custom lineage edges (e.g., "trained_on" between a ML model and a training dataset), (c) classification TypeDefs let you create domain-specific classification schemes.

> **Tip 3:** "How would you measure data governance maturity using Purview?" — Use Purview's Data Insights dashboards plus custom metrics: (a) Asset coverage: % of registered data sources that have been scanned, (b) Classification coverage: % of assets with at least one classification applied, (c) Glossary coverage: % of assets linked to a business glossary term, (d) Owner coverage: % of assets with a designated data owner, (e) Lineage coverage: % of assets with at least one lineage edge, (f) Scan freshness: average time since last scan per source. Target maturity: 90%+ scan coverage, 70%+ classification on sensitive sources, 100% PII-classified assets linked to owner. Report monthly in a governance committee.
