---
title: "Data Catalog — Intermediate"
topic: data-governance
subtopic: data-catalog
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [data-catalog, datahub, ingestion, lineage, dbt-integration]
---

# Data Catalog — Intermediate

## DataHub Ingestion Architecture

DataHub uses recipes to pull metadata from sources:

```yaml
# datahub_recipes/snowflake_recipe.yaml
source:
  type: snowflake
  config:
    account_id: "myaccount.us-east-1"
    username: "${SNOWFLAKE_USER}"
    password: "${SNOWFLAKE_PASSWORD}"
    role: "DATAHUB_ROLE"
    warehouse: "COMPUTE_WH"
    database_pattern:
      allow:
        - "^PROD$"
    schema_pattern:
      allow:
        - "^GOLD$"
        - "^SILVER$"
    include_table_lineage: true
    include_column_lineage: true
    profiling:
      enabled: true
      profile_table_level_only: false

sink:
  type: datahub-rest
  config:
    server: "http://datahub-gms:8080"
    token: "${DATAHUB_TOKEN}"

transformers:
  - type: simple_add_dataset_ownership
    config:
      owner_urns:
        - "urn:li:corpuser:data-platform-team"
```

```python
# Run ingestion programmatically
import datahub.emitter.mce_builder as builder
from datahub.ingestion.run.pipeline import Pipeline

pipeline = Pipeline.create({
    "source": {
        "type": "snowflake",
        "config": {
            "account_id": "myaccount.us-east-1",
            "username": "datahub_user",
            "password": "...",
        }
    },
    "sink": {"type": "datahub-rest", "config": {"server": "http://datahub-gms:8080"}},
})
pipeline.run()
pipeline.raise_from_status()
```

---

## dbt → DataHub Integration

Push dbt model metadata to catalog automatically after every run:

```yaml
# datahub_recipes/dbt_recipe.yaml
source:
  type: dbt
  config:
    manifest_path: "target/manifest.json"
    catalog_path: "target/catalog.json"
    sources_path: "target/sources.json"
    target_platform: snowflake
    target_platform_instance: prod
    # Ingest model descriptions, column descriptions, owners from meta
    enable_meta_mapping: true
    meta_mapping:
      owner:
        match: ".*"
        operation: "add_owner"
        config:
          owner_type: Group
      domain:
        match: ".*"
        operation: "add_domain"
    # Map dbt tags → DataHub tags
    tag_prefix: "dbt:"

sink:
  type: datahub-rest
  config:
    server: "http://datahub-gms:8080"
    token: "${DATAHUB_TOKEN}"
```

```bash
# In CI/CD after dbt run + docs generate
dbt docs generate
datahub ingest -c datahub_recipes/dbt_recipe.yaml
```

---

## Emitting Custom Metadata via Python SDK

```python
from datahub.emitter.mce_builder import make_dataset_urn, make_tag_urn, make_user_urn
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    GlobalTagsClass,
    TagAssociationClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
)
import datahub.emitter.mce_builder as builder

emitter = DatahubRestEmitter(gms_server="http://datahub-gms:8080", token="...")

def register_dataset(
    platform: str,
    dataset_name: str,
    description: str,
    owner: str,
    tags: list[str],
):
    """Emit a full dataset metadata event to DataHub."""
    dataset_urn = make_dataset_urn(platform=platform, name=dataset_name, env="PROD")
    
    # 1. Dataset properties
    emitter.emit_mce(
        builder.make_lineage_mce(
            upstream_urns=[],
            downstream_urn=dataset_urn,
        )
    )
    
    # 2. Description + custom properties
    dataset_props = DatasetPropertiesClass(
        description=description,
        customProperties={
            "team": owner,
            "ingested_by": "custom-pipeline",
        }
    )
    emitter.emit_mcp(
        builder.make_mcp(entity_urn=dataset_urn, aspect=dataset_props)
    )
    
    # 3. Tags
    tag_assocs = [TagAssociationClass(tag=make_tag_urn(t)) for t in tags]
    emitter.emit_mcp(
        builder.make_mcp(
            entity_urn=dataset_urn,
            aspect=GlobalTagsClass(tags=tag_assocs),
        )
    )
    
    # 4. Ownership
    emitter.emit_mcp(
        builder.make_mcp(
            entity_urn=dataset_urn,
            aspect=OwnershipClass(owners=[
                OwnerClass(
                    owner=make_user_urn(owner),
                    type=OwnershipTypeClass.DATAOWNER,
                )
            ]),
        )
    )
    
    print(f"Registered {dataset_name} in DataHub")
```

---

## Business Glossary Integration

Map business terms to physical columns:

```python
from datahub.metadata.schema_classes import (
    GlossaryTermAssociationClass,
    GlossaryTermsClass,
)

def tag_column_with_glossary_term(
    dataset_urn: str,
    column_name: str,
    glossary_term: str,  # e.g. "Revenue", "Churn", "CAC"
    emitter: DatahubRestEmitter,
):
    """Link a glossary term to a specific column."""
    term_urn = f"urn:li:glossaryTerm:{glossary_term}"
    
    schema_field_urn = f"{dataset_urn},{column_name}"
    
    emitter.emit_mcp(
        builder.make_mcp(
            entity_urn=schema_field_urn,
            aspect=GlossaryTermsClass(
                terms=[GlossaryTermAssociationClass(urn=term_urn)]
            ),
        )
    )
```

---

## Catalog Freshness Monitoring

Ensure your catalog stays up to date:

```python
from datetime import datetime, timedelta
import requests

def check_catalog_staleness(datahub_url: str, token: str, max_age_hours: int = 26) -> list[dict]:
    """Find production datasets not updated in DataHub recently."""
    
    stale = []
    cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
    
    # GraphQL query to DataHub
    query = """
    {
      search(input: {type: DATASET, query: "*", filters: [{field: "platform", value: "snowflake"}], count: 500}) {
        searchResults {
          entity {
            ... on Dataset {
              urn
              name
              lastIngested
            }
          }
        }
      }
    }
    """
    resp = requests.post(
        f"{datahub_url}/api/graphql",
        json={"query": query},
        headers={"Authorization": f"Bearer {token}"},
    )
    
    for result in resp.json()["data"]["search"]["searchResults"]:
        entity = result["entity"]
        last_ingested = entity.get("lastIngested")
        if last_ingested:
            last_dt = datetime.fromtimestamp(last_ingested / 1000)
            if last_dt < cutoff:
                stale.append({"urn": entity["urn"], "name": entity["name"], "last_ingested": last_dt})
    
    return stale
```

---

## Interview Tips

> **Tip 1:** "How do you keep a data catalog up to date?" — Automated ingestion on a schedule (daily or after each pipeline run). dbt integration emits metadata after every run. OpenLineage captures runtime lineage. Human curation for descriptions/owners is the hard part — governance CI checks enforce this at deploy time.

> **Tip 2:** "What is a recipe in DataHub?" — A YAML configuration file that tells DataHub where to pull metadata from (source) and where to push it (sink). Sources include Snowflake, BigQuery, dbt, Airflow, Looker. Sinks are DataHub REST or Kafka. Recipes are run by the ingestion framework on a schedule.

> **Tip 3:** "What's the difference between technical and business metadata?" — Technical metadata (schema, row count, update time) is auto-captured from systems. Business metadata (descriptions, glossary terms, owners) requires human input. The gap between them is the hardest governance challenge to solve — you need both for a useful catalog.
