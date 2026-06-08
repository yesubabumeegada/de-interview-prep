---
title: "Unity Catalog Governance — Intermediate"
topic: data-governance
subtopic: unity-catalog-governance
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [unity-catalog, databricks, lineage, column-masking, volumes, ml-governance]
---

# Unity Catalog Governance — Intermediate

## Metastore Setup and Workspace Assignment

```python
# Databricks SDK: set up Unity Catalog
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CreateMetastore, AssignRequest, CreateCatalog, CreateSchema
)

w = WorkspaceClient()

# 1. Create the metastore (one per region)
# Usually done by account admin via UI, but can be scripted
metastore = w.metastores.create(
    name="prod-metastore-us-east-1",
    storage_root="s3://company-unity-catalog-root/us-east-1/",
    region="us-east-1",
)

# 2. Assign workspace to metastore
w.metastores.assign(
    workspace_id=1234567890,  # Your workspace ID
    metastore_id=metastore.metastore_id,
    default_catalog_name="prod",
)

# 3. Create catalog structure
w.catalogs.create(name="prod", comment="Production data catalog")
w.catalogs.create(name="dev",  comment="Development/testing catalog")
w.catalogs.create(name="ml",   comment="ML models and features")

# 4. Create schemas
w.schemas.create(catalog_name="prod", name="gold",   comment="Gold/curated data layer")
w.schemas.create(catalog_name="prod", name="silver", comment="Silver/cleaned data layer")
w.schemas.create(catalog_name="prod", name="bronze", comment="Bronze/raw data layer")
```

---

## Table Tags and Comments

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableInfo, UpdateTableRequest

w = WorkspaceClient()

# Set table comment (description)
w.tables.update(
    full_name="prod.gold.orders",
    comment="Cleaned and deduped orders from all channels. SOT for revenue.",
)

# Set column comments
w.tables.update(
    full_name="prod.gold.orders",
    columns=[
        {
            "name": "customer_email",
            "comment": "Customer email at time of order. PII — hashed for non-approved roles.",
            "type_name": "STRING",
        }
    ]
)

# Add custom tags (key-value metadata)
# Via SQL
spark.sql("""
    ALTER TABLE prod.gold.orders 
    SET TAGS ('sensitivity' = 'restricted', 'owner' = 'revenue-team', 'domain' = 'sales')
""")

# Retrieve tags
result = spark.sql("SHOW TAGS ON TABLE prod.gold.orders")
result.show()
# +----------------+-------------+
# |key             |value        |
# +----------------+-------------+
# |sensitivity     |restricted   |
# |owner           |revenue-team |
# +----------------+-------------+
```

---

## Unity Catalog Lineage

UC captures lineage automatically for all Spark and SQL operations in Databricks:

```python
# Lineage is captured automatically — no configuration needed
# Just run your Spark jobs and queries

# Read lineage via REST API
import requests

def get_table_lineage(table_fqn: str, host: str, token: str) -> dict:
    """Get upstream and downstream lineage for a table."""
    headers = {"Authorization": f"Bearer {token}"}
    
    # Downstream lineage
    downstream = requests.get(
        f"https://{host}/api/2.0/lineage-tracking/table-lineage",
        params={"table_name": table_fqn, "include_entity_lineage": True},
        headers=headers,
    ).json()
    
    # Upstream lineage
    upstream = requests.get(
        f"https://{host}/api/2.0/lineage-tracking/table-lineage",
        params={"table_name": table_fqn, "include_entity_lineage": True, "direction": "upstream"},
        headers=headers,
    ).json()
    
    return {"downstream": downstream, "upstream": upstream}

lineage = get_table_lineage("prod.gold.orders", "myworkspace.azuredatabricks.net", TOKEN)
print(f"Upstream tables: {[t['name'] for t in lineage['upstream'].get('upstreamTables', [])]}")
print(f"Downstream tables: {[t['name'] for t in lineage['downstream'].get('downstreamTables', [])]}")
```

---

## Volumes for Governed File Access

```python
# Unity Catalog Volumes: governed access to files
# Replace raw S3/ADLS paths with /Volumes/catalog/schema/volume/

spark.sql("""
    CREATE VOLUME prod.bronze.raw_files
    COMMENT 'Raw landing zone for file-based ingestion'
""")

# Now use volume path instead of direct S3 path
# OLD (ungoverned): df = spark.read.csv("s3://bucket/raw/orders/")
# NEW (governed, audited): 
df = spark.read.csv("/Volumes/prod/bronze/raw_files/orders/")

# Write to volume (all access logged in audit trail)
df_processed.write.parquet("/Volumes/prod/silver/checkpoints/orders_checkpoint/")

# Grant access to volume
spark.sql("""
    GRANT READ VOLUME ON VOLUME prod.bronze.raw_files TO `data-engineers`
""")
spark.sql("""
    GRANT WRITE VOLUME ON VOLUME prod.bronze.raw_files TO `pipeline-service-account`
""")
```

---

## MLflow Model Governance in Unity Catalog

```python
import mlflow
from mlflow import MlflowClient

# Configure MLflow to use Unity Catalog as the model registry
mlflow.set_registry_uri("databricks-uc")

# Register a model to UC (catalog.schema.model_name format)
mlflow.register_model(
    model_uri="runs:/abc123/model",
    name="prod.ml.churn_model",
)

client = MlflowClient()

# Set model alias (replaces staging/production stage in UC)
client.set_registered_model_alias(
    name="prod.ml.churn_model",
    alias="production",
    version=3,
)

# Add governance tags
client.set_registered_model_tag(
    name="prod.ml.churn_model",
    key="owner",
    value="ml-team@company.com",
)

client.set_registered_model_tag(
    name="prod.ml.churn_model",
    key="training_data",
    value="prod.gold.customers",  # Links to training data for lineage
)

# Grant access
spark.sql("""
    GRANT EXECUTE ON MODEL prod.ml.churn_model TO `data-scientists`
""")

# Load model (access controlled by UC)
model = mlflow.pyfunc.load_model("models:/prod.ml.churn_model@production")
```

---

## Interview Tips

> **Tip 1:** "How does Unity Catalog handle lineage?" — UC captures lineage automatically for all Databricks SQL queries and Spark operations — no instrumentation needed. Column-level lineage is tracked. Lineage is visible in the Catalog Explorer UI and accessible via REST API. Supports cross-workspace lineage when multiple workspaces share the same metastore.

> **Tip 2:** "What is a UC Volume and how does it differ from a table?" — A Volume is a UC-governed location for unstructured/semi-structured files (not Delta tables). Think: raw CSVs, images, ML model artifacts. Unlike raw S3 paths: access is controlled by UC RBAC, all reads/writes are audited, lineage is tracked. Use Volumes when you need governance for non-tabular data.

> **Tip 3:** "How do you govern ML models in Unity Catalog?" — Register models in the UC model registry using the `databricks-uc` registry URI and three-level namespace (`prod.ml.model_name`). UC tracks: access control (GRANT EXECUTE), model lineage (training data → model), audit log. Aliases (production, staging, champion) replace the old stage-based promotion workflow.
