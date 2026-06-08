---
title: "Data Catalog — Real World"
topic: data-governance
subtopic: data-catalog
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [data-catalog, datahub, airflow, ingestion, production]
---

# Data Catalog — Real World Patterns

## Pattern 1: Airflow DAG for Automated Catalog Ingestion

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def check_ingestion_result(**context):
    """Verify DataHub ingestion succeeded."""
    import subprocess, json
    result = subprocess.run(
        ["datahub", "ingest", "check", "--run-id", context["task_instance"].xcom_pull("run_ingestion")],
        capture_output=True, text=True
    )
    if "FAILED" in result.stdout:
        raise ValueError(f"DataHub ingestion failed:\n{result.stdout}")
    print("DataHub ingestion completed successfully")

with DAG(
    "datahub_catalog_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",  # Daily at 6 AM UTC — before business hours
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    catchup=False,
) as dag:
    
    # 1. Run dbt docs generate to produce manifest + catalog
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="cd /opt/dbt && dbt docs generate --target prod",
    )
    
    # 2. Ingest from Snowflake (technical metadata)
    ingest_snowflake = BashOperator(
        task_id="ingest_snowflake",
        bash_command="datahub ingest -c /opt/datahub/recipes/snowflake_prod.yaml",
    )
    
    # 3. Ingest from dbt (business metadata, lineage)
    ingest_dbt = BashOperator(
        task_id="ingest_dbt",
        bash_command="datahub ingest -c /opt/datahub/recipes/dbt_prod.yaml",
    )
    
    # 4. Ingest from Airflow (pipeline metadata)
    ingest_airflow = BashOperator(
        task_id="ingest_airflow",
        bash_command="datahub ingest -c /opt/datahub/recipes/airflow_prod.yaml",
    )
    
    # 5. Ingest Looker dashboards (downstream lineage)
    ingest_looker = BashOperator(
        task_id="ingest_looker",
        bash_command="datahub ingest -c /opt/datahub/recipes/looker_prod.yaml",
    )
    
    # 6. Verify coverage
    check_coverage = PythonOperator(
        task_id="check_catalog_coverage",
        python_callable=check_ingestion_result,
    )
    
    dbt_docs >> [ingest_snowflake, ingest_dbt, ingest_airflow, ingest_looker] >> check_coverage
```

---

## Pattern 2: Self-Service Table Registration

When teams create tables outside of dbt (direct Spark writes), they need a self-service way to register in the catalog:

```python
# data_catalog_sdk.py — internal library for teams
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.emitter.mce_builder import make_dataset_urn
import datahub.emitter.mce_builder as builder
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    OwnershipClass,
    OwnerClass,
    OwnershipTypeClass,
)

DATAHUB_URL = "http://datahub-gms.internal:8080"

def register_table(
    table_name: str,              # e.g. "gold.customer_ltv"
    platform: str,                # "snowflake", "s3", "spark"
    description: str,
    owner_email: str,
    tags: list[str],
    pii_columns: list[str] = None,
    env: str = "PROD",
) -> str:
    """
    Register a new table in the data catalog.
    Returns the DataHub URN of the registered asset.
    
    Usage:
        urn = register_table(
            table_name="gold.customer_ltv",
            platform="snowflake",
            description="Customer lifetime value aggregated by month. Source of truth for CLV metric.",
            owner_email="ml-team@company.com",
            tags=["ml", "customer", "revenue"],
            pii_columns=["customer_email"],
        )
    """
    if len(description) < 50:
        raise ValueError("Description must be at least 50 characters for governance compliance")
    
    emitter = DatahubRestEmitter(gms_server=DATAHUB_URL)
    urn = make_dataset_urn(platform=platform, name=table_name, env=env)
    
    # Properties
    emitter.emit_mcp(builder.make_mcp(
        entity_urn=urn,
        aspect=DatasetPropertiesClass(
            description=description,
            customProperties={"registered_by": owner_email, "pii_columns": str(pii_columns or [])},
        ),
    ))
    
    # Ownership
    emitter.emit_mcp(builder.make_mcp(
        entity_urn=urn,
        aspect=OwnershipClass(owners=[
            OwnerClass(
                owner=f"urn:li:corpuser:{owner_email}",
                type=OwnershipTypeClass.DATAOWNER,
            )
        ]),
    ))
    
    # Tags (including PII tags for PII columns)
    all_tags = tags[:]
    if pii_columns:
        all_tags.append("has-pii")
    
    from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass
    emitter.emit_mcp(builder.make_mcp(
        entity_urn=urn,
        aspect=GlobalTagsClass(
            tags=[TagAssociationClass(tag=f"urn:li:tag:{t}") for t in all_tags]
        ),
    ))
    
    print(f"✅ Registered {table_name} in catalog: {urn}")
    return urn
```

---

## Pattern 3: Catalog Quality Audit

Weekly audit that finds catalog hygiene issues:

```sql
-- Weekly catalog audit: find assets needing attention
WITH audit AS (
    SELECT
        a.table_name,
        a.domain,
        a.owner,
        a.steward,
        a.description,
        a.last_ingested_at,
        a.monthly_query_count,
        -- Score each asset (0 = bad, 4 = perfect)
        (CASE WHEN a.description IS NOT NULL AND LENGTH(a.description) > 50 THEN 1 ELSE 0 END +
         CASE WHEN a.owner IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN a.steward IS NOT NULL THEN 1 ELSE 0 END +
         CASE WHEN a.last_ingested_at > NOW() - INTERVAL '48 hours' THEN 1 ELSE 0 END
        ) AS health_score
    FROM data_catalog.assets a
    WHERE a.is_production = TRUE
)
SELECT
    table_name,
    domain,
    health_score,
    monthly_query_count,
    CASE
        WHEN description IS NULL THEN 'Missing description'
        WHEN owner IS NULL THEN 'Missing owner'
        WHEN steward IS NULL THEN 'Missing steward'
        WHEN last_ingested_at < NOW() - INTERVAL '48 hours' THEN 'Stale ingestion'
        ELSE 'OK'
    END AS primary_issue
FROM audit
WHERE health_score < 4
ORDER BY monthly_query_count DESC, health_score ASC
LIMIT 50;
```

---

## Gotchas & Lessons Learned

| Issue | Lesson |
|---|---|
| Catalog ingestion breaks when source schema changes | Add retry + alerting to ingestion DAG; don't let stale catalog silently persist |
| Business descriptions drift out of date | Trigger review workflow when description >6 months old and table modified |
| Lineage graph has broken edges after table renames | Use URN-based lineage (platform + name + env) — renames require re-emitting lineage events |
| dbt + DataHub ingestion conflicts with auto-generated metadata | Run dbt ingestion AFTER Snowflake ingestion — dbt enriches, doesn't replace |
| Teams ignore catalog because search results are bad | Improve Elasticsearch weights: prioritize description match and usage count in ranking |
