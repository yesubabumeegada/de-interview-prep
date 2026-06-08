---
title: "Metadata Management — Real World"
topic: data-governance
subtopic: metadata-management
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [metadata, production, datahub, dbt, catalog-automation]
---

# Metadata Management — Real World Patterns

## Pattern 1: Metadata Pipeline DAG

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

def sync_operational_metadata(**context):
    """Sync pipeline run stats to the catalog for all tables."""
    import sqlalchemy as sa
    
    engine = sa.create_engine("postgresql://...")
    
    with engine.connect() as conn:
        # Get all tables updated in the last 24 hours
        updated_tables = conn.execute(sa.text("""
            SELECT 
                output_table,
                MAX(completed_at) AS last_updated,
                AVG(duration_seconds) AS avg_duration,
                SUM(rows_written) AS rows_written_24h,
                COUNT(CASE WHEN status = 'success' THEN 1 END) * 1.0 / COUNT(*) AS success_rate
            FROM pipeline_runs
            WHERE completed_at >= NOW() - INTERVAL '24 hours'
            GROUP BY output_table
        """)).fetchall()
    
    # Push to catalog
    for row in updated_tables:
        catalog_client.update_operational_metadata(
            table_name=row.output_table,
            last_updated=row.last_updated,
            avg_pipeline_duration_seconds=row.avg_duration,
            success_rate_24h=row.success_rate,
        )
    
    print(f"Synced operational metadata for {len(updated_tables)} tables")

def check_metadata_completeness(**context):
    """Find tables with incomplete metadata and alert stewards."""
    import sqlalchemy as sa
    
    engine = sa.create_engine("postgresql://...")
    
    with engine.connect() as conn:
        incomplete = conn.execute(sa.text("""
            SELECT table_name, steward,
                   CASE WHEN description IS NULL THEN 'missing_description' END AS issue1,
                   CASE WHEN owner IS NULL THEN 'missing_owner' END AS issue2,
                   CASE WHEN sensitivity IS NULL THEN 'missing_classification' END AS issue3
            FROM data_catalog.assets
            WHERE is_production = TRUE
              AND (description IS NULL OR owner IS NULL OR sensitivity IS NULL)
        """)).fetchall()
    
    # Group by steward
    by_steward = {}
    for row in incomplete:
        issues = [row.issue1, row.issue2, row.issue3]
        issues = [i for i in issues if i is not None]
        by_steward.setdefault(row.steward or "unassigned@company.com", []).append({
            "table": row.table_name,
            "issues": issues,
        })
    
    for steward, tables in by_steward.items():
        notification_client.send(
            to=steward,
            subject=f"[Action Needed] {len(tables)} tables with incomplete metadata",
            body="\n".join(f"  {t['table']}: {', '.join(t['issues'])}" for t in tables),
        )
    
    return len(incomplete)

with DAG(
    "metadata_management_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 7 * * *",
    default_args={"retries": 1},
    catchup=False,
) as dag:
    
    # 1. Generate fresh dbt docs
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="cd /opt/dbt && dbt docs generate --target prod",
    )
    
    # 2. Ingest dbt metadata to DataHub
    ingest_dbt = BashOperator(
        task_id="datahub_ingest_dbt",
        bash_command="datahub ingest -c /opt/datahub/recipes/dbt_prod.yaml",
    )
    
    # 3. Sync operational metadata from pipeline logs
    sync_operational = PythonOperator(
        task_id="sync_operational_metadata",
        python_callable=sync_operational_metadata,
    )
    
    # 4. Check completeness and alert
    check_completeness = PythonOperator(
        task_id="check_metadata_completeness",
        python_callable=check_metadata_completeness,
    )
    
    dbt_docs >> ingest_dbt >> sync_operational >> check_completeness
```

---

## Pattern 2: Metadata-as-Code Review

Track metadata changes in git like code:

```python
# metadata/tables/gold/orders.yaml — version controlled
table: gold.orders
version: 3.2.1
last_modified: 2024-01-15
modified_by: jane.smith@company.com

business_metadata:
  description: >
    Cleaned and deduplicated orders from all sales channels (web, mobile, in-store).
    Excludes cancelled orders. Source of truth for revenue reporting and finance reconciliation.
    Refreshed daily from silver.orders_cleaned after data quality validation.
  owner: revenue-team
  steward: jane.smith@company.com
  domain: sales
  sensitivity: restricted
  sla: "09:00 UTC daily"
  regulatory: [gdpr, ccpa]
  tags: [core, sot, revenue]

columns:
  order_id:
    description: "Unique order identifier — natural key from source system"
    sensitivity: internal
  
  customer_email:
    description: "Customer email at time of order placement"
    sensitivity: restricted
    pii_type: email
    masking: hash_sha256
    regulatory: [gdpr, ccpa]
  
  amount_usd:
    description: "Order total in USD including taxes and shipping"
    sensitivity: internal
    glossary_term: Revenue
```

```bash
# In CI: validate metadata YAML files
python scripts/validate_metadata.py metadata/tables/

# On merge: sync to catalog
python scripts/sync_metadata_to_catalog.py metadata/tables/gold/orders.yaml
```

---

## Pattern 3: Metadata Freshness Monitor

```python
import sqlalchemy as sa
from datetime import datetime, timedelta

def check_metadata_freshness(engine, max_age_hours: int = 26) -> list[dict]:
    """Alert when catalog metadata is stale (not refreshed recently)."""
    
    cutoff = datetime.utcnow() - timedelta(hours=max_age_hours)
    
    with engine.connect() as conn:
        stale = conn.execute(sa.text("""
            SELECT
                table_name,
                last_ingested_at,
                owner,
                steward,
                EXTRACT(EPOCH FROM (NOW() - last_ingested_at)) / 3600 AS hours_since_ingestion
            FROM data_catalog.assets
            WHERE is_production = TRUE
              AND last_ingested_at < :cutoff
            ORDER BY hours_since_ingestion DESC
        """), {"cutoff": cutoff}).fetchall()
    
    if stale:
        print(f"Warning: {len(stale)} tables have stale catalog metadata (>{max_age_hours}h old):")
        for row in stale[:10]:
            print(f"  {row.table_name}: last ingested {row.hours_since_ingestion:.1f}h ago")
        
        # Alert platform team
        notification_client.send(
            to="data-platform@company.com",
            subject=f"[Alert] {len(stale)} tables with stale catalog metadata",
            body=f"The following tables haven't been ingested to DataHub in >{max_age_hours}h...",
        )
    
    return [dict(r._mapping) for r in stale]
```

---

## Metadata Management Pitfalls

| Pitfall | Symptom | Fix |
|---|---|---|
| Metadata never updated after creation | Stale descriptions mislead analysts | Track metadata age; alert when description not reviewed in 90 days |
| Business and technical metadata in different systems | Inconsistency, hard to get full picture | Single catalog that ingests from all sources |
| No API for metadata | Every team queries DataHub differently | Internal metadata API with consistent interface |
| Glossary not linked to columns | "Revenue" definition exists but not linked to any table | Require linking during glossary term approval |
| Metadata quality not measured | No visibility into completeness | Weekly metadata quality scorecard |
