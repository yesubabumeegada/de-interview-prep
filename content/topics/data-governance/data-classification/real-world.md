---
title: "Data Classification — Real World"
topic: data-governance
subtopic: data-classification
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [data-classification, tagging, snowflake, production, automation]
---

# Data Classification — Real World Patterns

## Pattern 1: Classification Scan on New Table Registration

Classify automatically when a new table is created:

```python
# Airflow DAG: triggered when new table appears in Snowflake
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def classify_new_tables(**context):
    """Find and classify tables without sensitivity tags."""
    import sqlalchemy as sa
    engine = sa.create_engine("snowflake://...")
    
    with engine.connect() as conn:
        # Tables in production with no classification tag
        unclassified = conn.execute(sa.text("""
            SELECT t.table_schema, t.table_name
            FROM information_schema.tables t
            LEFT JOIN (
                SELECT DISTINCT object_name AS table_name
                FROM TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
                    'PROD.%', 'TABLE'
                ))
                WHERE tag_name = 'SENSITIVITY'
            ) tagged ON t.table_name = tagged.table_name
            WHERE t.table_schema NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
              AND t.table_catalog = 'PROD'
              AND tagged.table_name IS NULL
        """)).fetchall()
    
    if not unclassified:
        print("All tables are classified")
        return
    
    from auto_classifier import AutoClassifier
    classifier = AutoClassifier()
    
    for row in unclassified:
        table_fqn = f"{row.table_schema}.{row.table_name}"
        findings = classifier.classify_table(engine, row.table_schema, row.table_name)
        
        if not findings:
            # No PII detected — default to internal
            with engine.begin() as conn:
                conn.execute(sa.text(
                    f"ALTER TABLE PROD.{table_fqn} SET TAG sensitivity = 'internal'"
                ))
            print(f"Classified {table_fqn} as internal (no PII detected)")
        else:
            # PII detected — flag for human review before auto-tagging
            high_conf = [f for f in findings if f.confidence >= 0.85]
            if high_conf:
                with engine.begin() as conn:
                    conn.execute(sa.text(
                        f"ALTER TABLE PROD.{table_fqn} SET TAG sensitivity = 'restricted'"
                    ))
                print(f"Auto-classified {table_fqn} as restricted ({len(high_conf)} high-confidence PII columns)")
            else:
                # Notify data steward for manual review
                notify_steward(table_fqn, findings)
                print(f"Flagged {table_fqn} for manual classification review")

with DAG(
    "auto_classification_scan",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * *",  # Daily midnight
    catchup=False,
) as dag:
    
    classify = PythonOperator(
        task_id="classify_new_tables",
        python_callable=classify_new_tables,
    )
```

---

## Pattern 2: Classification in dbt Pre-Hook

Enforce classification at dbt model creation time:

```python
# macros/enforce_classification.sql — dbt macro
{% macro enforce_classification() %}
  {% set model_sensitivity = model.config.meta.get('sensitivity') %}
  {% if model_sensitivity is none %}
    {{ exceptions.raise_compiler_error(
      "Model '" ~ model.name ~ "' is missing 'meta.sensitivity' classification. "
      "Add: meta: {sensitivity: public|internal|confidential|restricted}"
    ) }}
  {% endif %}
  
  {% if model_sensitivity == 'restricted' %}
    {% set pii_columns = [] %}
    {% for col_name, col in model.columns.items() %}
      {% if 'pii' in col.tags %}
        {% do pii_columns.append(col_name) %}
      {% endif %}
    {% endfor %}
    
    {% if pii_columns | length == 0 %}
      {{ log("WARNING: Model '" ~ model.name ~ "' is classified as 'restricted' but has no PII-tagged columns", info=True) }}
    {% endif %}
  {% endif %}
{% endmacro %}
```

```yaml
# dbt_project.yml
models:
  my_project:
    gold:
      +pre-hook: "{{ enforce_classification() }}"
```

---

## Pattern 3: Classification Compliance Report

```sql
-- Weekly compliance report: classification health by domain
WITH classification_summary AS (
    SELECT
        t.table_schema AS domain,
        t.table_name,
        MAX(CASE WHEN tag.tag_name = 'SENSITIVITY' THEN tag.tag_value END) AS sensitivity,
        MAX(CASE WHEN tag.tag_name = 'REGULATORY' THEN tag.tag_value END) AS regulatory_scope,
        COUNT(CASE WHEN pii_tag.tag_value IS NOT NULL THEN 1 END) AS pii_column_count,
        a.owner,
        a.steward
    FROM information_schema.tables t
    LEFT JOIN TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        t.table_catalog || '.' || t.table_schema || '.' || t.table_name, 'TABLE'
    )) tag ON TRUE
    LEFT JOIN TABLE(INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS(
        t.table_catalog || '.' || t.table_schema || '.' || t.table_name, 'COLUMN'
    )) pii_tag ON pii_tag.tag_name = 'PII_TYPE'
    LEFT JOIN data_catalog.assets a ON a.table_name = t.table_name
    WHERE t.table_schema NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
    GROUP BY t.table_schema, t.table_name, a.owner, a.steward
)
SELECT
    domain,
    COUNT(*) AS total_tables,
    COUNT(sensitivity) AS classified_tables,
    COUNT(CASE WHEN sensitivity = 'restricted' AND pii_column_count = 0 THEN 1 END) AS restricted_without_pii_tags,
    COUNT(CASE WHEN sensitivity IS NULL THEN 1 END) AS unclassified_tables,
    ROUND(COUNT(sensitivity) * 100.0 / NULLIF(COUNT(*), 0), 1) AS classification_rate_pct
FROM classification_summary
GROUP BY domain
ORDER BY classification_rate_pct ASC;
```

---

## Classification Pitfalls

| Pitfall | Risk | Fix |
|---|---|---|
| Classifying tables but not columns | Masking can't be applied without column-level tags | Always classify at column level for PII |
| Manual classification only | Doesn't scale, human error | Automate with scanner + CI enforcement |
| Same sensitivity for all columns in a table | Overly restrictive or under-protective | Column-level classification: order_id = internal, customer_email = restricted |
| No review of auto-classifications | False positives restrict access wrongly | Human review queue for medium-confidence findings |
| Classification set once, never updated | Schema changes introduce unclassified columns | Re-scan on every schema change |
