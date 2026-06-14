---
title: "Data Governance & Lineage - Senior Deep Dive"
topic: databricks
subtopic: data-governance-lineage
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [databricks, unity-catalog, gdpr, compliance, data-mesh, governance-architecture]
---

# Data Governance & Lineage — Senior Deep Dive

## Governance Architecture for Multi-Team Lakehouse

```
┌───────────────────────────────────────────────────────────────┐
│                   Unity Catalog Structure                      │
│                                                               │
│  Catalogs (by environment/domain):                            │
│    prod.{domain}.*    — production data                       │
│    dev.{domain}.*     — development sandbox                   │
│    shared.*           — cross-domain shared assets            │
│                                                               │
│  Access Control Hierarchy:                                    │
│    Account Admins                                             │
│         ↓                                                     │
│    Catalog Owners (platform team)                             │
│         ↓                                                     │
│    Schema Owners (domain data teams)                          │
│         ↓                                                     │
│    Table Owners (pipeline owners)                             │
│         ↓                                                     │
│    Readers (analysts, data scientists, apps)                  │
└───────────────────────────────────────────────────────────────┘
```

---

## GDPR/CCPA Compliance Implementation

**Right to erasure (right to be forgotten):**

```python
def gdpr_erasure_request(customer_id: str, request_id: str):
    """Process a GDPR right-to-erasure request across all PII tables."""

    # 1. Find all tables that contain this customer's data
    pii_tables = spark.sql("""
        SELECT CONCAT(table_catalog, '.', table_schema, '.', table_name) AS full_name
        FROM system.information_schema.table_tags
        WHERE tag_name = 'contains_pii' AND tag_value = 'true'
          AND table_catalog = 'prod'
    """).collect()

    affected_tables = []
    for row in pii_tables:
        try:
            count = spark.sql(f"""
                SELECT COUNT(*) AS cnt FROM {row['full_name']}
                WHERE customer_id = '{customer_id}'
            """).collect()[0]["cnt"]
            if count > 0:
                affected_tables.append(row["full_name"])
        except Exception:
            pass  # Table may not have customer_id column

    # 2. Delete from each affected table
    for table in affected_tables:
        spark.sql(f"""
            DELETE FROM {table}
            WHERE customer_id = '{customer_id}'
        """)
        print(f"Deleted from {table}")

    # 3. Log the erasure for compliance audit trail
    spark.sql(f"""
        INSERT INTO prod.compliance.erasure_log
        VALUES ('{request_id}', '{customer_id}',
                current_timestamp(), '{",".join(affected_tables)}',
                current_user())
    """)

    return {"tables_affected": affected_tables, "status": "complete"}
```

**Data retention automation:**

```python
# Tag tables with retention policy, enforce via scheduled job
spark.sql("""
    ALTER TABLE prod.logs.raw_events
    SET TAGS ('retention_days' = '90', 'auto_delete' = 'true')
""")

# Daily retention enforcement job
retention_tables = spark.sql("""
    SELECT
        CONCAT(table_catalog, '.', table_schema, '.', table_name) AS full_name,
        tag_value AS retention_days
    FROM system.information_schema.table_tags
    WHERE tag_name = 'retention_days'
""").collect()

for row in retention_tables:
    spark.sql(f"""
        DELETE FROM {row['full_name']}
        WHERE event_date < DATEADD(day, -{row['retention_days']}, CURRENT_DATE())
    """)
```

---

## Data Mesh Governance Model

In a data mesh, domains own their data. Unity Catalog enables federated governance:

```python
# Domain ownership model
domains = {
    "sales": {
        "catalog": "prod",
        "schema_prefix": "sales_",
        "owner_group": "sales-data-engineers",
        "reader_group": "sales-analysts"
    },
    "marketing": {
        "catalog": "prod",
        "schema_prefix": "marketing_",
        "owner_group": "marketing-data-engineers",
        "reader_group": "marketing-analysts"
    }
}

# Provision new domain
def provision_domain(domain_name: str, config: dict):
    schema = f"prod.{domain_name}"

    # Create schema
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    # Set domain-level ownership
    spark.sql(f"ALTER SCHEMA {schema} OWNER TO `{config['owner_group']}`")

    # Grant domain engineers full access to their schema
    spark.sql(f"GRANT ALL PRIVILEGES ON SCHEMA {schema} TO `{config['owner_group']}`")

    # Grant analysts read access
    spark.sql(f"GRANT USE SCHEMA ON SCHEMA {schema} TO `{config['reader_group']}`")

    # Add governance tags
    spark.sql(f"""
        ALTER SCHEMA {schema}
        SET TAGS ('domain' = '{domain_name}',
                  'owner_team' = '{config["owner_group"]}')
    """)

    print(f"Domain {domain_name} provisioned")
```

---

## Attribute-Based Access Control (ABAC)

Fine-grained access beyond role-based controls:

```sql
-- Dynamic data masking based on user attributes
-- Users with 'data_steward' attribute see full PII, others see masked
CREATE OR REPLACE FUNCTION prod.security.dynamic_email_mask(email STRING)
RETURNS STRING
LANGUAGE SQL
RETURN (
    SELECT CASE
        WHEN EXISTS (
            SELECT 1 FROM system.information_schema.applicable_roles
            WHERE grantee = CURRENT_USER()
              AND role_name IN ('pii-stewards', 'compliance-team', 'account-admins')
        )
        THEN email
        ELSE CONCAT(LEFT(email, 1), '***@', SPLIT_PART(email, '@', 2))
    END
);

-- Row-level security based on user's cost center tag
CREATE OR REPLACE FUNCTION prod.security.cost_center_filter(cost_center STRING)
RETURNS BOOLEAN
LANGUAGE SQL
RETURN (
    cost_center = (
        SELECT user_metadata.cost_center
        FROM prod.hr.user_directory
        WHERE user_email = CURRENT_USER()
    )
    OR IS_ACCOUNT_GROUP_MEMBER('finance-global')
);

ALTER TABLE prod.finance.transactions
    SET ROW FILTER prod.security.cost_center_filter ON (cost_center);
```

---

## Lineage-Driven Impact Analysis Automation

```python
# Automated impact analysis before schema changes
def pre_change_impact_analysis(table: str, column: str = None) -> dict:
    """Report downstream dependencies before a breaking change."""

    if column:
        downstream = spark.sql(f"""
            SELECT DISTINCT
                target_table_full_name AS downstream_table,
                target_column_name AS downstream_column,
                entity_type,
                created_by AS pipeline_owner
            FROM system.lineage.column_lineage
            WHERE source_table_full_name = '{table}'
              AND source_column_name = '{column}'
        """).collect()
    else:
        downstream = spark.sql(f"""
            SELECT DISTINCT
                target_table_full_name AS downstream_table,
                entity_type
            FROM system.lineage.table_lineage
            WHERE source_table_full_name = '{table}'
        """).collect()

    # Find dashboard dependencies
    dashboards = spark.sql(f"""
        SELECT dashboard_name, owner_email
        FROM system.lineage.dashboard_lineage
        WHERE source_table_full_name = '{table}'
    """).collect()

    return {
        "table": table,
        "column": column,
        "downstream_tables": [r["downstream_table"] for r in downstream],
        "downstream_dashboards": [r["dashboard_name"] for r in dashboards],
        "total_impact": len(downstream) + len(dashboards)
    }

# Before deprecating a column:
impact = pre_change_impact_analysis("prod.sales.orders", "legacy_order_type")
if impact["total_impact"] > 0:
    print(f"WARNING: {impact['total_impact']} downstream dependencies:")
    for t in impact["downstream_tables"]:
        print(f"  - {t}")
```

---

## Interview Tips

> **Tip 1:** "How do you implement GDPR right-to-erasure in a lakehouse?" — "Three steps: (1) Tag all PII tables with `contains_pii = true` via Unity Catalog tags. (2) When an erasure request arrives, query `system.information_schema.table_tags` to find all PII tables, then DELETE WHERE customer_id = X from each. (3) Log the erasure request and all affected tables to an immutable audit table for compliance proof. Delta's GDPR-compliant deletes ensure the data is removed from all versions after VACUUM."

> **Tip 2:** "What's the challenge with data lineage in a data mesh?" — "In a data mesh, each domain manages its own data. Cross-domain lineage is hard: domain A's output is domain B's input, but they use different catalogs and the lineage graph needs to span them. Unity Catalog handles this at the account level — lineage is captured across all catalogs in the same UC metastore, so cross-domain impact analysis works with the same `system.lineage` queries."

> **Tip 3:** "How do you enforce data quality at the pipeline level, not just at table constraints?" — "Delta table constraints catch bad data at write time but don't give context (which pipeline wrote bad data, how many rows failed). Delta Live Tables expectations are better for pipelines: they capture quality metrics per batch (valid/invalid counts), write quarantined records to a separate table, and surface dashboards. For ad hoc quality checks, a scheduled SQL job queries quality rules against `system.lineage` to trace failures to their origin pipeline."
