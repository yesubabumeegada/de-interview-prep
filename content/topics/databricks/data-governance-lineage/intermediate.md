---
title: "Data Governance & Lineage - Intermediate"
topic: databricks
subtopic: data-governance-lineage
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [databricks, unity-catalog, lineage, audit, data-quality, classification]
---

# Data Governance & Lineage — Intermediate Concepts

## System Tables: The Governance Data Source

Unity Catalog exposes operational metadata via `system.*` tables — queryable with SQL:

```sql
-- All tables in your account with metadata
SELECT
    table_catalog,
    table_schema,
    table_name,
    table_owner,
    created_at,
    last_altered_at,
    comment
FROM system.information_schema.tables
WHERE table_catalog = 'prod'
  AND table_schema NOT IN ('information_schema')
ORDER BY last_altered_at DESC;

-- Storage usage by catalog
SELECT
    table_catalog,
    SUM(size_gb) AS total_size_gb,
    COUNT(*) AS table_count
FROM (
    SELECT
        table_catalog,
        ROUND(data_length / 1e9, 2) AS size_gb
    FROM system.information_schema.tables
)
GROUP BY 1
ORDER BY 2 DESC;

-- Find all PII-tagged tables
SELECT table_catalog, table_schema, table_name, tag_value
FROM system.information_schema.table_tags
WHERE tag_name = 'contains_pii' AND tag_value = 'true';
```

---

## Audit Logging with system.access

```sql
-- Who accessed what and when
SELECT
    event_time,
    user_identity.email AS user,
    action_name,
    request_params.full_name_arg AS resource,
    response.status_code
FROM system.access.audit
WHERE action_name IN ('getTable', 'createTable', 'dropTable', 'select')
  AND event_time >= DATEADD(day, -7, CURRENT_TIMESTAMP())
ORDER BY event_time DESC;

-- Users who accessed PII tables in the last 30 days
SELECT DISTINCT
    a.user_identity.email AS user,
    a.request_params.full_name_arg AS table_name,
    COUNT(*) AS access_count
FROM system.access.audit a
JOIN system.information_schema.table_tags t
    ON a.request_params.full_name_arg = CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name)
WHERE t.tag_name = 'contains_pii'
  AND t.tag_value = 'true'
  AND a.event_time >= DATEADD(day, -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY access_count DESC;
```

---

## Column-Level Lineage for Impact Analysis

Before dropping or modifying a column, check what downstream objects depend on it:

```sql
-- What tables/views use 'email' column from customers table?
SELECT
    target_table_full_name,
    target_column_name,
    entity_type,
    created_by,
    created_at
FROM system.lineage.column_lineage
WHERE source_table_full_name = 'prod.marketing.customers'
  AND source_column_name = 'email'
ORDER BY created_at DESC;

-- Full upstream lineage for a downstream table (recursive)
WITH RECURSIVE lineage AS (
    -- Base: direct parents
    SELECT
        source_table_full_name AS table_name,
        1 AS depth
    FROM system.lineage.table_lineage
    WHERE target_table_full_name = 'prod.reporting.executive_dashboard'

    UNION ALL

    -- Recurse: parents of parents
    SELECT
        tl.source_table_full_name,
        l.depth + 1
    FROM system.lineage.table_lineage tl
    JOIN lineage l ON tl.target_table_full_name = l.table_name
    WHERE l.depth < 5  -- max depth
)
SELECT DISTINCT table_name, MIN(depth) AS shortest_path
FROM lineage
GROUP BY 1
ORDER BY shortest_path;
```

---

## Automated Data Classification

Classify new tables automatically using a governance job:

```python
from databricks.sdk import WorkspaceClient
import re

w = WorkspaceClient()

PII_COLUMN_PATTERNS = [
    r'email', r'phone', r'ssn', r'social.?security',
    r'credit.?card', r'password', r'ip.?address', r'birth.?date',
    r'first.?name', r'last.?name', r'full.?name', r'address'
]

def classify_table(table_name: str) -> dict:
    """Auto-classify a table based on column names."""
    columns = spark.sql(f"DESCRIBE TABLE {table_name}").collect()
    col_names = [row["col_name"].lower() for row in columns]

    pii_cols = []
    for col in col_names:
        for pattern in PII_COLUMN_PATTERNS:
            if re.search(pattern, col):
                pii_cols.append(col)
                break

    contains_pii = len(pii_cols) > 0
    return {
        "contains_pii": contains_pii,
        "pii_columns": pii_cols,
        "classification": "confidential" if contains_pii else "internal"
    }

# Run on all new tables (daily governance job)
new_tables = spark.sql("""
    SELECT CONCAT(table_catalog, '.', table_schema, '.', table_name) AS full_name
    FROM system.information_schema.tables
    WHERE created_at >= DATEADD(day, -1, CURRENT_TIMESTAMP())
      AND table_catalog = 'prod'
""").collect()

for row in new_tables:
    classification = classify_table(row["full_name"])

    if classification["contains_pii"]:
        spark.sql(f"""
            ALTER TABLE {row['full_name']}
            SET TAGS ('contains_pii' = 'true',
                      'pii_columns' = '{",".join(classification["pii_columns"])}',
                      'data_classification' = 'confidential')
        """)
        print(f"Tagged {row['full_name']}: PII columns = {classification['pii_columns']}")
```

---

## Data Quality with Delta Constraints and Expectations

```sql
-- Enforce quality at write time with constraints
CREATE TABLE prod.sales.orders (
    order_id     STRING NOT NULL,
    customer_id  STRING NOT NULL,
    amount       DOUBLE,
    order_date   DATE,
    status       STRING,
    CONSTRAINT order_id_pk PRIMARY KEY (order_id),
    CONSTRAINT positive_amount CHECK (amount > 0),
    CONSTRAINT valid_status CHECK (status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled')),
    CONSTRAINT not_future_order CHECK (order_date <= CURRENT_DATE())
);

-- Test data quality (ad hoc monitoring)
SELECT
    'null_order_id' AS check_name,
    COUNT(*) AS failures
FROM prod.sales.orders WHERE order_id IS NULL
UNION ALL
SELECT 'negative_amount', COUNT(*)
FROM prod.sales.orders WHERE amount <= 0
UNION ALL
SELECT 'invalid_status', COUNT(*)
FROM prod.sales.orders WHERE status NOT IN ('pending','confirmed','shipped','delivered','cancelled');
```

---

## Interview Tips

> **Tip 1:** "How would you find all tables that depend on a table you want to modify?" — "Query `system.lineage.table_lineage` where `source_table_full_name` equals your table. For column-level impact: `system.lineage.column_lineage` filtered by `source_table_full_name` and `source_column_name`. For deep lineage, use a recursive CTE traversing the lineage graph."

> **Tip 2:** "How do you audit who accessed sensitive data?" — "`system.access.audit` contains every table access event with user identity, action type, timestamp, and resource name. Join with `system.information_schema.table_tags` to filter for PII-tagged tables and get a compliance report of who accessed what sensitive data and when."

> **Tip 3:** "What's the difference between Unity Catalog permissions and Hive metastore permissions?" — "UC uses account-level groups and principals that span all workspaces — one place to manage access across your entire Databricks estate. Hive metastore is workspace-local, requires managing groups separately per workspace, and doesn't support column masking, row filters, or cross-workspace lineage. UC is the modern replacement."
