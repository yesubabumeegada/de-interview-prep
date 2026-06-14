---
title: "Data Governance & Lineage - Real-World Examples"
topic: databricks
subtopic: data-governance-lineage
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [databricks, unity-catalog, governance, gdpr, compliance, lineage, production]
---

# Data Governance & Lineage — Real-World Production Examples

## Production Pattern: GDPR Compliance Automation

A European fintech processes 2M customer records. GDPR requires a 72-hour response window for right-to-erasure requests:

```python
# Automated erasure pipeline (triggered via Databricks Workflow)

def process_erasure_request(customer_id: str, request_id: str, requester_email: str):
    """Full GDPR erasure: find → delete → audit → notify."""

    start_time = datetime.now()
    tables_processed = []
    tables_skipped = []

    # 1. Discover all PII tables via UC tags
    pii_tables = spark.sql("""
        SELECT CONCAT(table_catalog, '.', table_schema, '.', table_name) AS full_name
        FROM system.information_schema.table_tags
        WHERE tag_name = 'gdpr_subject' AND tag_value = 'true'
    """).collect()

    for row in pii_tables:
        table = row["full_name"]
        try:
            # Check if table has customer_id column
            schema = spark.sql(f"DESCRIBE {table}").filter("col_name = 'customer_id'").count()
            if schema == 0:
                tables_skipped.append(table)
                continue

            # Count before deletion
            count_before = spark.sql(f"SELECT COUNT(*) AS n FROM {table} WHERE customer_id = '{customer_id}'").collect()[0]["n"]

            if count_before > 0:
                spark.sql(f"DELETE FROM {table} WHERE customer_id = '{customer_id}'")
                tables_processed.append({"table": table, "rows_deleted": count_before})

        except Exception as e:
            tables_skipped.append(f"{table} (error: {str(e)[:100]})")

    # 2. Vacuum to physically remove data (required for GDPR compliance)
    # Scheduled separately — VACUUM runs after retention period
    spark.sql(f"""
        INSERT INTO prod.compliance.vacuum_queue
        VALUES (current_timestamp(), 'erasure_request', '{request_id}',
                '{",".join([t["table"] for t in tables_processed])}')
    """)

    # 3. Write immutable audit record
    duration_seconds = (datetime.now() - start_time).seconds
    spark.sql(f"""
        INSERT INTO prod.compliance.erasure_audit_log
        VALUES (
            '{request_id}',
            '{customer_id}',
            current_timestamp(),
            '{requester_email}',
            {len(tables_processed)},
            {sum(t["rows_deleted"] for t in tables_processed)},
            {duration_seconds},
            'COMPLETE',
            '{json.dumps(tables_processed)}'
        )
    """)

    print(f"Erasure complete: {len(tables_processed)} tables, {sum(t['rows_deleted'] for t in tables_processed)} rows, {duration_seconds}s")

# Compliance dashboard
spark.sql("""
    SELECT
        DATE_TRUNC('month', completed_at) AS month,
        COUNT(*) AS requests,
        AVG(duration_seconds) AS avg_seconds,
        MAX(duration_seconds) AS max_seconds,
        SUM(CASE WHEN duration_seconds > 259200 THEN 1 ELSE 0 END) AS sla_breaches  -- 72h = 259200s
    FROM prod.compliance.erasure_audit_log
    GROUP BY 1
    ORDER BY 1 DESC
""").display()
```

---

## Production Pattern: Data Access Governance for HIPAA

A healthcare analytics company handles PHI (Protected Health Information) — strict controls required:

```sql
-- Column masking: PHI visible only to authorized roles
CREATE OR REPLACE FUNCTION prod.security.phi_mask_mrn(mrn STRING)
RETURNS STRING
LANGUAGE SQL
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('phi-authorized-researchers')
      OR IS_ACCOUNT_GROUP_MEMBER('clinical-data-stewards')
    THEN mrn
    ELSE SHA2(mrn, 256)  -- hash for de-identified analytics
END;

CREATE OR REPLACE FUNCTION prod.security.phi_mask_dob(dob DATE)
RETURNS DATE
LANGUAGE SQL
RETURN CASE
    WHEN IS_ACCOUNT_GROUP_MEMBER('phi-authorized-researchers')
    THEN dob
    ELSE DATE_TRUNC('year', dob)  -- age year only for de-identified
END;

ALTER TABLE prod.clinical.patient_records
    ALTER COLUMN mrn SET MASK prod.security.phi_mask_mrn;
ALTER TABLE prod.clinical.patient_records
    ALTER COLUMN date_of_birth SET MASK prod.security.phi_mask_dob;

-- Row filter: researchers can only access patients in their approved study
CREATE OR REPLACE FUNCTION prod.security.study_access_filter(study_id STRING)
RETURNS BOOLEAN
LANGUAGE SQL
RETURN (
    IS_ACCOUNT_GROUP_MEMBER('clinical-data-stewards')
    OR study_id IN (
        SELECT study_id FROM prod.compliance.researcher_study_approvals
        WHERE researcher_email = CURRENT_USER()
          AND approval_expiry > CURRENT_DATE()
    )
);

ALTER TABLE prod.clinical.patient_records
    SET ROW FILTER prod.security.study_access_filter ON (study_id);
```

**Weekly HIPAA access audit:**
```sql
SELECT
    user_identity.email AS researcher,
    request_params.full_name_arg AS table_accessed,
    COUNT(*) AS query_count,
    MIN(event_time) AS first_access,
    MAX(event_time) AS last_access
FROM system.access.audit
WHERE action_name = 'select'
  AND request_params.full_name_arg LIKE 'prod.clinical.%'
  AND event_time >= DATEADD(week, -1, CURRENT_TIMESTAMP())
GROUP BY 1, 2
ORDER BY query_count DESC;
```

---

## Production Pattern: Breaking Change Prevention via Lineage

A team planned to rename `prod.sales.orders.legacy_region_code` → `region_id`. Lineage check prevented a major incident:

```python
# Pre-deployment check in CI/CD pipeline
impact = pre_change_impact_analysis("prod.sales.orders", "legacy_region_code")

"""
Result:
  Downstream tables: 7
    - prod.reporting.regional_summary
    - prod.analytics.sales_by_region
    - prod.marketing.campaign_roi
    - prod.finance.quarterly_revenue
    - prod.bi.executive_dashboard
    - prod.ml.features.sales_features
    - prod.ops.fulfillment_metrics
  Downstream dashboards: 3
    - Executive Revenue Dashboard (CEO + CFO subscribers)
    - Regional Sales Performance (50 users)
    - Operations Daily Briefing (ops team)
  Total impact: 10 objects
"""

# This surfaced:
# 1. The BI executive dashboard would break silently (no error, just NULL values)
# 2. The ML feature store would produce wrong features (wrong join key)
# 3. 3 dashboards would show blank charts

# Resolution: coordinated migration with view compatibility layer
spark.sql("""
    -- Step 1: Add new column alongside old one
    ALTER TABLE prod.sales.orders ADD COLUMN region_id STRING;
    UPDATE prod.sales.orders SET region_id = legacy_region_code;

    -- Step 2: Create view for backward compatibility
    CREATE OR REPLACE VIEW prod.sales.orders_compat AS
    SELECT *, legacy_region_code AS region_code_deprecated  -- keep old name too
    FROM prod.sales.orders;

    -- Step 3: Migrate consumers over 2-sprint period (notified via Slack)
    -- Step 4: Drop legacy_region_code after all consumers migrated
""")
```

---

## Governance Metrics Dashboard

```sql
-- Weekly governance health report
WITH pii_tables AS (
    SELECT COUNT(*) AS total_pii_tables
    FROM system.information_schema.table_tags
    WHERE tag_name = 'contains_pii' AND tag_value = 'true'
),
untagged_tables AS (
    SELECT COUNT(*) AS total_untagged
    FROM system.information_schema.tables t
    LEFT JOIN system.information_schema.table_tags tg
        ON CONCAT(t.table_catalog, '.', t.table_schema, '.', t.table_name) =
           CONCAT(tg.table_catalog, '.', tg.table_schema, '.', tg.table_name)
           AND tg.tag_name = 'owner'
    WHERE t.table_catalog = 'prod' AND tg.tag_value IS NULL
),
stale_tables AS (
    SELECT COUNT(*) AS total_stale
    FROM system.information_schema.tables
    WHERE table_catalog = 'prod'
      AND last_altered_at < DATEADD(day, -90, CURRENT_TIMESTAMP())
)
SELECT
    p.total_pii_tables,
    u.total_untagged AS tables_missing_owner_tag,
    s.total_stale AS tables_not_updated_90d
FROM pii_tables p, untagged_tables u, stale_tables s;
```
