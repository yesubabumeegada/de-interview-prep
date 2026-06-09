---
title: "Catalog and Governance — Scenarios"
topic: data-lakehouse
subtopic: catalog-and-governance
content_type: scenario_question
tags: [catalog, unity-catalog, iceberg-catalog, governance, scenarios]
---

# Catalog and Governance — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: What is a Data Catalog and Why Does It Matter?

**Scenario:** You join a data team where analysts frequently complain they don't know what tables exist, what columns mean, or whether data is fresh. The team has no data catalog. Explain what a data catalog is and what immediate value it would provide.

<details>
<summary>💡 Hint</summary>

A data catalog is both a technical system (metadata store) and an organizational tool (discoverability, lineage, ownership). Think about the pain points: duplicate tables, undocumented schemas, no ownership, no freshness SLAs.

</details>

<details>
<summary>✅ Solution</summary>

**What is a Data Catalog?**

A data catalog is a metadata management system that provides:
1. **Discovery** — searchable inventory of all data assets (tables, dashboards, ML models)
2. **Documentation** — column descriptions, business definitions, owners
3. **Lineage** — where data comes from and where it flows
4. **Governance** — access policies, PII tagging, compliance tracking
5. **Quality signals** — freshness, row counts, test results

**Immediate Value for Your Team:**

| Problem | Catalog Solution |
|---------|-----------------|
| "What tables exist?" | Searchable asset inventory |
| "What does this column mean?" | Column-level descriptions and tags |
| "Is this data fresh?" | Freshness metadata and SLA tracking |
| "Who owns this?" | Ownership assignment |
| "Is it safe to use?" | PII/sensitivity tags, access controls |

**Popular Catalog Tools:**
- **Open-source:** Apache Atlas, OpenMetadata, DataHub
- **Cloud-native:** AWS Glue Data Catalog, Azure Purview, Google Data Catalog
- **Databricks:** Unity Catalog (combines catalog + governance)
- **Commercial:** Alation, Atlan, Collibra

**Quick Win — Add Column Descriptions in dbt:**
```yaml
# models/orders.yml
models:
  - name: orders
    description: "One row per customer order. Source: Salesforce CRM."
    columns:
      - name: order_id
        description: "Globally unique order identifier (UUID v4)"
      - name: customer_id
        description: "FK to dim_customers. Never null."
      - name: total_amount
        description: "Order total in USD, inclusive of tax and shipping"
        tests:
          - not_null
          - positive_value
```

When dbt generates docs (`dbt docs generate`), these flow into the catalog automatically if integrated.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Implementing Unity Catalog for a Multi-Workspace Databricks Environment

**Scenario:** Your company has 4 Databricks workspaces: prod, staging, dev, and a shared analytics workspace. Each has its own Hive metastore. You're tasked with migrating to Unity Catalog to enable centralized governance, cross-workspace access, and fine-grained column-level security. Design the migration plan.

<details>
<summary>💡 Hint</summary>

Unity Catalog uses a 3-level namespace: catalog.schema.table. A Unity Catalog metastore is account-level (shared across workspaces). Plan your catalog hierarchy, external location setup, and privilege model before migrating data.

</details>

<details>
<summary>✅ Solution</summary>

**Unity Catalog Hierarchy Design:**

```
Account Metastore (single, account-level)
├── catalog: prod
│   ├── schema: raw
│   ├── schema: silver
│   └── schema: gold
├── catalog: staging
│   └── schema: ...
├── catalog: dev
│   └── schema: ...
└── catalog: shared_analytics
    ├── schema: finance
    └── schema: marketing
```

**Step 1: Set Up External Locations**

```python
# In Databricks account console or via Terraform
# External location maps S3 paths to Unity Catalog

# SQL
CREATE EXTERNAL LOCATION prod_data
URL 's3://company-prod-data/'
WITH (STORAGE CREDENTIAL prod_s3_credential);

GRANT READ FILES ON EXTERNAL LOCATION prod_data TO `data-engineers`;
```

**Step 2: Migrate Hive Tables to Unity Catalog**

```python
# Upgrade existing Hive tables in-place
spark.sql("""
  UPGRADE TABLE hive_metastore.default.orders
  TO prod.raw.orders
""")

# For managed tables, use SYNC
spark.sql("""
  SYNC prod.raw FROM hive_metastore.default
  FULL
""")
```

**Step 3: Column-Level Security for PII**

```sql
-- Tag PII columns
ALTER TABLE prod.silver.customers
ALTER COLUMN ssn SET TAGS ('pii' = 'true', 'pii_type' = 'ssn');

ALTER TABLE prod.silver.customers
ALTER COLUMN email SET TAGS ('pii' = 'true', 'pii_type' = 'email');

-- Row/column filter function
CREATE OR REPLACE FUNCTION mask_ssn(ssn STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('pii_access_group') THEN ssn
  ELSE CONCAT('***-**-', RIGHT(ssn, 4))
END;

-- Apply column mask
ALTER TABLE prod.silver.customers
ALTER COLUMN ssn SET MASK mask_ssn;
```

**Step 4: Privilege Model**

```sql
-- Data engineering team: full access to raw/silver
GRANT USE CATALOG, USE SCHEMA, SELECT, MODIFY
  ON CATALOG prod TO `group:data-engineers`;

-- Analysts: read-only gold layer
GRANT USE CATALOG, USE SCHEMA, SELECT
  ON CATALOG prod.gold TO `group:analysts`;

-- Row-level filter for regional data
CREATE FUNCTION filter_by_region(region STRING)
RETURNS BOOLEAN
RETURN region = current_user_region();  -- custom function

ALTER TABLE prod.gold.sales
ADD ROW FILTER filter_by_region ON (region);
```

**Step 5: Lineage and Auditing**

Unity Catalog automatically captures:
- Column-level lineage from Spark SQL and notebooks
- Query audit logs in `system.access.audit`

```sql
-- Query audit log
SELECT user_name, action_name, request_params, response
FROM system.access.audit
WHERE event_time > CURRENT_TIMESTAMP - INTERVAL 1 DAY
  AND action_name = 'commandSubmit';
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Federated Catalog Architecture Across Multiple Clouds and Engines

**Scenario:** Your organization operates on AWS (primary), GCP (data science team), and Azure (EU regulatory). Each has its own data stores and tools (Trino on AWS, BigQuery on GCP, Synapse on Azure). Design a federated data catalog architecture that provides unified discovery, lineage, and governance without requiring data movement.

<details>
<summary>💡 Hint</summary>

Consider an open metadata standard (OpenMetadata, DataHub) as the central catalog, with connectors to each cloud's native catalog. For lineage, OpenLineage is the emerging standard (Marquez, Atlan support it). Cross-catalog querying can use Iceberg REST catalog with engine-specific connectors.

</details>

<details>
<summary>✅ Solution</summary>

**Federated Catalog Architecture:**

```
┌─────────────────────────────────────────────────────┐
│              Central Metadata Plane                  │
│         (OpenMetadata / DataHub)                     │
│  - Unified search across all clouds                  │
│  - Cross-cloud lineage (OpenLineage events)          │
│  - Policy propagation                               │
└────────┬──────────────────────┬──────────┬──────────┘
         │                      │          │
    ┌────▼────┐          ┌──────▼──┐  ┌───▼─────┐
    │  AWS    │          │   GCP   │  │  Azure  │
    │ Glue    │          │  Data   │  │ Purview │
    │ Catalog │          │ Catalog │  │         │
    │+ Iceberg│          │+BigQuery│  │+Synapse │
    └─────────┘          └─────────┘  └─────────┘
```

**1. OpenMetadata Connectors Setup:**

```yaml
# openmetadata-ingestion config for AWS Glue
source:
  type: glue
  serviceName: aws-prod
  serviceConnection:
    config:
      type: Glue
      awsConfig:
        awsRegion: us-east-1
  sourceConfig:
    config:
      type: DatabaseMetadata

# GCP BigQuery connector
source:
  type: bigquery
  serviceName: gcp-ds
  serviceConnection:
    config:
      type: BigQuery
      credentials:
        gcpConfig:
          type: service_account
          projectId: gcp-datascience
```

**2. OpenLineage for Cross-Cloud Lineage:**

Emit OpenLineage events from each engine:

```python
# Spark with OpenLineage
spark = SparkSession.builder     .config("spark.extraListeners",
            "io.openlineage.spark.agent.OpenLineageSparkListener")     .config("spark.openlineage.transport.type", "http")     .config("spark.openlineage.transport.url",
            "https://openmetadata.internal/api/v1/lineage")     .getOrCreate()
# All Spark SQL now auto-emits lineage events

# dbt with OpenLineage
# dbt-openlineage integration emits events on dbt run
```

**3. Federated Policy Propagation:**

```python
# Tag propagation: tag in central catalog → sync to each cloud
class PolicyPropagator:
    def propagate_pii_tag(self, table_fqn: str, column: str):
        # Central catalog tags column as PII
        openmetadata_client.add_tag(table_fqn, column, "PII")
        
        # AWS: update Lake Formation column tag
        lakeformation.add_lf_tags_to_resource(
            Resource={'TableWithColumns': {
                'DatabaseName': db, 'TableName': table,
                'ColumnNames': [column]
            }},
            LFTags=[{'TagKey': 'pii', 'TagValues': ['true']}]
        )
        
        # GCP: update BigQuery policy tag
        bigquery_client.update_column_policy_tag(
            table_ref, column, pii_policy_tag_id
        )
        
        # Azure: update Purview sensitivity label
        purview_client.set_sensitivity_label(
            table_fqn, column, "Confidential-PII"
        )
```

**4. Cross-Cloud Discovery Without Data Movement:**

Use Trino Federation + Iceberg REST catalog for query federation:

```sql
-- Trino can query AWS Glue Iceberg tables, GCS Iceberg tables
-- without moving data
SELECT a.customer_id, b.model_score
FROM aws_catalog.gold.customers a
JOIN gcp_catalog.ml_features.customer_scores b
  ON a.customer_id = b.customer_id
WHERE a.region = 'EU';
```

**Key Architecture Principles:**
1. **Metadata moves, data doesn't** — crawlers push metadata to central catalog
2. **OpenLineage as lingua franca** — engine-agnostic lineage events
3. **Policy at the catalog, enforced at the engine** — central definition, local enforcement
4. **Immutable audit trail** — all catalog events logged to append-only store

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What is the difference between a technical catalog and a business catalog?" — A technical catalog stores schema, statistics, and lineage (e.g., Glue, Hive Metastore). A business catalog adds ownership, descriptions, business glossary, and data quality (e.g., DataHub, Alation). Modern tools blend both.
> **Tip 2:** "How do you handle catalog sprawl across teams?" — Establish a governance committee, enforce naming conventions (domain.team.entity), use automated tagging for PII/sensitivity, and set stale-asset policies (archive tables unused for >90 days).
> **Tip 3:** "What is OpenLineage?" — OpenLineage is an open standard (CNCF) for capturing data lineage events. Supported by Spark, Airflow, dbt, Flink. Events are JSON payloads describing input/output datasets per job run, enabling cross-engine lineage graphs.
