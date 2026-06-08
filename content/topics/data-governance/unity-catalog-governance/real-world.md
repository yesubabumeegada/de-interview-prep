---
title: "Unity Catalog Governance — Real World"
topic: data-governance
subtopic: unity-catalog-governance
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [unity-catalog, databricks, production, access-control, lineage]
---

# Unity Catalog Governance — Real World Patterns

## Pattern 1: Automated UC Access Provisioning

Automate user/group provisioning when teams onboard:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import PermissionsChange, SecurableType

w = WorkspaceClient()

# Standard access bundles by role
ACCESS_BUNDLES = {
    "analyst": {
        "prod.gold": ["USE SCHEMA", "SELECT"],
        "prod.silver": ["USE SCHEMA"],  # Browse only
    },
    "engineer": {
        "prod.gold":   ["USE SCHEMA", "SELECT", "CREATE TABLE", "MODIFY"],
        "prod.silver": ["USE SCHEMA", "SELECT", "CREATE TABLE", "MODIFY"],
        "prod.bronze": ["USE SCHEMA", "SELECT", "CREATE TABLE", "MODIFY"],
    },
    "data_scientist": {
        "prod.gold":  ["USE SCHEMA", "SELECT"],
        "ml.models":  ["USE SCHEMA", "SELECT", "CREATE MODEL"],
        "ml.features": ["USE SCHEMA", "SELECT", "CREATE TABLE"],
    },
}

def provision_team_access(team_name: str, role: str, domain: str):
    """
    Provision a new team in Unity Catalog.
    team_name: e.g., "revenue-analysts"
    role: analyst | engineer | data_scientist
    domain: the domain this team owns
    """
    bundle = ACCESS_BUNDLES.get(role)
    if not bundle:
        raise ValueError(f"Unknown role: {role}. Use one of: {list(ACCESS_BUNDLES.keys())}")
    
    # Grant schema-level access
    for schema_fqn, privileges in bundle.items():
        catalog_name, schema_name = schema_fqn.split(".", 1)
        
        w.grants.update(
            securable_type=SecurableType.SCHEMA,
            full_name=schema_fqn,
            changes=[
                PermissionsChange(
                    principal=team_name,
                    add=privileges,
                )
            ],
        )
        print(f"Granted {privileges} on {schema_fqn} to {team_name}")
    
    # Grant catalog USE
    for schema_fqn in bundle:
        catalog_name = schema_fqn.split(".")[0]
        w.grants.update(
            securable_type=SecurableType.CATALOG,
            full_name=catalog_name,
            changes=[PermissionsChange(principal=team_name, add=["USE CATALOG"])],
        )
    
    print(f"Access provisioned for team '{team_name}' with role '{role}'")
    return {"team": team_name, "role": role, "domain": domain, "schemas_granted": list(bundle.keys())}

# Usage: onboard a new team
provision_team_access("revenue-analysts", "analyst", "sales")
provision_team_access("data-eng-platform", "engineer", "platform")
```

---

## Pattern 2: dbt + Unity Catalog Integration

Use dbt with UC as the target:

```yaml
# profiles.yml — dbt profile for Databricks UC
databricks_uc:
  target: prod
  outputs:
    prod:
      type: databricks
      host: myworkspace.azuredatabricks.net
      http_path: /sql/1.0/warehouses/abc123xyz
      token: "{{ env_var('DATABRICKS_TOKEN') }}"
      catalog: prod         # UC catalog (three-level namespace)
      schema: gold
      threads: 8
```

```yaml
# dbt_project.yml
models:
  my_project:
    gold:
      +catalog: prod        # Override catalog per layer
      +schema: gold
      +file_format: delta
      +materialized: table
    
    silver:
      +catalog: prod
      +schema: silver
```

```yaml
# models/gold/schema.yml — with UC-specific governance metadata
models:
  - name: orders
    description: "Source of truth for revenue reporting"
    config:
      grants:
        select: ['data-analysts', 'data-scientists']  # UC grants via dbt
    
    meta:
      owner: revenue-team
      sensitivity: restricted
      tags: [sot, revenue]
    
    columns:
      - name: customer_email
        description: "Customer email — masked for non-PII-approved roles"
        tags: [pii]
        meta:
          pii_type: email
          uc_mask: prod.gold.mask_email  # Reference to UC masking function
```

---

## Pattern 3: Unity Catalog Governance Audit Pipeline

Build a daily governance health check using UC system tables:

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("uc_governance_audit").getOrCreate()

def daily_governance_audit():
    """
    Daily audit using UC system tables:
    1. Unused grants (grant exists but user hasn't queried in 90 days)
    2. PII access anomalies (excessive queries on sensitive tables)
    3. New ungovernanced tables (no tags, no owner)
    """
    
    # 1. Unused grants
    grants_df = spark.sql("""
        SELECT g.principal, g.privilege_type, g.object_name
        FROM system.information_schema.table_privileges g
        LEFT JOIN (
            SELECT user_identity.email AS user, request_params.full_name_arg AS table_name,
                   MAX(event_time) AS last_used
            FROM system.access.audit
            WHERE event_time >= current_date() - 90
            GROUP BY user, table_name
        ) usage ON g.principal = usage.user AND g.object_name = usage.table_name
        WHERE g.table_catalog = 'prod'
          AND usage.last_used IS NULL  -- Grant exists but never used in 90 days
          AND g.principal NOT LIKE '%service%'  -- Exclude service accounts
    """)
    
    # 2. PII access anomalies (more than 100 queries in a day)
    pii_anomalies_df = spark.sql("""
        SELECT 
            DATE(event_time) AS access_date,
            user_identity.email AS user,
            request_params.full_name_arg AS table_name,
            COUNT(*) AS access_count
        FROM system.access.audit
        WHERE event_time >= current_date() - 7
          AND request_params.full_name_arg LIKE 'prod.gold.%'
        GROUP BY DATE(event_time), user_identity.email, request_params.full_name_arg
        HAVING COUNT(*) > 100
        ORDER BY access_count DESC
    """)
    
    # 3. New tables without governance tags
    ungoverned_df = spark.sql("""
        SELECT t.table_catalog, t.table_schema, t.table_name, t.created
        FROM system.information_schema.tables t
        LEFT JOIN system.information_schema.tags tag 
          ON tag.catalog_name = t.table_catalog 
          AND tag.schema_name = t.table_schema 
          AND tag.table_name = t.table_name
          AND tag.tag_name = 'sensitivity'
        WHERE t.table_catalog = 'prod'
          AND t.created >= current_date() - 7  -- New tables in last week
          AND tag.tag_value IS NULL
    """)
    
    # Write audit results to governance table
    grants_df.write.mode("append").saveAsTable("prod.governance.unused_grants_audit")
    pii_anomalies_df.write.mode("append").saveAsTable("prod.governance.pii_access_anomalies")
    ungoverned_df.write.mode("append").saveAsTable("prod.governance.ungoverned_tables")
    
    print(f"Audit complete: {grants_df.count()} unused grants, {pii_anomalies_df.count()} anomalies, {ungoverned_df.count()} ungoverned tables")

daily_governance_audit()
```

---

## Unity Catalog Gotchas

| Gotcha | Impact | Fix |
|---|---|---|
| Catalog-level grants cascade to all schemas | Over-permissive access | Grant at schema level, not catalog level |
| Column masking on views requires function ownership | Masking silently ignored | Grant function ownership to the service principal creating the view |
| UC doesn't support `DENY` — only `GRANT`/`REVOKE` | Can't block specific users within a role | Use row filters and column masks instead of explicit deny |
| Hive metastore tables need explicit migration | Mixed namespace (hive.schema.table + catalog.schema.table) | Use `UPGRADE TABLE` or `DEEP CLONE` to migrate |
| External location permissions separate from table permissions | Storage access error even with table GRANT | Grant both `USAGE` on external location AND table-level privilege |
| Identity federation lag | New users can't access for 10-15 min after AD sync | Run SCIM sync more frequently for time-sensitive onboarding |
