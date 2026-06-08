---
title: "Access Control & RBAC — Real World"
topic: data-governance
subtopic: access-control
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [access-control, rbac, iac, terraform, production]
---

# Access Control & RBAC — Real World Patterns

## Pattern 1: Terraform-Managed RBAC (Snowflake)

Manage all Snowflake access as code — version-controlled and auditable:

```hcl
# terraform/snowflake_rbac.tf

terraform {
  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.75"
    }
  }
}

# Create roles
resource "snowflake_role" "analyst_revenue" {
  name    = "ANALYST_REVENUE"
  comment = "Revenue team analysts — read access to gold.orders and gold.revenue_*"
}

resource "snowflake_role" "data_engineer" {
  name    = "DATA_ENGINEER"
  comment = "Data engineering team — read/write to bronze and silver, read gold"
}

resource "snowflake_role" "data_admin" {
  name    = "DATA_ADMIN"
  comment = "Data platform admins — full access"
}

# Role hierarchy
resource "snowflake_role_grants" "engineer_inherits_analyst" {
  role_name = snowflake_role.data_engineer.name
  roles     = [snowflake_role.analyst_revenue.name]
}

resource "snowflake_role_grants" "admin_inherits_engineer" {
  role_name = snowflake_role.data_admin.name
  roles     = [snowflake_role.data_engineer.name]
}

# Grant database/schema usage
resource "snowflake_database_grant" "gold_usage" {
  database_name = "PROD"
  privilege     = "USAGE"
  roles         = [snowflake_role.analyst_revenue.name]
}

resource "snowflake_schema_grant" "gold_schema_usage" {
  database_name = "PROD"
  schema_name   = "GOLD"
  privilege     = "USAGE"
  roles         = [snowflake_role.analyst_revenue.name]
}

# Grant SELECT on all current + future gold tables
resource "snowflake_table_grant" "gold_select" {
  database_name = "PROD"
  schema_name   = "GOLD"
  privilege     = "SELECT"
  roles         = [snowflake_role.analyst_revenue.name]
  on_all        = true
  on_future     = true
}

# Masking policy for PII
resource "snowflake_masking_policy" "email_mask" {
  name     = "EMAIL_MASK"
  database = "PROD"
  schema   = "GOLD"
  
  signature {
    column { name = "val" type = "STRING" }
  }
  
  masking_expression = <<-EOT
    CASE
      WHEN CURRENT_ROLE() IN ('DATA_ADMIN', 'PII_APPROVED') THEN val
      ELSE SHA2(val)
    END
  EOT
  
  return_data_type = "STRING"
}

# Assign users to roles (managed via variables/tfvars)
variable "analyst_revenue_users" {
  type = list(string)
  default = ["john.doe", "jane.smith"]
}

resource "snowflake_user_grant" "analyst_revenue" {
  for_each  = toset(var.analyst_revenue_users)
  user_name = each.value
  role_name = snowflake_role.analyst_revenue.name
}
```

---

## Pattern 2: Access Review Workflow

Quarterly access review to find stale grants:

```python
import sqlalchemy as sa
from datetime import datetime, timedelta
import smtplib
from email.mime.text import MIMEText

def quarterly_access_review(engine, snowflake_conn, notification_client):
    """
    Quarterly process:
    1. Find users with access who haven't used it in 90 days
    2. Notify their manager for review
    3. Auto-revoke if no response in 14 days
    """
    
    with engine.connect() as conn:
        # Find stale grants: access granted but not used recently
        stale_grants = conn.execute(sa.text("""
            SELECT
                g.user_email,
                g.role_name,
                g.granted_at,
                COALESCE(ql.last_query, g.granted_at) AS last_used_at,
                u.manager_email,
                DATE_DIFF('day', COALESCE(ql.last_query, g.granted_at), NOW()) AS days_since_use
            FROM active_grants g
            JOIN users u ON g.user_email = u.user_email
            LEFT JOIN (
                SELECT user_email, MAX(queried_at) AS last_query
                FROM query_logs
                WHERE queried_at >= NOW() - INTERVAL '365 days'
                GROUP BY user_email
            ) ql ON g.user_email = ql.user_email
            WHERE 
                DATE_DIFF('day', COALESCE(ql.last_query, g.granted_at), NOW()) > 90
                AND g.role_name != 'PUBLIC'
            ORDER BY days_since_use DESC
        """)).fetchall()
    
    print(f"Found {len(stale_grants)} stale access grants")
    
    # Group by manager
    by_manager = {}
    for grant in stale_grants:
        by_manager.setdefault(grant.manager_email, []).append(grant)
    
    # Send review requests to managers
    for manager, grants in by_manager.items():
        grant_list = "\n".join(
            f"  - {g.user_email}: {g.role_name} (last used: {g.last_used_at.date()}, {g.days_since_use} days ago)"
            for g in grants
        )
        
        notification_client.send(
            to=manager,
            subject=f"[Action Required] Quarterly access review — {len(grants)} stale grants",
            body=f"""
Please review the following stale access grants for your team members:

{grant_list}

For each grant, confirm in our portal within 14 days:
- KEEP: User still needs this access
- REVOKE: Remove access immediately

Portal: https://governance.company.com/access-review

If no response in 14 days, access will be automatically revoked.
            """.strip()
        )
    
    return by_manager
```

---

## Pattern 3: Automated Access Anomaly Detection

Alert on unusual access patterns:

```sql
-- Find users querying unusual amounts of PII data
WITH daily_pii_queries AS (
    SELECT
        user_email,
        DATE(queried_at) AS query_date,
        COUNT(*) AS daily_pii_queries,
        SUM(rows_returned) AS daily_rows_returned
    FROM query_logs ql
    JOIN data_catalog.assets a ON ql.table_name = a.table_name
    WHERE 'pii' = ANY(a.tags)
      AND queried_at >= NOW() - INTERVAL '30 days'
    GROUP BY user_email, DATE(queried_at)
),
user_baseline AS (
    SELECT
        user_email,
        AVG(daily_pii_queries) AS avg_daily_queries,
        STDDEV(daily_pii_queries) AS std_daily_queries,
        AVG(daily_rows_returned) AS avg_daily_rows
    FROM daily_pii_queries
    WHERE query_date < CURRENT_DATE - 7  -- baseline from historical data
    GROUP BY user_email
)
SELECT
    d.user_email,
    d.query_date,
    d.daily_pii_queries,
    d.daily_rows_returned,
    b.avg_daily_queries AS normal_queries,
    ROUND((d.daily_pii_queries - b.avg_daily_queries) / NULLIF(b.std_daily_queries, 0), 2) AS z_score
FROM daily_pii_queries d
JOIN user_baseline b ON d.user_email = b.user_email
WHERE d.query_date = CURRENT_DATE - 1  -- yesterday's anomalies
  AND (d.daily_pii_queries - b.avg_daily_queries) / NULLIF(b.std_daily_queries, 0) > 3
ORDER BY z_score DESC;
```

---

## Access Control Gotchas

| Gotcha | Impact | Fix |
|---|---|---|
| Schema-level grants don't cover new tables | New tables accessible by unexpected roles | Use GRANT ON FUTURE TABLES |
| Service account shared across pipelines | One compromise → all pipelines at risk | One service account per pipeline |
| Access never reviewed after employee role change | Analyst promoted to admin retains both | Quarterly access review + HR offboarding hook |
| Dynamic masking bypassed via view | View created without masking policy applied | Apply masking to source columns, not views |
| `PUBLIC` role has unexpected grants | Everyone in Snowflake has inadvertent access | Audit and revoke all non-trivial PUBLIC grants |
