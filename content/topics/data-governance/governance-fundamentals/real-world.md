---
title: "Governance Fundamentals — Real World"
topic: data-governance
subtopic: governance-fundamentals
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [data-governance, dbt, catalog, compliance, production]
---

# Governance Fundamentals — Real World Patterns

## Pattern 1: dbt-Based Governance Enforcement

Embed governance requirements directly in dbt models using metadata and tests:

```yaml
# models/gold/schema.yml
version: 2

models:
  - name: orders
    description: >
      Cleaned, deduped orders from all sales channels (web, mobile, in-store).
      Joins bronze.orders_raw → silver.orders → gold.orders.
      Excludes test and internal orders. Source of truth for revenue reporting.
    meta:
      owner: revenue-team
      steward: jane.smith@company.com
      domain: sales
      sensitivity: internal
      pii: false
      sla: "available by 09:00 UTC daily"
      tags: [core, revenue, sot]
    
    columns:
      - name: order_id
        description: Unique identifier for each order. Natural key from source system.
        tests:
          - unique
          - not_null
      
      - name: customer_email
        description: Customer email address used at time of order.
        meta:
          pii: true
          pii_type: email
          masking: hash_sha256_in_non_prod
        tags: [pii]
        tests:
          - not_null
      
      - name: order_total_usd
        description: Total order value in USD including tax and shipping.
        tests:
          - not_null
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 100000
```

```python
# scripts/governance_check_dbt.py
import subprocess
import json

def get_undocumented_models() -> list[str]:
    """Find dbt models missing description or owner."""
    result = subprocess.run(
        ["dbt", "ls", "--output", "json", "--select", "tag:core"],
        capture_output=True, text=True
    )
    
    undocumented = []
    manifest_path = "target/manifest.json"
    with open(manifest_path) as f:
        manifest = json.load(f)
    
    for node_id, node in manifest["nodes"].items():
        if node.get("resource_type") != "model":
            continue
        if not node.get("description"):
            undocumented.append(f"{node['name']}: missing description")
        if not node.get("meta", {}).get("owner"):
            undocumented.append(f"{node['name']}: missing meta.owner")
    
    return undocumented

issues = get_undocumented_models()
for issue in issues:
    print(f"⚠️  {issue}")
```

---

## Pattern 2: Automated Governance Dashboard

Build a SQL-based governance scorecard:

```sql
-- Governance scorecard dashboard query
WITH
asset_coverage AS (
    SELECT
        domain,
        COUNT(*) AS total_tables,
        COUNT(CASE WHEN description IS NOT NULL AND LENGTH(description) > 50 THEN 1 END) AS documented,
        COUNT(CASE WHEN owner IS NOT NULL THEN 1 END) AS has_owner,
        COUNT(CASE WHEN steward IS NOT NULL THEN 1 END) AS has_steward,
        COUNT(CASE WHEN last_updated > NOW() - INTERVAL '24 hours' THEN 1 END) AS freshness_ok
    FROM data_catalog.assets
    WHERE is_production = TRUE
    GROUP BY domain
),
dq_by_domain AS (
    SELECT
        a.domain,
        AVG(m.pass_rate) AS avg_dq_pass_rate
    FROM dq_metrics m
    JOIN data_catalog.assets a ON m.table_name = a.table_name
    WHERE m.measured_at >= NOW() - INTERVAL '7 days'
    GROUP BY a.domain
),
pii_coverage AS (
    SELECT
        a.domain,
        COUNT(CASE WHEN c.is_pii AND c.is_tagged_pii THEN 1 END) * 100.0
            / NULLIF(COUNT(CASE WHEN c.is_pii THEN 1 END), 0) AS pii_tag_rate
    FROM data_catalog.columns c
    JOIN data_catalog.assets a ON c.table_name = a.table_name
    GROUP BY a.domain
)
SELECT
    ac.domain,
    ac.total_tables,
    ROUND(ac.documented * 100.0 / ac.total_tables, 1) AS doc_coverage_pct,
    ROUND(ac.has_owner * 100.0 / ac.total_tables, 1) AS owner_coverage_pct,
    ROUND(dq.avg_dq_pass_rate * 100, 1) AS dq_pass_rate_pct,
    ROUND(pc.pii_tag_rate, 1) AS pii_tag_rate_pct,
    -- Overall governance score (weighted)
    ROUND(
        (ac.documented * 1.0 / ac.total_tables) * 0.25 +
        (ac.has_owner * 1.0 / ac.total_tables) * 0.25 +
        COALESCE(dq.avg_dq_pass_rate, 0) * 0.30 +
        COALESCE(pc.pii_tag_rate / 100.0, 0) * 0.20,
        3
    ) * 100 AS governance_score
FROM asset_coverage ac
LEFT JOIN dq_by_domain dq ON ac.domain = dq.domain
LEFT JOIN pii_coverage pc ON ac.domain = pc.domain
ORDER BY governance_score DESC;
```

---

## Pattern 3: Governance Issue Triage Workflow

```python
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
import sqlalchemy as sa

def send_weekly_governance_digest(engine, smtp_config: dict):
    """Send each domain steward a weekly digest of their open governance issues."""
    
    with engine.connect() as conn:
        # Get all open issues grouped by steward
        issues = conn.execute(sa.text("""
            SELECT 
                assigned_to AS steward_email,
                COUNT(*) AS total_open,
                SUM(CASE WHEN created_at < NOW() - INTERVAL '14 days' THEN 1 ELSE 0 END) AS stale_count,
                STRING_AGG(issue_id || ': ' || description, '\n' ORDER BY created_at) AS issue_list
            FROM governance_issues
            WHERE status = 'open'
            GROUP BY assigned_to
        """)).fetchall()
    
    for row in issues:
        body = f"""
Hi,

Your weekly Data Governance digest as of {datetime.utcnow().date()}:

Open Issues: {row.total_open}
Stale (> 14 days old): {row.stale_count}

Issues:
{row.issue_list}

Please resolve or update status at: https://governance.company.com/issues?assigned={row.steward_email}

---
Data Governance Team
        """.strip()
        
        msg = MIMEText(body)
        msg["Subject"] = f"[Governance Digest] {row.total_open} open issues assigned to you"
        msg["From"] = "governance@company.com"
        msg["To"] = row.steward_email
        
        with smtplib.SMTP(smtp_config["host"], smtp_config["port"]) as smtp:
            smtp.send_message(msg)
        
        print(f"Digest sent to {row.steward_email} ({row.total_open} issues)")
```

---

## Governance Anti-Patterns

| Anti-Pattern | Symptom | Fix |
|---|---|---|
| Governance theater | Policies exist but aren't enforced | Automate enforcement in CI/CD |
| Ownership vacuum | All tables owned by "data-team" generic group | Assign individual owners, link to on-call |
| Documentation after-the-fact | Teams document tables months later | Gate deploys on documentation completeness |
| PII discovered via audit | PII found in non-prod during security review | Classify columns at schema design time |
| Governance team bottleneck | 3-week wait to publish a new dataset | Self-service with guardrails, not gatekeeping |
| No governance KPIs | Can't tell if program is working | Track catalog coverage, PII tagging, DQ rates |
