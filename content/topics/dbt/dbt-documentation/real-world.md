---
title: "dbt Documentation - Real World"
topic: dbt
subtopic: dbt-documentation
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [dbt, documentation, production, governance, exposures, freshness]
---

# dbt Documentation — Real World Patterns

## Pattern 1: The Documentation-as-Contract Workflow

**Problem:** The analytics team has 300 models. Nobody knows what anything means. New engineers spend their first week just reading SQL to understand the data.

**Solution: Documentation sprint + enforced coverage**

**Phase 1 — Inventory what exists:**
```bash
# Find all undocumented models
python3 << 'EOF'
import json, subprocess

result = subprocess.run(['dbt', 'ls', '--output', 'json'], capture_output=True, text=True)
manifest = json.loads(open('target/manifest.json').read())

for node_id, node in manifest['nodes'].items():
    if node.get('resource_type') == 'model' and node.get('package_name') == 'analytics':
        desc = node.get('description', '').strip()
        cols = node.get('columns', {})
        documented_cols = sum(1 for c in cols.values() if c.get('description', '').strip())
        total_cols = len(cols)
        if not desc or documented_cols < total_cols * 0.5:
            print(f"NEEDS DOCS: {node['name']} ({documented_cols}/{total_cols} cols)")
EOF
```

**Phase 2 — Prioritize by impact:**
- Mart-level models (used by BI) first
- Models with exposures (known consumers) second
- Staging models third

**Phase 3 — Enforce in CI:**
```yaml
# .github/workflows/docs_check.yml
- name: Enforce documentation on mart models
  run: |
    python scripts/docs_gate.py \
      --manifest target/manifest.json \
      --require-description "tag:mart" \
      --require-column-desc "tag:mart"
```

## Pattern 2: Multi-Team Documentation Ownership

**Problem:** 6 teams contribute to one dbt project. Nobody knows who owns which models or whom to ask when something breaks.

**Solution: Structured `meta` fields + Slack integration**

```yaml
# models/marts/finance/schema.yml
models:
  - name: fct_revenue
    description: Daily revenue aggregated by product line and region.
    meta:
      owner_team: finance-analytics
      owner_slack: "#finance-data"
      owner_email: finance-data@company.com
      sla_tier: critical
      oncall_runbook: https://notion.so/finance-data/fct-revenue-runbook
      last_reviewed: "2024-11-01"
      reviewed_by: jane.doe@company.com

  - name: fct_churn
    description: Monthly customer churn calculation using 90-day activity window.
    meta:
      owner_team: product-analytics
      owner_slack: "#product-data"
      sla_tier: high
```

**Slack alert with owner routing:**
```python
# When a model fails, route to the right team
def get_model_owner_slack(model_name: str, manifest: dict) -> str:
    for node_id, node in manifest['nodes'].items():
        if node['name'] == model_name:
            return node.get('meta', {}).get('owner_slack', '#data-alerts')
    return '#data-alerts'

# In Airflow failure callback:
slack_channel = get_model_owner_slack(failed_model, manifest)
send_slack_alert(channel=slack_channel, message=f"Model {failed_model} failed!")
```

## Pattern 3: Source Freshness as Data SLA

**Problem:** Finance team's daily report uses Stripe data. When Fivetran has a sync delay, the report silently shows yesterday's numbers. Finance makes decisions on stale data without knowing.

```yaml
# models/staging/schema.yml
sources:
  - name: stripe
    database: raw_data
    schema: stripe
    
    tables:
      - name: charges
        loaded_at_field: _fivetran_synced
        freshness:
          warn_after: {count: 4, period: hour}   # Warn at 4h
          error_after: {count: 8, period: hour}  # Fail at 8h (before 9AM stand-up)
        
        description: |
          Stripe charge events. Synced hourly via Fivetran.
          **SLA:** Data should be < 4 hours old for finance reporting.
          If freshness check fails, contact #data-platform Slack.
```

```bash
# In the nightly pipeline, run freshness FIRST
dbt source freshness --output json > freshness_results.json

# Parse results and alert
python scripts/freshness_alerter.py freshness_results.json
# → Sends to Slack if any source is stale BEFORE running expensive dbt build
```

## Pattern 4: Exposure-Driven Impact Analysis

**Problem:** Platform team wants to refactor `fct_orders`. They need to know what will break before merging.

```yaml
# exposures.yml — comprehensive exposure documentation
exposures:
  - name: weekly_sales_report
    type: dashboard
    maturity: high
    url: https://tableau.company.com/views/WeeklySales
    depends_on:
      - ref('fct_orders')
      - ref('fct_revenue')
      - ref('dim_customers')
    owner:
      name: Sales Analytics Team
      email: sales-analytics@company.com

  - name: revenue_forecasting_model
    type: ml
    maturity: medium
    depends_on:
      - ref('fct_orders')
      - ref('fct_revenue')
    owner:
      name: Data Science
      email: ds@company.com
```

```bash
# Before merging a change to fct_orders:
dbt ls --select "fct_orders+" --resource-type exposure
# Output: weekly_sales_report, revenue_forecasting_model

# Now you know exactly who to notify before deploying
```

## Pattern 5: Self-Hosted Docs with Auth

**Problem:** Company needs dbt docs hosted internally — stakeholders must be able to browse but it must be behind SSO.

```yaml
# docker-compose.yml
services:
  dbt-docs:
    image: nginx:alpine
    volumes:
      - ./target:/usr/share/nginx/html/docs:ro
    ports:
      - "80:80"
  
  oauth-proxy:
    image: quay.io/oauth2-proxy/oauth2-proxy
    environment:
      OAUTH2_PROXY_PROVIDER: google
      OAUTH2_PROXY_CLIENT_ID: ${GOOGLE_CLIENT_ID}
      OAUTH2_PROXY_CLIENT_SECRET: ${GOOGLE_CLIENT_SECRET}
      OAUTH2_PROXY_EMAIL_DOMAIN: company.com   # only company.com emails
      OAUTH2_PROXY_UPSTREAM: http://dbt-docs:80
    ports:
      - "4180:4180"
```

```bash
# Deploy script (runs after dbt docs generate in CI)
dbt docs generate
rsync -av target/ deploy@docs-server:/var/www/dbt-docs/
```

## Pattern 6: Doc Blocks for a Living Data Dictionary

**Problem:** Business definitions change. "Active customer" means something different to marketing vs. finance. Maintaining this in a wiki leads to drift.

**Solution: Authoritative definitions in dbt doc blocks**

```markdown
<!-- models/docs/business_definitions.md -->

{% docs active_customer %}
**Active Customer** — A customer who has placed at least one order in the past 90 days
and whose account is not suspended.

**Finance definition (ARR):** Active as of the last day of the reporting month.
**Marketing definition (campaigns):** Active in the past 60 days.

When in doubt, use this definition (90-day, end of period).
Last reviewed: 2024-10-15 by Jane Doe (Head of Analytics).
{% enddocs %}

{% docs churned_customer %}
**Churned Customer** — A previously active customer who has not placed an order
in 90+ days.

Note: A customer can re-activate. Track re-activations separately in `fct_reactivations`.
Last reviewed: 2024-10-15 by Jane Doe.
{% enddocs %}
```

```yaml
# Used consistently across all models
models:
  - name: fct_cohort_retention
    columns:
      - name: is_active
        description: "{{ doc('active_customer') }}"
  
  - name: dim_customers
    columns:
      - name: is_churned
        description: "{{ doc('churned_customer') }}"
```

**Result:** When the definition changes, update one doc block → all referencing models instantly reflect the new definition in their generated docs.

## Real Interview Scenarios

**Scenario:** A new stakeholder asks "How do I know if this dashboard is showing current data?" What have you built to answer this?

**Answer:** Three layers of visibility:
1. **Source freshness dashboard** — `dbt source freshness` runs before every build. Results are published to a Slack channel `#data-freshness`. Any source older than the SLA sends a Slack alert.
2. **Exposure documentation** — every dashboard has an exposure definition that links to the freshness of its upstream sources.
3. **Data loaded_at fields** — every mart model has a `_data_updated_at` column derived from the max source timestamp. The dashboard template shows this timestamp in the footer: "Data as of: [timestamp]."

**Scenario:** You're doing a code review and notice a new model has zero descriptions in `schema.yml`. How do you handle this?

**Answer:** Two tracks simultaneously:
1. **Immediate:** Reject the PR with a comment explaining the documentation requirements. Provide a template or point to an existing well-documented model as an example.
2. **Systemic:** If this is happening repeatedly, add a CI check that fails PRs where new mart models have no model-level or column-level descriptions. Use a `docs_gate.py` script parsing `manifest.json`. Make the gate fail loudly with helpful error messages and a link to documentation standards.
