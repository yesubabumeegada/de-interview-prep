---
title: "dbt Documentation - Senior Deep Dive"
topic: dbt
subtopic: dbt-documentation
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [dbt, documentation, contracts, metadata, lineage, governance]
---

# dbt Documentation — Senior Deep Dive

## Model Contracts — Enforced Documentation

Model contracts (dbt 1.5+) transform documentation into compile-time guarantees. They enforce column presence, data types, and constraints.

```yaml
# models/marts/schema.yml
models:
  - name: fct_orders
    config:
      contract:
        enforced: true   # dbt will validate schema at build time
    columns:
      - name: order_id
        data_type: varchar
        constraints:
          - type: not_null
          - type: unique    # Enforced at warehouse level (not just dbt test)
        description: Unique identifier for each order.

      - name: customer_id
        data_type: varchar
        constraints:
          - type: not_null
        description: "{{ doc('customer_id') }}"

      - name: order_date
        data_type: date
        constraints:
          - type: not_null

      - name: total_amount
        data_type: numeric
        description: "{{ doc('total_amount') }}"
```

**What contract enforcement does:**
1. At compile time: verifies the SELECT list includes all declared columns with matching names
2. At run time: verifies data types match (adapter-specific behavior)
3. In dbt Mesh: contracts are required for cross-project `ref()` — they define the interface

**When contracts fail:**
```
Compilation Error
  Contract violation: ...
  Expected: order_id (varchar), customer_id (varchar), order_date (date), total_amount (numeric)
  Got: order_id (varchar), customer_id (varchar), order_date (timestamp)  ← mismatch!
```

## Model Versioning — Documentation for Breaking Changes

When you need to change a public model's interface, versioning communicates the change:

```yaml
# models/marts/schema.yml
models:
  - name: fct_orders
    latest_version: 2
    description: Order fact table. v2 introduces normalized currency fields.

    versions:
      - v: 1
        defined_in: fct_orders_v1    # points to fct_orders_v1.sql
        description: |
          **Deprecated as of 2024-Q3.** 
          Use v2. This version uses a single `amount` field in USD only.
          Will be removed in 2025-Q1.

      - v: 2
        description: |
          Current version. Adds `original_currency`, `original_amount`,
          and `exchange_rate_used` for multi-currency support.
```

```sql
-- fct_orders_v1.sql — legacy model kept for backward compatibility
{{ config(deprecation_date='2025-01-01') }}
-- Consumers using ref('fct_orders', v=1) will see a warning after this date
select order_id, customer_id, order_date, amount_usd as total_amount
from {{ ref('int_orders_enriched') }}
```

## Metadata Propagation Architecture

### Reading `manifest.json` for Data Governance

`manifest.json` is a machine-readable representation of your entire project. Tools use it to:

```python
# Example: build a data catalog from manifest.json
import json

with open('target/manifest.json') as f:
    manifest = json.load(f)

for node_id, node in manifest['nodes'].items():
    if node['resource_type'] == 'model':
        print(f"Model: {node['name']}")
        print(f"  Description: {node.get('description', 'NO DESCRIPTION')}")
        print(f"  Columns: {len(node.get('columns', {}))}")
        print(f"  Tags: {node.get('tags', [])}")
        for col_name, col in node.get('columns', {}).items():
            pii = col.get('meta', {}).get('contains_pii', False)
            if pii:
                print(f"  PII COLUMN: {col_name}")
```

### Custom Metadata Patterns

```yaml
# Embedding governance metadata
models:
  - name: fct_transactions
    meta:
      owner_team: data-platform
      owner_slack: "#data-platform"
      sla_tier: critical           # critical | high | medium | low
      freshness_sla_hours: 2
      data_classification: confidential
      gdpr_relevant: true
      retention_days: 2555         # 7 years
      lineage_certified: true      # manually reviewed lineage

    columns:
      - name: card_number
        meta:
          contains_pii: true
          pii_type: payment_card_number
          masking_required: true
```

### Automated PII Audit

```bash
# Script to find all PII columns across the project
python3 << 'EOF'
import json

with open('target/manifest.json') as f:
    manifest = json.load(f)

print("PII Columns Inventory:")
for node_id, node in manifest['nodes'].items():
    for col_name, col in node.get('columns', {}).items():
        if col.get('meta', {}).get('contains_pii'):
            print(f"  {node['name']}.{col_name} — {col.get('meta', {}).get('pii_type', 'unclassified')}")
EOF
```

## Documentation-Driven Development

### The Documentation-First Workflow

```
1. Define public interface in schema.yml (columns, types, descriptions)
   ↓
2. Enable contract enforcement
   ↓  
3. Write the model SQL to satisfy the contract
   ↓
4. CI validates contract at compile time
   ↓
5. Publish: downstream teams ref() with confidence
```

This mirrors API-first design in software engineering.

### Automated Documentation Quality Gates

```yaml
# .github/workflows/dbt_docs_quality.yml
- name: Check documentation coverage
  run: |
    python scripts/check_docs.py \
      --manifest target/manifest.json \
      --require-model-descriptions \
      --require-column-descriptions-for-pks \
      --fail-below-coverage 0.8  # fail if <80% of columns have descriptions
```

```python
# scripts/check_docs.py
import json
import sys

with open('target/manifest.json') as f:
    manifest = json.load(f)

missing_descriptions = []
for node_id, node in manifest['nodes'].items():
    if node['resource_type'] == 'model' and node['package_name'] == 'my_project':
        if not node.get('description'):
            missing_descriptions.append(f"Model missing description: {node['name']}")

if missing_descriptions:
    for msg in missing_descriptions:
        print(msg)
    sys.exit(1)

print("Documentation quality check passed!")
```

## Hosting dbt Docs

### Static Site on S3 + CloudFront

```bash
# After dbt docs generate
aws s3 sync target/ s3://company-dbt-docs/ \
  --exclude "*.py" \
  --exclude "*.sql" \
  --delete

# CloudFront distribution serves it with auth (Cognito/SSO)
```

### dbt Cloud: Built-in Docs Hosting

dbt Cloud hosts docs at `https://cloud.getdbt.com/accounts/{account_id}/docs`. Access controlled via dbt Cloud SSO.

### GitHub Pages (Simple)

```yaml
# .github/workflows/deploy_docs.yml
- name: Deploy docs to GitHub Pages
  if: github.ref == 'refs/heads/main'
  run: |
    dbt docs generate
    cp -r target/ docs/
    git add docs/
    git commit -m "chore: update dbt docs"
    git push
```

## Advanced Lineage Patterns

### Documenting Sources You Don't Control

```yaml
sources:
  - name: vendor_data
    description: |
      Data provided by ExternalVendor Inc. via SFTP, loaded daily at 3 AM.
      Schema documentation: https://docs.externalvendor.com/data-schema
      Contact: vendor-support@externalvendor.com
      
      **Known issues:**
      - The `region_code` field uses vendor's internal codes, not ISO 3166
      - Timestamps are Eastern Time, not UTC (converted in staging)
      - ~0.1% of records have null `product_id` (vendor bug, acknowledged)
    
    tables:
      - name: daily_sales
        description: Daily sales rollup by product and region.
        loaded_at_field: report_date
        freshness:
          error_after: {count: 25, period: hour}  # SFTP sometimes delayed
```

### Exposure-Based Impact Analysis

```bash
# Find all exposures that depend on a specific model
dbt ls --select "fct_revenue+" --resource-type exposure

# What changes if stg_orders breaks?
dbt ls --select "stg_orders+" \
  --resource-type exposure,model,test
```

## Interview Questions for Seniors

**Q: What are model contracts in dbt and why are they important for data mesh architectures?**
A: Contracts enforce column names, data types, and constraints at compile/build time. In dbt Mesh, cross-project refs require contracts on public models — they define the interface that downstream teams depend on. Without contracts, a column rename or type change breaks downstream projects silently at query time. With contracts, it fails loudly at compile time, giving the upstream team clear feedback before deployment.

**Q: How would you implement a data governance process using dbt's documentation system?**
A: Embed governance metadata in `meta` fields (owner, PII flags, classification). Write CI scripts that parse `manifest.json` to enforce rules: all models must have descriptions, PII columns must have `masking_required: true`, all mart models must have exposures. Add contract enforcement on public models. Publish docs to a static site with SSO. Use scheduled freshness checks as data SLA monitoring.

**Q: A business analyst asks "why did my dashboard break?" How does dbt documentation help you answer this?**
A: The lineage graph shows exactly which models feed the dashboard (via exposures). You can trace upstream: dashboard → fct_orders → stg_orders → source. Check source freshness logs to see if data is stale. Check the dbt run history to see if a recent deployment changed a model the dashboard depends on. The exposure definition also has an owner contact to loop in the right team.

**Q: What's the difference between `meta` and `tags` in dbt schema.yml?**
A: `tags` are simple string labels used for model selection (`dbt run --select tag:pii`) and filtering. `meta` is a free-form dictionary for arbitrary key-value metadata not used by dbt itself but read by external tools, scripts, or governance processes. Use `tags` for operational grouping; use `meta` for governance metadata, ownership, and classification.
