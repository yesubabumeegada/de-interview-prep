---
title: "dbt Documentation - Intermediate"
topic: dbt
subtopic: dbt-documentation
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [dbt, documentation, exposures, sources, freshness, metadata]
---

# dbt Documentation — Intermediate

## Exposures — Documenting Downstream Consumers

Exposures document what consumes your dbt models — dashboards, applications, ML models. This completes the lineage picture: raw source → dbt models → BI tool.

```yaml
# models/exposures.yml
version: 2

exposures:
  - name: executive_revenue_dashboard
    type: dashboard          # dashboard | notebook | analysis | ml | application
    maturity: high           # low | medium | high (your confidence in the data)
    url: https://tableau.mycompany.com/views/ExecutiveRevenue
    description: |
      Executive-facing dashboard showing daily, weekly, and monthly revenue.
      Used by CFO and VP Sales in the Monday morning stand-up.
      Updated nightly at 6 AM UTC.

    depends_on:
      - ref('fct_revenue')
      - ref('dim_customers')
      - ref('dim_products')

    owner:
      name: Sarah Chen
      email: sarah.chen@company.com

    tags: ['finance', 'executive']

  - name: customer_churn_model
    type: ml
    maturity: medium
    description: |
      ML model predicting 30-day churn probability for each customer.
      Runs daily in SageMaker. Outputs to `ml_predictions.churn_scores`.

    depends_on:
      - ref('fct_user_activity')
      - ref('dim_customers')
      - ref('fct_orders')

    owner:
      name: Data Science Team
      email: datascience@company.com
```

**Why exposures matter:**
1. **Lineage** — see in dbt docs: source → model → dashboard
2. **Impact analysis** — when `fct_revenue` changes, you can see which dashboards are affected
3. **Ownership** — clear accountability for each downstream consumer

## Source Freshness — Production-Grade Setup

```yaml
sources:
  - name: stripe
    database: raw_data
    schema: stripe_prod
    description: Stripe payment processing data via Fivetran.

    freshness:
      # Global defaults (overridden per-table)
      warn_after: {count: 6, period: hour}
      error_after: {count: 12, period: hour}

    tables:
      - name: charges
        loaded_at_field: _fivetran_synced
        description: All payment charge attempts.
        freshness:
          # More aggressive SLA for this critical table
          warn_after: {count: 2, period: hour}
          error_after: {count: 4, period: hour}

      - name: customers
        loaded_at_field: updated_at
        description: Stripe customer records.
        # Inherits global freshness settings

      - name: disputes
        loaded_at_field: _fivetran_synced
        freshness: null  # Disable freshness check — disputes table updates weekly
        description: Disputed charges. Updated weekly by Stripe.
```

```bash
# Run freshness check only
dbt source freshness

# Run freshness as part of full pipeline
dbt source freshness && dbt run && dbt test
```

**Freshness in CI:** Add `dbt source freshness` as a pipeline step. If sources are stale, the pipeline warns/fails before wasting compute on downstream models.

## Advanced `schema.yml` Patterns

### Model-Level Tests

Tests can live at the model level (not column level):

```yaml
models:
  - name: fct_orders
    tests:
      # Unique combination of columns
      - unique:
          column_name: "(order_id || '-' || line_item_id)"

      # dbt_utils composite uniqueness
      - dbt_utils.unique_combination_of_columns:
          combination_of_columns:
            - order_id
            - line_item_id

    columns:
      - name: order_id
        tests:
          - not_null
```

### Column Tags and Metadata

```yaml
models:
  - name: dim_customers
    columns:
      - name: email
        description: Customer email address. PII — do not expose in BI tools.
        tags: ['pii', 'gdpr']
        meta:
          contains_pii: true
          data_classification: confidential
          masking_policy: email_mask

      - name: customer_id
        description: Surrogate key for customer.
        meta:
          primary_key: true
```

`meta` is a free-form dict — you can store any metadata. Tools like Atlan, DataHub, or custom scripts can read `meta` from `manifest.json`.

### Inheriting Descriptions with `+` Config

```yaml
# models/staging/schema.yml
models:
  - name: stg_orders
    description: "Staged orders from Shopify"
    config:
      tags: ['staging', 'shopify']
    columns:
      - name: customer_id
        description: "{{ doc('customer_id') }}"
        tags: ['foreign_key']
```

## `dbt docs generate` — What It Produces

```bash
dbt docs generate
# Creates: target/catalog.json + target/manifest.json + target/index.html
```

**`catalog.json`** — contains column-level metadata queried from the warehouse (actual data types, row counts from `information_schema`)

**`manifest.json`** — contains the compiled project graph (models, tests, sources, macros, exposures)

**`index.html`** — the single-page app that reads both JSON files to render the docs UI

These artifacts can be hosted as static files (S3, Netlify, GitHub Pages):

```bash
# Deploy docs to S3
aws s3 sync target/ s3://my-dbt-docs/ --exclude "*.py" --exclude "*.sql"
aws s3 website s3://my-dbt-docs/ --index-document index.html
```

## Documentation Best Practices

### The Description Completeness Principle

A good column description answers:
1. **What** is this column? (data type, format)
2. **Where** does it come from? (source system, derivation)
3. **What** edge cases or gotchas exist?

```yaml
# Bad description:
- name: amount
  description: Amount in dollars.

# Good description:
- name: amount
  description: |
    Transaction amount in USD, stored as a decimal with 2 decimal places.
    For international transactions, converted from original currency using
    the exchange rate at transaction time (not current rate).
    Negative values indicate refunds. Cannot be zero — zero-amount
    transactions are filtered out upstream.
```

### Team Conventions

```yaml
# Establish these conventions across your team:
# 1. All models must have descriptions
# 2. All columns used in joins (FKs) must have descriptions
# 3. All PII columns must have meta.contains_pii: true
# 4. All staging models must document their source
# 5. All mart models must have at least one exposure
```

### Documentation Coverage Audit

```bash
# Find models without descriptions
dbt ls --output json | python scripts/audit_descriptions.py

# Or use the Elementary package for automated coverage metrics
dbt run --select elementary
```

## Interview Questions

**Q: What is an exposure in dbt?**
A: A YAML definition of a downstream consumer of dbt models — a dashboard, application, ML model, or report. Exposures appear in the lineage graph and document who consumes what data, with ownership information.

**Q: What is the difference between a doc block and an inline description?**
A: Both document columns/models. Inline descriptions are written directly in `schema.yml`. Doc blocks are defined in `.md` files with `{% docs name %}` tags and referenced with `{{ doc('name') }}`. Doc blocks are preferable for descriptions shared across many models, eliminating repetition.

**Q: How does dbt source freshness work?**
A: dbt runs `select max(loaded_at_field) from source_table` for each source with freshness configured. It compares the result against the current time. If the delta exceeds `warn_after`, it logs a warning. If it exceeds `error_after`, the command exits with a non-zero code (failure).

**Q: What does `dbt docs generate` actually query?**
A: It queries `information_schema` in your warehouse to get actual column data types and table statistics (row counts). This produces `catalog.json`. It also compiles `manifest.json` from the project files. The combination makes the docs accurate to the actual warehouse state.
