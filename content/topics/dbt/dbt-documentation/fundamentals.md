---
title: "dbt Documentation - Fundamentals"
topic: dbt
subtopic: dbt-documentation
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [dbt, documentation, schema.yml, docs, lineage]
---

# dbt Documentation — Fundamentals

## Why dbt Documentation Matters

dbt's documentation system creates a living, auto-generated data catalog directly from your project. It captures:
- Model descriptions (what the model represents)
- Column-level descriptions (what each field means)
- Data lineage (where data flows from source to final table)
- Test coverage (what quality checks are in place)
- Source freshness status

This is crucial for data teams — without documentation, stakeholders can't trust or self-serve data.

## `schema.yml` — The Documentation File

`schema.yml` files live in your `models/` directory (any subdirectory). They define descriptions, tests, and metadata for sources, models, and columns.

### Basic Model Documentation

```yaml
# models/staging/schema.yml
version: 2

models:
  - name: stg_orders
    description: |
      Staging model for raw orders from the Shopify source.
      One row per order. Includes all orders regardless of status.
      Cleaned and type-cast from the raw source.
    columns:
      - name: order_id
        description: Unique identifier for each order. Natural key from Shopify.
        tests:
          - unique
          - not_null

      - name: customer_id
        description: Foreign key to dim_customers. References the customer who placed the order.
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id

      - name: status
        description: Current status of the order.
        tests:
          - accepted_values:
              values: ['pending', 'processing', 'shipped', 'delivered', 'cancelled', 'refunded']

      - name: order_date
        description: Date the order was placed. Derived from order_placed_at timestamp.
        tests:
          - not_null

      - name: total_amount
        description: Total order value in USD, inclusive of discounts and exclusive of tax.
```

## Source Documentation

Sources are documented in `schema.yml` under a `sources:` key:

```yaml
# models/staging/schema.yml
sources:
  - name: shopify
    description: Raw data from Shopify e-commerce platform, loaded via Fivetran.
    database: raw_data
    schema: shopify

    freshness:
      warn_after: {count: 12, period: hour}
      error_after: {count: 24, period: hour}

    tables:
      - name: orders
        description: Raw orders table. Contains one row per order event (can have duplicates for status updates).
        loaded_at_field: _fivetran_synced  # Field used for freshness check
        columns:
          - name: id
            description: Shopify-generated order ID. Not unique due to order edits.
          - name: created_at
            description: Timestamp when the order was first created in Shopify (UTC).
          - name: financial_status
            description: Payment status. One of pending, authorized, paid, refunded, voided.

      - name: customers
        description: Raw customer records from Shopify.
        loaded_at_field: _fivetran_synced
```

## Doc Blocks — Reusable Descriptions

For descriptions used in multiple places (e.g., `customer_id` appears in 20 models), use doc blocks:

**Step 1: Create a `.md` file with doc blocks:**

```markdown
<!-- models/docs/column_descriptions.md -->

{% docs customer_id %}
Unique identifier for a customer in our system.
Foreign key to `dim_customers.customer_id`.
This is an internal surrogate key, not the Shopify customer ID.
{% enddocs %}

{% docs order_date %}
Date the order was placed by the customer, in the customer's local timezone
normalized to UTC. Derived from the `order_placed_at` timestamp.
{% enddocs %}

{% docs total_amount %}
Total order value in USD. Inclusive of all discounts applied.
Exclusive of sales tax and shipping fees.
Converted from original currency using the exchange rate at order time.
{% enddocs %}
```

**Step 2: Reference doc blocks in schema.yml:**

```yaml
models:
  - name: stg_orders
    columns:
      - name: customer_id
        description: "{{ doc('customer_id') }}"

      - name: order_date
        description: "{{ doc('order_date') }}"

      - name: total_amount
        description: "{{ doc('total_amount') }}"
```

## Generating and Serving Documentation

```bash
# Step 1: Generate the docs artifact (docs/catalog.json)
dbt docs generate

# Step 2: Serve locally
dbt docs serve
# Opens http://localhost:8080 in your browser
```

**In dbt Cloud:** Documentation is automatically generated after each job run if "Generate docs" is enabled. Access via the "Documentation" tab in dbt Cloud UI.

## The Lineage Graph

`dbt docs serve` includes an interactive lineage graph showing:
- Source → staging → intermediate → mart data flow
- Which models reference which sources
- Test coverage indicators

Navigate with `+model_name` (upstream) and `model_name+` (downstream) selectors in the DAG explorer.

## Key Interview Points

**Q: What command generates dbt documentation?**
A: `dbt docs generate` creates the catalog. `dbt docs serve` launches a local web server to view it.

**Q: What is a doc block?**
A: A reusable description defined in a `.md` file between `{% docs name %}` and `{% enddocs %}` tags. Referenced with `{{ doc('name') }}` in schema.yml. Eliminates copy-paste of common column descriptions.

**Q: Where do you define column descriptions in dbt?**
A: In `schema.yml` files under the `columns:` key for each model. Can also reference doc blocks from `.md` files.

**Q: What does `loaded_at_field` do in source definitions?**
A: It specifies which column to use for source freshness checks. When you run `dbt source freshness`, dbt queries `max(loaded_at_field)` and compares against `warn_after` / `error_after` thresholds.
