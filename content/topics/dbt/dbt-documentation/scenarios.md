---
title: "dbt Documentation - Scenario Questions"
topic: dbt
subtopic: dbt-documentation
content_type: scenario_question
tags: [dbt, documentation, scenarios, interview]
---

# dbt Documentation — Scenario Questions

<article data-difficulty="junior">

## Scenario: Adding Documentation to an Undocumented Model

You inherit a `stg_customers` model with no `schema.yml` entry. The model selects from `source('crm', 'raw_customers')` and has columns: `customer_id`, `email`, `full_name`, `created_at`, `country_code`, `is_active`.

**Write the complete `schema.yml` entry for this model and its source.**

<details>
<summary>✅ Solution</summary>

```yaml
# models/staging/schema.yml
version: 2

sources:
  - name: crm
    description: Customer relationship management data, loaded from Salesforce via Fivetran.
    database: raw_data
    schema: salesforce

    tables:
      - name: raw_customers
        description: Raw customer records from Salesforce. May contain duplicates due to merge events.
        loaded_at_field: _fivetran_synced
        freshness:
          warn_after: {count: 12, period: hour}
          error_after: {count: 24, period: hour}

models:
  - name: stg_customers
    description: |
      Staging model for customer records sourced from Salesforce CRM.
      One row per unique customer. Deduplicates on customer_id,
      keeping the most recently updated record.
      Filters out test and internal accounts.
    columns:
      - name: customer_id
        description: Unique identifier for each customer. Surrogate key generated from Salesforce account ID.
        tests:
          - unique
          - not_null

      - name: email
        description: Primary email address for the customer. Lowercased and trimmed.
        tests:
          - not_null
          - unique

      - name: full_name
        description: Customer's full name as entered in CRM. May include middle names or titles.

      - name: created_at
        description: Timestamp when the customer account was first created in Salesforce (UTC).
        tests:
          - not_null

      - name: country_code
        description: ISO 3166-1 alpha-2 country code for the customer's billing address.
        tests:
          - not_null
          - accepted_values:
              values: ['US', 'CA', 'GB', 'DE', 'FR', 'AU', 'JP', 'BR', 'MX', 'IN']
              quote: true

      - name: is_active
        description: |
          Boolean flag indicating whether the customer account is active.
          False for churned, suspended, or deleted accounts.
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
```

**Key points:**
- Source defined with freshness check and `loaded_at_field`
- Model has a multi-sentence description explaining what it represents, how it's deduped, and what it excludes
- Every column has a description
- Appropriate tests: `unique` + `not_null` on PKs, `accepted_values` for categorical fields
- `email` has `unique` because each customer should have one account per email

</details>
</article>

---

<article data-difficulty="mid">

## Scenario: Reusable Descriptions with Doc Blocks

Your project has 15 models that all use `customer_id` as a foreign key. Each model's `schema.yml` has a slightly different description for the same column. The data governance team wants a single authoritative definition that propagates everywhere.

**Refactor to use doc blocks. What are the tradeoffs?**

<details>
<summary>✅ Solution</summary>

**Step 1 — Create a docs file:**

```markdown
<!-- models/docs/column_descriptions.md -->

{% docs customer_id %}
Unique surrogate key for a customer in our data platform.

- **Source:** Generated from MD5 hash of Salesforce account ID
- **Format:** 32-character hex string
- **References:** `dim_customers.customer_id`
- **Note:** This is NOT the Salesforce account ID (stored separately as `sf_account_id`)

To look up a customer: `select * from dim_customers where customer_id = '...'`
{% enddocs %}

{% docs order_id %}
Unique identifier for an order.

- **Source:** Shopify order ID, prefixed with source system: `shopify_{id}`
- **References:** `fct_orders.order_id`
- **Note:** Order IDs are not reused even after cancellation.
{% enddocs %}

{% docs order_date %}
Date the customer placed the order, in UTC.

Derived from `order_placed_at` timestamp, truncated to date.
Note: For orders placed near midnight UTC, this may differ from the customer's local date.
{% enddocs %}
```

**Step 2 — Reference in schema.yml files:**

```yaml
# models/marts/fct_orders/schema.yml
models:
  - name: fct_orders
    columns:
      - name: customer_id
        description: "{{ doc('customer_id') }}"
        tests:
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_id

      - name: order_id
        description: "{{ doc('order_id') }}"
        tests:
          - unique
          - not_null

      - name: order_date
        description: "{{ doc('order_date') }}"

# models/staging/schema.yml
models:
  - name: stg_orders
    columns:
      - name: customer_id
        description: "{{ doc('customer_id') }}"  # same definition, propagated
      - name: order_id
        description: "{{ doc('order_id') }}"
```

**Tradeoffs:**

| Aspect | Doc Blocks | Inline Descriptions |
|---|---|---|
| Consistency | Single source of truth | Risk of drift across files |
| Update cost | Change once, all models updated | Must update every schema.yml |
| Readability | Need to jump to .md file | Description inline, easy to read |
| Discoverability | Need to know doc block names | Self-contained |
| Richness | Markdown, multi-paragraph | Same — Markdown supported inline too |

**Recommendation:** Use doc blocks for:
- Columns used in 3+ models (customer_id, order_id, user_id)
- Business definitions that change over time
- Complex descriptions (multi-paragraph, links, notes)

Use inline descriptions for:
- Model-level descriptions (unique to each model)
- Columns unique to one model

</details>
</article>

---

<article data-difficulty="senior">

## Scenario: Building a Documentation Governance System

You're leading a data platform team of 8 engineers. The current state: 400 models, ~30% have any documentation, no freshness checks on sources, no exposures defined. The head of data asks you to implement documentation standards that scale.

**Design a complete governance system for dbt documentation. Include: standards, enforcement, and incentives.**

<details>
<summary>✅ Solution</summary>

**The Three-Layer Documentation Governance System**

---

**Layer 1: Standards (what to document)**

Define tiers with minimum requirements:

```yaml
# Documentation standards (enforced in CI)
tiers:
  critical:    # mart models used in executive dashboards
    - model description: required
    - all columns: description required
    - owner meta: required
    - at least 1 exposure: required
    - contract enforced: required
    
  high:        # mart models, non-critical
    - model description: required
    - primary key columns: description required
    - owner meta: required
    
  standard:    # staging and intermediate
    - model description: required
    - primary key columns: description required
```

Assign tiers via tags:

```yaml
# dbt_project.yml
models:
  my_project:
    marts:
      executive:
        +tags: ['critical']
      standard:
        +tags: ['high']
    staging:
      +tags: ['standard']
```

---

**Layer 2: Enforcement (CI gates)**

```python
# scripts/docs_gate.py
import json, sys

RULES = {
    'critical': {
        'require_model_desc': True,
        'require_all_column_desc': True,
        'require_owner_meta': True,
        'require_exposure': True,
    },
    'high': {
        'require_model_desc': True,
        'require_pk_column_desc': True,
        'require_owner_meta': True,
    },
    'standard': {
        'require_model_desc': True,
        'require_pk_column_desc': True,
    },
}

def check_model(node, rules, exposures_by_dep):
    violations = []
    name = node['name']
    
    if rules.get('require_model_desc') and not node.get('description', '').strip():
        violations.append(f"  {name}: missing model description")
    
    if rules.get('require_owner_meta') and not node.get('meta', {}).get('owner_team'):
        violations.append(f"  {name}: missing meta.owner_team")
    
    if rules.get('require_exposure'):
        node_ref = f"ref('{name}')"
        if node_ref not in exposures_by_dep:
            violations.append(f"  {name}: no exposure defined")
    
    return violations

# ... (full implementation)
```

```yaml
# .github/workflows/docs_gate.yml
- name: Documentation quality gate
  run: |
    dbt compile
    python scripts/docs_gate.py \
      --manifest target/manifest.json \
      --fail-on-violations
```

---

**Layer 3: Process and Incentives**

**PR template — checklist:**
```markdown
## Documentation Checklist
- [ ] Model description added/updated in schema.yml
- [ ] All new columns have descriptions
- [ ] If mart model: owner meta updated
- [ ] If public model: contract enforced
- [ ] If new dashboard consumption: exposure added
```

**Weekly docs coverage report — automated:**
```python
# Post to #data-platform every Monday
coverage = calculate_coverage(manifest)
slack.post(f"📊 Docs coverage: {coverage['critical']}% critical, "
           f"{coverage['high']}% high tier models documented.")
```

**Recognition:** Publish a "best-documented model" monthly to #data-platform. Celebrate teams that improve coverage.

**Migration plan (to avoid big-bang):**
- Week 1-2: Enable gate for NEW models only (grandfather existing)
- Month 2: Gate on critical tier models (highest impact)
- Month 3-4: Gate on high tier
- Month 6: Gate on all models

**Measuring success:**
```
Month 1: 30% → target 50%
Month 3: 50% → target 70%
Month 6: 70% → target 85%
Year 1: 85% → target 95%
```

Track via `manifest.json` parsing in a weekly scheduled job that writes to a `dbt_docs_coverage` table and feeds a Grafana dashboard.

</details>
</article>
