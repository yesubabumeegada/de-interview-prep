---
title: "dbt Packages - Senior Deep Dive"
topic: dbt
subtopic: dbt-packages
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [dbt, packages, dispatch, macros, architecture, internal-packages]
---

# dbt Packages — Senior Deep Dive

## Building Internal Packages

Large organizations maintain internal dbt packages to share logic across teams. Understanding when and how to build them separates senior engineers from mid-level.

### When to Extract a Package

- A macro is used in 3+ projects
- A set of generic tests encodes company-wide data contracts
- A team maintains shared staging models for a central data source (e.g., Salesforce, Stripe)

### Internal Package Structure

```
shared_dbt_package/
├── dbt_project.yml
├── macros/
│   ├── fiscal_quarter.sql
│   ├── hash_pii.sql
│   └── tests/
│       └── generic/
│           └── is_valid_country_code.sql
├── models/
│   └── sources/
│       └── salesforce/
│           ├── stg_sf_accounts.sql
│           └── schema.yml
└── README.md
```

```yaml
# dbt_project.yml for the package
name: 'company_shared'
version: '1.0.0'
config-version: 2
require-dbt-version: [">=1.5.0", "<2.0.0"]

macro-paths: ["macros"]
model-paths: ["models"]

models:
  company_shared:
    sources:
      salesforce:
        +schema: shared_salesforce
        +materialized: view
```

### Consuming an Internal Package

```yaml
# In a consuming project's packages.yml
packages:
  - git: "https://github.com/mycompany/dbt-shared.git"
    revision: v1.2.3   # Tag-based versioning

  # For local development:
  - local: ../../dbt-shared
```

## Advanced Dispatch Patterns

### Full Dispatch Architecture

```yaml
# dbt_project.yml
dispatch:
  # Override dbt built-ins (e.g., dateadd, datediff)
  - macro_namespace: dbt
    search_order: ['company_overrides', 'dbt']

  # Override dbt_utils macros
  - macro_namespace: dbt_utils
    search_order: ['company_overrides', 'dbt_utils']
```

### Wrapping Package Macros With Telemetry

```sql
-- macros/dbt_utils/surrogate_key.sql
-- Override to add logging/telemetry around key generation
{% macro surrogate_key(field_list) %}
    {%- if execute -%}
        {{ log("surrogate_key called with: " ~ field_list, info=false) }}
    {%- endif -%}
    {{ return(dbt_utils.surrogate_key(field_list)) }}
{% endmacro %}
```

### Namespace-Safe Macro Calls

When writing package macros that may themselves be dispatched, always use the full dispatch pattern:

```sql
-- Good: allows consumers to override
{% macro fiscal_quarter(date_col) %}
    {{ return(adapter.dispatch('fiscal_quarter', 'company_shared')(date_col)) }}
{% endmacro %}

{% macro default__fiscal_quarter(date_col) %}
    case
        when month({{ date_col }}) between 2 and 4 then 'Q1'
        when month({{ date_col }}) between 5 and 7 then 'Q2'
        when month({{ date_col }}) between 8 and 10 then 'Q3'
        else 'Q4'
    end
{% endmacro %}

{% macro snowflake__fiscal_quarter(date_col) %}
    -- Snowflake-specific implementation
    case
        when date_part('month', {{ date_col }}) between 2 and 4 then 'Q1'
        when date_part('month', {{ date_col }}) between 5 and 7 then 'Q2'
        when date_part('month', {{ date_col }}) between 8 and 10 then 'Q3'
        else 'Q4'
    end
{% endmacro %}
```

## Cross-Database Compatibility

### The Adapter Dispatch Pattern in Packages

```sql
-- macros/utils/array_agg_distinct.sql
{% macro array_agg_distinct(column) %}
    {{ return(adapter.dispatch('array_agg_distinct', 'my_package')(column)) }}
{% endmacro %}

{% macro default__array_agg_distinct(column) %}
    array_agg(distinct {{ column }})
{% endmacro %}

{% macro bigquery__array_agg_distinct(column) %}
    array_agg(distinct {{ column }} ignore nulls)
{% endmacro %}

{% macro spark__array_agg_distinct(column) %}
    collect_set({{ column }})
{% endmacro %}
```

### Testing Cross-DB Macros

```yaml
# integration_tests/dbt_project.yml (inside the package repo)
models:
  integration_tests:
    +materialized: table

# Run against multiple targets in CI:
# dbt test --target snowflake
# dbt test --target bigquery
# dbt test --target redshift
```

## Package Security and Governance

### Auditing Package Macros

Before adding a package, audit it:

```bash
# Review what macros a package adds
ls dbt_packages/dbt_utils/macros/

# Check if any macro overrides your existing macros
grep -r "{% macro " dbt_packages/ | grep -f <(grep -r "{% macro " macros/)
```

### Private Package Access in CI/CD

```bash
# GitHub Actions: use SSH key or token
# In packages.yml:
- git: "https://{{ env_var('GITHUB_TOKEN') }}@github.com/mycompany/dbt-shared.git"
  revision: v1.2.3

# Or configure git credentials in CI:
git config --global url."https://${GITHUB_TOKEN}@github.com/".insteadOf "https://github.com/"
```

### Package Pinning Strategy for Production

```yaml
packages:
  # Production: exact pins only
  - package: dbt-labs/dbt_utils
    version: 1.1.1

  # Never do this in production:
  # - package: dbt-labs/dbt_utils
  #   version: [">=1.0.0"]   # too loose
```

## dbt-audit-helper: Production Workflows

### Automated Regression Testing

```sql
-- analyses/regression_check.sql
-- Run this after a major refactor to find differences

{% set old_etl = adapter.get_relation(
    database='analytics',
    schema='dbt_prod_old',
    identifier='fct_revenue'
) %}

{% set new_etl = ref('fct_revenue') %}

{% if old_etl is not none %}
    {{ audit_helper.compare_relations(
        a_relation=old_etl,
        b_relation=new_etl,
        primary_key='revenue_id',
        summarize=true
    ) }}
{% else %}
    select 'old relation not found — skipping comparison' as message
{% endif %}
```

### Using audit_helper in CI

```yaml
# .github/workflows/dbt_audit.yml
- name: Audit refactored models
  run: |
    dbt run --select fct_revenue+
    dbt run-operation audit_helper.compare_relations \
      --args "$(cat .github/audit_args.json)"
  env:
    DBT_TARGET: ci
```

## Package Ecosystem Architecture Patterns

### Package as Data Contract Layer

Senior engineers often structure the package ecosystem as:

```
Layer 1: Source packages (one per raw source)
  └── dbt-stripe-source
  └── dbt-salesforce-source
  └── dbt-segment-source

Layer 2: Shared utility package
  └── dbt-company-utils  (macros, generic tests, fiscal calendar)

Layer 3: Domain-specific packages
  └── dbt-marketing-models
  └── dbt-finance-models

Layer 4: Application projects (consume above)
  └── marketing-analytics (consumes stripe-source + segment-source + marketing-models)
  └── finance-analytics (consumes stripe-source + salesforce-source + finance-models)
```

### Managing Package Dependency Conflicts

When two packages require conflicting versions of a third package:

```yaml
# Problem: package A requires dbt_utils >=1.0.0,<1.1.0
#          package B requires dbt_utils >=1.1.0,<1.2.0

# Solution 1: Pin to a compatible version and test
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.0   # Satisfies B; test if A still works

# Solution 2: Fork package A and update its constraint
# Solution 3: Contact maintainers to relax the constraint
```

## Interview Questions for Seniors

**Q: How would you architect a multi-project dbt setup with shared logic?**
A: Extract shared macros, generic tests, and source staging models into an internal package hosted on GitHub. Version it with tags. Each consuming project pins to a specific tag. CI/CD for the package runs its own integration tests against all supported warehouses before tagging a release.

**Q: What are the performance implications of `dbt_utils.get_column_values`?**
A: It issues a `select distinct` against the target table at compile time. For very large tables, this can slow compilation. Mitigate by running against a smaller table (like a lookup/dim) or by caching results in a variable. Also, because it runs at compile time, the table must exist — this breaks on first run in a fresh environment without a `--defer` state.

**Q: How do you handle a package that has a macro name collision with your project?**
A: Configure the `dispatch` block in `dbt_project.yml` to specify `search_order`, placing your project first. This ensures your macro takes precedence. Document the override and add a comment in your macro explaining why it overrides the package.

**Q: How would you version-control and release an internal dbt package?**
A: Use semantic versioning (MAJOR.MINOR.PATCH). Maintain a CHANGELOG. Tag releases in git. Protect the main branch; require PR reviews. Run integration tests in CI against all supported warehouses before merging. Pin consuming projects to specific tags, not branches.
