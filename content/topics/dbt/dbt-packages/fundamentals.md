---
title: "dbt Packages - Fundamentals"
topic: dbt
subtopic: dbt-packages
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [dbt, packages, dbt-utils, hub, packages.yml]
---

# dbt Packages — Fundamentals

## What Are dbt Packages?

dbt packages are reusable collections of models, macros, tests, and seeds that you can install into your dbt project. They work like libraries in traditional software development — someone else solves a common problem once, and you import that solution.

Packages are distributed via:
- **dbt Hub** (hub.getdbt.com) — the official registry
- **GitHub repositories** — public or private repos
- **Local paths** — for monorepo or internal packages

## The `packages.yml` File

Create a `packages.yml` file at the root of your dbt project (same level as `dbt_project.yml`):

```yaml
packages:
  # Install from dbt Hub (recommended)
  - package: dbt-labs/dbt_utils
    version: 1.1.1

  # Install from GitHub
  - git: "https://github.com/dbt-labs/dbt-utils.git"
    revision: 1.1.1

  # Install from a local path (monorepo use case)
  - local: ../shared_dbt_package
```

After editing `packages.yml`, run:

```bash
dbt deps
```

This downloads packages into the `dbt_packages/` directory (git-ignored by default).

## Version Pinning

Always pin to exact versions in production. Unpinned packages can break when authors release breaking changes.

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1          # exact pin — safest

  - package: calogica/dbt_expectations
    version: [">=0.9.0", "<0.10.0"]  # range pin — some flexibility
```

## Core Packages Every DE Should Know

### 1. dbt-utils (dbt-labs/dbt_utils)

The most widely used package. Provides utility macros across four categories:

**SQL generators:**
```sql
-- generate_series: creates a sequence of numbers/dates
{{ dbt_utils.generate_series(upper_bound=10) }}

-- date_spine: generates a table of dates (great for gap-filling)
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="cast('2023-01-01' as date)",
    end_date="cast('2023-12-31' as date)"
) }}
```

**Cross-database macros:**
```sql
-- Works on Snowflake, BigQuery, Redshift, DuckDB, etc.
{{ dbt_utils.datediff('day', 'start_date', 'end_date') }}
{{ dbt_utils.dateadd('month', 3, 'order_date') }}
{{ dbt_utils.safe_divide(numerator, denominator) }}
{{ dbt_utils.pivot(column='status', values=['active', 'inactive']) }}
```

**Key generation:**
```sql
-- surrogate_key: hashes multiple columns into a stable surrogate key
select
    {{ dbt_utils.surrogate_key(['order_id', 'line_item_id']) }} as pk,
    *
from {{ ref('stg_orders') }}
```

**Generic tests (in schema.yml):**
```yaml
models:
  - name: orders
    columns:
      - name: order_date
        tests:
          - dbt_utils.not_null_proportion:
              at_least: 0.95
          - dbt_utils.accepted_range:
              min_value: 0
              max_value: 1000000
```

### 2. dbt-expectations (calogica/dbt_expectations)

Port of Great Expectations to dbt. Provides expressive data quality tests:

```yaml
models:
  - name: orders
    tests:
      - dbt_expectations.expect_table_row_count_to_be_between:
          min_value: 1000
          max_value: 10000000
    columns:
      - name: amount
        tests:
          - dbt_expectations.expect_column_values_to_be_between:
              min_value: 0
              max_value: 99999
          - dbt_expectations.expect_column_values_to_not_be_null:
              row_condition: "status != 'cancelled'"
      - name: email
        tests:
          - dbt_expectations.expect_column_values_to_match_regex:
              regex: "^[a-zA-Z0-9._%+\\-]+@[a-zA-Z0-9.\\-]+\\.[a-zA-Z]{2,}$"
```

### 3. dbt-audit-helper (dbt-labs/dbt_audit_helper)

Compares two relations — great for validating refactors:

```sql
-- In an analysis file or run as a macro
{{ audit_helper.compare_relations(
    a_relation=ref('old_model'),
    b_relation=ref('new_model'),
    primary_key='order_id'
) }}
```

### 4. dbt-codegen (dbt-labs/dbt_codegen)

Generates boilerplate YAML from existing database tables:

```bash
# Generate source YAML from an existing schema
dbt run-operation generate_source \
  --args '{"schema_name": "raw", "table_names": ["orders", "customers"]}'

# Generate model YAML from an existing model
dbt run-operation generate_model_yaml \
  --args '{"model_name": "stg_orders"}'
```

## How dbt Resolves Package Macros

When two packages define the same macro name, dbt uses a **dispatch** system. The lookup order is:

1. Your project's macros (highest priority)
2. Packages listed in `dispatch` config
3. The `dbt` global namespace (lowest priority)

```yaml
# dbt_project.yml
dispatch:
  - macro_namespace: dbt_utils
    search_order: ['my_project', 'dbt_utils']
```

This lets you override any package macro in your own project.

## Key Interview Points

| Concept | What to Know |
|---|---|
| `dbt deps` | Downloads packages from `packages.yml` |
| Version pinning | Always pin exact versions in production |
| `dbt_packages/` | Where packages are installed; should be gitignored |
| Dispatch | How dbt resolves macro naming conflicts |
| `generate_surrogate_key` | Hashes columns into a deterministic surrogate key |
| `date_spine` | Generates a continuous date range for gap-filling |

## Common Interview Questions

**Q: Where do packages get installed?**
A: Into the `dbt_packages/` directory at the project root. This directory should be in `.gitignore`.

**Q: What command installs dbt packages?**
A: `dbt deps`

**Q: What is the purpose of `dbt_utils.surrogate_key`?**
A: It concatenates and hashes a list of columns to produce a deterministic surrogate key, useful when your source data has no natural primary key.

**Q: How do you override a macro from a package?**
A: Create a macro with the same name in your project and configure the `dispatch` block in `dbt_project.yml` to search your project first.
