---
title: "dbt Models & Materializations - Senior Deep Dive"
topic: dbt
subtopic: models-and-materializations
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [dbt, performance, partitioning, model-versioning, advanced]
---

# dbt Models & Materializations — Senior Deep Dive

## Model Versioning (dbt 1.5+)

Safely evolve model interfaces without breaking downstream consumers:

```yaml
# models/marts/schema.yml
models:
  - name: dim_customers
    latest_version: 2
    versions:
      - v: 1
        defined_in: dim_customers_v1   # legacy file
        deprecation_date: "2024-06-01"
      - v: 2
        # default: defined_in dim_customers_v2.sql
        columns:
          - include: all
          - name: customer_uuid        # new column in v2
```

Consumers reference specific versions:
```sql
-- Locked to v1 until migration complete
SELECT * FROM {{ ref('dim_customers', v=1) }}

-- Opt into v2
SELECT * FROM {{ ref('dim_customers', v=2) }}
```

## Deferred Execution (Slim CI)

Avoid rebuilding upstream models in CI by deferring to prod:

```bash
# In CI pipeline
dbt run \
  --select state:modified+ \
  --defer \
  --state ./prod-manifest \
  --target ci
```

With `--defer`, if `stg_orders` is unchanged, dbt reads from the **production** `stg_orders` table instead of rebuilding it. This makes CI 10-100x faster on large projects.

## Optimizing Incremental Models at Petabyte Scale

### Strategy Comparison at Scale

| Strategy | Best For | Pitfall |
|---|---|---|
| `append` | Immutable events, IOT | Duplicates on reruns |
| `merge` | Slowly-changing data | Slow on very large tables |
| `insert_overwrite` | Date-partitioned data | Entire partition replaced |
| `microbatch` | Large historical backfills | Requires dbt 1.9+ |

### Multi-Key Incremental (avoiding duplicates without merge)

```sql
{{ config(
    materialized='incremental',
    unique_key=['order_id', 'line_item_id'],
    incremental_strategy='merge',
    merge_update_columns=['status', 'updated_at', 'total_amount']
) }}
-- Only update specific columns on merge — prevents overwriting audit fields
```

### Pre/Post Hook Optimization

```sql
{{ config(
    materialized='incremental',
    unique_key='event_id',
    pre_hook=[
        "ALTER TABLE {{ this }} SUSPEND RECLUSTER"
    ],
    post_hook=[
        "ALTER TABLE {{ this }} RESUME RECLUSTER",
        "ALTER TABLE {{ this }} CLUSTER BY (event_date, user_id)"
    ]
) }}
```

## Custom Schema Logic

Override where models land with a custom macro:

```sql
-- macros/generate_schema_name.sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- elif target.name == 'prod' -%}
        {# In prod: use the custom schema directly (no prefix) #}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {# In dev/ci: prefix with user schema to avoid collisions #}
        {{ default_schema }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

Result:
- Dev: `dbt_jsmith_finance`
- Prod: `finance`

## Relation Caching and Metadata

dbt caches relation metadata to avoid repeated `SHOW TABLES` calls:

```python
# Understand what dbt sees internally via python API
import dbt.lib
# Use dbt list to inspect the graph without running
```

```bash
# List all models with their materialization
dbt ls --select "*" --output json | jq '.[] | {name, config.materialized}'

# Check what would run
dbt ls --select state:modified+ --state ./prod-manifest
```

## Performance Anti-Patterns

### Anti-Pattern 1: Table for Every Model

```sql
-- BAD: staging model as table — rebuilds 500M rows each run
{{ config(materialized='table') }}
SELECT * FROM {{ source('raw', 'events') }}

-- GOOD: staging as view — no rebuild cost
{{ config(materialized='view') }}
SELECT * FROM {{ source('raw', 'events') }}
```

### Anti-Pattern 2: Missing Partition Filter on Incremental

```sql
-- BAD: incremental without partition pruning scans full table
{% if is_incremental() %}
WHERE status = 'new'   -- no time filter → full scan
{% endif %}

-- GOOD: filter on the partition column
{% if is_incremental() %}
WHERE event_date >= (SELECT MAX(event_date) FROM {{ this }})
    AND status = 'new'
{% endif %}
```

### Anti-Pattern 3: Ephemeral in Hot Path

```sql
-- BAD: ephemeral model referenced by 10 downstream models
-- = same CTE duplicated 10 times, 10x compute cost
{{ config(materialized='ephemeral') }}
SELECT * FROM hugely_expensive_join

-- GOOD: materialize as table if referenced by multiple models
{{ config(materialized='table') }}
```

## Cost Attribution with Query Tags

```yaml
# dbt_project.yml
query-comment:
  comment: "dbt={{ dbt_version }}, target={{ target.name }}, model={{ node.unique_id }}, run_id={{ invocation_id }}"
  append: true
  job-label: true   # BigQuery job labels
```

This enables cost breakdown by model in warehouse query history.

## ⚡ Cheat Sheet

**Materialization decision tree**
```
Data size < 1M rows AND no performance concern → view
Data reused by 3+ downstream models → table
Data grows by append (events, logs) → incremental (append strategy)
Data has updates (orders, users) → incremental (merge/delete+insert)
Model is a building block, not a destination → ephemeral
Need history tracking → snapshot (SCD Type 2)
```

**Incremental merge**
```sql
{{ config(materialized='incremental', unique_key='order_id', incremental_strategy='merge') }}
SELECT id as order_id, status, updated_at FROM {{ source('app', 'orders') }}
{% if is_incremental() %}
  WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

**`on_schema_change` options**
| Value | Behavior |
|---|---|
| `ignore` (default) | New columns in model ignored; target schema unchanged |
| `fail` | Error if schema changes detected |
| `append_new_columns` | Add new columns; don't remove old |
| `sync_all_columns` | Add new + remove dropped columns |

**Partition + cluster (BigQuery/Databricks)**
```sql
{{ config(
    partition_by={"field": "created_date", "data_type": "date", "granularity": "month"},
    cluster_by=["customer_id", "region"]
) }}
```

**Custom schema strategy**
```python
# macros/generate_schema_name.sql — override default schema naming
{% macro generate_schema_name(custom_schema_name, node) %}
  {{ target.schema }}_{{ custom_schema_name | trim if custom_schema_name else '' }}
{% endmacro %}
```

**Performance rules**
- Break complex models into stages (ephemeral → incremental) to isolate slow transforms
- `--threads`: parallel model execution; safe to set 8–32 for independent models
- `--defer + --state`: CI runs only changed models against prod upstream (50–90% faster CI)
- Pre/post hooks: run `ANALYZE`/`VACUUM` after large table builds
