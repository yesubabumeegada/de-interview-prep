---
title: "Deployment Strategies - Intermediate"
topic: ci-cd
subtopic: deployment-strategies
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [ci-cd, deployment,blue-green,canary,rollback]
---

# Deployment Strategies — Intermediate

See fundamentals for core concepts. This section covers intermediate patterns and real-world implementation.

## Zero-Downtime Schema Migrations

The hardest deployment challenge for DE is schema changes that affect running pipelines:

```sql
-- ❌ Dangerous: rename column breaks all running queries immediately
ALTER TABLE orders RENAME COLUMN customer_id TO client_id;

-- ✅ Safe: 3-phase migration
-- Phase 1: Add new column (backward compatible)
ALTER TABLE orders ADD COLUMN client_id INT;
UPDATE orders SET client_id = customer_id;

-- Phase 2: Deploy code that reads BOTH columns (tolerates either)
-- All queries work during transition

-- Phase 3: Drop old column (after all code deployed)
ALTER TABLE orders DROP COLUMN customer_id;
```

## dbt Deployment Strategy

```bash
# Slim CI: only run changed models
dbt run --select state:modified+ --defer --state prod-artifacts/
dbt test --select state:modified+

# Full refresh on schedule (not every deploy)
dbt run --full-refresh --target prod  # weekly only
```

## Feature Flags with dbt Variables

```sql
-- models/fct_revenue.sql
{% if var('enable_revenue_v2', false) %}
  {{ ref('int_revenue_v2') }}
{% else %}
  {{ ref('int_revenue_v1') }}
{% endif %}
```

```bash
# Enable in staging
dbt run --vars '{"enable_revenue_v2": true}' --target staging

# Production still uses v1
dbt run --target prod
```
