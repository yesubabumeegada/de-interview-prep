---
title: "Recursive Queries - Real-World Production Examples"
topic: sql
subtopic: recursive-queries
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [sql, recursive-cte, data-lineage, org-chart, bom, production, etl]
---

# Recursive Queries — Real-World Production Examples

## Scenario 1: Data Lineage Impact Analysis

**Business context:** Your data team receives an alert that the `raw_customer_events` table in S3 has been corrupted for the past 3 hours. You need to immediately identify every downstream table, dashboard, and model that has been affected so the on-call engineer can send targeted notifications and pause dependent pipelines.

The warehouse maintains a `table_lineage` table that dbt and Airflow populate automatically as part of the CI/CD pipeline.

```sql
-- Schema:
-- table_lineage(source_table TEXT, target_table TEXT, pipeline_name TEXT, refresh_schedule TEXT)
-- dashboard_sources(dashboard_id INT, dashboard_name TEXT, source_table TEXT)

WITH RECURSIVE downstream_impact AS (
    -- Anchor: direct consumers of the corrupted table
    SELECT 
        tl.target_table                                     AS affected_table,
        tl.pipeline_name,
        tl.refresh_schedule,
        1                                                   AS distance_from_source,
        ARRAY['raw_customer_events'::TEXT, tl.target_table] AS impact_chain
    FROM table_lineage tl
    WHERE tl.source_table = 'raw_customer_events'

    UNION ALL

    -- Recursive: find second-order and further downstream dependencies
    SELECT 
        tl.target_table,
        tl.pipeline_name,
        tl.refresh_schedule,
        di.distance_from_source + 1,
        di.impact_chain || tl.target_table
    FROM table_lineage tl
    JOIN downstream_impact di ON tl.source_table = di.affected_table
    WHERE NOT tl.target_table = ANY(di.impact_chain)   -- Prevent cycles
      AND di.distance_from_source < 15                  -- Safety limit
),

-- Deduplicate: keep shortest path to each affected table
first_impact AS (
    SELECT DISTINCT ON (affected_table)
        affected_table,
        pipeline_name,
        refresh_schedule,
        distance_from_source,
        array_to_string(impact_chain, ' → ') AS lineage_path
    FROM downstream_impact
    ORDER BY affected_table, distance_from_source
),

-- Join to find impacted dashboards
impacted_dashboards AS (
    SELECT 
        ds.dashboard_name,
        ds.dashboard_id,
        fi.affected_table,
        fi.distance_from_source AS table_depth
    FROM dashboard_sources ds
    JOIN first_impact fi ON ds.source_table = fi.affected_table
)

-- Final output: combined impact report
SELECT 
    'TABLE'          AS asset_type,
    fi.affected_table AS asset_name,
    fi.distance_from_source,
    fi.refresh_schedule,
    fi.lineage_path
FROM first_impact fi

UNION ALL

SELECT 
    'DASHBOARD',
    id.dashboard_name,
    id.table_depth + 1,
    NULL,
    id.affected_table || ' → Dashboard: ' || id.dashboard_name
FROM impacted_dashboards id

ORDER BY distance_from_source, asset_type, asset_name;
```

**Result (example):**

| asset_type | asset_name | distance_from_source | refresh_schedule | lineage_path |
|------------|------------|---------------------|-----------------|--------------|
| TABLE | stg_customer_events | 1 | hourly | raw_customer_events → stg_customer_events |
| TABLE | fct_user_sessions | 2 | hourly | raw_customer_events → stg_customer_events → fct_user_sessions |
| TABLE | mart_engagement | 3 | daily | raw_customer_events → stg_customer_events → fct_user_sessions → mart_engagement |
| DASHBOARD | User Retention | 4 | NULL | mart_engagement → Dashboard: User Retention |

**Why this works in production:**
- The recursive CTE automatically follows the entire dependency chain regardless of depth
- `DISTINCT ON` ensures each table appears once with its shortest path (most direct lineage)
- The `UNION ALL` at the end combines table impacts with dashboard impacts in one query
- This query runs in the incident response runbook — engineers paste the corrupted table name and get the full blast radius in seconds

---

## Scenario 2: E-Commerce Category Tree for Navigation

**Business context:** Your e-commerce platform has a product category hierarchy managed by the merchandising team. Categories can be nested 4–6 levels deep (e.g., Home → Kitchen → Appliances → Coffee Makers → Drip Coffee Makers). The frontend team needs:

1. Full breadcrumb paths for SEO meta tags
2. A flattened category table for the BI team's Tableau dashboards
3. Category-level product counts that roll up to parent categories

```sql
-- Schema:
-- categories(category_id INT, category_name TEXT, parent_category_id INT, is_active BOOLEAN)
-- products(product_id INT, category_id INT, status TEXT)

WITH RECURSIVE category_hierarchy AS (
    -- Anchor: top-level categories (no parent)
    SELECT 
        category_id,
        category_name,
        parent_category_id,
        is_active,
        category_name                               AS breadcrumb,
        ARRAY[category_id]                          AS ancestor_ids,
        1                                           AS depth,
        category_id                                 AS root_category_id
    FROM categories
    WHERE parent_category_id IS NULL
      AND is_active = TRUE

    UNION ALL

    -- Recursive: build breadcrumb and track ancestors
    SELECT 
        c.category_id,
        c.category_name,
        c.parent_category_id,
        c.is_active,
        ch.breadcrumb || ' > ' || c.category_name,
        ch.ancestor_ids || c.category_id,
        ch.depth + 1,
        ch.root_category_id
    FROM categories c
    JOIN category_hierarchy ch ON c.parent_category_id = ch.category_id
    WHERE c.is_active = TRUE
      AND ch.depth < 10                            -- Max depth guard
      AND NOT c.category_id = ANY(ch.ancestor_ids) -- Cycle guard
),

-- Count direct products per category
direct_product_counts AS (
    SELECT 
        category_id,
        COUNT(*) AS direct_product_count
    FROM products
    WHERE status = 'active'
    GROUP BY category_id
),

-- For each category, count products in entire subtree
-- (using the ancestor_ids to find which categories are under each node)
subtree_products AS (
    SELECT 
        ancestor_cat                                AS category_id,
        COUNT(DISTINCT p.product_id)               AS subtree_product_count
    FROM category_hierarchy ch
    CROSS JOIN LATERAL UNNEST(ch.ancestor_ids) AS ancestor_cat
    JOIN products p ON p.category_id = ch.category_id
    WHERE p.status = 'active'
    GROUP BY ancestor_cat
)

-- Final output: rich category table
SELECT 
    ch.category_id,
    ch.category_name,
    ch.breadcrumb                                   AS full_path,
    ch.depth,
    ch.root_category_id,
    COALESCE(dpc.direct_product_count, 0)          AS direct_products,
    COALESCE(sp.subtree_product_count, 0)          AS total_products_in_subtree,
    -- For BI flattening: fixed-depth columns
    (ch.ancestor_ids)[1]                            AS level_1_id,
    (ch.ancestor_ids)[2]                            AS level_2_id,
    (ch.ancestor_ids)[3]                            AS level_3_id,
    (ch.ancestor_ids)[4]                            AS level_4_id
FROM category_hierarchy ch
LEFT JOIN direct_product_counts dpc ON ch.category_id = dpc.category_id
LEFT JOIN subtree_products sp ON ch.category_id = sp.category_id
ORDER BY ch.breadcrumb;
```

**Result (sample rows):**

| category_id | category_name | full_path | depth | direct_products | total_products_in_subtree |
|------------|--------------|-----------|-------|----------------|--------------------------|
| 1 | Home | Home | 1 | 0 | 8450 |
| 5 | Kitchen | Home > Kitchen | 2 | 12 | 3200 |
| 18 | Appliances | Home > Kitchen > Appliances | 3 | 0 | 890 |
| 42 | Coffee Makers | Home > Kitchen > Appliances > Coffee Makers | 4 | 245 | 245 |

**Production notes:**
- The `LATERAL UNNEST(ancestor_ids)` pattern efficiently computes subtree product counts by "expanding" each category's ancestor list — every product's category contributes to all its ancestors' counts
- Fixed-depth columns (`level_1_id` through `level_4_id`) allow Tableau to create hierarchical drill-downs without recursion at query time
- This query runs daily in a dbt model and results are stored in a materialized table, so the frontend and BI tools never recurse at request time

---

## Scenario 3: Airflow DAG Dependency Scheduling

**Business context:** Your data platform has 200+ Airflow DAGs with complex inter-dependencies. A platform engineer needs to determine the critical path — the sequence of jobs that, if delayed, will push back the morning SLA report. You need to find all DAGs that must complete before `morning_sla_report` runs, ordered by their position in the dependency chain.

```sql
-- Schema:
-- dag_dependencies(upstream_dag TEXT, downstream_dag TEXT)
-- dag_run_stats(dag_id TEXT, avg_duration_minutes NUMERIC, p95_duration_minutes NUMERIC)

WITH RECURSIVE upstream_deps AS (
    -- Anchor: direct dependencies of the final report
    SELECT 
        dd.upstream_dag                                        AS dag_id,
        dd.upstream_dag                                        AS direct_dep_of,
        1                                                      AS levels_before_report,
        ARRAY['morning_sla_report'::TEXT, dd.upstream_dag]    AS dep_chain
    FROM dag_dependencies dd
    WHERE dd.downstream_dag = 'morning_sla_report'

    UNION ALL

    -- Recursive: find what those DAGs depend on
    SELECT 
        dd.upstream_dag,
        ud.dag_id                                   AS direct_dep_of,
        ud.levels_before_report + 1,
        ud.dep_chain || dd.upstream_dag
    FROM dag_dependencies dd
    JOIN upstream_deps ud ON dd.downstream_dag = ud.dag_id
    WHERE NOT dd.upstream_dag = ANY(ud.dep_chain)  -- Cycle guard
      AND ud.levels_before_report < 20
),

-- Deduplicate to get each DAG's shortest distance to final report
critical_dags AS (
    SELECT DISTINCT ON (dag_id)
        dag_id,
        levels_before_report,
        array_to_string(REVERSE(dep_chain), ' → ') AS execution_order_path
    FROM upstream_deps
    ORDER BY dag_id, levels_before_report
),

-- Annotate with runtime stats
annotated AS (
    SELECT 
        cd.dag_id,
        cd.levels_before_report,
        cd.execution_order_path,
        COALESCE(drs.avg_duration_minutes, 0)  AS avg_runtime_min,
        COALESCE(drs.p95_duration_minutes, 0)  AS p95_runtime_min
    FROM critical_dags cd
    LEFT JOIN dag_run_stats drs ON cd.dag_id = drs.dag_id
),

-- Compute cumulative critical path time per level
level_summary AS (
    SELECT 
        levels_before_report,
        COUNT(*)                           AS dag_count,
        SUM(avg_duration_minutes)          AS total_avg_minutes,
        MAX(p95_duration_minutes)          AS max_p95_minutes  -- Bottleneck per level
    FROM annotated
    GROUP BY levels_before_report
)

-- Output 1: Per-DAG detail
SELECT 
    a.dag_id,
    a.levels_before_report,
    a.avg_runtime_min,
    a.p95_runtime_min,
    -- Flag DAGs that could single-handedly blow the SLA
    CASE WHEN a.p95_runtime_min > 30 THEN 'HIGH RISK' ELSE 'OK' END AS sla_risk,
    a.execution_order_path
FROM annotated a
ORDER BY a.levels_before_report DESC, a.p95_runtime_min DESC;
```

**Sample output:**

| dag_id | levels_before_report | avg_runtime_min | p95_runtime_min | sla_risk | execution_order_path |
|--------|---------------------|-----------------|-----------------|---------|---------------------|
| raw_event_loader | 5 | 12.3 | 18.1 | OK | raw_event_loader → stg_events → ... |
| ga4_api_extract | 5 | 45.2 | 67.8 | HIGH RISK | ga4_api_extract → stg_ga4 → ... |
| customer_snapshot | 4 | 8.1 | 11.4 | OK | customer_snapshot → ... |

**Why this matters in production:**
- The platform team uses this query to identify which upstream DAGs need SLA guarantees and alerting
- `HIGH RISK` DAGs (p95 > 30 min) are candidates for optimization or parallelization
- The `levels_before_report` column maps directly to Airflow's task dependency layers, making it easy to explain to non-technical stakeholders
- Running this recursively means the team doesn't need to maintain a separate dependency documentation spreadsheet — the query always reflects the current state of DAG configurations

---

## Interview Tips

> **Tip 1:** "How do you handle recursive queries in BigQuery?" — "BigQuery supports `WITH RECURSIVE` but it has a default recursion limit. For very deep hierarchies I'd pre-flatten the hierarchy in a dbt model during off-peak hours and store the results as a materialized table. BigQuery also has `GENERATE_DATE_ARRAY` and `GENERATE_ARRAY` which replace recursive date/number series entirely."

> **Tip 2:** "How would you test a recursive CTE for correctness?" — "I test in layers: first I run just the anchor query and verify the root nodes are correct. Then I add the UNION ALL with a WHERE depth <= 1 and verify level 1 results. I incrementally increase the depth limit. For cycle detection, I insert a known cycle in a test dataset and verify the query doesn't loop. Finally I compare row counts against a known-good iteration script (Python or temp table loop)."

> **Tip 3:** "What's the largest hierarchy you've recursed through in production?" — "I've used recursive CTEs on category trees with ~50,000 nodes and 8 levels deep. At that scale, PostgreSQL handles it well in under 2 seconds with a proper index on `parent_id`. Beyond 500,000 nodes, I'd move to a materialized path or closure table — the recursive CTE overhead per iteration becomes significant when each level has thousands of rows."
