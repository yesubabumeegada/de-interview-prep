---
title: "Looker & Looker Studio — Real-World Applications"
topic: gcp
subtopic: looker
content_type: study_material
difficulty_level: mid-level
tags: [gcp, looker, lookml, case-study, interview]
---

# Looker & Looker Studio — Real-World Applications

Three production stories at the Looker/data-platform boundary — the territory DE interviews actually probe.

## Case Study 1: The Dashboard That Cost $6K a Month

**Context:** A retail company's "Executive Daily" dashboard — 24 tiles, auto-refreshed hourly, embedded on office TVs — was traced to ~$6K/month of BigQuery on-demand spend.

**Investigation:**

```sql
-- Find Looker-generated queries by bytes billed
SELECT
  REGEXP_EXTRACT(query, r'history_slug:\s*(\w+)') AS history_slug,
  COUNT(*) AS runs,
  ROUND(AVG(total_bytes_billed)/POW(10,9), 1) AS avg_gb,
  ROUND(SUM(total_bytes_billed)/POW(10,12), 2) AS total_tb
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE user_email = 'looker-sa@prod.iam.gserviceaccount.com'
  AND creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY 1 ORDER BY total_tb DESC LIMIT 20;
```

Joined back through Looker's System Activity: every tile queried the raw `fct_sales` table (2.1 TB, **unpartitioned**), and the hourly refresh ran 24/7 — including 1am, for TVs nobody watched. Worse, the underlying data only updated **once nightly**.

**Fixes, in order applied:**

1. **Partitioned and clustered the fact table** (date partition + `store_id, sku` clustering). Average tile scan: 290 GB → 4 GB.
2. **Datagroup tied to ETL completion** instead of `max_cache_age: 1 hour`:

```lookml
datagroup: sales_nightly {
  sql_trigger: SELECT MAX(run_id) FROM ops.dwh_runs WHERE status = 'success' ;;
  max_cache_age: "26 hours"
}
```

With nightly-updated data, hourly refresh had been recomputing identical results 23 times a day.

3. **Aggregate table** for the daily-grain rollup all 24 tiles actually displayed:

```lookml
aggregate_table: daily_store_rollup {
  query: {
    dimensions: [sales.sale_date, sales.store_region]
    measures: [sales.revenue, sales.units, sales.margin]
  }
  materialization: { datagroup_trigger: sales_nightly }
}
```

**Result:** ~$6K → under $150/month for the same dashboard. The lesson that lands in interviews: *the dashboard wasn't redesigned at all* — every fix was at the data-platform layer.

## Case Study 2: Migrating Metric Logic Out of a 600-Line PDT

**Context:** A fintech's "north star" metric — adjusted net revenue — lived in a 600-line LookML derived table written over two years by three departed analysts. Finance's dbt-based monthly reporting computed it *differently*; quarter-end reconciliation took days.

**The migration (8 weeks, zero dashboard downtime):**

1. **Freeze + characterize.** Locked the PDT file (CODEOWNERS), then captured its output as a regression baseline:

```sql
CREATE TABLE audit.pdt_baseline_2024q1 AS
SELECT * FROM `looker_scratch.LR_adjusted_revenue_pdt`;
```

2. **Rebuild in dbt** as three tested models (`int_revenue_adjustments`, `int_fx_normalized`, `fct_adjusted_revenue`) with schema tests and a reconciliation test against the baseline:

```yaml
- name: fct_adjusted_revenue
  tests:
    - dbt_utils.equality:
        compare_model: ref('pdt_baseline_2024q1')
        compare_columns: [month, entity_id, adjusted_net_revenue]
```

The equality test failed initially — and that failure was the *finding*: the PDT silently dropped refunds processed >90 days after sale. Finance's number had been right; the dashboard's wrong, for two years.

3. **Swap the view source** — the LookML view's `derived_table:` became `sql_table_name: analytics.fct_adjusted_revenue`, dimensions/measures unchanged, so every dashboard kept working:

```lookml
view: adjusted_revenue {
  sql_table_name: analytics.fct_adjusted_revenue ;;  # was: derived_table { 600 lines }
  ...
}
```

4. **Content validation** (Spectacles) on the swap PR confirmed zero broken Looks/dashboards before merge.

**Interview takeaway:** the technical swap is trivial; the value is the *process* — baseline, rebuild with tests, reconcile (surfacing a real bug), atomic cutover behind a stable interface.

## Case Study 3: Looker Studio Sprawl at a Startup

**Context:** A 60-person startup ran everything on free Looker Studio against BigQuery. By year two: 200+ reports, 9 different definitions of "active user," and a $3K/month BigQuery bill dominated by report viewers re-firing queries.

**Pragmatic fix (no budget for enterprise Looker):**

1. **A thin governed layer in dbt**: `metrics_daily` mart tables — one row per day per entity with the *official* metric values, tested and documented.
2. **Reports repointed to marts** instead of raw events; the marts are small (MBs), so viewer interactions scan pennies.
3. **Extracted data sources** for the heaviest exec dashboards — snapshot the mart daily, zero per-view query cost.
4. **BI Engine reservation** (small, 2 GB) to keep the remaining direct dashboards interactive.
5. **A "metrics contract" doc**: any new report computing its own version of an official metric fails design review — social governance where tooling governance is unaffordable.

**Result:** BigQuery BI spend −85%; "which number is right?" escalations effectively ended.

**Interview takeaway:** semantic-layer *thinking* doesn't require semantic-layer *licensing* — official metrics can be governed as mart tables with tests and a contract.

## Patterns Worth Quoting

| Pattern | One-liner for the interview |
|---|---|
| Fix the platform, not the dashboard | "Partitioning + a datagroup tied to ETL cut a dashboard's cost 97% with zero UI changes." |
| PDT → dbt behind a stable view | "Swap `derived_table` for `sql_table_name`; dashboards never notice the migration." |
| Reconciliation as discovery | "The dbt equality test against the PDT baseline found a 2-year-old refund bug." |
| Cache discipline matches data cadence | "Hourly refresh of nightly data is 23 wasted recomputes a day." |
| Govern metrics without tooling | "Official metrics as tested dbt marts + a contract beats nine definitions of 'active user'." |
