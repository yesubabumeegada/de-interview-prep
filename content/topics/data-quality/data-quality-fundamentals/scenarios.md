---
title: "Data Quality Fundamentals — Scenarios"
topic: data-quality
subtopic: data-quality-fundamentals
content_type: scenario_question
tags: [data-quality, interview, scenarios, problem-solving]
---

# Data Quality Fundamentals — Interview Scenarios




<article data-difficulty="junior">

## 🟢 Junior: Null Order IDs

**Scenario:** Your orders table has 5,000 rows where `order_id` is NULL. How do you handle this?

<details>
<summary>💡 Hint</summary>

1. **Don't silently drop** — first understand why they're null. Check the source system. Is it a bug? A new record type? 2. **Quarantine** the NULL rows to a separate table with a `dq_failure_reason` column 3. **Alert** the data owner / upstream engineering team 4. **Do not** pass NULL PKs to...

</details>

<details>
<summary>✅ Solution</summary>

1. **Don't silently drop** — first understand why they're null. Check the source system. Is it a bug? A new record type?
2. **Quarantine** the NULL rows to a separate table with a `dq_failure_reason` column
3. **Alert** the data owner / upstream engineering team
4. **Do not** pass NULL PKs to downstream tables — they break joins and aggregations
5. Add a DQ rule that **fails the pipeline** if NULL PKs exceed 0% (zero tolerance for PK nulls)
6. Track the quarantine table so you know when the upstream fix lands and you can replay

**Follow-up:** What if the business says "some orders come from a legacy system that doesn't generate IDs yet"?
→ Generate a surrogate key with a `LEGACY_` prefix, flag rows with `is_surrogate_pk = TRUE`

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Revenue Discrepancy

**Scenario:** The finance team reports that your DW shows $10M in weekly revenue, but the source OLTP system shows $10.5M. How do you investigate?

<details>
<summary>💡 Hint</summary>

First establish whether the gap is consistent across time (structural issue) or only recent (pipeline incident). Then work down the layers: row counts match? If not, find where rows are dropped (Silver filters, dedup rules). If counts match, compare subtotals by dimension (date, region, order type) to isolate the segment. Common culprits: currency conversion, cancelled/refunded orders handled differently, pipeline lag for late-arriving records, and Silver-layer business-rule filters the OLTP doesn't apply.

</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Scope the gap**
```sql
-- Check if it's all weeks or just recent
SELECT
    DATE_TRUNC('week', order_date) AS week,
    SUM(amount) AS dw_revenue
FROM gold.orders
GROUP BY 1
ORDER BY 1 DESC;
-- Compare against finance's numbers
```

**Step 2: Check row counts**
```sql
SELECT COUNT(*) FROM gold.orders WHERE order_date >= '2024-01-08';
-- Compare to OLTP
```

**Step 3: Check for filtering issues**
```sql
-- What's being filtered at Silver?
SELECT dq_status, COUNT(*), SUM(amount)
FROM silver.orders
GROUP BY 1;
-- Are there $500K of WARNING/FAILED rows being excluded?
```

**Step 4: Check deduplication**
```sql
-- Are there duplicate orders being collapsed?
SELECT order_id, COUNT(*) AS cnt
FROM silver.orders
GROUP BY 1
HAVING cnt > 1;
```

**Root cause resolution:**
- Missing orders (ingestion gap) → fix pipeline, backfill
- Filtered by DQ rules → review rules, potentially widen threshold or fix upstream
- Currency conversion error → fix exchange rate join
- Timezone issue → align all timestamps to UTC at ingestion

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing a DQ Framework

**Scenario:** Your company has 300 tables across 5 data domains. You're asked to build a scalable DQ framework. What does it look like?

<details>
<summary>💡 Hint</summary>

Think about the four concerns that must be separate: *rule definition* (how domain teams express expectations without needing engineering), *execution* (at what stage in the pipeline and with what engine), *results storage* (time-series metric store for trending), and *action* (alerting, blocking, quarantine, scorecard). The key design question for 300 tables is automation: you can't manually write rules for every table — consider profiling-driven rule generation and domain-level defaults.

</details>

<details>
<summary>✅ Solution</summary>

**Architecture:**

```mermaid
flowchart TD
    A[Rule Config YAML / dbt tests] --> B[Rule Compiler]
    B --> C[DQ Runner - PySpark / dbt]
    C --> D[Metrics Store - Delta Table]
    D --> E[Observability Dashboard]
    D --> F[Alert Engine]
    F --> G[Slack / PagerDuty]
    G --> H[Data Steward]
    D --> I[DQ Scorecard API]
    I --> J[Data Catalog]
```

**Key design decisions:**

1. **Rule storage:** YAML config files per table, versioned in Git. PR review before new rules go live.
2. **Runner:** dbt tests for SQL transformations, Great Expectations for ingestion, custom PySpark for complex cross-table checks.
3. **Metrics store:** Append-only Delta table with `(table, rule, run_id, evaluated_at, pass_rate)`. Never update in place.
4. **Alerting:** Route critical failures to PagerDuty. Warnings to Slack #data-quality channel. Info to daily email digest.
5. **Ownership:** Every table has a registered `data_owner` in the catalog. Alerts go to that person.
6. **SLAs:** Define per-table: "orders must have ≥99.9% completeness by 8 AM UTC daily."
7. **Scoring:** Weighted DQ score per domain, tracked week-over-week. Red = <95%, Yellow = 95-99%, Green = ≥99%.

**Interview key points:**
- Rules as code (Git, PR review, versioning)
- Separation of concerns: rule definition ≠ runner ≠ metrics store ≠ alerting
- Data ownership model — every alert has a human owner
- DQ is a first-class citizen: block pipelines for critical, alert for warnings

</details>

</article>