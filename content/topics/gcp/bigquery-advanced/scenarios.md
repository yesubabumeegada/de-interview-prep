---
title: "BigQuery Advanced — Scenario Questions"
topic: gcp
subtopic: bigquery-advanced
content_type: scenario_question
tags: [gcp, bigquery, partitioning, security, optimization, interview]
---

# BigQuery Advanced — Scenario Questions

<article data-difficulty="junior">

## 🟢 Junior: Partition Design Gone Wrong

**Scenario:** Your team created a BigQuery table `events` partitioned by `_PARTITIONTIME` (ingestion-time) and clustered by `user_id`. A junior analyst reports that queries filtering on `event_timestamp` (the actual business timestamp in the data) are still scanning the full table despite specifying a narrow date range. They wrote:

```sql
SELECT * FROM `project.dataset.events`
WHERE event_timestamp BETWEEN '2024-01-01' AND '2024-01-07';
```

The query scans 5TB instead of the expected ~100GB. Explain what's happening and provide a corrected table design and query.

<details>
<summary>✅ Solution</summary>

**Root cause:** The table is partitioned by `_PARTITIONTIME` (ingestion time — when data was loaded), but the WHERE clause filters on `event_timestamp` (business event time). These are different columns. BigQuery cannot use the ingestion-time partition metadata to prune based on `event_timestamp`, so it scans all partitions.

This is especially common with mobile event pipelines where events are buffered on device and sent hours or days late — the event may have `event_timestamp = 2024-01-01` but be loaded (ingested) on `2024-01-03`.

**Fix 1: Switch to column partitioning on a materialized DATE column**

```sql
-- Recreate table with column-based partitioning
CREATE OR REPLACE TABLE `project.dataset.events`
(
  event_id STRING,
  user_id INT64,
  event_type STRING,
  event_timestamp TIMESTAMP,
  event_date DATE,  -- add this derived column
  properties JSON
)
PARTITION BY event_date  -- partition by business date
CLUSTER BY user_id, event_type;

-- Load with the materialized date
INSERT INTO `project.dataset.events`
SELECT
  event_id,
  user_id,
  event_type,
  event_timestamp,
  DATE(event_timestamp) AS event_date,  -- derive at load time
  properties
FROM `project.dataset.raw_events`;
```

**Corrected query:**
```sql
-- Now partition pruning works correctly
SELECT user_id, event_type, event_timestamp
FROM `project.dataset.events`
WHERE event_date BETWEEN '2024-01-01' AND '2024-01-07';
-- Scans only 7 partitions, ~100GB
```

**Fix 2 (if you can't recreate):** Filter on `_PARTITIONDATE` to prune ingestion-time partitions, and add a secondary filter on `event_timestamp`. This reduces scan but requires knowing the ingestion lag:

```sql
SELECT * FROM `project.dataset.events`
WHERE _PARTITIONDATE BETWEEN '2024-01-01' AND '2024-01-10'  -- generous window for late arrivals
  AND event_timestamp BETWEEN '2024-01-01' AND '2024-01-07';
```

**Key takeaway for interviews:** Never assume `_PARTITIONTIME` ≈ your business timestamp. Column-based partitioning on a materialized date field is the safer, more explicit choice for event data.

</details>

</article>

<article data-difficulty="mid">

## 🟡 Mid-Level: Designing a Row-Level Security Model for Sales Data

**Scenario:** You're the data engineer for a 500-person sales organization. The sales pipeline table (`sales.pipeline`) contains every deal across all territories (AMER, EMEA, APAC). Requirements:

1. Individual sales reps see only their own deals (`owner_email = current user`).
2. Regional managers see all deals in their region.
3. Revenue ops team sees everything.
4. A `pipeline_summary` materialized view aggregates by region/stage — this should respect RLS too.
5. Performance: the table has 50M rows; RLS must not add more than 100ms to query latency.

Design the complete RLS implementation and address the materialized view requirement.

<details>
<summary>✅ Solution</summary>

**RLS Implementation:**

```sql
-- Policy 1: Reps see their own deals
CREATE OR REPLACE ROW ACCESS POLICY rep_policy
ON `project.sales.pipeline`
GRANT TO ("domain:company.com")  -- all company users get this base policy
FILTER USING (owner_email = SESSION_USER());

-- Policy 2: Regional managers see their region
CREATE OR REPLACE ROW ACCESS POLICY amer_manager_policy
ON `project.sales.pipeline`
GRANT TO ("group:amer-managers@company.com")
FILTER USING (territory = 'AMER');

CREATE OR REPLACE ROW ACCESS POLICY emea_manager_policy
ON `project.sales.pipeline`
GRANT TO ("group:emea-managers@company.com")
FILTER USING (territory = 'EMEA');

CREATE OR REPLACE ROW ACCESS POLICY apac_manager_policy
ON `project.sales.pipeline`
GRANT TO ("group:apac-managers@company.com")
FILTER USING (territory = 'APAC');

-- Policy 3: Revenue ops sees everything
CREATE OR REPLACE ROW ACCESS POLICY revenue_ops_policy
ON `project.sales.pipeline`
GRANT TO ("group:revenue-ops@company.com")
FILTER USING (TRUE);
```

**Key design notes:**
- A user matching MULTIPLE policies sees the UNION of their matching policies. An AMER manager who is also `domain:company.com` sees: (their own deals) UNION (all AMER deals) = all AMER deals. This works correctly.
- Revenue ops users match the `revenue_ops_policy` (TRUE) plus the `rep_policy` — union of (their own deals) and (all deals) = all deals. Correct.
- `SESSION_USER()` is evaluated at query time per-user — no hardcoded email lists to maintain.

**Materialized View Problem:**

Materialized views do NOT inherit row access policies from base tables. The MV computes its results using the service account that runs the refresh job — it sees ALL rows. Users querying the MV bypass RLS.

**Solutions:**

Option A — Don't use a materialized view for sensitive aggregations. Use a regular VIEW instead:
```sql
CREATE OR REPLACE VIEW `project.sales.pipeline_summary` AS
SELECT
  territory,
  stage,
  COUNT(*) AS deal_count,
  SUM(deal_value) AS total_value
FROM `project.sales.pipeline`  -- RLS is enforced here at query time
GROUP BY territory, stage;
```
Regular views enforce RLS on the underlying table at query time per-user. A rep querying this view only counts their own deals.

Option B — If you need MV performance, create separate MVs per region and control access at the MV level via dataset/table IAM. Revenue ops accesses an unrestricted MV; regional managers access region-specific MVs.

**Performance:**
`SESSION_USER()` comparison is a simple string equality — sub-millisecond. The `owner_email` column should be in the clustering spec to speed up physical reads. With 50M rows and proper clustering, RLS overhead is negligible.

```sql
-- Ensure owner_email is in clustering for rep-level RLS performance
CREATE OR REPLACE TABLE `project.sales.pipeline`
PARTITION BY close_date_month
CLUSTER BY territory, owner_email, stage;
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing a Cost-Optimal Multi-Tenant BigQuery Architecture

**Scenario:** You're joining a data platform team at a company with 15 internal business units (BUs), each with 5-30 analysts. Current state:
- All BUs share one BigQuery project (`data-warehouse`)
- $180,000/month BigQuery bill on on-demand pricing
- No visibility into which BU is spending what
- Two incidents last quarter: a rogue `SELECT *` on a 200TB table cost $1,000; a scheduled query forgot a WHERE clause and ran daily for 2 weeks before anyone noticed
- Some BUs process PII (HR, Finance), others don't

Design a BigQuery architecture that: (a) provides cost isolation and chargeback, (b) prevents future runaway costs, (c) enforces PII governance, (d) optimizes the overall bill. Include your slot reservation strategy recommendation.

<details>
<summary>✅ Solution</summary>

**Architecture Overview:**

```
Organization: company.com
├─ Folder: data-platform
│   ├─ Project: bq-admin (holds reservations, billing export)
│   ├─ Project: data-raw (raw data landing, restricted access)
│   └─ Project: data-shared (cross-BU views and aggregations)
└─ Folder: business-units
    ├─ Project: bu-finance (Finance BU workspace)
    ├─ Project: bu-hr (HR BU workspace)
    ├─ Project: bu-marketing (Marketing BU workspace)
    └─ ... (one project per BU)
```

**Cost Isolation — Separate Projects per BU:**
Each BU gets its own GCP project. BigQuery jobs run in the BU's project → costs appear on that project's billing. This enables:
- Cloud Billing budgets with alerts per BU project.
- Billing export to BigQuery → join with INFORMATION_SCHEMA for query-level chargeback reports.
- BU project admins can see their own INFORMATION_SCHEMA.JOBS.

**Slot Reservation Strategy:**

At $180K/month on-demand, that's ~36,000 TB scanned/month (at $5/TB). Switching to Enterprise committed slots:
- Baseline: 2,000 slots annually ≈ $70K/year ≈ $5,800/month (saving ~$175K/month).
- Add autoscaling up to 4,000 slots for peak hours.

```
RESERVATION: "interactive" (500 slots, Enterprise)
  → ASSIGNMENTS: all BU projects, job_type=QUERY

RESERVATION: "etl-batch" (1,000 slots, Enterprise)
  → ASSIGNMENTS: data-raw, data-shared, job_type=BATCH

RESERVATION: "bi-engine" — replaced by BI Engine memory reservations
  → 50GB BI Engine per BU project for dashboard queries
```

**Preventing Runaway Costs:**

1. `maximum_bytes_billed` set at the project level via Organization Policy or enforced in dbt/orchestration job configs.
2. `require_partition_filter = TRUE` on all tables > 100GB.
3. Custom quotas: 5TB/day per-user query limit via BigQuery Admin quotas UI.
4. Scheduled query monitoring: Cloud Monitoring alert if a scheduled query scans more than 10x its 30-day average (anomaly detection from INFORMATION_SCHEMA).

```python
# Enforce max_bytes_billed in all dbt project profiles
# profiles.yml
production:
  type: bigquery
  method: oauth
  project: bu-marketing
  dataset: analytics
  maximum_bytes_billed: 107374182400  # 100 GB
  job_labels:
    team: marketing
    environment: production
```

**PII Governance:**

- HR and Finance datasets live in their BU projects with stricter IAM (no `allAuthenticatedUsers`, no cross-BU reader access).
- Policy Tags taxonomy created in Data Catalog for PII fields (SSN, salary, email, DOB).
- `Fine-Grained Reader` on PII policy tags granted only to approved service accounts and data stewards.
- Data Catalog entry created for every PII-tagged table — visible to all in the catalog but accessible only with proper tag permissions.
- VPC Service Controls perimeter around Finance and HR projects to prevent data exfiltration (covered in IAM/Security module).

**Cross-BU Data Access:**
- `data-shared` project holds views on top of raw tables.
- Views are parameterized by BU project in authorized views configuration.
- Each BU's project is an authorized viewer of the views it needs — not the raw tables.

```sql
-- Authorized view in data-shared allowing marketing to see non-PII product data
CREATE OR REPLACE VIEW `data-shared.views.product_sales` AS
SELECT
  product_id, product_name, category,
  sale_date, quantity, revenue
  -- No customer PII included
FROM `data-raw.sales.transactions`;
-- Grant access: data-shared project is authorized viewer of data-raw.sales
```

**Cost Attribution Report** (run daily, send to BU leads):

```sql
SELECT
  project_id AS bu_project,
  DATE(creation_time) AS date,
  SUM(total_bytes_processed) / POW(1024, 4) AS tb_processed,
  SUM(total_bytes_processed) / POW(1024, 4) * 5 AS est_cost_usd,
  COUNT(*) AS query_count
FROM `bq-admin.billing.jobs_export`  -- billing export view
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
GROUP BY 1, 2
ORDER BY date DESC, est_cost_usd DESC;
```

**Summary of savings/improvements:**
- ~$170K/month savings from on-demand → slot reservations.
- Full cost attribution and chargeback capability per BU.
- PII protected at column and project levels.
- Runaway queries prevented via byte limits + partition filter enforcement.
- Anomaly detection catches rogue scheduled queries within 1 day.

</details>

</article>
