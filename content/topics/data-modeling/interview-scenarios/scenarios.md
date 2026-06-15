---
title: "Data Modeling Interview Scenarios"
description: "Hands-on schema design walkthroughs for DE interviews"
content_type: scenario_question
topic: data-modeling
subtopic: interview-scenarios
tags: [data-modeling, schema-design, interview, star-schema, dimensional-modeling]
---

# Data Modeling Interview Scenarios

Work through these scenarios as if you were in a live interview. State your grain, draw your schema, and explain your decisions out loud before checking the solution.

---

<article data-difficulty="junior">

## Scenario 1: Retail Sales Star Schema

**Time allowed:** 15–20 minutes

You are interviewing for a junior data engineer role at a national retail chain. The interviewer asks:

> "Design a star schema for our retail sales reporting. We need to answer questions like: How much did we sell by product category this month? Which stores had the highest revenue last quarter? Who are our top 10 customers by total spend?"

### Your Task

1. Declare the grain of the fact table
2. Identify all dimension tables needed
3. List the columns in the fact table and at least one dimension table
4. Identify what type of key you will use for joins and why

---

<details>
<summary>✅ Solution</summary>

### Step 1: Clarifying Questions (always ask first)

- "Is the grain at the order level or the line-item level?" → Line item gives us the most flexibility
- "Do products change categories over time? Should historical sales reflect the old or new category?"
- "Is there a promotion or discount concept we need to track?"

### Step 2: Grain Declaration

**One row per order line item — one product on one sales transaction.**

This is the finest grain available and gives analysts maximum flexibility to aggregate up to order, store, day, or category.

### Step 3: Fact Table

```sql
CREATE TABLE fact_sales (
    sales_line_key      BIGINT PRIMARY KEY,         -- surrogate key
    date_key            INT    NOT NULL,             -- FK → dim_date
    customer_key        INT    NOT NULL,             -- FK → dim_customer
    product_key         INT    NOT NULL,             -- FK → dim_product
    store_key           INT    NOT NULL,             -- FK → dim_store
    promotion_key       INT    NOT NULL DEFAULT -1,  -- FK → dim_promotion, -1 = no promo
    transaction_id      VARCHAR(50),                 -- degenerate dimension (receipt/order #)
    quantity_sold       INT            NOT NULL,
    unit_price_usd      DECIMAL(10,2)  NOT NULL,
    discount_amount_usd DECIMAL(10,2)  NOT NULL DEFAULT 0,
    gross_revenue_usd   DECIMAL(10,2)  NOT NULL,
    cost_of_goods_usd   DECIMAL(10,2)  NOT NULL,
    gross_margin_usd    DECIMAL(10,2)  NOT NULL
);
```

### Step 4: Dimension Tables

```sql
CREATE TABLE dim_date (
    date_key        INT PRIMARY KEY,   -- YYYYMMDD
    full_date       DATE,
    day_of_week     VARCHAR(10),
    month_name      VARCHAR(10),
    month_number    INT,
    quarter         INT,
    year            INT,
    fiscal_year     INT,
    fiscal_quarter  INT,
    is_holiday      BOOLEAN,
    is_weekend      BOOLEAN
);

CREATE TABLE dim_customer (
    customer_key    INT PRIMARY KEY,
    customer_id     VARCHAR(50),
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(255),
    city            VARCHAR(100),
    state           VARCHAR(50),
    country         VARCHAR(50),
    loyalty_tier    VARCHAR(50),    -- 'Gold', 'Silver', 'Standard'
    -- SCD2 columns for loyalty_tier and address changes
    effective_date  DATE,
    expiry_date     DATE,
    is_current      BOOLEAN
);

CREATE TABLE dim_product (
    product_key     INT PRIMARY KEY,
    product_id      VARCHAR(50),
    product_name    VARCHAR(255),
    brand           VARCHAR(100),
    category        VARCHAR(100),
    subcategory     VARCHAR(100),
    unit_cost_usd   DECIMAL(10,2),
    is_active       BOOLEAN,
    effective_date  DATE,
    expiry_date     DATE,
    is_current      BOOLEAN
    -- SCD2 applied: if product moves categories, history is preserved
);

CREATE TABLE dim_store (
    store_key       INT PRIMARY KEY,
    store_id        VARCHAR(50),
    store_name      VARCHAR(100),
    city            VARCHAR(100),
    state           VARCHAR(50),
    region          VARCHAR(50),
    store_type      VARCHAR(50),   -- 'flagship', 'outlet', 'kiosk'
    sq_footage      INT,
    open_date       DATE
);

CREATE TABLE dim_promotion (
    promotion_key   INT PRIMARY KEY,
    promotion_id    VARCHAR(50),
    promotion_name  VARCHAR(100),
    discount_type   VARCHAR(50),   -- 'percentage', 'fixed', 'bogo'
    discount_value  DECIMAL(5,2),
    start_date      DATE,
    end_date        DATE
);
```

### Step 5: Schema Diagram

```
                    dim_date
                       |
dim_customer     fact_sales     dim_product
       └──────────────┼──────────────┘
                      │
             dim_store    dim_promotion
```

### Step 6: Key Design Decisions to Explain

**Surrogate keys:** Every dimension uses an integer surrogate key as the primary key and as the foreign key in `fact_sales`. The natural key (`customer_id`, `product_id`) is preserved as an attribute for traceability, but joins always use the surrogate.

**SCD2 on `dim_customer` and `dim_product`:** If a customer upgrades to Gold loyalty tier, we want historical sales to reflect the tier they were at the time of purchase. Same for product category changes.

**`promotion_key = -1` for no promotion:** Rather than using NULL, we insert a sentinel row (`promotion_key = -1`, `promotion_name = 'No Promotion'`) in `dim_promotion`. This prevents fact rows from being dropped in JOIN queries when no promotion applied.

**`transaction_id` as degenerate dimension:** The receipt/order number belongs in the fact table because there's nothing to say about it — no attributes, no dimension table needed. Analysts can use it for drill-down or deduplication.

**Answering the three business questions:**

```sql
-- "How much did we sell by product category this month?"
SELECT p.category, SUM(f.gross_revenue_usd) AS revenue
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key AND p.is_current = true
JOIN dim_date   d ON f.date_key     = d.date_key
WHERE d.year = 2024 AND d.month_number = 12
GROUP BY p.category
ORDER BY revenue DESC;

-- "Top 10 customers by total spend"
SELECT c.first_name, c.last_name, SUM(f.gross_revenue_usd) AS lifetime_value
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key AND c.is_current = true
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY lifetime_value DESC
LIMIT 10;
```

</details>

</article>

---

<article data-difficulty="mid">

## Scenario 2: SaaS Subscription Platform — SCD2, Upgrades, and MRR

**Time allowed:** 25–30 minutes

You are interviewing for a mid-level data engineer role at a B2B SaaS company. The interviewer asks:

> "Design a data warehouse schema for our subscription business. We need to track MRR (Monthly Recurring Revenue), handle customers who upgrade or downgrade between plans, and report on churn. Customers can change plans at any time, and we need to know what plan they were on at any given point."

### Your Task

1. Identify the business processes and the appropriate fact tables
2. Handle plan upgrades and downgrades with SCD2
3. Model MRR accurately — including how to handle mid-month plan changes
4. Design a schema that can answer: "What was our MRR as of the last day of each month for the past 2 years?"

---

<details>
<summary>✅ Solution</summary>

### Step 1: Clarifying Questions

- "Is MRR recognized at the start of the billing period or evenly distributed across the month?"
- "Are plans billed monthly or annually? Do annual plans recognize monthly or annually?"
- "What defines 'churn' — cancellation, non-renewal, or 90 days without payment?"
- "Are there free trials that convert to paid?"

Assumed answers for this design: monthly billing, MRR recognized at plan start, churn = cancellation or non-renewal.

### Step 2: Business Processes → Fact Tables

1. **Subscription events** — plan starts, upgrades, downgrades, cancellations (accumulating snapshot or event-level)
2. **MRR snapshots** — monthly point-in-time snapshot of each account's MRR (periodic snapshot fact)
3. **Invoices/payments** — financial fact (optional for this scenario, covered separately)

### Step 3: Dimension — dim_plan with SCD2

This is the key modeling challenge. Plans change, and we need to know the plan at the time of any event.

```sql
CREATE TABLE dim_plan (
    plan_key            INT PRIMARY KEY,        -- surrogate key
    plan_id             VARCHAR(50),            -- natural key (e.g. 'pro_monthly')
    plan_name           VARCHAR(100),           -- 'Professional', 'Enterprise'
    billing_cycle       VARCHAR(20),            -- 'monthly', 'annual'
    mrr_amount_usd      DECIMAL(10,2),          -- monthly recurring value of this plan
    max_seats           INT,
    feature_tier        VARCHAR(50),            -- 'basic', 'pro', 'enterprise'
    -- SCD2 tracking
    effective_date      DATE,
    expiry_date         DATE,
    is_current          BOOLEAN
);
-- SCD2 applied so that if plan pricing changes, historical subscriptions
-- reflect the price that was in effect at the time
```

### Step 4: Dimension — dim_account with SCD2

```sql
CREATE TABLE dim_account (
    account_key         INT PRIMARY KEY,
    account_id          VARCHAR(50),
    company_name        VARCHAR(255),
    industry            VARCHAR(100),
    employee_count_band VARCHAR(50),    -- '1-10', '11-50', '51-200', '200+'
    csm_owner           VARCHAR(100),   -- customer success manager
    acquisition_channel VARCHAR(100),
    -- SCD2 for csm_owner, industry, employee_count_band
    effective_date      DATE,
    expiry_date         DATE,
    is_current          BOOLEAN
);
```

### Step 5: Subscription Event Fact Table

**Grain:** One row per subscription state change event (new, upgrade, downgrade, cancel, reactivation).

```sql
CREATE TABLE fact_subscription_event (
    event_key           BIGINT PRIMARY KEY,
    event_date_key      INT     REFERENCES dim_date(date_key),
    account_key         INT     REFERENCES dim_account(account_key),
    plan_key            INT     REFERENCES dim_plan(plan_key),         -- new plan
    previous_plan_key   INT     REFERENCES dim_plan(plan_key),         -- prior plan (NULL for new)
    event_type_key      INT     REFERENCES dim_event_type(event_type_key),
    subscription_id     VARCHAR(50),                                    -- degenerate dim
    seat_count          INT,
    mrr_change_usd      DECIMAL(10,2),    -- positive = expansion, negative = contraction
    new_mrr_usd         DECIMAL(10,2),    -- MRR after this event
    prior_mrr_usd       DECIMAL(10,2)     -- MRR before this event
);

CREATE TABLE dim_event_type (
    event_type_key  INT PRIMARY KEY,
    event_type      VARCHAR(50),           -- 'new', 'upgrade', 'downgrade', 'cancel', 'reactivation'
    mrr_category    VARCHAR(50)            -- 'new_mrr', 'expansion', 'contraction', 'churn', 'reactivation'
);
```

### Step 6: MRR Periodic Snapshot Fact Table

This is the key to answering "What was our MRR on the last day of each month?"

**Grain:** One row per account per month (monthly snapshot).

```sql
CREATE TABLE fact_mrr_snapshot (
    snapshot_month_key  INT     REFERENCES dim_date(date_key),  -- last day of each month
    account_key         INT     REFERENCES dim_account(account_key),
    plan_key            INT     REFERENCES dim_plan(plan_key),
    subscription_id     VARCHAR(50),
    seat_count          INT,
    mrr_usd             DECIMAL(10,2),      -- semi-additive: sum across accounts OK, not across time
    status              VARCHAR(50)         -- 'active', 'trial', 'paused'
);
```

Note: `mrr_usd` is a **semi-additive fact**. You can sum it across all accounts in a given month to get total company MRR. You cannot sum it across months (that would double-count). For time-based analysis, use the snapshot at the end of each period.

### Step 7: Handling Mid-Month Plan Changes

**The challenge:** A customer upgrades from $100/month to $200/month on the 15th. How do you handle MRR for that month?

**Option A — Pro-rated approach (for revenue, not pure MRR):** Record the partial-month amounts in the financial invoicing fact. MRR snapshots always use the full plan amount as of the last day of the month.

**Option B — End-of-month snapshot (standard SaaS MRR definition):** MRR is defined as the plan amount the customer is on as of the last day of the month. A mid-month upgrade shows in the MRR snapshot at the new higher amount for that month.

For the `fact_mrr_snapshot`, apply Option B: the `plan_key` in the snapshot reflects the plan active on the last day of the snapshot month.

```sql
-- Build monthly MRR snapshot (run on last day of each month)
INSERT INTO fact_mrr_snapshot
SELECT
    20241231 AS snapshot_month_key,
    s.account_key,
    s.plan_key,
    s.subscription_id,
    s.seat_count,
    p.mrr_amount_usd * s.seat_count AS mrr_usd,
    'active' AS status
FROM dim_subscription_current s   -- current active subscriptions as of today
JOIN dim_plan p ON s.plan_key = p.plan_key AND p.is_current = true
WHERE s.status = 'active';
```

### Step 8: MRR Bridge — Tracking Net New, Expansion, Contraction, Churn

A standard SaaS MRR waterfall report:

```sql
-- MRR movement between two months
SELECT
    et.mrr_category,
    SUM(f.mrr_change_usd) AS total_mrr_change
FROM fact_subscription_event f
JOIN dim_event_type et ON f.event_type_key = et.event_type_key
JOIN dim_date d ON f.event_date_key = d.date_key
WHERE d.year = 2024 AND d.month_number = 12
GROUP BY et.mrr_category;

-- Result:
-- new_mrr:      +$45,000   (new customers)
-- expansion:    +$12,000   (upgrades)
-- contraction:  -$3,000    (downgrades)
-- churn:        -$8,000    (cancellations)
-- reactivation: +$2,000    (win-backs)
-- Net MRR change: +$48,000
```

### Step 9: Key Design Decisions

**SCD2 on `dim_plan`:** If the Professional plan price increases from $100 to $120, we create a new `plan_key` for the new price point. Historical subscriptions keep their original `plan_key`, preserving the original MRR amounts for historical accuracy.

**`previous_plan_key` in the event fact:** Allows easy detection of upgrades vs downgrades without self-joining. If `new_mrr_usd > prior_mrr_usd`, it's expansion; if lower, it's contraction.

**Semi-additive MRR:** In BI tools, always use `SUM(mrr_usd)` for a specific month filter. For trend analysis, use snapshots at month-end, not `SUM` across all months.

</details>

</article>

---

<article data-difficulty="senior">

## Scenario 3: Unified Fintech Data Model — Transactions, Events, Risk Scores, Late Data, and ML Features

**Time allowed:** 40–45 minutes

You are interviewing for a senior data engineer role at a fintech company. The interviewer asks:

> "Design a unified data model that supports three teams: (1) the analytics team needs OLAP reporting on transaction volumes, fraud rates, and customer behavior; (2) the ML team needs to serve features for a real-time fraud detection model; (3) the risk team needs to run risk scoring on both historical and incoming transactions. The data includes payment transactions, user events (clicks, logins, app actions), and risk scores assigned by a model. Some transactions arrive late — up to 72 hours after the event timestamp. You need to design for correctness, performance, and late data handling."

### Your Task

1. Design the core schema to serve all three consumer types
2. Explain how you handle late-arriving transactions (up to 72 hours late)
3. Design a feature store-compatible model for the ML team
4. Discuss the partitioning and physical design for petabyte-scale data
5. Identify the trade-offs between serving OLAP and ML from the same or different models

---

<details>
<summary>✅ Solution</summary>

### Step 1: Clarifying Questions

- "How many transactions per day, and what's the P99 latency for late arrivals?"
- "Does the ML model need point-in-time correct features — i.e., no future leakage?"
- "Is risk scoring batch (nightly) or real-time (milliseconds)?"
- "Are user events and transactions in the same pipeline or separate streams?"
- "What's the regulatory retention requirement for financial transactions?"

Assumed answers: 50M transactions/day, P99 late arrival = 72 hours, ML features must be point-in-time correct (no leakage), risk scoring is near-real-time (5-minute SLA), 7-year retention.

### Step 2: Layered Architecture

Rather than one monolithic model, design three layers:

```
Source Layer (Raw)
       │
       ▼
Integration Layer (cleansed, typed, deduplicated)
       │
       ├──► Dimensional Layer (OLAP — star schema for analytics team)
       │
       ├──► Feature Store (point-in-time correct features for ML team)
       │
       └──► Risk Layer (enriched events for risk scoring)
```

### Step 3: Integration Layer — Core Transaction Table

**Grain:** One row per payment transaction attempt (not per status update).

```sql
CREATE TABLE int_transactions (
    transaction_id          VARCHAR(64)  NOT NULL,
    user_id                 VARCHAR(64)  NOT NULL,
    merchant_id             VARCHAR(64),
    event_timestamp         TIMESTAMP    NOT NULL,    -- when transaction occurred (event time)
    processing_timestamp    TIMESTAMP    NOT NULL,    -- when we received it (processing time)
    ingestion_timestamp     TIMESTAMP    NOT NULL,    -- when loaded into DW
    amount_usd              DECIMAL(14,4) NOT NULL,
    currency                VARCHAR(3),
    transaction_type        VARCHAR(50),              -- 'purchase', 'refund', 'transfer', 'withdrawal'
    channel                 VARCHAR(50),              -- 'web', 'mobile', 'api', 'pos'
    merchant_category_code  VARCHAR(10),
    country_code            VARCHAR(3),
    status                  VARCHAR(50),              -- 'approved', 'declined', 'pending', 'reversed'
    decline_reason          VARCHAR(100),
    -- Late data tracking
    arrival_delay_hours     DECIMAL(6,2),             -- (processing_timestamp - event_timestamp) in hours
    is_late_arrival         BOOLEAN,                  -- arrival_delay_hours > 1
    -- Idempotency
    record_hash             CHAR(64),                 -- hash of business key for dedup
    _dbt_updated_at         TIMESTAMP
)
PARTITION BY RANGE (DATE(event_timestamp))
CLUSTER BY user_id, transaction_type;
```

**Three timestamps are critical for fintech:**
- `event_timestamp` — when the transaction happened in the real world (use for analytics, risk calculations)
- `processing_timestamp` — when the payment processor recorded it (use for SLA monitoring)
- `ingestion_timestamp` — when we loaded it into the warehouse (use for pipeline monitoring)

### Step 4: Handling Late-Arriving Transactions

**The challenge:** A transaction with `event_timestamp = 2024-12-15 02:00 UTC` arrives in the pipeline at `2024-12-17 14:00 UTC` — 60 hours late. Our daily batch pipeline for Dec 15 has already run.

**Strategy: Micro-partitions + Re-processing Window**

```sql
-- Partition by event_timestamp date (not ingestion date)
-- This means the late transaction goes into the Dec 15 partition

-- Downstream models use a 72-hour lookback window for completeness
-- dbt model example:
SELECT *
FROM int_transactions
WHERE DATE(event_timestamp) = '{{ var("run_date") }}'
   OR (
     DATE(event_timestamp) BETWEEN '{{ var("run_date") }}' - INTERVAL 3 DAY
                               AND '{{ var("run_date") }}'
     AND DATE(ingestion_timestamp) = '{{ var("run_date") }}'
   )
-- This picks up both on-time records AND any late arrivals from the past 3 days
```

**Alternative: Watermark-Based Streaming**

For near-real-time risk scoring, use a streaming processor (Flink, Spark Streaming) with watermarks:

```
Watermark = max(event_timestamp seen so far) - 72 hours

Events older than the watermark are considered "late" and handled separately:
- If late <= 72h: include in current batch, recompute affected aggregates
- If late > 72h: flag as out-of-window, load but exclude from standard aggregates
```

**Idempotent Reprocessing:**

```sql
-- Upsert pattern — safe to rerun for any date without double-counting
MERGE INTO fact_transactions AS target
USING staging_transactions AS source
ON target.transaction_id = source.transaction_id
WHEN MATCHED THEN
    UPDATE SET status = source.status, _dbt_updated_at = source._dbt_updated_at
WHEN NOT MATCHED THEN
    INSERT (...) VALUES (...);
```

### Step 5: Dimensional Layer for OLAP (Analytics Team)

**Grain of fact_transaction:** One row per transaction attempt.

```sql
CREATE TABLE fact_transaction (
    transaction_key         BIGINT,
    event_date_key          INT     REFERENCES dim_date(date_key),
    user_key                INT     REFERENCES dim_user(user_key),
    merchant_key            INT     REFERENCES dim_merchant(merchant_key),
    channel_flags_key       INT     REFERENCES dim_channel_flags(channel_flags_key),
    transaction_id          VARCHAR(64),                               -- degenerate dim
    amount_usd              DECIMAL(14,4),
    is_approved             BOOLEAN,
    is_fraudulent           BOOLEAN,                                   -- post-review label
    fraud_score             DECIMAL(5,4),                              -- 0.0000 to 1.0000
    days_since_last_txn     INT,                                       -- pre-computed feature
    arrival_delay_hours     DECIMAL(6,2)
)
PARTITION BY RANGE (event_date_key)
CLUSTER BY user_key;
```

### Step 6: Feature Store Model for ML Team

The ML team needs **point-in-time correct** features — for any transaction, they need to know what the user's behavior looked like at the exact moment the transaction occurred, with no data from the future leaking in.

**Feature table design — user behavioral features:**

```sql
CREATE TABLE features_user_behavior (
    user_id                 VARCHAR(64)   NOT NULL,
    feature_timestamp       TIMESTAMP     NOT NULL,   -- when this feature vector is valid AS OF
    -- Rolling window features (pre-computed by feature pipeline)
    txn_count_1h            INT,
    txn_count_24h           INT,
    txn_count_7d            INT,
    total_spend_24h_usd     DECIMAL(14,4),
    total_spend_7d_usd      DECIMAL(14,4),
    unique_merchants_7d     INT,
    unique_countries_7d     INT,
    avg_txn_amount_30d      DECIMAL(10,4),
    stddev_txn_amount_30d   DECIMAL(10,4),
    days_since_account_open INT,
    failed_txn_ratio_24h    DECIMAL(5,4),
    PRIMARY KEY (user_id, feature_timestamp)
)
PARTITION BY RANGE (DATE(feature_timestamp));
```

**Point-in-time correct join pattern (dbt macro):**

```sql
-- For ML training: join features to each transaction as of the transaction's event_timestamp
SELECT
    t.transaction_id,
    t.amount_usd,
    t.is_fraudulent,                    -- label
    f.txn_count_1h,
    f.txn_count_24h,
    f.total_spend_24h_usd,
    f.failed_txn_ratio_24h,
    f.unique_countries_7d
FROM int_transactions t
INNER JOIN LATERAL (
    SELECT *
    FROM features_user_behavior f
    WHERE f.user_id = t.user_id
      AND f.feature_timestamp <= t.event_timestamp    -- no future leakage
    ORDER BY f.feature_timestamp DESC
    LIMIT 1
) f ON true;
```

This `LATERAL JOIN` (or equivalent in BigQuery/Snowflake using `QUALIFY ROW_NUMBER()`) ensures that for each transaction, we use only features computed before the transaction occurred.

### Step 7: Risk Scoring Layer

```sql
CREATE TABLE fact_risk_scores (
    transaction_key         BIGINT,
    transaction_id          VARCHAR(64),
    scoring_timestamp       TIMESTAMP,
    model_version           VARCHAR(50),
    fraud_score             DECIMAL(5,4),
    risk_tier               VARCHAR(20),   -- 'low', 'medium', 'high', 'critical'
    rule_triggered          VARCHAR(255),  -- human-readable rule that fired (if any)
    decision                VARCHAR(50),   -- 'approve', 'review', 'decline'
    review_outcome          VARCHAR(50),   -- 'confirmed_fraud', 'false_positive', 'pending'
    reviewed_by             VARCHAR(100),
    review_timestamp        TIMESTAMP
);
```

The risk layer is **append-only** — every re-scoring of a transaction creates a new row. This gives full audit history of how scores changed as more information arrived (critical for regulatory compliance).

### Step 8: Trade-offs Between Shared vs Separate Models

| Dimension | Single Unified Model | Separate OLAP + ML Models |
|---|---|---|
| Data freshness | Same data for all consumers | Can tune refresh rate per consumer |
| Query performance | Risk of contention between BI and ML workloads | Each model optimized independently |
| Storage cost | Lower — one copy | Higher — data duplicated |
| Governance | Simpler — one source of truth | Need sync mechanism to prevent drift |
| Feature leakage risk | Higher — harder to enforce PIT-correct joins | Lower — ML feature table explicitly PIT-correct |
| Operational complexity | Lower | Higher — two pipelines to maintain |

**Recommendation for this fintech scenario:**

> "I recommend a layered approach: one integration layer (source of truth), a dimensional star schema materialized for the analytics team optimized for OLAP queries, and a separate feature store table for ML with point-in-time correctness explicitly enforced. The risk scoring table is append-only for full audit trails. All three layers read from the same integration layer, so consistency is maintained. The duplication cost on cloud columnar storage is minimal compared to the risk of a BI query interfering with a real-time fraud scoring workload."

### Step 9: Physical Design Summary

```
fact_transaction          → PARTITION BY event_date, CLUSTER BY user_key
int_transactions          → PARTITION BY event_timestamp date, CLUSTER BY user_id, transaction_type
features_user_behavior    → PARTITION BY feature_timestamp date, PRIMARY KEY (user_id, feature_timestamp)
fact_risk_scores          → PARTITION BY scoring_timestamp date, no clustering (append-only log)
```

**Retention policies:**
- `int_transactions`: 7 years (regulatory requirement)
- `features_user_behavior`: 2 years (ML training window)
- `fact_risk_scores`: 7 years (audit requirement)
- `fact_transaction` (OLAP): 7 years with cold storage tiering for data > 2 years old

</details>

</article>
