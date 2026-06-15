---
title: "Vertex AI — Scenario Questions"
topic: gcp
subtopic: vertex-ai
content_type: scenario_question
tags: [gcp, vertex-ai, feature-store, pipelines, bqml, interview]
---

# Vertex AI — Scenario Questions

<article data-difficulty="junior">

## 🟢 Junior: Setting Up a BQML Churn Model

**Scenario:** You're a junior data engineer at a SaaS company. Your manager asks you to build a simple churn prediction model using BigQuery ML. You have a BigQuery table `project.features.user_features` with columns: `user_id`, `logins_last_30d` (INT64), `support_tickets_last_90d` (INT64), `days_since_last_login` (INT64), `plan_type` (STRING), `churned` (INT64 — 0 or 1). Write the SQL to: (1) train a logistic regression model, (2) evaluate it, (3) generate predictions for all current users in a table called `project.features.current_users`.

<details>
<summary>✅ Solution</summary>

**Step 1: Train the model**

```sql
-- Train a logistic regression classifier
CREATE OR REPLACE MODEL `project.ml_models.churn_logreg_v1`
OPTIONS (
  model_type = 'LOGISTIC_REG',
  input_label_cols = ['churned'],
  data_split_method = 'AUTO_SPLIT',  -- BQ auto splits 80/20 train/eval
  auto_class_weights = TRUE,         -- handle class imbalance (churners are usually <10%)
  max_iterations = 20,
  l1_reg = 0.01
)
AS
SELECT
  logins_last_30d,
  support_tickets_last_90d,
  days_since_last_login,
  plan_type,   -- BQML auto one-hot encodes STRING columns
  churned
FROM `project.features.user_features`
WHERE churned IS NOT NULL;  -- exclude rows with unknown labels
```

**Step 2: Evaluate the model**

```sql
-- Evaluate on the held-out eval split (auto-split created this)
SELECT
  precision,
  recall,
  f1_score,
  roc_auc,
  accuracy
FROM ML.EVALUATE(MODEL `project.ml_models.churn_logreg_v1`);

-- More detailed: ROC curve points
SELECT *
FROM ML.ROC_CURVE(MODEL `project.ml_models.churn_logreg_v1`);
```

**Step 3: Generate predictions for current users**

```sql
-- Batch inference on current users
SELECT
  user_id,
  predicted_churned,                                    -- 0 or 1
  predicted_churned_probs[OFFSET(0)].prob AS prob_stay, -- probability of not churning
  predicted_churned_probs[OFFSET(1)].prob AS prob_churn -- probability of churning
FROM ML.PREDICT(
  MODEL `project.ml_models.churn_logreg_v1`,
  (
    SELECT
      user_id,
      logins_last_30d,
      support_tickets_last_90d,
      days_since_last_login,
      plan_type
    FROM `project.features.current_users`
  )
)
ORDER BY prob_churn DESC;

-- Save to a predictions table
CREATE OR REPLACE TABLE `project.predictions.churn_scores_latest` AS
SELECT
  user_id,
  predicted_churned_probs[OFFSET(1)].prob AS churn_probability,
  CURRENT_DATE() AS prediction_date
FROM ML.PREDICT(
  MODEL `project.ml_models.churn_logreg_v1`,
  (SELECT user_id, logins_last_30d, support_tickets_last_90d,
          days_since_last_login, plan_type
   FROM `project.features.current_users`)
);
```

**Key points to mention in an interview:**
- `AUTO_SPLIT` creates a reproducible 80/20 split — no need to manually split your data.
- `auto_class_weights = TRUE` is important when churners are rare (e.g., 5% of users) — without it, the model may predict "no churn" for everyone and still get 95% accuracy.
- `plan_type` being STRING is fine — BQML automatically one-hot encodes categorical features.
- The `predicted_churned_probs` array is ordered by label value: index 0 = prob(churned=0), index 1 = prob(churned=1).

</details>

</article>

<article data-difficulty="mid">

## 🟡 Mid-Level: Diagnosing Feature Store Train-Serve Skew

**Scenario:** A production recommendation model has been running for 6 months. Last month, Vertex AI Model Monitoring triggered alerts for `purchase_count_30d` feature drift (PSI = 0.42, threshold = 0.2). The model's precision dropped from 0.71 to 0.58. You're tasked with investigating and fixing the issue.

The feature pipeline runs daily in Dataflow, computes features from BigQuery, and writes to the Feature Store. Training data was generated 6 months ago using the same pipeline.

Describe your investigation approach, the likely root causes, and how you'd fix the issue while the production model is still running.

<details>
<summary>✅ Solution</summary>

**Investigation Approach:**

**Step 1: Quantify the distribution shift**

```sql
-- Compare feature distributions: training baseline vs. current
WITH training_dist AS (
  SELECT
    APPROX_QUANTILES(purchase_count_30d, 100) AS quantiles,
    AVG(purchase_count_30d) AS mean,
    STDDEV(purchase_count_30d) AS stddev,
    COUNT(*) AS n
  FROM `project.ml.training_data_snapshot`  -- snapshot from 6 months ago
),
current_dist AS (
  SELECT
    APPROX_QUANTILES(purchase_count_30d, 100) AS quantiles,
    AVG(purchase_count_30d) AS mean,
    STDDEV(purchase_count_30d) AS stddev,
    COUNT(*) AS n
  FROM `project.features.current_user_features`
  WHERE feature_date = CURRENT_DATE()
)
SELECT
  t.mean AS train_mean, c.mean AS current_mean,
  (c.mean - t.mean) / t.mean AS mean_drift_pct,
  t.stddev AS train_stddev, c.stddev AS current_stddev
FROM training_dist t, current_dist c;
```

**Step 2: Check for pipeline changes**

```bash
# Review git history for the feature pipeline
git log --since="6 months ago" -- dbt/models/features/user_purchase_features.sql
git diff HEAD~30 -- dbt/models/features/user_purchase_features.sql
```

**Step 3: Check for source data changes**

```sql
-- Did the source orders table schema or distribution change?
SELECT
  DATE_TRUNC(order_date, MONTH) AS month,
  COUNT(*) AS order_count,
  COUNT(DISTINCT user_id) AS unique_buyers,
  AVG(order_amount) AS avg_order
FROM `project.raw.orders`
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
GROUP BY 1
ORDER BY 1;
```

**Likely Root Causes (in order of frequency):**

1. **dbt model change**: someone modified the SQL for `purchase_count_30d` — changed the time window, added/removed filters, changed the join logic. Even a "minor" change like adding `WHERE status = 'completed'` (to exclude returns) would shift the entire distribution.

2. **Upstream source table change**: the orders table started including a new order status, payment type, or product category that was excluded before. The feature value inflated.

3. **Seasonal drift**: 6 months ago = springtime; now = holiday season. Purchase counts genuinely shifted. This is real-world drift, not a bug — but the model needs retraining.

4. **Missing data / NULL handling change**: nulls being treated differently (NULLs → 0 vs. NULLs → excluded) alters the distribution.

**Fix Strategy (without taking the model offline):**

**Immediate**: shadow-run a retrained model alongside the production model. Route 5% of traffic to the new model (A/B test via Vertex AI Endpoints traffic splitting).

```python
# Update endpoint traffic split: 95% old model, 5% retrained model
endpoint = aiplatform.Endpoint("projects/my-project/locations/us-central1/endpoints/churn-endpoint")
endpoint.update(
    traffic_split={
        "old-model-id": 95,
        "retrained-model-id": 5
    }
)
```

**Short-term**: retrain on recent data (last 3 months) with the current feature SQL.

```sql
CREATE OR REPLACE MODEL `project.ml_models.rec_model_v2`
OPTIONS (model_type = 'BOOSTED_TREE_CLASSIFIER', ...)
AS
SELECT *
FROM `project.features.user_features_history`
WHERE feature_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY);
```

**Long-term fixes**:
1. Add automated drift detection to the feature pipeline: compare yesterday's distribution to a rolling 30-day baseline before writing to Feature Store.
2. Set up Data Catalog change notifications: any schema change to source tables triggers a Slack alert to the ML platform team.
3. Tighten Model Monitoring thresholds: PSI 0.2 was too loose; set to 0.1 with weekly review.
4. Store training data snapshots in BigQuery permanently — enables exact training-vs-current comparisons.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing a Feature Platform for 20 ML Models

**Scenario:** You're the ML platform engineer at a fintech with 20 production ML models across 5 teams (fraud, credit risk, customer experience, marketing, operations). Current state:
- No Feature Store — every team computes features independently in their own BigQuery datasets
- 40+ duplicate feature computations across teams (same "user transaction history" logic, computed 8 different ways)
- Training data generated ad-hoc with no point-in-time correctness guarantee
- Fraud model had a data leakage incident: training used future transaction counts, giving 92% accuracy that dropped to 71% in production
- On-call team gets paged 3x/week for stale feature data

Design a Feature Platform that eliminates these problems. Address: Feature Store architecture choice, governance model, pipeline design, point-in-time correctness, migration strategy, and monitoring.

<details>
<summary>✅ Solution</summary>

**Architecture Decision: Feature Store 2.0 (BigQuery-native)**

Given this is a fintech (compliance-heavy, batch-oriented fraud scoring), Feature Store 2.0 is the right choice:
- Batch and near-real-time serving acceptable (fraud scoring is async, not inline)
- Single infrastructure to manage (BigQuery already the data warehouse)
- Audit trail and data residency requirements met natively in BigQuery
- SQL-native feature exploration for compliance teams

**Feature Platform Architecture:**

```
Source Systems (Core Banking, CRM, Events)
    ↓ (Pub/Sub + Dataflow)
Raw Layer: BigQuery `data_raw` dataset
    ↓ (dbt transformations)
Feature Layer: BigQuery `feature_store` dataset
    ├─ user_transaction_features (daily + 4-hourly for fraud)
    ├─ user_credit_features (daily)
    ├─ product_features (daily)
    └─ entity_relationship_features (weekly)
    ↓ (Feature Store 2.0 ingestion)
Vertex AI Feature Registry (metadata, lineage, access control)
    ↓
Consumers: 20 models read from BigQuery via Feature Store offline serving
```

**Governance Model:**

```python
# Feature ownership enforced in Terraform
resource "google_bigquery_table" "user_transaction_features" {
  dataset_id = "feature_store"
  table_id   = "user_transaction_features"

  labels = {
    owner          = "data-platform-team"
    consumers      = "fraud,credit-risk,marketing"
    pii_level      = "tier-2"  # transaction amounts, not identity
    sla            = "4-hourly"
    data_steward   = "compliance-team"
  }
}

# Feature Registry entry with access control
resource "google_vertex_ai_feature_group" "user_transactions" {
  name        = "user_transaction_features"
  project     = var.project_id
  region      = "us-central1"

  big_query {
    big_query_source {
      input_uri = "bq://${var.project_id}.feature_store.user_transaction_features"
    }
    entity_id_columns = ["user_id"]
  }
}
```

**New feature onboarding process**:
1. Data engineer submits PR adding feature SQL to dbt.
2. Feature Registry entry created with `owner`, `consumers`, `pii_level`, `sla` labels.
3. For PII features: Data Steward approval required before PR merge.
4. New consumer teams submit PR updating `consumers` label — creates paper trail.
5. Automated alert if feature dependency graph detects circular dependencies.

**Point-in-Time Correctness: Systematic Solution**

Every feature table has a `feature_as_of_date` column. All training dataset generation uses a shared PIT join utility:

```sql
-- Shared utility view: generate training datasets with PIT correctness
-- (parameterized via BigQuery scripting or dbt variable)
CREATE OR REPLACE VIEW `feature_store.pit_training_view` AS
WITH label_events AS (
  SELECT user_id, event_time AS label_time, label
  FROM `ml_labels.churn_labels`
),
ranked_features AS (
  SELECT
    e.user_id,
    e.label_time,
    e.label,
    f.txn_count_30d,
    f.avg_txn_amount_90d,
    f.days_since_last_txn,
    ROW_NUMBER() OVER (
      PARTITION BY e.user_id, e.label_time
      ORDER BY f.feature_as_of_date DESC
    ) AS rn
  FROM label_events e
  JOIN `feature_store.user_transaction_features` f
    ON e.user_id = f.user_id
    AND f.feature_as_of_date < e.label_time  -- strict PIT: only use past data
)
SELECT user_id, label_time, label, txn_count_30d, avg_txn_amount_90d, days_since_last_txn
FROM ranked_features
WHERE rn = 1;
```

All teams **must** use this view for training data generation — enforced by code review policy and documented as the only approved method.

**Fixing the Fraud Data Leakage Incident**

```sql
-- Post-mortem: what caused 92% → 71% drop?
-- The original training query used a SUBQUERY with no PIT filter:
SELECT
  e.*,
  (SELECT COUNT(*) FROM transactions t WHERE t.user_id = e.user_id) AS total_txns
  -- ^^ This counts ALL transactions, including future ones! Leakage.
FROM fraud_labels e;

-- Correct version with PIT:
SELECT
  e.*,
  (SELECT COUNT(*) FROM transactions t
   WHERE t.user_id = e.user_id
     AND t.txn_date < e.fraud_event_date) AS total_txns_before_event
FROM fraud_labels e;
```

**Prevention**: add a CI check that scans all training SQL for table scans without a timestamp filter — flag any query joining `transactions` without a date condition as a potential leakage risk.

**Pipeline Design: Tiered Freshness**

```python
# Cloud Composer DAG: tiered feature freshness
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("feature_platform", schedule_interval="0 */4 * * *") as dag:

    # Tier 1: 4-hourly for fraud features (high business impact)
    run_fraud_features = BashOperator(
        task_id="fraud_features_4h",
        bash_command="dbt run --select tag:fraud_feature --target prod"
    )

    validate_fraud_features = BashOperator(
        task_id="validate_fraud_features",
        bash_command="great_expectations checkpoint run fraud_feature_suite"
    )

    # Tier 2: daily features (run once, after midnight UTC)
    run_daily_features = BashOperator(
        task_id="daily_features",
        bash_command="dbt run --select tag:daily_feature --target prod",
        # Only run at midnight
        execution_delta=timedelta(hours=4)
    )

    run_fraud_features >> validate_fraud_features
```

**Monitoring: Eliminate 3x/Week On-Call Pages**

Root cause of pages: features not updating, no alerting until a model produced wrong outputs.

```sql
-- Freshness monitoring query (Cloud Monitoring alert)
SELECT
  table_name,
  MAX(feature_as_of_date) AS latest_feature_date,
  CURRENT_DATE() AS today,
  DATE_DIFF(CURRENT_DATE(), MAX(feature_as_of_date), DAY) AS days_stale,
  CASE
    WHEN table_name LIKE '%fraud%' AND DATE_DIFF(CURRENT_DATE(), MAX(feature_as_of_date), DAY) > 0 THEN 'CRITICAL'
    WHEN DATE_DIFF(CURRENT_DATE(), MAX(feature_as_of_date), DAY) > 1 THEN 'WARNING'
    ELSE 'OK'
  END AS status
FROM (
  SELECT 'user_transaction_features' AS table_name, feature_as_of_date FROM `feature_store.user_transaction_features`
  UNION ALL
  SELECT 'user_credit_features', feature_as_of_date FROM `feature_store.user_credit_features`
)
GROUP BY table_name
HAVING status != 'OK';
```

This query runs every 15 minutes via Cloud Scheduler → BigQuery scheduled query → results logged to a monitoring table → Cloud Monitoring reads the table and pages on-call if any row with `status = CRITICAL` appears.

**Migration Strategy (Don't Break 20 Running Models)**

Phase 1 (Month 1-2): Audit. Map all 40+ feature computations → identify duplicates → build canonical feature definitions.

Phase 2 (Month 3-4): Build. Implement Feature Platform alongside existing pipelines. Run both in parallel.

Phase 3 (Month 5-6): Migrate. Team by team, redirect model training/inference to Feature Platform. Validate each team's models produce equivalent results before cutting over.

Phase 4 (Month 7): Decommission. Shut down legacy feature pipelines. Final cost and reliability audit.

**Expected outcomes**: 60% reduction in feature compute cost (eliminating duplicate pipelines), zero data leakage incidents (PIT enforced), on-call pages reduced from 3x/week to <1x/month (freshness monitoring), and 10+ new models can be built in weeks instead of months (features already exist in registry).

</details>

</article>
