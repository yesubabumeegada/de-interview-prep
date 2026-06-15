---
title: "Vertex AI — Real-World Patterns"
topic: gcp
subtopic: vertex-ai
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [gcp, vertex-ai, mlops, production, patterns, interview]
---

# Vertex AI — Real-World Patterns

These are the patterns, failure modes, and integration designs data engineers encounter when actually running ML infrastructure in production on GCP. Each section reflects common engineering decisions and war stories from real ML platform work.

---

## Pattern 1: The Weekly Churn Prediction Pipeline

**Scenario**: A subscription business needs weekly churn scores for 5M users, written back to BigQuery for the CRM team to trigger retention campaigns.

**End-to-end architecture**:

```
Sunday 11pm: Cloud Composer DAG triggers
    Step 1: dbt run → compute feature SQL → materialize to BigQuery
    Step 2: Vertex AI Pipeline triggered via REST API
        Component A: validate feature data (Great Expectations)
        Component B: run Vertex AI Batch Prediction (reads BQ, writes BQ)
        Component C: evaluate prediction distribution (sanity checks)
        Component D: write status to monitoring table
    Step 3: Composer waits for pipeline success → send Slack notification
    Monday 8am: CRM team's Looker dashboard shows fresh churn scores
```

```python
# Composer DAG: trigger Vertex AI pipeline and wait
from airflow import DAG
from airflow.providers.google.cloud.operators.vertex_ai.pipeline_job import (
    RunPipelineJobOperator
)
from datetime import datetime

with DAG("churn_prediction_weekly", schedule_interval="0 23 * * 0") as dag:
    run_pipeline = RunPipelineJobOperator(
        task_id="run_churn_pipeline",
        project_id="my-project",
        region="us-central1",
        display_name="churn-weekly-{{ ds }}",
        template_path="gs://my-pipelines/churn/v2.3/pipeline.json",
        parameter_values={
            "project_id": "my-project",
            "feature_date": "{{ ds }}",
            "output_table": "my-project.predictions.churn_{{ ds_nodash }}"
        },
        failure_policy="PIPELINE_FAILURE_POLICY_FAIL_SLOW"
    )
```

**Production lesson learned**: the first version wrote predictions to a date-suffixed table (`churn_20240107`). CRM had to update their Looker dashboard SQL every week. Fixed by writing to a static table with an `upsert` pattern and adding `prediction_date` as a column.

---

## Pattern 2: Train-Serve Skew Incident

**Incident**: A recommendation model's click-through rate dropped 35% over two weeks. Model Monitoring didn't alert because the monitoring threshold was set too loose (30% PSI threshold vs. the 15% that was actually occurring).

**Root cause investigation**:

```sql
-- Compare training feature distribution vs. current serving distribution
WITH training_stats AS (
  SELECT
    AVG(purchase_count_30d) AS avg_purchase_30d,
    STDDEV(purchase_count_30d) AS std_purchase_30d,
    APPROX_QUANTILES(purchase_count_30d, 100)[OFFSET(50)] AS median_purchase_30d
  FROM `project.ml.training_data_v3`
  WHERE training_date = '2023-10-01'
),
current_stats AS (
  SELECT
    AVG(purchase_count_30d) AS avg_purchase_30d,
    STDDEV(purchase_count_30d) AS std_purchase_30d,
    APPROX_QUANTILES(purchase_count_30d, 100)[OFFSET(50)] AS median_purchase_30d
  FROM `project.features.current_user_features`
  WHERE feature_date = CURRENT_DATE()
)
SELECT
  t.avg_purchase_30d AS train_avg,
  c.avg_purchase_30d AS current_avg,
  (c.avg_purchase_30d - t.avg_purchase_30d) / t.avg_purchase_30d AS relative_drift
FROM training_stats t, current_stats c;
```

**Root cause found**: A dbt model change 3 weeks earlier had changed the purchase count window from `30 days` to `calendar month`. For December, this meant Christmas purchase spikes were included in December features but not January features — the distribution shifted dramatically at month-end.

**Fix implemented**:
1. Tightened Model Monitoring PSI threshold to 10%.
2. Added data contract tests in dbt: any change to feature SQL triggers a notification to the ML platform team.
3. Added a post-feature-compute validation step in the Vertex AI pipeline that checks feature distribution stats against stored baselines and fails the pipeline if drift exceeds threshold.

---

## Pattern 3: Feature Store Staleness Handling

**Scenario**: The Feature Store has features computed daily (purchase history, login patterns). But a batch prediction job runs every 4 hours for fraud detection. The fraud model reads stale features from the Feature Store until the daily refresh.

**Solutions evaluated**:

Option A: **Increase refresh frequency to 4-hourly**
- Cost: 6x more compute in the feature pipeline.
- Complexity: incremental compute vs. full refresh.
- Chosen for high-value features (login anomaly score).

Option B: **Real-time feature computation at serving time**
- For fraud, some features are computed on-the-fly at inference time from raw event streams.
- Uses Dataflow to compute a "transaction velocity" feature from the last 1 hour of Pub/Sub events.
- Written to Feature Store online store (Bigtable) every minute.

```python
# Dataflow pipeline: real-time feature computation → Feature Store online store
import apache_beam as beam
from apache_beam.transforms.window import SlidingWindows

with beam.Pipeline(options=streaming_options) as p:
    (
        p
        | "ReadPubSub" >> beam.io.ReadFromPubSub(topic="projects/my-project/topics/transactions")
        | "ParseJSON" >> beam.Map(json.loads)
        | "Window" >> beam.WindowInto(SlidingWindows(size=3600, period=60))  # 1hr window, 1min period
        | "KeyByUser" >> beam.Map(lambda x: (x["user_id"], x["amount"]))
        | "SumByUser" >> beam.CombinePerKey(sum)
        | "WriteToFeatureStore" >> beam.ParDo(WriteToFeatureStoreDoFn())
    )
```

Option C: **Accept staleness, retrain more frequently**
- For lower-risk fraud tiers, accepted that features could be up to 24 hours stale.
- Model retrained weekly (vs. monthly) to adapt to recent patterns.

**Production decision**: tiered approach — Tier 1 fraud (high-value transactions) uses real-time Dataflow features; Tier 2 (low-value) uses daily batch features.

---

## Pattern 4: Multi-Model Feature Sharing

**Scenario**: 8 different ML models across 3 teams all need "user engagement features" (purchase count, login frequency, session duration). Without Feature Store, each team computes these independently — 8 slightly different SQL queries, 8 different computation schedules.

**Architecture after Feature Store adoption**:

```
Feature Pipelines (owned by Data Platform team):
    - user_engagement_features (daily, BigQuery → Feature Store)
    - user_transaction_features (daily)
    - product_features (hourly)

Consumer models:
    - Churn model: uses user_engagement_features + user_transaction_features
    - Recommendation model: uses user_engagement_features + product_features
    - Fraud model: uses user_transaction_features (real-time)
    - LTV model: uses all three feature groups
```

**Governance enforcement**:

```python
# Feature Registry entry with approval required for new consumers
feature = aiplatform.Feature.get("user_engagement_features/purchase_count_30d")
feature.update(
    labels={
        "owner": "data-platform",
        "consumers": "churn,recommendations,ltv",  # tracked in registry
        "sla": "daily-by-6am-utc",
        "approved_consumers": "churn-team,recommendations-team,ltv-team"
    }
)
```

Any new team wanting to use the feature must submit a PR updating the `approved_consumers` label — creating a paper trail and ensuring the feature platform team knows about new dependencies before adding compute load.

---

## Pattern 5: BigQuery ML in Production

**Scenario**: A small team (2 data engineers, no dedicated ML engineer) needs a churn model. Decision: use BQML to avoid managing a separate training infrastructure.

```sql
-- Weekly retrain via scheduled query (Cloud Scheduler → BigQuery)
CREATE OR REPLACE MODEL `project.ml_models.churn_v_latest`
OPTIONS (
  model_type = 'BOOSTED_TREE_CLASSIFIER',
  num_parallel_tree = 1,
  max_iterations = 50,
  learn_rate = 0.1,
  input_label_cols = ['churned'],
  data_split_method = 'SEQ',
  data_split_eval_fraction = 0.2,
  data_split_col = 'training_date',  -- train on older, evaluate on newer
  enable_global_explain = TRUE
)
AS
SELECT
  purchase_count_30d,
  login_count_7d,
  avg_basket_size_90d,
  days_since_last_purchase,
  customer_segment,
  churned
FROM `project.features.user_training_data`
WHERE training_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY);
```

**Production wrinkle**: BQML model training is atomic (CREATE OR REPLACE) — the model is unavailable during training. For a 30-minute training job, there's a 30-minute window where the model can't serve batch predictions. Fix: train to a temp model name, evaluate, then rename via `ALTER MODEL`.

```sql
-- Atomic model swap pattern
CREATE OR REPLACE MODEL `project.ml_models.churn_challenger` OPTIONS (...) AS SELECT ...;

-- Evaluate challenger
-- If AUC > threshold, promote:
-- BQ doesn't have a RENAME MODEL, so use BigQuery copy:
-- bq cp project:ml_models.churn_challenger project:ml_models.churn_champion
```

---

## Key Anti-Patterns to Avoid

| Anti-Pattern | Problem | Fix |
|---|---|---|
| Different SQL for train vs. serve | Train-serve skew almost guaranteed | Single SQL view used for both |
| No point-in-time join in training data | Data leakage → inflated training metrics | Implement PIT join from feature history |
| Feature Store with no staleness monitoring | Silent staleness degrades model quality | Alert when features exceed SLA age |
| Batch prediction results in date-suffixed tables | Downstream consumers break weekly | Write to static table with `prediction_date` column |
| BQML training during prediction window | Model unavailable during training | Train challenger model, then promote atomically |
| All models sharing one Feature Store ingestion pipeline | One failure blocks all models | Separate pipeline per feature group with independent SLAs |
