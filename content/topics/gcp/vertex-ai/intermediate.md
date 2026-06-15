---
title: "Vertex AI — Intermediate"
topic: gcp
subtopic: vertex-ai
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [gcp, vertex-ai, pipelines, batch-prediction, model-monitoring, interview]
---

# Vertex AI — Intermediate

At the mid-level, you're expected to build and orchestrate ML pipelines, run batch predictions at scale from BigQuery, understand model monitoring for production systems, and know where the data engineering responsibilities end and where the ML engineer's begin. This layer covers Vertex AI Pipelines, Batch Prediction jobs, model monitoring, and Vertex AI Search.

---

## Vertex AI Pipelines: Kubeflow-Based ML Orchestration

Vertex AI Pipelines is a managed service for running ML pipelines defined with the **Kubeflow Pipelines (KFP) SDK** or **TensorFlow Extended (TFX)**. For data engineers, it's conceptually similar to Airflow — a DAG of steps — but designed specifically for ML workloads with built-in artifact tracking, metadata logging, and caching.

### Why Data Engineers Use Pipelines

- **Feature computation steps**: extract raw data from BigQuery → compute features → load to Feature Store.
- **Data validation steps**: validate schema, check for null rates, distribution shifts before training.
- **Training orchestration**: trigger training jobs, evaluate metrics, register models only if metrics pass thresholds.
- **Integration with Cloud Composer**: Airflow DAGs can trigger Vertex AI Pipelines via the `VertexAIPipelineRunOperator`.

### Building a Pipeline with KFP SDK

```python
from kfp import dsl
from kfp.v2 import compiler
from google.cloud import aiplatform

# Define pipeline components as Python functions
@dsl.component(
    base_image="python:3.10",
    packages_to_install=["google-cloud-bigquery", "pandas", "pandas-gbq"]
)
def extract_features_from_bq(
    project_id: str,
    dataset_id: str,
    feature_date: str,
    output_uri: dsl.Output[dsl.Dataset]
):
    """Extract features from BigQuery and save to GCS."""
    from google.cloud import bigquery
    import pandas as pd

    client = bigquery.Client(project=project_id)
    query = f"""
        SELECT user_id, purchase_count_30d, avg_basket_size_90d, churned
        FROM `{project_id}.{dataset_id}.user_features`
        WHERE feature_date = '{feature_date}'
    """
    df = client.query(query).to_dataframe()
    df.to_parquet(output_uri.path, index=False)


@dsl.component(
    base_image="python:3.10",
    packages_to_install=["scikit-learn", "pandas", "google-cloud-aiplatform"]
)
def train_model(
    training_data: dsl.Input[dsl.Dataset],
    model_output: dsl.Output[dsl.Model],
    metrics_output: dsl.Output[dsl.Metrics]
):
    """Train a model and log metrics."""
    import pandas as pd
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.metrics import roc_auc_score
    import pickle

    df = pd.read_parquet(training_data.path)
    X = df.drop("churned", axis=1)
    y = df["churned"]

    model = GradientBoostingClassifier(n_estimators=100, max_depth=4)
    model.fit(X, y)
    auc = roc_auc_score(y, model.predict_proba(X)[:, 1])

    metrics_output.log_metric("train_auc", auc)

    with open(model_output.path, "wb") as f:
        pickle.dump(model, f)


# Compose into a pipeline
@dsl.pipeline(name="churn-training-pipeline")
def churn_pipeline(project_id: str, dataset_id: str, feature_date: str):
    extract_task = extract_features_from_bq(
        project_id=project_id,
        dataset_id=dataset_id,
        feature_date=feature_date
    )
    train_task = train_model(
        training_data=extract_task.outputs["output_uri"]
    )


# Compile and run
compiler.Compiler().compile(
    pipeline_func=churn_pipeline,
    package_path="churn_pipeline.json"
)

aiplatform.init(project="my-project", location="us-central1")
pipeline_job = aiplatform.PipelineJob(
    display_name="churn-training-weekly",
    template_path="churn_pipeline.json",
    parameter_values={
        "project_id": "my-project",
        "dataset_id": "features",
        "feature_date": "2024-01-07"
    }
)
pipeline_job.submit()
```

### Pipeline Caching

KFP caches component outputs by fingerprinting inputs. If you re-run a pipeline with the same inputs, cached steps are skipped — useful for debugging downstream steps without re-running expensive upstream computation.

```python
# Disable caching for a specific component (e.g., time-sensitive data extraction)
extract_task.set_caching_options(enable_caching=False)
```

---

## Vertex AI Datasets: Managed Tabular Datasets

Vertex AI Datasets wrap data sources (BigQuery tables, GCS files) with metadata for use in AutoML or custom training. For data engineers, the key action is creating a dataset that points to a BigQuery table:

```python
dataset = aiplatform.TabularDataset.create(
    display_name="churn_training_dataset",
    bq_source="bq://my-project.features.user_training_data"
)
print(f"Dataset resource name: {dataset.resource_name}")
# Use dataset.resource_name in training job configs
```

The dataset tracks data lineage — which model versions were trained on which data snapshot. This matters for reproducibility audits and MLOps maturity.

---

## Batch Prediction Jobs from BigQuery

Vertex AI Batch Prediction runs inference on a large dataset without deploying a persistent endpoint. The input can be BigQuery tables; output goes to BigQuery or GCS. This is the data engineer's primary ML integration pattern.

```python
batch_prediction_job = aiplatform.BatchPredictionJob.create(
    job_display_name="churn-batch-inference-weekly",
    model_name="projects/my-project/locations/us-central1/models/churn-model@3",
    instances_format="bigquery",
    predictions_format="bigquery",
    bigquery_source="bq://my-project.features.current_user_features",
    bigquery_destination_prefix="bq://my-project.predictions",
    machine_type="n1-standard-4",
    starting_replica_count=5,
    max_replica_count=20,  # autoscale
    generate_explanation=True  # Shapley values for feature importance
)

batch_prediction_job.wait()
print(f"Job state: {batch_prediction_job.state}")
```

**Key parameters for interviews:**
- `starting_replica_count` / `max_replica_count`: autoscaling for the prediction fleet.
- `generate_explanation`: enables Explainable AI (Shapley values) — useful for regulated industries.
- Output lands in BigQuery at `bq://my-project.predictions.churn-batch-inference-weekly_*`.

**Orchestration pattern**: batch prediction jobs are typically triggered by Airflow/Composer after the feature pipeline completes, then write predictions back to BigQuery where downstream analytics/dashboards read them.

```python
# Airflow operator for Vertex AI Batch Prediction
from airflow.providers.google.cloud.operators.vertex_ai.batch_prediction_job import (
    CreateBatchPredictionJobOperator
)

run_batch_prediction = CreateBatchPredictionJobOperator(
    task_id="vertex_batch_prediction",
    project_id="my-project",
    region="us-central1",
    job_display_name="churn-weekly-{{ ds }}",
    model_name="churn-model",
    instances_format="bigquery",
    predictions_format="bigquery",
    bigquery_source="bq://my-project.features.current_user_features",
    bigquery_destination_prefix="bq://my-project.predictions"
)
```

---

## Model Monitoring for Drift Detection

Vertex AI Model Monitoring detects **training-serving skew** (distribution difference between training data and live inputs) and **prediction drift** (input distribution changing over time in production). For data engineers, this is critical because drift often indicates upstream data pipeline changes.

### Types of Monitoring

| Monitor Type | What It Detects | Data Engineer Relevance |
|---|---|---|
| Training-serving skew | Input features differ from training distribution | Upstream pipeline changed a feature computation |
| Prediction drift | Input distribution changes over time | New user cohort, seasonality, data source issue |
| Feature attribution drift | Shapley-based feature importance shifts | Model no longer using expected features |

```python
# Set up monitoring on a deployed endpoint
monitoring_config = aiplatform.ModelMonitoringConfig(
    alert_config=aiplatform.ModelMonitoringAlertConfig(
        email_alert_config=aiplatform.ModelMonitoringAlertConfig.EmailAlertConfig(
            user_emails=["data-platform@company.com"]
        )
    ),
    objective_configs=[
        aiplatform.ModelDeploymentMonitoringObjectiveConfig(
            deployed_model_id="churn-model-v3",
            objective_config=aiplatform.ModelMonitoringObjectiveConfig(
                training_dataset=aiplatform.ModelMonitoringObjectiveConfig.TrainingDataset(
                    bigquery_source=aiplatform.BigQuerySource(
                        input_uri="bq://my-project.features.user_training_data"
                    ),
                    target_field="churned"
                ),
                training_prediction_skew_detection_config=aiplatform.ModelMonitoringObjectiveConfig.TrainingPredictionSkewDetectionConfig(
                    skew_thresholds={
                        "purchase_count_30d": aiplatform.ThresholdConfig(value=0.3),
                        "avg_basket_size_90d": aiplatform.ThresholdConfig(value=0.3)
                    }
                )
            )
        )
    ],
    logging_sampling_strategy=aiplatform.SamplingStrategy(
        random_sample_config=aiplatform.SamplingStrategy.RandomSampleConfig(
            sample_rate=0.1  # sample 10% of production traffic
        )
    ),
    monitor_interval=Duration(seconds=3600)
)
```

**Data engineer alert**: when monitoring triggers a skew alert, the investigation starts with the data pipeline, not the model. Common causes:
- A feature SQL query changed (different computation window, join logic).
- A source table schema changed (NULL rates increased, new categories appeared).
- An upstream system started sending data in a different timezone or format.

---

## Vertex AI Search and Conversation (RAG Use Cases)

Vertex AI Search (formerly Enterprise Search) enables data engineers to build search and retrieval-augmented generation (RAG) systems over structured and unstructured data.

**Data engineering role**:
- Indexing: ingest documents (PDFs, HTML, structured data from BigQuery) into a Vertex AI Search datastore.
- Grounding: connecting Gemini model responses to verified data sources via search retrieval.
- Chunking and embedding: structuring documents for optimal retrieval.

```python
from google.cloud import discoveryengine_v1beta as discoveryengine

# Create a data store and ingest BigQuery data
client = discoveryengine.DocumentServiceClient()
parent = f"projects/my-project/locations/global/collections/default_collection/dataStores/my-datastore/branches/default_branch"

# Ingest from BigQuery
import_config = discoveryengine.ImportDocumentsRequest(
    parent=parent,
    bigquery_source=discoveryengine.BigQuerySource(
        project_id="my-project",
        dataset_id="knowledge_base",
        table_id="product_descriptions",
        data_schema="document"
    ),
    reconciliation_mode=discoveryengine.ImportDocumentsRequest.ReconciliationMode.INCREMENTAL
)
```

For data engineers, the primary concern is keeping the search index fresh — typically via daily or incremental BigQuery exports triggered by Airflow/Composer.
