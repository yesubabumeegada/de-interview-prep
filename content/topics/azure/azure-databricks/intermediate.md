---
title: "Azure Databricks — Intermediate"
topic: azure
subtopic: azure-databricks
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [azure, databricks, delta-live-tables, structured-streaming, mlflow, job-orchestration]
---

# Azure Databricks — Intermediate

## Delta Live Tables (DLT)

```python
# Delta Live Tables: declarative ETL framework
# Define what tables should look like; DLT manages execution, data quality, lineage

import dlt
from pyspark.sql import functions as F

# 1. Bronze layer: raw ingestion (streaming from ADLS)
@dlt.table(
    name="bronze_orders",
    comment="Raw orders ingested from Event Hubs / ADLS landing zone",
    table_properties={"quality": "bronze"}
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")            # Auto Loader
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", "/checkpoints/bronze_orders/schema")
        .load("abfss://bronze@account.dfs.core.windows.net/orders/")
    )

# 2. Silver layer: cleansed with expectations (data quality)
@dlt.expect_all_or_drop({
    "valid_order_id":    "order_id IS NOT NULL",
    "valid_customer_id": "customer_id > 0",
    "positive_amount":   "amount > 0",
    "recent_date":       "order_date >= '2020-01-01'"
})
@dlt.table(
    name="silver_orders",
    comment="Cleansed and validated orders",
    partition_cols=["order_date"],
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
)
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .select(
            F.col("order_id").cast("bigint"),
            F.col("customer_id").cast("int"),
            F.col("amount").cast("decimal(18,2)"),
            F.to_date("order_date").alias("order_date"),
            F.upper(F.trim("region")).alias("region"),
            F.current_timestamp().alias("processed_at")
        )
    )

# 3. Gold layer: aggregated business metric
@dlt.table(
    name="gold_daily_revenue",
    comment="Daily revenue aggregated by region",
    table_properties={"quality": "gold"}
)
def gold_daily_revenue():
    return (
        dlt.read("silver_orders")        # batch read (no stream needed for Gold)
        .groupBy("order_date", "region")
        .agg(
            F.sum("amount").alias("total_revenue"),
            F.count("*").alias("order_count"),
            F.avg("amount").alias("avg_order_value")
        )
    )

# DLT automatically:
#   - Creates and manages Delta tables
#   - Tracks lineage between tables
#   - Monitors data quality expectations
#   - Handles schema evolution
#   - Restarts failed streams
#   - Provides a visual DAG in Databricks UI
```

---

## Auto Loader (cloudFiles)

```python
# Auto Loader: efficient incremental ingestion from ADLS Gen2
# Tracks new files automatically (no manual watermarking needed)

# Basic Auto Loader streaming ingestion
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", "abfss://checkpoints@account.dfs.core.windows.net/orders/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    # Performance options:
    .option("cloudFiles.maxFilesPerTrigger", "1000")  # process 1000 new files per trigger
    .option("cloudFiles.maxBytesPerTrigger", "10g")   # or max 10GB per trigger
    .load("abfss://bronze@account.dfs.core.windows.net/orders/")
)

# Write to Delta Silver with checkpointing
(
    df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "abfss://checkpoints@account.dfs.core.windows.net/orders/checkpoint")
    .option("mergeSchema", "true")  # handle schema evolution
    .trigger(processingTime="5 minutes")
    .table("silver.orders_raw")
)

# Auto Loader file tracking:
# Default: uses Azure Event Grid notifications (real-time, no listing overhead)
# Fallback: file listing mode (S3-style prefix listing, less efficient)
# For ADLS Gen2: Event Grid mode is automatic — set up is managed by Auto Loader

# Benefits over manual file tracking:
#   No missed files (tracked by file notification system)
#   No duplicate processing (checkpoint prevents re-processing)
#   Handles out-of-order file arrivals
#   Schema inference + evolution built-in
#   Dead letter handling: malformed files logged, don't stop the stream
```

---

## MLflow Integration

```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import roc_auc_score
import pandas as pd

# MLflow: experiment tracking, model registry, serving
# Built into Databricks — no setup needed, experiments tied to notebook

# Experiment tracking
mlflow.set_experiment("/Users/engineer@company.com/fraud_detection")

with mlflow.start_run(run_name="GBM_v1"):
    # Log parameters
    params = {
        "n_estimators": 200,
        "learning_rate": 0.1,
        "max_depth": 4
    }
    mlflow.log_params(params)
    
    # Train model
    clf = GradientBoostingClassifier(**params)
    clf.fit(X_train, y_train)
    
    # Log metrics
    auc = roc_auc_score(y_test, clf.predict_proba(X_test)[:, 1])
    mlflow.log_metric("auc_roc", auc)
    mlflow.log_metric("training_samples", len(X_train))
    
    # Log model
    mlflow.sklearn.log_model(
        clf,
        artifact_path="model",
        registered_model_name="fraud_detection_gbm",
        input_example=X_train.head(5)
    )
    
    print(f"Run ID: {mlflow.active_run().info.run_id}, AUC: {auc:.4f}")

# Model registry: promote through stages
client = mlflow.MlflowClient()
# Move to Staging after validation
client.transition_model_version_stage(
    name="fraud_detection_gbm",
    version=3,
    stage="Production",
    archive_existing_versions=True
)

# Load Production model for scoring
model = mlflow.sklearn.load_model("models:/fraud_detection_gbm/Production")
predictions = model.predict(X_new)
```

---

## Databricks Workflows (Job Orchestration)

```python
# Databricks Workflows: orchestrate multi-task pipelines natively
# Define via UI or JSON API or Terraform

# Job JSON definition (via Databricks API):
job_definition = {
    "name": "daily_medallion_pipeline",
    "schedule": {
        "quartz_cron_expression": "0 0 2 * * ?",  # 2 AM daily
        "timezone_id": "UTC",
        "pause_status": "UNPAUSED"
    },
    "tasks": [
        {
            "task_key": "ingest_bronze",
            "description": "Ingest raw data from ADLS landing zone",
            "notebook_task": {
                "notebook_path": "/Shared/pipelines/01_ingest_bronze",
                "base_parameters": {"run_date": "{{job.start_time.iso_date}}"}
            },
            "job_cluster_key": "etl_cluster",
            "libraries": [{"pypi": {"package": "delta-spark==2.4.0"}}]
        },
        {
            "task_key": "transform_silver",
            "depends_on": [{"task_key": "ingest_bronze"}],
            "notebook_task": {
                "notebook_path": "/Shared/pipelines/02_transform_silver",
                "base_parameters": {"run_date": "{{job.start_time.iso_date}}"}
            },
            "job_cluster_key": "etl_cluster"
        },
        {
            "task_key": "aggregate_gold",
            "depends_on": [{"task_key": "transform_silver"}],
            "notebook_task": {
                "notebook_path": "/Shared/pipelines/03_aggregate_gold"
            },
            "job_cluster_key": "etl_cluster"
        }
    ],
    "job_clusters": [{
        "job_cluster_key": "etl_cluster",
        "new_cluster": {
            "spark_version": "13.3.x-scala2.12",
            "node_type_id": "Standard_DS4_v2",
            "autoscale": {"min_workers": 2, "max_workers": 8},
            "azure_attributes": {"availability": "SPOT_WITH_FALLBACK_AZURE"}
        }
    }],
    "max_retries": 2,
    "retry_on_timeout": True,
    "email_notifications": {
        "on_failure": ["team@company.com"],
        "on_success": []
    }
}
```

---

## Interview Tips

> **Tip 1:** "What is Delta Live Tables and when would you use it over regular Spark notebooks?" — DLT is a declarative ETL framework: you define WHAT the tables should contain (using Python decorators or SQL), and DLT handles HOW to run them (dependency order, incremental processing, restart on failure). Key advantages: (a) built-in data quality expectations (`@dlt.expect_all_or_drop`) with automatic quarantine, (b) automatic lineage tracking visible in UI, (c) handles incremental updates without manual CDC logic, (d) integrates with Unity Catalog for governance. Use DLT for: new production pipelines where you want managed orchestration + data quality. Use raw notebooks for: one-off analysis, existing complex pipelines, non-standard transformations DLT doesn't support.

> **Tip 2:** "How does Auto Loader differ from standard Spark readStream from ADLS?" — Standard `readStream` from ADLS requires manually listing new files (expensive for millions of files). Auto Loader uses Azure Event Grid notifications: when a new file lands in ADLS, Event Grid fires an event, Auto Loader receives it and queues the file for processing — zero listing overhead. Auto Loader also handles: schema inference and evolution, bad file handling, checkpoint management. For production ingestion from ADLS: always use Auto Loader (cloudFiles format) over manual readStream listing.

> **Tip 3:** "What's the cost breakdown of Azure Databricks?" — Total cost = VM cost (Azure charges for VMs) + DBU license (Databricks charges per DBU-hour). Example: Standard_DS4_v2 worker = $0.22/hour (Azure VM) + 0.75 DBU × $0.07 = $0.27/hour total. For 10 workers, 8 hours: $21.6/day. Optimizations: Spot VMs (70% cheaper, add fallback), cluster pools (reduce cold start, reuse VMs), right-size clusters (don't over-provision), auto-terminate All-Purpose clusters (30-min TTL), use Job clusters for production (cheaper DBU rate).
