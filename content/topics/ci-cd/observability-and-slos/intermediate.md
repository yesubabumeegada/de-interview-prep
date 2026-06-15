---
title: "Observability and SLOs for Data Pipelines - Intermediate"
topic: ci-cd
subtopic: observability-and-slos
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [ci-cd, observability, prometheus, grafana, airflow-metrics, spark-metrics, data-quality, anomaly-detection]
---

# Observability and SLOs for Data Pipelines — Intermediate

## Prometheus + Grafana for Airflow Metrics

Prometheus is an open-source metrics collection system that scrapes metrics from targets at regular intervals. Grafana is the visualization layer. Together, they're the most common open-source observability stack for Airflow and Spark in production.

### Airflow StatsD → Prometheus

Airflow natively emits metrics via StatsD. The typical production path is:

```
Airflow → StatsD → StatsD Exporter → Prometheus → Grafana
```

```yaml
# airflow.cfg (or environment variables)
[metrics]
statsd_on = True
statsd_host = localhost
statsd_port = 8125
statsd_prefix = airflow

# Key Airflow StatsD metrics:
# airflow.dag.task_duration.{dag_id}.{task_id}   — task execution time
# airflow.dag_processing.processes                — number of DAG parsing processes
# airflow.executor.running_tasks                  — tasks currently running
# airflow.executor.queued_tasks                   — tasks waiting in queue
# airflow.dag.{dag_id}.{task_id}.success          — task success count
# airflow.dag.{dag_id}.{task_id}.failed           — task failure count
```

### Prometheus Scrape Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 30s

scrape_configs:
  - job_name: 'airflow'
    static_configs:
      - targets: ['airflow-statsd-exporter:9102']

  - job_name: 'spark'
    static_configs:
      - targets: ['spark-master:4040']
    metrics_path: '/metrics/prometheus'
```

### Key Grafana Panels for Data Pipeline Monitoring

```promql
# Task success rate (last 24h) — core SLO metric
sum(rate(airflow_dag_task_success[24h])) /
(sum(rate(airflow_dag_task_success[24h])) + sum(rate(airflow_dag_task_failed[24h])))

# Average DAG run duration by dag_id
avg by (dag_id) (airflow_dag_run_duration_success)

# Tasks in queue (alert if > 50 for > 5 minutes — potential executor bottleneck)
airflow_executor_queued_tasks

# DAG failure rate — alert if any DAG fails more than 2x in 1 hour
sum by (dag_id) (rate(airflow_dag_task_failed[1h])) > 2

# Spark: executor memory used
spark_executor_used_on_heap_memory / spark_executor_max_on_heap_memory
```

## Data Volume Anomaly Detection

Volume anomalies are one of the most valuable data quality signals. A table that usually receives 100,000 rows/day and suddenly receives 50,000 is almost certainly broken, even if no pipeline task failed.

### Simple Statistical Anomaly Detection

```python
# scripts/detect_volume_anomaly.py
import boto3
import snowflake.connector
import numpy as np
from datetime import datetime, timedelta

def detect_volume_anomaly(table_name: str, date_column: str, threshold_stds: float = 3.0):
    """
    Compare today's row count to a 30-day rolling average.
    Alert if today's count is more than threshold_stds standard deviations from average.
    """
    conn = snowflake.connector.connect(**get_snowflake_creds())

    # Get 30 days of historical row counts
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT
            DATE({date_column}) as partition_date,
            COUNT(*) as row_count
        FROM {table_name}
        WHERE {date_column} >= DATEADD('day', -31, CURRENT_DATE())
          AND {date_column} < CURRENT_DATE()  -- Exclude today
        GROUP BY 1
        ORDER BY 1
    """)

    historical = cursor.fetchall()
    counts = [row[1] for row in historical]

    mean = np.mean(counts)
    std = np.std(counts)

    # Get today's count
    cursor.execute(f"""
        SELECT COUNT(*)
        FROM {table_name}
        WHERE DATE({date_column}) = CURRENT_DATE()
    """)
    today_count = cursor.fetchone()[0]

    # Z-score: how many standard deviations from mean?
    z_score = (today_count - mean) / std if std > 0 else 0

    result = {
        'table': table_name,
        'today_count': today_count,
        'historical_mean': mean,
        'historical_std': std,
        'z_score': z_score,
        'is_anomaly': abs(z_score) > threshold_stds,
        'direction': 'low' if z_score < 0 else 'high',
    }

    if result['is_anomaly']:
        severity = 'critical' if abs(z_score) > 5 else 'warning'
        send_slack_alert(
            f"[{severity.upper()}] Volume anomaly in {table_name}: "
            f"{today_count:,} rows (expected ~{mean:,.0f} ± {std:,.0f}). "
            f"Z-score: {z_score:.1f}"
        )

    return result
```

### Airflow Task for Daily Volume Check

```python
from airflow.operators.python import PythonOperator

# Add to your daily dbt run DAG
volume_check = PythonOperator(
    task_id='check_volume_anomalies',
    python_callable=lambda: [
        detect_volume_anomaly('analytics.fct_orders', 'order_created_at'),
        detect_volume_anomaly('analytics.fct_events', 'event_timestamp'),
        detect_volume_anomaly('analytics.dim_customers', 'updated_at'),
    ],
    dag=dag,
)

# Run volume checks AFTER dbt build
dbt_run >> volume_check
```

## Pipeline Latency Tracking

Latency — the time from a source event to data availability for consumers — is the SLI for freshness SLOs. Track it at each stage of your pipeline:

```python
# Instrument your ETL code with latency tracking
import time
import boto3
from datetime import datetime

cloudwatch = boto3.client('cloudwatch')

def track_pipeline_stage(stage_name: str, pipeline_id: str):
    """Decorator to track stage latency in CloudWatch."""
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration_ms = (time.time() - start_time) * 1000
                status = 'success'
                return result
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                status = 'failure'
                raise
            finally:
                cloudwatch.put_metric_data(
                    Namespace='DataPlatform/Pipelines',
                    MetricData=[
                        {
                            'MetricName': 'StageDurationMs',
                            'Dimensions': [
                                {'Name': 'PipelineId', 'Value': pipeline_id},
                                {'Name': 'StageName', 'Value': stage_name},
                                {'Name': 'Status', 'Value': status},
                            ],
                            'Value': duration_ms,
                            'Unit': 'Milliseconds',
                            'Timestamp': datetime.utcnow(),
                        }
                    ]
                )
        return wrapper
    return decorator

# Usage
@track_pipeline_stage('extract_from_api', 'orders_pipeline')
def extract_orders(date: str):
    # ... extraction logic ...
    pass

@track_pipeline_stage('load_to_s3', 'orders_pipeline')
def load_to_s3(df, date: str):
    # ... load logic ...
    pass
```

### Latency Breakdown Example

For an end-to-end pipeline with a freshness SLO of < 4 hours, budget each stage:

```
Order placed (source)
  ↓  [~5 min] CDC / streaming ingestion to Kafka
Kafka topic
  ↓  [~10 min] Kafka Connect → S3 (raw zone)
S3 raw zone
  ↓  [~30 min] Glue/Spark ETL transformation
S3 curated zone
  ↓  [~15 min] Snowflake COPY INTO
Snowflake staging table
  ↓  [~20 min] dbt transformation run
Snowflake fct_orders (consumer-facing)
  ↓  [~5 min] BI tool cache refresh
Dashboard

Total: ~85 minutes (well within 4-hour SLO)
Budget remaining: ~155 minutes (error budget for incidents)
```

## OpenTelemetry for Data Pipelines

OpenTelemetry (OTel) is the emerging standard for instrumentation. It provides a vendor-neutral SDK for collecting metrics, logs, and traces that can be exported to any backend (Datadog, Jaeger, Tempo, Grafana):

```python
from opentelemetry import trace, metrics
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import BatchSpanProcessor

# Initialize tracing
provider = TracerProvider()
exporter = OTLPSpanExporter(endpoint="http://otel-collector:4317")
provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)

tracer = trace.get_tracer("data-pipeline")
meter = metrics.get_meter("data-pipeline")

# Create metrics instruments
row_counter = meter.create_counter(
    "pipeline.rows_processed",
    unit="rows",
    description="Total rows processed by the pipeline"
)

pipeline_duration = meter.create_histogram(
    "pipeline.duration_seconds",
    unit="seconds",
    description="Pipeline stage duration"
)

# Instrument your pipeline with traces and metrics
def run_etl_pipeline(date: str, pipeline_id: str):
    with tracer.start_as_current_span("etl_pipeline") as span:
        span.set_attribute("pipeline.id", pipeline_id)
        span.set_attribute("pipeline.date", date)

        with tracer.start_as_current_span("extract") as extract_span:
            start = time.time()
            df = extract_source_data(date)
            duration = time.time() - start

            extract_span.set_attribute("rows.extracted", len(df))
            pipeline_duration.record(duration, {"stage": "extract", "pipeline": pipeline_id})
            row_counter.add(len(df), {"stage": "extract", "pipeline": pipeline_id})

        with tracer.start_as_current_span("transform") as transform_span:
            start = time.time()
            df_transformed = apply_transformations(df)
            duration = time.time() - start

            transform_span.set_attribute("rows.output", len(df_transformed))
            pipeline_duration.record(duration, {"stage": "transform", "pipeline": pipeline_id})
```

## dbt Artifacts for Observability

dbt generates rich artifacts that are goldmines for pipeline observability:

```python
# Parse dbt run_results.json for post-run monitoring
import json

def analyze_dbt_run(run_results_path: str):
    with open(run_results_path) as f:
        results = json.load(f)

    for result in results['results']:
        model_name = result['unique_id'].split('.')[-1]
        status = result['status']
        execution_time = result['execution_time']
        rows_affected = result.get('adapter_response', {}).get('rows_affected', 0)

        # Send to CloudWatch
        cloudwatch.put_metric_data(
            Namespace='DataPlatform/dbt',
            MetricData=[
                {
                    'MetricName': 'ModelDurationSeconds',
                    'Dimensions': [
                        {'Name': 'ModelName', 'Value': model_name},
                        {'Name': 'Status', 'Value': status},
                    ],
                    'Value': execution_time,
                    'Unit': 'Seconds',
                },
                {
                    'MetricName': 'RowsAffected',
                    'Dimensions': [{'Name': 'ModelName', 'Value': model_name}],
                    'Value': rows_affected,
                    'Unit': 'Count',
                }
            ]
        )

        # Alert on slow models
        if execution_time > 300:  # > 5 minutes
            send_slack_alert(
                f"Slow dbt model: {model_name} took {execution_time:.0f}s "
                f"(threshold: 300s). Investigate query performance."
            )
```

## Key Interview Takeaways

- **Prometheus + Grafana** is the standard open-source monitoring stack for Airflow; know the key metrics (task duration, failure rate, queue depth)
- **Volume anomaly detection** using Z-scores catches data problems that don't cause pipeline failures — essential for production data quality
- **Pipeline latency tracking** at each stage reveals bottlenecks and tells you where your 4-hour SLO budget is being spent
- **OpenTelemetry** is the emerging standard for vendor-neutral instrumentation — supports metrics, logs, and traces from a single SDK
- **dbt artifacts** (`run_results.json`, `manifest.json`) contain rich execution metadata you can use to build your own monitoring dashboard
