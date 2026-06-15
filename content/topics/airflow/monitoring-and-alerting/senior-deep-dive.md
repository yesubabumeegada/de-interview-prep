---
title: "Airflow Monitoring and Alerting - Senior Deep Dive"
topic: airflow
subtopic: monitoring-and-alerting
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [airflow, monitoring, opentelemetry, tracing, observability, sla, dead-letter-queue, great-expectations]
---

# Airflow Monitoring and Alerting — Senior Deep Dive

## Distributed Tracing for Airflow Tasks (OpenTelemetry)

Standard Airflow logs are per-task. Distributed tracing links tasks in a DAG run into a single trace, enabling end-to-end latency analysis.

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

# Initialize tracer (in Airflow plugin or callback)
provider = TracerProvider()
provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://jaeger:4317"))
)
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("airflow.dag")

def traced_task(python_callable):
    """Decorator to add OpenTelemetry tracing to any task."""
    def wrapper(**context):
        dag_id = context["dag"].dag_id
        task_id = context["task_instance"].task_id
        run_id = context["run_id"]
        
        # Use run_id as trace context so all tasks in a DAG run share a trace
        with tracer.start_as_current_span(
            f"{dag_id}.{task_id}",
            attributes={
                "airflow.dag_id": dag_id,
                "airflow.task_id": task_id,
                "airflow.run_id": run_id,
                "airflow.execution_date": context["ds"],
                "airflow.try_number": context["task_instance"].try_number,
            }
        ) as span:
            try:
                result = python_callable(**context)
                span.set_status(trace.StatusCode.OK)
                return result
            except Exception as e:
                span.record_exception(e)
                span.set_status(trace.StatusCode.ERROR, str(e))
                raise
    return wrapper

# Usage
@traced_task
def extract_data(**context):
    # ... extraction logic
    pass
```

---

## Structured JSON Logging with Correlation IDs

Standard Airflow logs are plaintext. In production with 500 DAGs, you need structured logs that ship to a log aggregator (Datadog, Splunk, ELK).

```python
# airflow_local_settings.py — custom JSON log handler
import json
import logging
import time

class JSONTaskLogHandler(logging.Handler):
    def __init__(self, base_handler):
        super().__init__()
        self.base_handler = base_handler

    def emit(self, record):
        # Inject Airflow task context into every log line
        log_entry = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(record.created)),
            "level": record.levelname,
            "message": record.getMessage(),
            "dag_id": getattr(record, "dag_id", None),
            "task_id": getattr(record, "task_id", None),
            "run_id": getattr(record, "run_id", None),
            "execution_date": getattr(record, "execution_date", None),
            "try_number": getattr(record, "try_number", None),
            "filename": record.filename,
            "lineno": record.lineno,
        }
        # Emit as JSON to the underlying handler
        record.msg = json.dumps(log_entry)
        record.args = ()
        self.base_handler.emit(record)
```

Correlation IDs let you link a single DAG run's logs across all tasks and external systems:

```python
import uuid

def generate_run_correlation_id(**context):
    """First task generates a run-wide correlation ID stored in XCom."""
    correlation_id = str(uuid.uuid4())
    context["task_instance"].xcom_push(key="correlation_id", value=correlation_id)
    return correlation_id

def task_with_correlation(**context):
    """Every task reads the correlation ID and adds it to all log messages."""
    correlation_id = context["task_instance"].xcom_pull(
        task_ids="generate_correlation_id", key="correlation_id"
    )
    logger = logging.getLogger(__name__)
    # All subsequent log calls include the correlation_id
    logger.info("Processing batch", extra={"correlation_id": correlation_id})
```

---

## Dead Letter Queue Pattern for Failed Tasks

When tasks fail permanently, data shouldn't be silently dropped. Use a dead letter queue:

```python
def dlq_failure_callback(context):
    """Route failed task payload to a dead letter queue for later reprocessing."""
    import boto3
    
    ti = context["task_instance"]
    sqs = boto3.client("sqs", region_name="us-east-1")
    
    # Retrieve the payload that failed
    failed_payload = ti.xcom_pull(key="input_payload")
    
    dlq_message = {
        "dag_id": context["dag"].dag_id,
        "task_id": ti.task_id,
        "execution_date": context["ds"],
        "run_id": context["run_id"],
        "try_number": ti.try_number,
        "exception": str(context.get("exception", "")),
        "payload": failed_payload,
        "failed_at": datetime.utcnow().isoformat(),
    }
    
    sqs.send_message(
        QueueUrl=Variable.get("dlq_url"),
        MessageBody=json.dumps(dlq_message),
        MessageAttributes={
            "dag_id": {"DataType": "String", "StringValue": context["dag"].dag_id},
        }
    )
    
    # Also alert on-call
    pagerduty_failure_callback(context)
```

---

## SLA Enforcement Architecture

A robust SLA enforcement system escalates through stages:

```
Task misses SLA
    │
    ▼ (T+0)
sla_miss_callback → Slack #data-alerts (warning)
    │
    ▼ (T+30min, still not done)
CloudWatch Alarm → PagerDuty (page on-call engineer)
    │
    ▼ (T+1hr, still not done)
Escalation Lambda → Page engineering manager + skip to next DAG
    │
    ▼ (T+2hr, still not done)
Executive notification (VP Data) + incident record created
```

Implementation:

```python
def tiered_sla_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """SLA miss with escalation based on severity."""
    from datetime import datetime, timezone
    
    for sla in slas:
        # Calculate how late we are
        overdue_minutes = (datetime.now(timezone.utc) - sla.execution_date).seconds / 60
        
        # Always: Slack warning
        slack_notify(f"SLA miss: {dag.dag_id}.{sla.task_id} ({overdue_minutes:.0f}min late)")
        
        # If > 30 min: PagerDuty
        if overdue_minutes > 30:
            pagerduty_trigger(dag.dag_id, sla.task_id, severity="warning")
        
        # If > 60 min: critical escalation
        if overdue_minutes > 60:
            pagerduty_trigger(dag.dag_id, sla.task_id, severity="critical")
            notify_manager(dag.dag_id, sla.task_id, overdue_minutes)
```

---

## Health Check Endpoints

Airflow exposes health check endpoints for use with load balancers and monitoring:

```bash
# /health — overall health (scheduler + metadatabase)
curl http://airflow-webserver:8080/health
# Response:
{
  "metadatabase": {"status": "healthy"},
  "scheduler": {
    "status": "healthy",
    "latest_scheduler_heartbeat": "2024-01-15T10:30:00+00:00"
  }
}

# /dag_stats — count of DAG runs by state
curl http://airflow-webserver:8080/dag_stats
```

Use in Kubernetes:

```yaml
livenessProbe:
  httpGet:
    path: /health
    port: 8080
  initialDelaySeconds: 30
  periodSeconds: 10
readinessProbe:
  httpGet:
    path: /health
    port: 8080
```

---

## Scheduler Lag Detection

The scheduler converts scheduled DAG runs into queued task instances. If it falls behind, tasks run late even when workers are available:

```python
# Detect scheduler lag via metadata DB
SELECT
    dag_id,
    execution_date,
    EXTRACT(EPOCH FROM (queued_dttm - scheduled_dttm)) AS scheduling_lag_seconds
FROM task_instance
WHERE state = 'running'
    AND scheduled_dttm IS NOT NULL
    AND queued_dttm IS NOT NULL
    AND queued_dttm - scheduled_dttm > INTERVAL '60 seconds'
ORDER BY scheduling_lag_seconds DESC;
```

Alert if scheduler heartbeat interval exceeds 10 seconds:

```promql
# PromQL
max_over_time(airflow_scheduler_heartbeat[5m]) > 10
```

---

## Metadata DB Query Monitoring

Slow queries in the metadata DB cause scheduler and UI lag:

```sql
-- PostgreSQL slow query log for Airflow
ALTER SYSTEM SET log_min_duration_statement = '1000'; -- Log queries > 1 second
SELECT pg_reload_conf();

-- Find slow Airflow queries
SELECT query, mean_exec_time, calls, total_exec_time
FROM pg_stat_statements
WHERE query LIKE '%task_instance%' OR query LIKE '%dag_run%'
ORDER BY mean_exec_time DESC
LIMIT 10;
```

Common causes of slow queries:
- Missing index on `task_instance.execution_date` for large tables
- `dag_run` table growing unboundedly (increase `max_dagruns_to_create_per_loop` or purge old runs)
- `log` table not purged (use `airflow db clean`)

---

## Data Quality Alerting with Great Expectations

Integrate data quality checks into task callbacks:

```python
from great_expectations.data_context import DataContext

def data_quality_callback(context):
    """Run GE validation after task completes, alert on failures."""
    gx_context = DataContext("/opt/airflow/great_expectations")
    
    result = gx_context.run_checkpoint(
        checkpoint_name=f"{context['dag'].dag_id}_quality_check",
        batch_request={
            "datasource_name": "warehouse",
            "data_connector_name": "default_inferred_data_connector_name",
            "data_asset_name": "fact_orders",
            "data_connector_query": {"batch_identifiers": {"date": context["ds"]}},
        }
    )
    
    if not result.success:
        failed_expectations = [
            r for r in result.run_results.values()
            if not r["validation_result"]["success"]
        ]
        slack_notify(
            f":warning: Data quality failure on `{context['dag'].dag_id}`\n"
            f"Failed checks: {len(failed_expectations)}\n"
            f"Date: {context['ds']}"
        )
        raise ValueError(f"Data quality check failed: {failed_expectations}")
```

---

## Key Takeaways

- OpenTelemetry spans per task enable end-to-end tracing of DAG run latency across all tasks
- Structured JSON logging with correlation IDs makes logs searchable and linkable across tasks and external systems
- Dead letter queues capture failed task payloads for reprocessing without data loss
- SLA enforcement needs tiered escalation: Slack → PagerDuty → manager escalation at increasing time thresholds
- `/health` endpoint reports scheduler + metadatabase health; use for Kubernetes probes and uptime monitoring
- Slow metadata DB queries are the most common cause of scheduler lag; monitor with `pg_stat_statements`
