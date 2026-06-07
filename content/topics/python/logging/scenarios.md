---
title: "Python Logging - Scenario Questions"
topic: python
subtopic: logging
content_type: scenario_question
tags: [python, logging, interview, scenarios]
---

# Scenario Questions — Python Logging

<article data-difficulty="junior">

## 🟢 Junior: Replace print() with Proper Logging

**Scenario:** You inherited this ETL script that uses print() everywhere. Refactor it to use proper logging with appropriate levels, formatters, and handlers. The script should log to both console and a file.

```python
# Current code (BAD)
def run_etl(source_path, target_path):
    print(f"Starting ETL from {source_path}")
    data = read_csv(source_path)
    print(f"Read {len(data)} rows")
    
    clean = []
    for row in data:
        if row.get('email') is None:
            print(f"WARNING: row {row['id']} has no email, skipping")
            continue
        clean.append(row)
    
    print(f"Writing {len(clean)} clean rows to {target_path}")
    write_csv(clean, target_path)
    print("Done!")
```

<details>
<summary>💡 Hint</summary>

Use `logging.getLogger(__name__)`, configure with `basicConfig` or `dictConfig`, and choose appropriate levels: INFO for progress, WARNING for skipped records, ERROR for failures.

</details>

<details>
<summary>✅ Solution</summary>

```python
import logging
from logging.handlers import RotatingFileHandler

# Configure logging at module level
def setup_logging(log_file: str = "etl.log") -> None:
    """Configure logging with console and file handlers."""
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    
    # Console handler — INFO and above
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter(
        '%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    ))
    
    # File handler — all levels, with rotation
    file_handler = RotatingFileHandler(
        log_file, maxBytes=5_000_000, backupCount=3
    )
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s'
    ))
    
    logger.addHandler(console)
    logger.addHandler(file_handler)

# Refactored ETL function
logger = logging.getLogger(__name__)

def run_etl(source_path: str, target_path: str) -> None:
    logger.info("Starting ETL from %s to %s", source_path, target_path)
    
    try:
        data = read_csv(source_path)
    except FileNotFoundError:
        logger.error("Source file not found: %s", source_path)
        raise
    
    logger.info("Read %d rows from source", len(data))
    
    clean = []
    skip_count = 0
    for row in data:
        if row.get('email') is None:
            skip_count += 1
            logger.debug("Row %s missing email, skipping", row.get('id'))
            continue
        clean.append(row)
    
    if skip_count > 0:
        logger.warning("Skipped %d rows due to missing email (%.1f%%)",
                      skip_count, skip_count / len(data) * 100)
    
    logger.info("Writing %d clean rows to %s", len(clean), target_path)
    write_csv(clean, target_path)
    logger.info("ETL complete: %d/%d rows written", len(clean), len(data))

if __name__ == '__main__':
    setup_logging()
    try:
        run_etl("input.csv", "output.csv")
    except Exception:
        logger.critical("ETL job failed", exc_info=True)
        raise
```

**Key improvements:**
- Appropriate log levels (INFO for progress, WARNING for data issues, ERROR for failures)
- File rotation prevents disk fill
- Individual skipped records logged at DEBUG (visible in file, not console)
- Summary warning for bulk data issues
- Exception logging with traceback (`exc_info=True`)

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Add Structured JSON Logging with Context to an ETL Pipeline

**Scenario:** Your team's ETL pipeline produces plain text logs that are hard to search in CloudWatch. Redesign the logging to:
1. Output structured JSON
2. Include a correlation ID (run_id) in every log line
3. Track metrics (row counts, durations) as structured fields
4. Add contextual information (pipeline name, stage, environment)

<details>
<summary>💡 Hint</summary>

Use structlog with contextvars for automatic context propagation. Emit timing and counts as JSON fields (not inside the message string) so they're queryable in CloudWatch Insights.

</details>

<details>
<summary>✅ Solution</summary>

```python
import structlog
import time
import uuid
from contextvars import ContextVar
from functools import wraps
from typing import Any, Callable

# Configure structlog for JSON output
def configure_logging(env: str = "production") -> None:
    processors = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.StackInfoRenderer(),
    ]
    
    if env == "development":
        processors.append(structlog.dev.ConsoleRenderer())
    else:
        processors.append(structlog.processors.JSONRenderer())
    
    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(20),
    )

# Context management
def init_pipeline_context(pipeline_name: str, env: str = "prod") -> str:
    """Bind context that appears in ALL subsequent log lines."""
    run_id = str(uuid.uuid4())[:8]
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        run_id=run_id,
        pipeline=pipeline_name,
        environment=env,
    )
    return run_id

# Stage tracking decorator
def log_stage(stage_name: str):
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            log = structlog.get_logger()
            structlog.contextvars.bind_contextvars(stage=stage_name)
            
            log.info("stage_started")
            start = time.perf_counter()
            
            try:
                result = func(*args, **kwargs)
                duration = time.perf_counter() - start
                log.info("stage_complete",
                        duration_seconds=round(duration, 3),
                        **_extract_metrics(result))
                return result
            except Exception as e:
                duration = time.perf_counter() - start
                log.error("stage_failed",
                         duration_seconds=round(duration, 3),
                         error_type=type(e).__name__,
                         error_message=str(e)[:500])
                raise
        return wrapper
    return decorator

def _extract_metrics(result: Any) -> dict:
    """Pull out numeric metrics from the result for logging."""
    if isinstance(result, dict):
        return {k: v for k, v in result.items() if isinstance(v, (int, float))}
    if isinstance(result, list):
        return {"output_rows": len(result)}
    return {}

# Pipeline implementation
log = structlog.get_logger()

@log_stage("extract")
def extract(source: str) -> list[dict]:
    records = read_from_source(source)
    log.info("extraction_detail", rows=len(records), source=source)
    return records

@log_stage("transform")
def transform(records: list[dict]) -> list[dict]:
    valid = [r for r in records if r.get("user_id")]
    invalid = len(records) - len(valid)
    if invalid > 0:
        log.warning("records_dropped", dropped_count=invalid,
                   reason="missing_user_id")
    return valid

@log_stage("load")
def load(records: list[dict], target: str) -> dict:
    rows_loaded = write_to_target(records, target)
    return {"rows_loaded": rows_loaded}

# Entry point
def main():
    configure_logging(env="production")
    run_id = init_pipeline_context("daily_user_sync", env="prod")
    
    log.info("pipeline_started")
    data = extract("s3://raw/users/")
    clean = transform(data)
    load(clean, "redshift://analytics.users")
    log.info("pipeline_complete")

# Every log line automatically includes:
# {"run_id":"a3f8b2c1","pipeline":"daily_user_sync","environment":"prod",
#  "stage":"extract","event":"stage_complete","duration_seconds":4.231,...}
```

**CloudWatch Insights queries made possible by structured logging:**
```
# Find slow stages
fields @timestamp, pipeline, stage, duration_seconds
| filter duration_seconds > 30
| sort duration_seconds desc

# Track error rate by pipeline
fields pipeline, run_id
| filter level = "error"
| stats count() by pipeline, bin(1h)
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design Observability for a Distributed Data Platform

**Scenario:** You're the tech lead for a data platform with: 15 Airflow DAGs, 8 Spark jobs, 5 microservices (FastAPI), and data flowing through S3, Kafka, and Redshift. Currently, debugging production issues requires SSH-ing into individual machines and grepping log files. Design a comprehensive observability solution.

**Requirements:**
- Correlate logs across services for a single data flow
- Alert on data quality issues within 5 minutes
- Support 50GB/day of log volume at reasonable cost
- Enable developers to self-serve debugging

<details>
<summary>💡 Hint</summary>

Think about the three pillars: logs, metrics, traces. Consider how a single record flows from Kafka → Spark → Redshift and how you'd trace that journey.

</details>

<details>
<summary>✅ Solution</summary>

```python
# === ARCHITECTURE OVERVIEW ===
# 
# All services → structured JSON logs → Fluent Bit → OpenSearch
# All services → metrics → Prometheus → Grafana
# All services → traces → OpenTelemetry → Jaeger/X-Ray
#
# Correlation: trace_id propagated through Kafka headers, 
# HTTP headers, and Spark broadcast variables

# === 1. SHARED LOGGING LIBRARY (internal package) ===
# data_platform_logging/core.py

import structlog
from opentelemetry import trace
from opentelemetry.propagate import inject, extract

def configure_platform_logging(service_name: str) -> None:
    """Standard logging config installed by all platform services."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _inject_trace_context,      # Add trace_id/span_id
            _inject_service_metadata,   # Add service, version, host
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer()
        ]
    )
    structlog.contextvars.bind_contextvars(service=service_name)

def _inject_trace_context(logger, method_name, event_dict):
    """Add OpenTelemetry trace context to every log."""
    span = trace.get_current_span()
    if span.is_recording():
        ctx = span.get_span_context()
        event_dict["trace_id"] = format(ctx.trace_id, '032x')
        event_dict["span_id"] = format(ctx.span_id, '016x')
    return event_dict

def _inject_service_metadata(logger, method_name, event_dict):
    import os
    event_dict["host"] = os.environ.get("HOSTNAME", "unknown")
    event_dict["version"] = os.environ.get("APP_VERSION", "dev")
    return event_dict

# === 2. KAFKA TRACE PROPAGATION ===
def produce_with_trace(producer, topic: str, value: bytes) -> None:
    """Propagate trace context through Kafka headers."""
    headers = {}
    inject(headers)  # Injects traceparent header
    producer.produce(
        topic=topic,
        value=value,
        headers=[(k, v.encode()) for k, v in headers.items()]
    )

def consume_with_trace(message) -> None:
    """Extract trace context from consumed Kafka message."""
    headers = {k: v.decode() for k, v in message.headers() or []}
    ctx = extract(headers)
    # Continue the trace in this service
    tracer = trace.get_tracer(__name__)
    with tracer.start_as_current_span("process_message", context=ctx):
        process(message.value())

# === 3. LOG VOLUME MANAGEMENT (50GB/day) ===
SAMPLING_CONFIG = {
    "DEBUG": 0.0,      # Never in production
    "INFO": 0.10,      # 10% sample
    "WARNING": 1.0,    # Always keep
    "ERROR": 1.0,      # Always keep
    "CRITICAL": 1.0,   # Always keep
}

def sampling_processor(logger, method_name, event_dict):
    """Sample logs based on level to manage volume."""
    import random
    level = event_dict.get("level", "info").upper()
    rate = SAMPLING_CONFIG.get(level, 1.0)
    
    if random.random() > rate:
        raise structlog.DropEvent
    
    event_dict["_sampled"] = True
    event_dict["_sample_rate"] = rate
    return event_dict

# === 4. ALERTING RULES (as code) ===
ALERT_RULES = {
    "data_quality_failure": {
        "query": 'level="error" AND event="data_quality_alert"',
        "threshold": 1,
        "window_minutes": 5,
        "severity": "P2",
        "notify": ["#data-alerts", "oncall-data"],
    },
    "pipeline_failure": {
        "query": 'level="critical" AND event="pipeline_failed"',
        "threshold": 1,
        "window_minutes": 1,
        "severity": "P1",
        "notify": ["#data-critical", "oncall-data", "pagerduty"],
    },
    "high_latency": {
        "query": 'event="stage_complete" AND duration_seconds > 300',
        "threshold": 3,
        "window_minutes": 15,
        "severity": "P3",
        "notify": ["#data-alerts"],
    },
}
```

**Architecture Decision Record:**

| Component | Choice | Rationale |
|-----------|--------|-----------|
| Log format | Structured JSON | Queryable, parseable by machines |
| Shipping | Fluent Bit sidecar | Low resource, Kubernetes native |
| Storage | OpenSearch | Cost-effective for 50GB/day vs CloudWatch |
| Tracing | OpenTelemetry + X-Ray | Vendor-neutral, AWS native backend |
| Metrics | Prometheus + Grafana | Industry standard, good for custom metrics |
| Alerting | Grafana Alerting | Single pane, routing to Slack/PagerDuty |
| Correlation | trace_id in headers | Links across Kafka, HTTP, Spark |
| Cost control | Level-based sampling | 10% INFO = ~35% volume reduction |

</details>

</article>
