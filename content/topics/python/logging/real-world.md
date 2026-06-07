---
title: "Python Logging - Real-World Production Examples"
topic: python
subtopic: logging
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [python, logging, production, etl, monitoring]
---

# Python Logging — Real-World Production Examples

## Pattern 1: Structured Logging for ETL Pipelines

A complete logging setup for production ETL with JSON output and correlation IDs.

```python
import structlog
import uuid
import time
from datetime import datetime
from typing import Any
from contextvars import ContextVar

# Global context for pipeline run
_run_context: ContextVar[dict[str, Any]] = ContextVar('run_context', default={})

def configure_pipeline_logging() -> None:
    """Call once at application startup."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.CallsiteParameterAdder(
                parameters=[structlog.processors.CallsiteParameter.FUNC_NAME]
            ),
            structlog.processors.JSONRenderer()
        ],
        wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO+
    )

def start_pipeline_run(pipeline_name: str) -> str:
    """Initialize logging context for a new pipeline run."""
    run_id = str(uuid.uuid4())[:8]
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(
        pipeline=pipeline_name,
        run_id=run_id,
        started_at=datetime.utcnow().isoformat()
    )
    return run_id

# Usage in pipeline stages
logger = structlog.get_logger()

def extract(source_path: str) -> list[dict]:
    logger.info("extraction_started", source=source_path)
    start = time.time()
    
    records = read_from_source(source_path)
    
    logger.info("extraction_complete",
               source=source_path,
               rows=len(records),
               duration_seconds=round(time.time() - start, 2))
    return records

def transform(records: list[dict]) -> list[dict]:
    logger.info("transform_started", input_rows=len(records))
    start = time.time()
    
    valid = []
    invalid_count = 0
    for record in records:
        try:
            valid.append(apply_rules(record))
        except ValueError as e:
            invalid_count += 1
            logger.warning("record_invalid",
                          record_id=record.get("id"),
                          error=str(e))
    
    logger.info("transform_complete",
               input_rows=len(records),
               output_rows=len(valid),
               invalid_rows=invalid_count,
               duration_seconds=round(time.time() - start, 2))
    return valid

# Output (every line includes pipeline, run_id automatically):
# {"pipeline":"daily_users","run_id":"a3f8b2c1","event":"extraction_started",...}
# {"pipeline":"daily_users","run_id":"a3f8b2c1","event":"extraction_complete","rows":50000,...}
```

---

## Pattern 2: Performance Logging with Decorators

A reusable decorator that logs function timing, inputs, and outputs.

```python
import structlog
import time
from functools import wraps
from typing import ParamSpec, TypeVar, Callable

P = ParamSpec('P')
R = TypeVar('R')
logger = structlog.get_logger()

def log_performance(
    log_args: bool = False,
    warn_threshold_seconds: float = 30.0
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorator: log function duration, warn if slow."""
    def decorator(func: Callable[P, R]) -> Callable[P, R]:
        @wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
            func_name = f"{func.__module__}.{func.__qualname__}"
            
            log_data: dict = {"function": func_name}
            if log_args:
                log_data["kwargs"] = {k: str(v)[:100] for k, v in kwargs.items()}
            
            logger.info("function_started", **log_data)
            start = time.perf_counter()
            
            try:
                result = func(*args, **kwargs)
                duration = time.perf_counter() - start
                
                log_data["duration_seconds"] = round(duration, 3)
                log_data["status"] = "success"
                
                if duration > warn_threshold_seconds:
                    logger.warning("function_slow", **log_data)
                else:
                    logger.info("function_complete", **log_data)
                
                return result
            except Exception as e:
                duration = time.perf_counter() - start
                log_data["duration_seconds"] = round(duration, 3)
                log_data["status"] = "error"
                log_data["error_type"] = type(e).__name__
                log_data["error_message"] = str(e)[:200]
                logger.error("function_failed", **log_data)
                raise
        return wrapper
    return decorator

# Usage
@log_performance(log_args=True, warn_threshold_seconds=10.0)
def load_to_warehouse(table: str, records: list[dict], mode: str = "append") -> int:
    # ... actual load logic ...
    return len(records)

load_to_warehouse(table="analytics.users", records=data, mode="overwrite")
# Logs: {"event":"function_complete","function":"etl.load_to_warehouse",
#         "duration_seconds":8.234,"status":"success","kwargs":{"table":"analytics.users",...}}
```

---

## Pattern 3: Log-Based Data Quality Alerting

Monitor data quality metrics via logs and trigger alerts when thresholds breach.

```python
import structlog
from dataclasses import dataclass, field
from datetime import datetime

logger = structlog.get_logger()

@dataclass
class QualityMetrics:
    table: str
    check_time: str = field(default_factory=lambda: datetime.utcnow().isoformat())
    total_rows: int = 0
    null_rate: dict[str, float] = field(default_factory=dict)
    duplicate_rate: float = 0.0
    freshness_hours: float = 0.0

@dataclass
class QualityThreshold:
    max_null_rate: float = 0.05       # 5% max nulls per column
    max_duplicate_rate: float = 0.01  # 1% max duplicates
    max_freshness_hours: float = 24   # Data must be < 24h old
    min_row_count: int = 100          # Must have at least 100 rows

def run_quality_checks(
    table: str,
    df,
    thresholds: QualityThreshold
) -> QualityMetrics:
    """Run DQ checks and emit structured logs for alerting."""
    metrics = QualityMetrics(table=table, total_rows=len(df))
    alerts = []
    
    # Check null rates per column
    for col in df.columns:
        null_rate = df[col].isnull().sum() / len(df)
        metrics.null_rate[col] = round(null_rate, 4)
        if null_rate > thresholds.max_null_rate:
            alerts.append(f"Column '{col}' null rate {null_rate:.2%} exceeds {thresholds.max_null_rate:.2%}")
    
    # Check duplicate rate
    dup_count = df.duplicated().sum()
    metrics.duplicate_rate = round(dup_count / len(df), 4)
    if metrics.duplicate_rate > thresholds.max_duplicate_rate:
        alerts.append(f"Duplicate rate {metrics.duplicate_rate:.2%} exceeds threshold")
    
    # Check row count
    if metrics.total_rows < thresholds.min_row_count:
        alerts.append(f"Row count {metrics.total_rows} below minimum {thresholds.min_row_count}")
    
    # Emit structured log for monitoring
    if alerts:
        logger.error("data_quality_alert",
                    table=table,
                    alerts=alerts,
                    alert_count=len(alerts),
                    metrics={"null_rates": metrics.null_rate,
                            "duplicate_rate": metrics.duplicate_rate,
                            "row_count": metrics.total_rows})
    else:
        logger.info("data_quality_passed",
                   table=table,
                   row_count=metrics.total_rows,
                   max_null_rate=max(metrics.null_rate.values(), default=0))
    
    return metrics

# CloudWatch Insights query for alerting:
# fields @timestamp, table, alert_count
# | filter event = "data_quality_alert"
# | stats count() by table
# | sort count desc
```

---

## Pattern 4: Airflow Task Logging Best Practices

```python
import logging
from airflow.decorators import task, dag
from airflow.models import Variable
from datetime import datetime

@dag(schedule='@daily', start_date=datetime(2024, 1, 1), catchup=False)
def daily_pipeline():
    
    @task
    def extract(**context) -> dict:
        """Airflow captures task logs automatically."""
        logger = logging.getLogger('airflow.task')
        
        # Include Airflow context in logs
        run_id = context['run_id']
        ds = context['ds']
        logger.info("Extraction started | run_id=%s | ds=%s", run_id, ds)
        
        source = Variable.get("source_path")
        records = read_source(source, ds)
        
        # Log metrics for Airflow UI visibility
        logger.info(
            "Extraction complete | rows=%d | source=%s | ds=%s",
            len(records), source, ds
        )
        
        return {"row_count": len(records), "source": source}
    
    @task
    def transform(extract_result: dict, **context) -> dict:
        logger = logging.getLogger('airflow.task')
        
        input_rows = extract_result["row_count"]
        logger.info("Transform started | input_rows=%d", input_rows)
        
        # Log warnings for data issues (visible in Airflow task logs)
        invalid_count = 0
        for record in get_records():
            if not validate(record):
                invalid_count += 1
                if invalid_count <= 10:  # Don't spam logs
                    logger.warning("Invalid record: %s", record.get("id"))
        
        if invalid_count > 10:
            logger.warning("... and %d more invalid records", invalid_count - 10)
        
        output_rows = input_rows - invalid_count
        logger.info("Transform complete | output_rows=%d | invalid=%d", 
                   output_rows, invalid_count)
        return {"row_count": output_rows}
    
    @task
    def load(transform_result: dict, **context) -> None:
        logger = logging.getLogger('airflow.task')
        rows = transform_result["row_count"]
        
        logger.info("Load started | rows=%d | target=warehouse.users", rows)
        # ... load logic ...
        logger.info("Load complete | rows_loaded=%d", rows)
    
    data = extract()
    transformed = transform(data)
    load(transformed)

daily_pipeline()
```

---

## Production Logging Checklist

| Category | Requirement | Implementation |
|----------|-------------|----------------|
| Format | JSON structured output | structlog or python-json-logger |
| Context | Correlation ID in every log | contextvars + structlog binding |
| Levels | Consistent level usage across team | Document level guidelines |
| Performance | No logging in tight loops | Aggregate metrics, log summaries |
| Sensitive data | PII redacted from logs | Custom filter or structlog processor |
| Retention | Logs rotated and archived | CloudWatch retention policy or logrotate |
| Alerting | ERROR logs trigger alerts | CloudWatch Alarm or DataDog monitor |
| Cost | High-volume logs sampled | Sampling filter on DEBUG/INFO |
| Searchability | Key fields as top-level JSON keys | Not buried in message string |
| Testing | Logging doesn't break tests | caplog fixture in pytest |

---

## Interview Tips

> **Tip 1:** "How would you set up logging for a production ETL service?" — "Structured JSON output via structlog to stdout. A correlation ID (run_id) bound via contextvars at the start of each pipeline run so every log line is traceable. Metrics in log fields (row counts, durations, error counts) for dashboards. ERROR+ logs trigger alerts via CloudWatch Alarm or DataDog monitor. PII filter to redact sensitive fields before output."

> **Tip 2:** "How do you handle logging in Airflow?" — "Airflow captures stdout/stderr from each task and stores it per-task-instance. Use `logging.getLogger('airflow.task')` inside task functions. Include Airflow context (run_id, ds) in log messages for traceability. Log summary metrics at the end of each task so operators can see row counts in the Airflow UI without clicking into full logs."

> **Tip 3:** "How do you balance log verbosity with cost?" — "Level-based separation: DEBUG goes nowhere in production, INFO sampled at 10% for cost, WARNING+ kept at 100%. Use deterministic sampling (hash on key field) so all logs for a given entity are either kept or dropped together. Always keep full context around errors — increase sampling window when error rate spikes."
