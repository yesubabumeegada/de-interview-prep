---
title: "Python Logging - Senior Deep Dive"
topic: python
subtopic: logging
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [python, logging, distributed-systems, elk, observability]
---

# Python Logging — Senior Deep Dive

## Logging in Distributed Systems: Spark Driver vs Executor

In PySpark, logging behaves differently on the driver and executors.

```python
import logging
from pyspark.sql import SparkSession

# DRIVER-SIDE: Standard Python logging works normally
logger = logging.getLogger('spark_job')
logger.setLevel(logging.INFO)

def run_spark_job():
    spark = SparkSession.builder.appName("etl_job").getOrCreate()
    logger.info("Job started on driver")  # This works fine
    
    df = spark.read.parquet("s3://raw/events/")
    logger.info("Read %d partitions", df.rdd.getNumPartitions())
    
    # EXECUTOR-SIDE: Logger must be configured INSIDE the function
    def process_partition(iterator):
        """Runs on executor — logger must be set up here."""
        import logging
        exec_logger = logging.getLogger('spark_job.executor')
        exec_logger.setLevel(logging.INFO)
        
        count = 0
        for row in iterator:
            count += 1
        exec_logger.info("Processed %d rows in partition", count)
        yield count
    
    # mapPartitions runs on executors
    results = df.rdd.mapPartitions(process_partition).collect()
    logger.info("Total partitions processed: %d", len(results))
```

**Key challenges in distributed logging:**
- Executors don't share the driver's logging config
- Log output from executors goes to executor stdout (captured by YARN/K8s)
- Correlation IDs must be passed explicitly (broadcast variable or closure)
- Log aggregation requires a central service (CloudWatch, Datadog)

```python
# Pattern: Pass correlation ID to executors via broadcast
run_id = str(uuid.uuid4())[:8]
broadcast_run_id = spark.sparkContext.broadcast(run_id)

def transform_with_logging(partition):
    import logging
    logger = logging.getLogger('executor')
    rid = broadcast_run_id.value
    logger.info("[%s] Processing partition", rid)
    # ...
```

---

## Custom Handlers for Cloud Services

### CloudWatch Logs Handler

```python
import logging
import boto3
import time
from typing import Any

class CloudWatchHandler(logging.Handler):
    """Send logs directly to AWS CloudWatch Logs."""
    
    def __init__(self, log_group: str, log_stream: str, region: str = 'us-east-1'):
        super().__init__()
        self.client = boto3.client('logs', region_name=region)
        self.log_group = log_group
        self.log_stream = log_stream
        self.sequence_token: str | None = None
        self._buffer: list[dict[str, Any]] = []
        self._buffer_size = 50
        self._ensure_log_group_exists()
    
    def _ensure_log_group_exists(self) -> None:
        try:
            self.client.create_log_group(logGroupName=self.log_group)
        except self.client.exceptions.ResourceAlreadyExistsException:
            pass
        try:
            self.client.create_log_stream(
                logGroupName=self.log_group,
                logStreamName=self.log_stream
            )
        except self.client.exceptions.ResourceAlreadyExistsException:
            pass
    
    def emit(self, record: logging.LogRecord) -> None:
        self._buffer.append({
            'timestamp': int(record.created * 1000),
            'message': self.format(record)
        })
        if len(self._buffer) >= self._buffer_size:
            self.flush()
    
    def flush(self) -> None:
        if not self._buffer:
            return
        kwargs = {
            'logGroupName': self.log_group,
            'logStreamName': self.log_stream,
            'logEvents': sorted(self._buffer, key=lambda x: x['timestamp'])
        }
        if self.sequence_token:
            kwargs['sequenceToken'] = self.sequence_token
        
        try:
            response = self.client.put_log_events(**kwargs)
            self.sequence_token = response.get('nextSequenceToken')
        except Exception as e:
            self.handleError(logging.LogRecord('', 0, '', 0, str(e), None, None))
        finally:
            self._buffer = []
```

### DataDog Metrics from Logs

```python
import logging
from datadog import statsd

class MetricsExtractionHandler(logging.Handler):
    """Extract metrics from structured log fields and emit to DataDog."""
    
    METRIC_FIELDS = {
        'rows_processed': 'pipeline.rows_processed',
        'duration_seconds': 'pipeline.duration',
        'error_count': 'pipeline.errors',
    }
    
    def emit(self, record: logging.LogRecord) -> None:
        for field, metric_name in self.METRIC_FIELDS.items():
            value = getattr(record, field, None)
            if value is not None:
                tags = [
                    f"pipeline:{getattr(record, 'pipeline_name', 'unknown')}",
                    f"stage:{getattr(record, 'stage', 'unknown')}",
                ]
                statsd.gauge(metric_name, value, tags=tags)
```

---

## OpenTelemetry Integration

Combine logs with traces and metrics for full observability.

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.instrumentation.logging import LoggingInstrumentor
import logging
import structlog

# Setup OpenTelemetry tracing
trace.set_tracer_provider(TracerProvider())
tracer = trace.get_tracer(__name__)

# Auto-inject trace context into logs
LoggingInstrumentor().instrument()

logger = structlog.get_logger()

def process_batch(batch_id: str, records: list[dict]) -> int:
    with tracer.start_as_current_span("process_batch") as span:
        span.set_attribute("batch_id", batch_id)
        span.set_attribute("record_count", len(records))
        
        # Log automatically includes trace_id and span_id
        logger.info("batch_processing_started",
                   batch_id=batch_id,
                   count=len(records))
        
        processed = transform(records)
        
        logger.info("batch_processing_complete",
                   batch_id=batch_id,
                   output_count=len(processed))
        
        return len(processed)

# Log output includes trace correlation:
# {"event":"batch_processing_started","trace_id":"abc123","span_id":"def456",...}
```

---

## Log-Based Alerting

```python
import logging
import structlog
from dataclasses import dataclass

@dataclass
class AlertRule:
    name: str
    log_field: str
    threshold: float
    window_seconds: int
    comparison: str  # "gt", "lt", "eq"

class AlertingHandler(logging.Handler):
    """Monitor log metrics and trigger alerts on threshold breaches."""
    
    def __init__(self, rules: list[AlertRule]):
        super().__init__()
        self.rules = rules
        self._windows: dict[str, list[tuple[float, float]]] = {}
    
    def emit(self, record: logging.LogRecord) -> None:
        import time
        now = time.time()
        
        for rule in self.rules:
            value = getattr(record, rule.log_field, None)
            if value is None:
                continue
            
            key = rule.name
            if key not in self._windows:
                self._windows[key] = []
            
            self._windows[key].append((now, float(value)))
            
            # Prune old entries
            cutoff = now - rule.window_seconds
            self._windows[key] = [
                (t, v) for t, v in self._windows[key] if t > cutoff
            ]
            
            # Check threshold
            avg_value = sum(v for _, v in self._windows[key]) / len(self._windows[key])
            if rule.comparison == "gt" and avg_value > rule.threshold:
                self._fire_alert(rule, avg_value)
    
    def _fire_alert(self, rule: AlertRule, value: float) -> None:
        # Send to PagerDuty, Slack, etc.
        print(f"ALERT: {rule.name} — value {value:.2f} exceeds {rule.threshold}")

# Usage
rules = [
    AlertRule("high_error_rate", "error_count", threshold=100, window_seconds=300, comparison="gt"),
    AlertRule("slow_processing", "duration_seconds", threshold=60, window_seconds=60, comparison="gt"),
]
logger.addHandler(AlertingHandler(rules))
```

---

## Centralized Logging Architecture

### ELK Stack (Elasticsearch, Logstash, Kibana)

```python
# Application: emit JSON logs to stdout
import structlog

structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer()
    ]
)

# Filebeat config (sidecar or agent) ships to Logstash
# filebeat.yml:
# filebeat.inputs:
#   - type: container
#     paths: ['/var/log/containers/*.log']
#     json.keys_under_root: true
# output.logstash:
#   hosts: ["logstash:5044"]
```

| Component | Role | Scaling Strategy |
|-----------|------|------------------|
| Filebeat | Ship logs from source | One per node/container |
| Logstash | Parse, enrich, route | Horizontal (stateless) |
| Elasticsearch | Store and index | Sharding + replicas |
| Kibana | Query and visualize | Single instance usually |

---

## Sampling Strategies for High-Volume Logs

At 1M+ events/second, logging everything is expensive.

```python
import logging
import random
import hashlib

class SamplingFilter(logging.Filter):
    """Sample logs based on configurable strategies."""
    
    def __init__(self, rate: float = 0.1, deterministic_key: str | None = None):
        super().__init__()
        self.rate = rate  # 0.1 = keep 10%
        self.deterministic_key = deterministic_key
    
    def filter(self, record: logging.LogRecord) -> bool:
        # Always keep ERROR and above
        if record.levelno >= logging.ERROR:
            return True
        
        # Deterministic sampling: same key always sampled same way
        if self.deterministic_key:
            key_value = getattr(record, self.deterministic_key, '')
            hash_val = int(hashlib.md5(str(key_value).encode()).hexdigest(), 16)
            return (hash_val % 100) < (self.rate * 100)
        
        # Random sampling
        return random.random() < self.rate

class AdaptiveSamplingFilter(logging.Filter):
    """Increase sampling rate when error rate is high."""
    
    def __init__(self, base_rate: float = 0.01, error_boost_rate: float = 1.0):
        super().__init__()
        self.base_rate = base_rate
        self.error_boost_rate = error_boost_rate
        self._recent_errors = 0
        self._recent_total = 0
    
    def filter(self, record: logging.LogRecord) -> bool:
        self._recent_total += 1
        if record.levelno >= logging.ERROR:
            self._recent_errors += 1
            return True  # Always keep errors
        
        # Boost sampling when error rate is high
        error_rate = self._recent_errors / max(self._recent_total, 1)
        effective_rate = self.base_rate + (error_rate * self.error_boost_rate)
        
        # Reset counters periodically
        if self._recent_total > 10000:
            self._recent_errors = 0
            self._recent_total = 0
        
        return random.random() < min(effective_rate, 1.0)
```

---

## Interview Tips

> **Tip 1:** "How do you handle logging in PySpark?" — "The driver uses standard Python logging. Executors require separate setup — either configure logging inside map/mapPartitions functions, or use Spark's log4j integration. The key challenge is correlation: pass a run_id via broadcast variables so you can trace a job across driver and executor logs in your central logging system."

> **Tip 2:** "How would you design observability for a 10TB/day data pipeline?" — "Three pillars: (1) Structured logs with correlation IDs flowing to Elasticsearch or CloudWatch for debugging. (2) Metrics (row counts, latencies, error rates) emitted to DataDog/Prometheus for dashboards and alerting. (3) Distributed tracing with OpenTelemetry linking stages across services. For high volume, sample DEBUG/INFO logs but always keep ERROR+."

> **Tip 3:** "What sampling strategies work for high-volume logging?" — "Three approaches: (1) Rate-based sampling — keep N% of INFO logs, always keep ERROR+. (2) Deterministic sampling by key — same user_id always logged or not (useful for debugging specific accounts). (3) Adaptive sampling — increase rate when error rate spikes, so you capture more context around failures. The goal is cost control without losing signal."

## ⚡ Cheat Sheet

**Log Levels and When to Use**
| Level | Use For |
|-------|---------|
| DEBUG | Detailed trace (disable in prod) |
| INFO | Normal operations, record counts, durations |
| WARNING | Unexpected but recoverable (backpressure, retry) |
| ERROR | Failed operation, partial data loss |
| CRITICAL | System cannot continue, pipeline halted |

**Structured Logging Must-Haves**
- Always include: `pipeline`, `step`, `run_id`, `timestamp`, `level`
- Use `structlog` or `python-json-logger` — not `logging.Formatter` with `%s`
- Correlation ID: generate `run_id = uuid4()[:8]` at job start; pass everywhere
- In Spark: broadcast `run_id` to executors; configure logging inside `mapPartitions`

**PySpark Logging Rules**
- Driver: standard `logging.getLogger()` works normally
- Executor: must configure logging INSIDE `mapPartitions` / UDF functions (not module-level)
- Executor logs go to YARN/K8s stdout — aggregate via CloudWatch/Datadog agent
- Pass `run_id` via `spark.sparkContext.broadcast(run_id)`

**Custom Handler Patterns**
- `CloudWatchHandler`: buffer 50 events → `put_log_events()` in batches; track `sequenceToken`
- `MetricsExtractionHandler`: read structured fields from `LogRecord` → emit to statsd/DataDog
- `AlertingHandler`: sliding window per rule; average value > threshold → fire alert

**ELK Stack Components**
| Component | Role | Scale Strategy |
|-----------|------|----------------|
| Filebeat | Ship logs | One per node/container |
| Logstash | Parse + enrich + route | Horizontal (stateless) |
| Elasticsearch | Index + store | Sharding + replicas |
| Kibana | Query + visualize | Single instance |

**Sampling Strategies (High Volume)**
- Always keep `ERROR` and above — never sample errors
- Random sampling: keep `rate=0.01` (1%) of INFO in steady state
- Deterministic: `hash(user_id) % 100 < rate*100` — same user always in/out sample
- Adaptive: boost rate when error rate spikes — more context during failures
- OpenTelemetry: inject `trace_id` + `span_id` into every log automatically
