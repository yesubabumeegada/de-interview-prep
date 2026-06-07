---
title: "Error Handling - Intermediate"
topic: etl-concepts
subtopic: error-handling
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [etl, error-handling, circuit-breaker, poison-pill, kafka, alerting]
---

# Error Handling — Intermediate

## Circuit Breaker Pattern

The circuit breaker prevents a pipeline from repeatedly hammering a failing dependency, giving it time to recover.

```python
from enum import Enum
from datetime import datetime, timedelta
from threading import Lock

class CircuitState(Enum):
    CLOSED   = "closed"    # Normal operation
    OPEN     = "open"      # Failing; reject calls
    HALF_OPEN = "half_open" # Testing recovery

class CircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: int = 60,     # seconds
        success_threshold: int = 2      # successes needed to close
    ):
        self.failure_threshold  = failure_threshold
        self.recovery_timeout   = recovery_timeout
        self.success_threshold  = success_threshold

        self.state            = CircuitState.CLOSED
        self.failure_count    = 0
        self.success_count    = 0
        self.last_failure_at  = None
        self._lock            = Lock()

    def call(self, fn, *args, **kwargs):
        """Execute fn through the circuit breaker."""
        with self._lock:
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    print("Circuit: OPEN → HALF_OPEN (testing recovery)")
                else:
                    raise RuntimeError(
                        f"Circuit OPEN. Dependency unavailable since {self.last_failure_at}"
                    )

        try:
            result = fn(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise

    def _on_success(self):
        with self._lock:
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.success_threshold:
                    self.state         = CircuitState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    print("Circuit: HALF_OPEN → CLOSED (recovered)")
            elif self.state == CircuitState.CLOSED:
                self.failure_count = 0  # Reset on success

    def _on_failure(self):
        with self._lock:
            self.failure_count  += 1
            self.last_failure_at = datetime.utcnow()

            if self.failure_count >= self.failure_threshold:
                self.state = CircuitState.OPEN
                print(f"Circuit: CLOSED → OPEN after {self.failure_count} failures")

    def _should_attempt_reset(self) -> bool:
        if self.last_failure_at is None:
            return False
        return datetime.utcnow() > self.last_failure_at + timedelta(seconds=self.recovery_timeout)

# Usage
db_circuit = CircuitBreaker(failure_threshold=5, recovery_timeout=60)

def safe_db_call(sql: str, params: dict):
    def _execute():
        with engine.connect() as conn:
            return conn.execute(sa.text(sql), params).fetchall()
    return db_circuit.call(_execute)
```

---

## Poison Pill Messages

A **poison pill** is a message that always causes the consumer to fail, no matter how many times it's retried. Without handling, it blocks the entire queue.

### Detection and Routing

```python
from confluent_kafka import Consumer, Producer
import json

class PoisonPillConsumer:
    def __init__(self, consumer, dlq_producer, dlq_topic: str, max_retries: int = 3):
        self.consumer    = consumer
        self.dlq         = dlq_producer
        self.dlq_topic   = dlq_topic
        self.max_retries = max_retries
        self.retry_counts: dict[str, int] = {}  # {message_key: retry_count}

    def process(self, msg):
        """Process message; route to DLQ after max_retries."""
        key = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"

        try:
            event = json.loads(msg.value())
            self._process_event(event)
            # Success — clear retry count
            self.retry_counts.pop(key, None)
            self.consumer.commit(message=msg, asynchronous=False)

        except Exception as e:
            retries = self.retry_counts.get(key, 0) + 1
            self.retry_counts[key] = retries

            print(f"Message {key} failed (attempt {retries}): {e}")

            if retries >= self.max_retries:
                print(f"Routing poison pill to DLQ after {retries} attempts")
                self._send_to_dlq(msg, error=e, attempts=retries)
                self.retry_counts.pop(key, None)
                self.consumer.commit(message=msg, asynchronous=False)
            # else: don't commit — Kafka will re-deliver on next poll

    def _send_to_dlq(self, msg, error: Exception, attempts: int):
        """Send failed message to DLQ with error metadata."""
        import traceback, time
        self.dlq.produce(
            topic=self.dlq_topic,
            key=msg.key(),
            value=msg.value(),
            headers={
                "original_topic":   msg.topic().encode(),
                "original_partition": str(msg.partition()).encode(),
                "original_offset":  str(msg.offset()).encode(),
                "error_type":       type(error).__name__.encode(),
                "error_message":    str(error)[:500].encode(),
                "stack_trace":      traceback.format_exc()[:2000].encode(),
                "failed_at":        str(int(time.time())).encode(),
                "attempts":         str(attempts).encode(),
            }
        )
        self.dlq.flush()

    def _process_event(self, event: dict):
        # Business logic
        raise NotImplementedError
```

---

## Error Handling in PySpark

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, current_timestamp, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.getOrCreate()

def process_with_error_capture(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Apply transformation; capture malformed rows separately.
    Returns (good_df, error_df).
    """
    from pyspark.sql.functions import struct, to_json

    def safe_transform(row_struct):
        """UDF wrapper that catches exceptions and returns error info."""
        try:
            # Business transformation
            order_id   = row_struct["order_id"]
            amount_usd = float(row_struct["amount_str"])  # Can throw ValueError
            status     = row_struct["status"].strip().lower()

            if amount_usd < 0:
                raise ValueError(f"Negative amount: {amount_usd}")

            return json.dumps({
                "order_id": order_id,
                "amount_usd": amount_usd,
                "status": status,
                "error": None
            })
        except Exception as e:
            return json.dumps({
                "order_id": row_struct.get("order_id"),
                "error": str(e),
                "raw_data": str(row_struct)
            })

    safe_udf = udf(safe_transform, StringType())

    result = df.withColumn("processed", safe_udf(struct([col(c) for c in df.columns])))

    # Split good and bad rows
    from pyspark.sql.functions import from_json, get_json_object
    result = result.withColumn("error", get_json_object(col("processed"), "$.error"))

    good_df  = result.filter(col("error").isNull())
    error_df = result.filter(col("error").isNotNull()) \
                     .withColumn("captured_at", current_timestamp())

    return good_df, error_df

# Usage
raw_df = spark.read.json("s3://raw-data/orders/")
good_df, error_df = process_with_error_capture(raw_df)

good_df.write.mode("append").parquet("s3://processed/orders/")
error_df.write.mode("append").parquet("s3://errors/orders/")

print(f"Good: {good_df.count()}, Errors: {error_df.count()}")
```

---

## Alerting and Notification Patterns

### Multi-Channel Alert Router

```python
from dataclasses import dataclass
from enum import Enum

class AlertSeverity(Enum):
    LOW      = "low"
    MEDIUM   = "medium"
    HIGH     = "high"
    CRITICAL = "critical"

@dataclass
class Alert:
    pipeline:   str
    task:       str
    severity:   AlertSeverity
    message:    str
    run_date:   str
    error:      str = None
    log_url:    str = None

class AlertRouter:
    def __init__(self, slack_client, pagerduty_client, email_client):
        self.slack   = slack_client
        self.pager   = pagerduty_client
        self.email   = email_client

    def route(self, alert: Alert):
        """Route alert to appropriate channel based on severity."""
        if alert.severity == AlertSeverity.CRITICAL:
            self.pager.create_incident(
                title=f"CRITICAL: {alert.pipeline}.{alert.task}",
                body=self._format_body(alert),
                severity="critical"
            )
            self.slack.send(channel="#data-incidents", text=self._format_slack(alert))

        elif alert.severity == AlertSeverity.HIGH:
            self.slack.send(channel="#data-alerts-high", text=self._format_slack(alert))
            self.pager.create_incident(
                title=f"HIGH: {alert.pipeline}.{alert.task}",
                body=self._format_body(alert),
                severity="warning"
            )

        elif alert.severity == AlertSeverity.MEDIUM:
            self.slack.send(channel="#data-alerts", text=self._format_slack(alert))

        else:  # LOW
            self.slack.send(channel="#data-alerts-low", text=self._format_slack(alert))

    def _format_slack(self, alert: Alert) -> str:
        severity_emoji = {
            AlertSeverity.CRITICAL: ":red_circle:",
            AlertSeverity.HIGH:     ":large_orange_circle:",
            AlertSeverity.MEDIUM:   ":large_yellow_circle:",
            AlertSeverity.LOW:      ":white_circle:",
        }
        return f"""
{severity_emoji[alert.severity]} *{alert.severity.value.upper()}*: {alert.pipeline}.{alert.task}
*Date:* {alert.run_date}
*Message:* {alert.message}
{f'*Error:* `{alert.error[:200]}`' if alert.error else ''}
{f'<{alert.log_url}|View Logs>' if alert.log_url else ''}
        """.strip()

    def _format_body(self, alert: Alert) -> str:
        return f"Pipeline: {alert.pipeline}\nTask: {alert.task}\nDate: {alert.run_date}\nError: {alert.error}"
```

---

## Batch vs. Streaming Error Handling Comparison

| Concern | Batch Pipeline | Streaming Pipeline |
|---|---|---|
| Retry granularity | Entire task or partition | Individual message |
| DLQ | Table with failed records | Separate Kafka topic |
| Poison pill | Record skipped; logged | Max retry → DLQ topic |
| Error visibility | Airflow logs, monitoring | Consumer lag, DLQ depth |
| Recovery | Re-run failed tasks | Replay DLQ after fix |
| Partial failure | Continue with error threshold | Skip bad messages |
| State on failure | Idempotent re-run restores | Checkpoints restore position |

---

## Error Metrics to Track

```python
ERROR_METRICS = {
    "error_rate":            "Errors / Total records processed",
    "dlq_depth":             "Number of messages in DLQ (growing = ongoing issue)",
    "retry_rate":            "Retries / Total attempts (high = systemic instability)",
    "circuit_open_duration": "Time circuit breaker spent OPEN (dependency health)",
    "p99_error_latency":     "Time to detect + route an error to DLQ",
    "mean_time_to_recovery": "Average time from failure detection to resolution",
}
```

---

## Interview Tips

> **Tip 1:** The circuit breaker pattern prevents cascading failures. Without it, a slow/down dependency causes all pipeline workers to pile up, exhausting thread pools and database connections. The circuit breaker fails fast and gives the dependency time to recover.

> **Tip 2:** Distinguish the poison pill detection problem: without a max-retry limit, one bad message blocks the entire Kafka partition forever. The fix is to count retries and route to DLQ after the threshold — not just indefinitely retry.

> **Tip 3:** PySpark error handling via UDFs with error capture columns (`safe_transform` pattern) is the practical way to handle malformed records at scale. Bad rows go to a separate error dataset for investigation.

> **Tip 4:** Alerting routing by severity is a sign of operational maturity. Not every warning needs to wake someone up at 3 AM — reserve PagerDuty for truly critical failures.

> **Tip 5:** Track DLQ depth as a metric. A DLQ that's steadily growing means the root cause of the failures hasn't been fixed and you have accumulating technical debt.
