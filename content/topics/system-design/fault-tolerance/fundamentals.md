---
title: "Fault Tolerance & Reliability — Fundamentals"
topic: system-design
subtopic: fault-tolerance
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [system-design, fault-tolerance, reliability, retry, idempotency, checkpointing]
---

# Fault Tolerance & Reliability — Fundamentals


## 🎯 Analogy

Think of fault tolerance like redundancy in aircraft: two engines aren't because both are likely to fail, but because one can fail and you still land safely. In data systems, idempotency, retries, dead-letter queues, and checkpointing are your backup engines.

---
## Why Fault Tolerance Matters

In distributed data systems, failures are not exceptions — they are expected:
- Machines crash (disk failure, OOM, hardware fault)
- Networks partition (timeout, packet loss)
- Services restart (deployments, autoscaling)
- Dependencies are unavailable (rate limiting, outages)

A fault-tolerant system continues to operate correctly (or degrades gracefully) when components fail.

---

## Key Reliability Concepts

### SLA, SLO, SLI

```
SLI (Service Level Indicator): a metric that measures reliability
  Examples: pipeline success rate, data freshness lag, query latency p99

SLO (Service Level Objective): target value for an SLI
  Examples: pipeline success rate > 99.9%, data lag < 15 minutes, p99 query < 5s

SLA (Service Level Agreement): a contractual commitment based on SLOs
  Usually weaker than SLO (buffer for incidents)
  Example: SLO = 99.9% availability, SLA = 99.5% (0.4% buffer)

Availability expressed as nines:
  99%    = 87.6 hours downtime/year
  99.9%  = 8.76 hours downtime/year
  99.99% = 52.6 minutes downtime/year
  99.999%=  5.26 minutes downtime/year
```

---

## Retry Patterns

```python
# Basic retry with exponential backoff + jitter
import time
import random

def retry_with_backoff(func, max_attempts=5, base_delay=1.0, max_delay=60.0):
    for attempt in range(max_attempts):
        try:
            return func()
        except TransientError as e:
            if attempt == max_attempts - 1:
                raise  # last attempt: re-raise
            
            # Exponential backoff: 1s, 2s, 4s, 8s, 16s
            delay = min(base_delay * (2 ** attempt), max_delay)
            # Add jitter: prevent thundering herd (all retries at same time)
            jitter = random.uniform(0, delay * 0.1)
            time.sleep(delay + jitter)
        except PermanentError:
            raise  # non-retryable: fail immediately

# Retryable vs non-retryable errors:
# Retryable:   network timeout, rate limit (429), temporary unavailability (503)
# Non-retryable: auth error (401/403), not found (404), malformed request (400)
```

---

## Checkpointing

Checkpointing saves progress so a restarted job can resume where it left off:

```
Without checkpointing:
  Job at 70% complete → crash → restart from 0% → wasted work

With checkpointing:
  Job at 70% complete → checkpoint saved at 50% → crash → restart from 50% → only 20% redo

Checkpointing in Spark Structured Streaming:
  .option("checkpointLocation", "s3://bucket/checkpoints/orders")
  Stores: Kafka offsets + partial aggregation state
  On restart: reads from last checkpoint, resumes from that Kafka offset

Airflow checkpointing (XCom or state tables):
  task_instance.xcom_push('last_processed_id', 12345)
  # On retry: task reads this value and skips already-processed records

When to checkpoint:
  - Every N records (not too frequent → overhead; not too rare → too much redo)
  - After expensive operations (before writing to destination)
  - Natural transaction boundaries (end of each batch)
```

---

## Dead Letter Queues (DLQ)

When a message consistently fails processing, send it to a DLQ instead of blocking:

```
Normal flow: Kafka topic → consumer → process → success

Failure flow: Kafka topic → consumer → 3 retries fail → Dead Letter Queue
  DLQ stores: original message + error details + failure timestamp + retry count

DLQ benefits:
  - Failed messages don't block the main consumer
  - Messages are preserved for investigation (not lost)
  - Can replay DLQ messages after fixing the bug

DLQ pattern in Python:
  def process_message(msg):
      try:
          result = transform(msg)
          write_to_sink(result)
      except Exception as e:
          if msg.retry_count >= 3:
              send_to_dlq(msg, error=str(e))
          else:
              msg.retry_count += 1
              retry_queue.put(msg)

DLQ monitoring:
  Alert if DLQ depth > 0 (any failure needs investigation)
  Dashboard: DLQ message rate, oldest DLQ message age
```

---

## Circuit Breaker Pattern

Prevent cascading failures when a downstream service is degraded:

```
States:
  CLOSED:   normal operation; requests flow through
  OPEN:     failure threshold exceeded; requests fail immediately (no waiting)
  HALF-OPEN: probe state; allow a few requests to test recovery

Transition:
  CLOSED → OPEN:     when error rate > threshold (e.g., >50% in 60s window)
  OPEN → HALF-OPEN:  after timeout (e.g., 30 seconds)
  HALF-OPEN → CLOSED: if probe requests succeed
  HALF-OPEN → OPEN:  if probe requests fail

Benefits:
  - Fast failure: instead of waiting for timeout on each request, fail immediately
  - Allows downstream service to recover without being bombarded
  - Controlled degradation with fallback behavior
```

---


## ▶️ Try It Yourself

```python
import time, logging
from functools import wraps

logger = logging.getLogger(__name__)

def retry_with_backoff(max_retries: int = 3, base_delay: float = 1.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries - 1:
                        raise
                    delay = base_delay * (2 ** attempt)
                    logger.warning("Attempt %d failed, retrying in %.1fs: %s", attempt+1, delay, e)
                    time.sleep(delay)
        return wrapper
    return decorator

@retry_with_backoff(max_retries=3)
def load_to_warehouse(data: list) -> int:
    import random
    if random.random() < 0.5:
        raise ConnectionError("Warehouse unavailable")
    return len(data)

try:
    rows = load_to_warehouse([1, 2, 3])
    print(f"Loaded {rows} rows successfully")
except ConnectionError:
    print("Sending to dead-letter queue for manual investigation")
```

> **Run it:** Copy the snippet into a REPL or file — no external services needed for the basic example.

---
## Interview Tips

> **Tip 1:** "What is the difference between fault tolerance and high availability?" — Fault tolerance: the system continues working correctly even when components fail (no data loss, no errors). High availability: the system remains accessible and responsive (uptime). A system can be highly available but not fault tolerant (serves stale data during a failure). In DE: fault tolerance = pipeline retries and produces correct output; HA = pipeline runs 24/7 without downtime.

> **Tip 2:** "Why should retries use exponential backoff with jitter?" — Exponential backoff prevents hammering a struggling service (doubles wait time each attempt). Jitter adds randomness to the delay — without it, many clients would retry at exactly the same time after a failure (thundering herd), potentially overwhelming the service again when it recovers. Jitter spreads retries over time, smoothing the load.

> **Tip 3:** "When would you use a Dead Letter Queue?" — When a message fails processing after N retries and you need to: (1) not block the main pipeline while investigating, (2) preserve the failed message for debugging, (3) replay it after fixing the bug. Examples: a malformed event from a mobile app, a record that violates a foreign key constraint, a message that triggers an edge-case bug. DLQs decouple failure handling from the happy path.
