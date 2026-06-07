---
title: "Python Error Handling - Scenario Questions"
topic: python
subtopic: error-handling
content_type: scenario_question
tags: [python, error-handling, interview, scenarios, retry, circuit-breaker, dlq]
---

# Scenario Questions — Python Error Handling

<article data-difficulty="junior">

## Junior: Fix the Bare Except Anti-Pattern

**Scenario:** A colleague wrote this data extraction function. It "works" but swallows all errors, making debugging impossible. Refactor it with proper error handling.

```python
# BROKEN CODE
def extract_user_events(api_url, date_range):
    try:
        response = requests.get(api_url, params={"start": date_range[0], "end": date_range[1]})
        data = response.json()
        events = []
        for item in data["results"]:
            events.append({
                "user_id": item["user_id"],
                "amount": float(item["amount"]),
                "timestamp": item["ts"]
            })
        return events
    except:
        return []
```

<details>
<summary>Hint</summary>

Think about what can go wrong at each step: network errors, bad JSON, missing fields, type conversion. Each deserves different handling.

</details>

<details>
<summary>Solution</summary>

```python
import requests
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class ExtractionError(Exception):
    def __init__(self, message: str, retryable: bool = False):
        super().__init__(message)
        self.retryable = retryable

def extract_user_events(api_url: str, date_range: tuple[str, str]) -> list[dict]:
    """
    Extract user events with proper error handling.
    Raises ExtractionError for caller to handle.
    Returns list of valid events (skipping malformed records).
    """
    # Step 1: Network request
    try:
        response = requests.get(
            api_url,
            params={"start": date_range[0], "end": date_range[1]},
            timeout=30
        )
        response.raise_for_status()
    except requests.Timeout:
        raise ExtractionError(f"API timeout: {api_url}", retryable=True)
    except requests.ConnectionError:
        raise ExtractionError(f"Cannot connect to {api_url}", retryable=True)
    except requests.HTTPError as e:
        retryable = e.response.status_code >= 500
        raise ExtractionError(f"HTTP {e.response.status_code}", retryable=retryable)
    
    # Step 2: Parse JSON
    try:
        data = response.json()
    except ValueError:
        raise ExtractionError("Invalid JSON response", retryable=False)
    
    # Step 3: Extract records (skip individual bad records)
    if "results" not in data:
        raise ExtractionError("Response missing 'results' field", retryable=False)
    
    events = []
    skipped = 0
    for i, item in enumerate(data["results"]):
        try:
            events.append({
                "user_id": item["user_id"],
                "amount": float(item["amount"]),
                "timestamp": item["ts"]
            })
        except (KeyError, ValueError, TypeError) as e:
            skipped += 1
            logger.warning(f"Skipping malformed record {i}: {e}")
    
    if skipped > 0:
        logger.info(f"Extracted {len(events)} events, skipped {skipped} malformed")
    
    return events
```

**Key improvements:**
- Specific exception types for each failure mode
- Retryable vs non-retryable distinction
- Individual bad records don't kill the whole extraction
- Logging provides debugging visibility
- Caller can make informed retry decisions

</details>

</article>

<article data-difficulty="mid-level">

## Mid-Level: Design Retry Logic with Dead-Letter Queue

**Scenario:** Design a function that processes records from a Kafka topic. Requirements:
1. Each record gets up to 3 processing attempts
2. Exponential backoff between retries
3. After 3 failures, send to a dead-letter topic
4. Track and report success/failure metrics
5. Don't let one bad record block the stream

<details>
<summary>Hint</summary>

Think about per-record retry state, distinguishing transient vs permanent failures, and how the DLQ preserves data for later investigation.

</details>

<details>
<summary>Solution</summary>

```python
import time
import json
import logging
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)

@dataclass
class ProcessingMetrics:
    processed: int = 0
    succeeded: int = 0
    retried: int = 0
    dead_lettered: int = 0
    
    @property
    def success_rate(self) -> float:
        return self.succeeded / max(self.processed, 1)

@dataclass
class RetryPolicy:
    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    retryable_errors: tuple = (ConnectionError, TimeoutError, IOError)

def process_with_dlq(
    records,  # Kafka consumer or any iterable
    processor: Callable[[Dict], None],
    dlq_producer,  # Kafka producer for DLQ topic
    retry_policy: RetryPolicy = None
) -> ProcessingMetrics:
    """
    Process records with per-record retry and dead-letter queue.
    """
    policy = retry_policy or RetryPolicy()
    metrics = ProcessingMetrics()
    
    for record in records:
        metrics.processed += 1
        success = _process_single_record(
            record, processor, dlq_producer, policy, metrics
        )
        
        if not success:
            metrics.dead_lettered += 1
        else:
            metrics.succeeded += 1
        
        # Log progress periodically
        if metrics.processed % 1000 == 0:
            logger.info(
                f"Progress: {metrics.processed} processed, "
                f"{metrics.success_rate:.1%} success rate, "
                f"{metrics.dead_lettered} dead-lettered"
            )
    
    return metrics

def _process_single_record(
    record: Dict,
    processor: Callable,
    dlq_producer,
    policy: RetryPolicy,
    metrics: ProcessingMetrics
) -> bool:
    """Process one record with retries. Returns True if successful."""
    last_error = None
    
    for attempt in range(1, policy.max_attempts + 1):
        try:
            processor(record)
            return True
        except policy.retryable_errors as e:
            last_error = e
            metrics.retried += 1
            
            if attempt < policy.max_attempts:
                delay = min(
                    policy.base_delay * (2 ** (attempt - 1)),
                    policy.max_delay
                )
                logger.warning(
                    f"Attempt {attempt}/{policy.max_attempts} failed: {e}. "
                    f"Retrying in {delay:.1f}s"
                )
                time.sleep(delay)
        except Exception as e:
            # Non-retryable error — send to DLQ immediately
            last_error = e
            break
    
    # All retries exhausted or non-retryable error
    _send_to_dlq(record, last_error, dlq_producer, policy.max_attempts)
    return False

def _send_to_dlq(record, error, dlq_producer, attempts_made):
    """Send failed record to dead-letter queue with context."""
    dlq_record = {
        "original_record": record,
        "error_type": type(error).__name__,
        "error_message": str(error)[:1000],
        "attempts_made": attempts_made,
        "dead_lettered_at": datetime.utcnow().isoformat(),
        "retryable": isinstance(error, (ConnectionError, TimeoutError)),
    }
    dlq_producer.send("pipeline-dlq", json.dumps(dlq_record).encode())
    logger.warning(f"Record sent to DLQ: {type(error).__name__}: {error}")

# Usage
metrics = process_with_dlq(
    records=kafka_consumer,
    processor=transform_and_load,
    dlq_producer=kafka_dlq_producer,
    retry_policy=RetryPolicy(max_attempts=3, base_delay=2.0)
)
print(f"Final metrics: {metrics}")
```

</details>

</article>

<article data-difficulty="senior">

## Senior: Implement a Circuit Breaker for Multi-Service Pipeline

**Scenario:** Your pipeline depends on 3 external services (enrichment API, geo-lookup, fraud detection). Design a circuit breaker system that:
1. Opens after 5 consecutive failures per service
2. Half-opens after 30 seconds to test recovery
3. Allows the pipeline to degrade gracefully (skip enrichment if service is down)
4. Publishes circuit state changes as metrics
5. Supports per-service configuration

<details>
<summary>Hint</summary>

Think about the state machine (closed/open/half-open), thread safety, how to handle the half-open test requests, and what "graceful degradation" means for each service.

</details>

<details>
<summary>Solution</summary>

```python
import time
import threading
from enum import Enum
from typing import Callable, TypeVar, Optional, Dict
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)
T = TypeVar('T')

class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"

@dataclass
class CircuitConfig:
    failure_threshold: int = 5
    recovery_timeout: float = 30.0
    success_threshold: int = 2  # Successes needed to close from half-open
    
@dataclass
class CircuitMetrics:
    state_changes: int = 0
    total_calls: int = 0
    total_failures: int = 0
    total_short_circuits: int = 0

class ServiceCircuitBreaker:
    """Per-service circuit breaker with metrics."""
    
    def __init__(self, service_name: str, config: CircuitConfig = None):
        self.service_name = service_name
        self._config = config or CircuitConfig()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time = 0.0
        self._lock = threading.Lock()
        self.metrics = CircuitMetrics()
    
    @property
    def state(self) -> CircuitState:
        with self._lock:
            if (self._state == CircuitState.OPEN and 
                time.time() - self._last_failure_time >= self._config.recovery_timeout):
                self._transition_to(CircuitState.HALF_OPEN)
            return self._state
    
    def call(self, func: Callable[..., T], *args, **kwargs) -> T:
        self.metrics.total_calls += 1
        
        if self.state == CircuitState.OPEN:
            self.metrics.total_short_circuits += 1
            raise CircuitOpenError(self.service_name, self._time_until_retry())
        
        try:
            result = func(*args, **kwargs)
            self._record_success()
            return result
        except Exception as e:
            self._record_failure()
            raise
    
    def call_with_fallback(
        self, func: Callable[..., T], fallback: Callable[..., T], *args, **kwargs
    ) -> T:
        """Call with automatic fallback when circuit is open."""
        try:
            return self.call(func, *args, **kwargs)
        except CircuitOpenError:
            logger.info(f"Circuit open for {self.service_name}, using fallback")
            return fallback(*args, **kwargs)
    
    def _record_success(self):
        with self._lock:
            self._failure_count = 0
            if self._state == CircuitState.HALF_OPEN:
                self._success_count += 1
                if self._success_count >= self._config.success_threshold:
                    self._transition_to(CircuitState.CLOSED)
    
    def _record_failure(self):
        with self._lock:
            self._failure_count += 1
            self._last_failure_time = time.time()
            self.metrics.total_failures += 1
            
            if self._state == CircuitState.HALF_OPEN:
                self._transition_to(CircuitState.OPEN)
            elif self._failure_count >= self._config.failure_threshold:
                self._transition_to(CircuitState.OPEN)
    
    def _transition_to(self, new_state: CircuitState):
        old_state = self._state
        self._state = new_state
        self._success_count = 0
        self.metrics.state_changes += 1
        logger.warning(
            f"Circuit '{self.service_name}': {old_state.value} -> {new_state.value}"
        )
        publish_metric("circuit_state_change", {
            "service": self.service_name,
            "from": old_state.value,
            "to": new_state.value,
        })
    
    def _time_until_retry(self) -> float:
        return max(0, self._config.recovery_timeout - (time.time() - self._last_failure_time))

class CircuitOpenError(Exception):
    def __init__(self, service: str, retry_in: float):
        super().__init__(f"Circuit open for '{service}'. Retry in {retry_in:.0f}s")
        self.service = service
        self.retry_in = retry_in

# Multi-service pipeline with graceful degradation
class EnrichmentPipeline:
    def __init__(self):
        self.geo_breaker = ServiceCircuitBreaker("geo_lookup", CircuitConfig(failure_threshold=5))
        self.fraud_breaker = ServiceCircuitBreaker("fraud_detection", CircuitConfig(failure_threshold=3))
        self.profile_breaker = ServiceCircuitBreaker("profile_api", CircuitConfig(failure_threshold=5))
    
    def enrich_record(self, record: dict) -> dict:
        # Geo enrichment — degrade to "unknown" if service down
        record["geo"] = self.geo_breaker.call_with_fallback(
            lambda r: geo_api.lookup(r["ip"]),
            lambda r: {"country": "unknown", "city": "unknown"},
            record
        )
        
        # Fraud check — degrade to "unchecked" flag
        record["fraud_score"] = self.fraud_breaker.call_with_fallback(
            lambda r: fraud_api.score(r),
            lambda r: {"score": None, "checked": False},
            record
        )
        
        return record
```

</details>

</article>
