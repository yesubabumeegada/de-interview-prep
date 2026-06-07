---
title: "Python Error Handling - Real-World Production Examples"
topic: python
subtopic: error-handling
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [python, error-handling, production, etl, retry, notifications, graceful-shutdown]
---

# Python Error Handling — Real-World Production Examples

## Pattern 1: ETL Error Handling with Record Quarantine

Production pipeline that separates good records from bad without stopping:

```python
"""
ETL pipeline with three-tier error handling:
1. Record-level: quarantine individual bad records
2. Batch-level: retry failed batches
3. Pipeline-level: halt if error threshold exceeded
"""
import json
import logging
from datetime import datetime
from typing import Iterator, Dict, List, Tuple
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

@dataclass
class QuarantineEntry:
    record: Dict
    error_type: str
    error_message: str
    step: str
    timestamp: str = field(default_factory=lambda: datetime.utcnow().isoformat())

class ETLErrorHandler:
    """Production error handler with quarantine and thresholds."""
    
    def __init__(
        self,
        pipeline_name: str,
        error_threshold: float = 0.05,
        quarantine_path: str = "s3://data-lake/quarantine/"
    ):
        self.pipeline_name = pipeline_name
        self.error_threshold = error_threshold
        self.quarantine_path = quarantine_path
        self._quarantine_buffer: List[QuarantineEntry] = []
        self._total_processed = 0
        self._total_errors = 0
    
    def process_records(
        self,
        records: Iterator[Dict],
        transform_func,
        step_name: str
    ) -> Iterator[Dict]:
        """Process records, quarantining failures."""
        for record in records:
            self._total_processed += 1
            try:
                yield transform_func(record)
            except (ValueError, KeyError, TypeError) as e:
                self._handle_record_error(record, e, step_name)
            except Exception as e:
                # Unexpected error — quarantine but also log at ERROR level
                logger.error(f"Unexpected error in {step_name}: {e}", exc_info=True)
                self._handle_record_error(record, e, step_name)
            
            # Check threshold periodically
            if self._total_processed % 10000 == 0:
                self._check_threshold()
    
    def _handle_record_error(self, record: Dict, error: Exception, step: str):
        self._total_errors += 1
        entry = QuarantineEntry(
            record=record,
            error_type=type(error).__name__,
            error_message=str(error)[:500],
            step=step
        )
        self._quarantine_buffer.append(entry)
        
        # Flush quarantine buffer periodically
        if len(self._quarantine_buffer) >= 1000:
            self._flush_quarantine()
    
    def _check_threshold(self):
        error_rate = self._total_errors / max(self._total_processed, 1)
        if error_rate > self.error_threshold:
            self._flush_quarantine()
            raise PipelineHaltError(
                f"Error rate {error_rate:.2%} exceeds threshold "
                f"{self.error_threshold:.2%}. "
                f"Processed: {self._total_processed}, Errors: {self._total_errors}"
            )
    
    def _flush_quarantine(self):
        if not self._quarantine_buffer:
            return
        # Write to S3 quarantine location
        partition = datetime.utcnow().strftime("%Y/%m/%d/%H")
        path = f"{self.quarantine_path}{self.pipeline_name}/{partition}/batch.jsonl"
        write_jsonl_to_s3(path, [e.__dict__ for e in self._quarantine_buffer])
        logger.info(f"Flushed {len(self._quarantine_buffer)} records to quarantine")
        self._quarantine_buffer = []
    
    def finalize(self) -> Dict:
        """Call at pipeline end — flush remaining and return stats."""
        self._flush_quarantine()
        return {
            "total_processed": self._total_processed,
            "total_errors": self._total_errors,
            "error_rate": self._total_errors / max(self._total_processed, 1),
            "quarantine_path": self.quarantine_path,
        }

# Usage
handler = ETLErrorHandler("daily_user_pipeline", error_threshold=0.02)

raw_records = extract_from_source()
clean_records = handler.process_records(raw_records, validate_and_transform, "transform")
load_to_warehouse(clean_records)

stats = handler.finalize()
logger.info(f"Pipeline stats: {stats}")
```

---

## Pattern 2: API Retry with Exponential Backoff

Production-grade API client with intelligent retry logic:

```python
"""
API client with comprehensive retry handling.
Handles: rate limits, transient failures, auth refresh, circuit breaking.
"""
import time
import random
import requests
from typing import Optional, Dict, Any
from dataclasses import dataclass, field
import logging

logger = logging.getLogger(__name__)

@dataclass
class RetryConfig:
    max_attempts: int = 5
    base_delay: float = 1.0
    max_delay: float = 60.0
    jitter_factor: float = 0.5
    retryable_status_codes: tuple = (429, 500, 502, 503, 504)

class ResilientAPIClient:
    """API client with production error handling."""
    
    def __init__(
        self,
        base_url: str,
        auth_token: str,
        retry_config: RetryConfig = None
    ):
        self.base_url = base_url
        self._session = requests.Session()
        self._session.headers["Authorization"] = f"Bearer {auth_token}"
        self._config = retry_config or RetryConfig()
        self._consecutive_failures = 0
    
    def get(self, endpoint: str, params: Dict = None) -> Dict[str, Any]:
        """GET with retry, rate limit handling, and circuit breaking."""
        url = f"{self.base_url}/{endpoint}"
        
        for attempt in range(1, self._config.max_attempts + 1):
            try:
                response = self._session.get(url, params=params, timeout=30)
                
                # Rate limit handling
                if response.status_code == 429:
                    retry_after = self._get_retry_after(response)
                    logger.warning(f"Rate limited. Waiting {retry_after}s")
                    time.sleep(retry_after)
                    continue
                
                # Retryable server errors
                if response.status_code in self._config.retryable_status_codes:
                    self._wait_with_backoff(attempt)
                    continue
                
                # Non-retryable client errors
                if 400 <= response.status_code < 500:
                    raise APIClientError(
                        f"Client error {response.status_code}: {response.text[:200]}",
                        status_code=response.status_code,
                        retryable=False
                    )
                
                response.raise_for_status()
                self._consecutive_failures = 0
                return response.json()
                
            except requests.ConnectionError as e:
                self._consecutive_failures += 1
                if attempt == self._config.max_attempts:
                    raise APIConnectionError(
                        f"Connection failed after {attempt} attempts: {e}"
                    ) from e
                self._wait_with_backoff(attempt)
                
            except requests.Timeout as e:
                if attempt == self._config.max_attempts:
                    raise APITimeoutError(
                        f"Request timed out after {attempt} attempts"
                    ) from e
                self._wait_with_backoff(attempt)
        
        raise APIExhaustedError(f"All {self._config.max_attempts} attempts failed")
    
    def _wait_with_backoff(self, attempt: int):
        delay = min(
            self._config.base_delay * (2 ** (attempt - 1)),
            self._config.max_delay
        )
        jitter = delay * self._config.jitter_factor * random.random()
        total_wait = delay + jitter
        logger.info(f"Retry attempt {attempt}, waiting {total_wait:.1f}s")
        time.sleep(total_wait)
    
    def _get_retry_after(self, response) -> float:
        retry_header = response.headers.get("Retry-After", "5")
        try:
            return float(retry_header)
        except ValueError:
            return 5.0

class APIClientError(Exception):
    def __init__(self, message, status_code=None, retryable=False):
        super().__init__(message)
        self.status_code = status_code
        self.retryable = retryable
```

---

## Pattern 3: Pipeline Failure Notifications

Multi-channel alerting system for pipeline failures:

```python
"""
Pipeline notification system.
Routes alerts based on severity and time-of-day.
"""
from abc import ABC, abstractmethod
from typing import List, Dict
from datetime import datetime, time
from dataclasses import dataclass
from enum import Enum
import logging

logger = logging.getLogger(__name__)

class AlertSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

@dataclass
class PipelineAlert:
    pipeline_name: str
    severity: AlertSeverity
    message: str
    details: Dict = None
    timestamp: datetime = None
    
    def __post_init__(self):
        self.timestamp = self.timestamp or datetime.utcnow()

class NotificationChannel(ABC):
    @abstractmethod
    def send(self, alert: PipelineAlert) -> bool:
        ...

class SlackNotifier(NotificationChannel):
    def __init__(self, webhook_url: str, channel: str):
        self.webhook_url = webhook_url
        self.channel = channel
    
    def send(self, alert: PipelineAlert) -> bool:
        import requests
        severity_emoji = {
            AlertSeverity.INFO: "info",
            AlertSeverity.WARNING: "warning",
            AlertSeverity.ERROR: "x",
            AlertSeverity.CRITICAL: "rotating_light",
        }
        payload = {
            "channel": self.channel,
            "text": f"[{alert.severity.value.upper()}] {alert.pipeline_name}: {alert.message}",
        }
        try:
            resp = requests.post(self.webhook_url, json=payload, timeout=5)
            return resp.status_code == 200
        except Exception:
            return False

class PagerDutyNotifier(NotificationChannel):
    def __init__(self, api_key: str, service_id: str):
        self.api_key = api_key
        self.service_id = service_id
    
    def send(self, alert: PipelineAlert) -> bool:
        # PagerDuty API integration
        logger.info(f"PagerDuty alert: {alert.message}")
        return True

class AlertRouter:
    """Routes alerts to appropriate channels based on rules."""
    
    def __init__(self):
        self._rules: List[tuple] = []
    
    def add_rule(
        self,
        min_severity: AlertSeverity,
        channel: NotificationChannel,
        business_hours_only: bool = False
    ):
        self._rules.append((min_severity, channel, business_hours_only))
        return self
    
    def route(self, alert: PipelineAlert):
        """Send alert to all matching channels."""
        severity_order = list(AlertSeverity)
        alert_idx = severity_order.index(alert.severity)
        is_business_hours = self._is_business_hours()
        
        for min_severity, channel, bh_only in self._rules:
            min_idx = severity_order.index(min_severity)
            if alert_idx >= min_idx:
                if bh_only and not is_business_hours:
                    continue
                try:
                    channel.send(alert)
                except Exception as e:
                    logger.error(f"Failed to send alert via {channel}: {e}")
    
    def _is_business_hours(self) -> bool:
        now = datetime.utcnow().time()
        return time(9, 0) <= now <= time(17, 0)

# Setup
router = AlertRouter()
router.add_rule(AlertSeverity.INFO, SlackNotifier(WEBHOOK, "#data-pipeline-info"), business_hours_only=True)
router.add_rule(AlertSeverity.ERROR, SlackNotifier(WEBHOOK, "#data-pipeline-alerts"))
router.add_rule(AlertSeverity.CRITICAL, PagerDutyNotifier(PD_KEY, SERVICE_ID))

# Usage in pipeline
try:
    run_pipeline()
except PipelineHaltError as e:
    router.route(PipelineAlert(
        pipeline_name="daily_user_etl",
        severity=AlertSeverity.CRITICAL,
        message=str(e),
        details={"error_rate": e.error_rate, "records_affected": e.count}
    ))
```

---

## Pattern 4: Graceful Shutdown

Handle termination signals to complete in-progress work:

```python
"""
Graceful shutdown for long-running data pipelines.
Handles SIGTERM/SIGINT to finish current batch before stopping.
"""
import signal
import threading
import logging
from typing import Iterator, Dict
from contextlib import contextmanager

logger = logging.getLogger(__name__)

class GracefulShutdown:
    """
    Coordinates graceful shutdown across pipeline components.
    
    Analogy: Like closing a restaurant — stop seating new guests,
    let current diners finish, then clean up and close.
    """
    
    def __init__(self):
        self._shutdown_requested = threading.Event()
        self._in_progress = threading.Event()
        self._register_signals()
    
    def _register_signals(self):
        signal.signal(signal.SIGTERM, self._handle_signal)
        signal.signal(signal.SIGINT, self._handle_signal)
    
    def _handle_signal(self, signum, frame):
        sig_name = signal.Signals(signum).name
        logger.warning(f"Received {sig_name} — initiating graceful shutdown")
        self._shutdown_requested.set()
    
    @property
    def should_continue(self) -> bool:
        """Check if pipeline should keep processing."""
        return not self._shutdown_requested.is_set()
    
    @contextmanager
    def batch_context(self, batch_id: str):
        """Mark a batch as in-progress for safe shutdown."""
        self._in_progress.set()
        logger.debug(f"Starting batch {batch_id}")
        try:
            yield
        finally:
            self._in_progress.clear()
            logger.debug(f"Completed batch {batch_id}")
    
    def wait_for_completion(self, timeout: float = 30.0) -> bool:
        """Wait for in-progress work to complete."""
        if self._in_progress.is_set():
            logger.info("Waiting for in-progress batch to complete...")
            self._in_progress.wait(timeout)
        return not self._in_progress.is_set()

# Usage in streaming pipeline
shutdown = GracefulShutdown()

def run_streaming_pipeline(source: Iterator[Dict]):
    """Process batches until shutdown is requested."""
    batch = []
    batch_num = 0
    
    for record in source:
        if not shutdown.should_continue:
            logger.info("Shutdown requested — finishing current batch")
            break
        
        batch.append(record)
        
        if len(batch) >= 10000:
            batch_num += 1
            with shutdown.batch_context(f"batch_{batch_num}"):
                process_and_load(batch)
            batch = []
    
    # Process remaining records in buffer
    if batch:
        batch_num += 1
        with shutdown.batch_context(f"batch_{batch_num}_final"):
            process_and_load(batch)
    
    logger.info(f"Pipeline shutdown complete. Processed {batch_num} batches.")

# In main
if __name__ == "__main__":
    source = kafka_consumer_stream("events-topic")
    run_streaming_pipeline(source)
```

---

## Interview Tips

> **Tip 1:** The quarantine pattern shows data stewardship. Frame it: "Every record is valuable. If I can't process it now, I quarantine it with full error context so we can fix the issue and reprocess later. The quarantine is partitioned by date and pipeline for easy investigation. We monitor quarantine growth rate as a data quality signal."

> **Tip 2:** For retry patterns, discuss the difference between retryable and non-retryable errors. A 503 is retryable (server overloaded), a 400 is not (bad request). Retrying non-retryable errors wastes resources and masks bugs. This distinction shows you think about error semantics, not just mechanics.

> **Tip 3:** Graceful shutdown is a senior-level concern that most candidates miss. In interviews, mention: "Our streaming pipeline handles SIGTERM by completing the current batch, committing offsets, and flushing buffers before exiting. This prevents data loss during deployments and autoscaling events."
