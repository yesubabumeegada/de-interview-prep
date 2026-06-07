---
title: "Error Handling - Real World"
topic: etl-concepts
subtopic: error-handling
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [etl, error-handling, production, case-study, circuit-breaker, dlq]
---

# Error Handling — Real World

## Case Study 1: Cascading Failures from Missing Circuit Breaker

### Problem

A data platform team had 40 pipelines that all read from the same OLTP PostgreSQL database. When the database became slow during a traffic spike (Black Friday), all 40 pipelines simultaneously retried, each adding more load. This cascading effect took the source DB down for 45 minutes.

### Solution: Database Circuit Breaker with Shared State

```python
import redis
from datetime import datetime, timedelta

class SharedCircuitBreaker:
    """
    Circuit breaker backed by Redis for shared state across workers.
    All pipeline workers consult the same circuit state.
    """
    def __init__(self, redis_client: redis.Redis, service_name: str):
        self.redis        = redis_client
        self.service      = service_name
        self.state_key    = f"circuit:{service_name}:state"
        self.failure_key  = f"circuit:{service_name}:failures"
        self.last_fail_key = f"circuit:{service_name}:last_failure"

    def is_open(self) -> bool:
        """Check if circuit is open (should NOT call the service)."""
        state = self.redis.get(self.state_key)
        return state == b"open"

    def record_failure(self):
        """Record a failure; open circuit if threshold exceeded."""
        pipe = self.redis.pipeline()
        pipe.incr(self.failure_key)
        pipe.expire(self.failure_key, 60)  # Reset failure count every 60s
        failures, _ = pipe.execute()

        if failures >= 5:  # 5 failures in 60 seconds → open circuit
            self.redis.setex(self.state_key, 60, "open")  # Open for 60 seconds
            print(f"Circuit for {self.service} OPENED after {failures} failures")
            # Alert operations team
            send_alert(f"Circuit breaker opened for {self.service}")

    def record_success(self):
        """Record success; may help close circuit."""
        self.redis.delete(self.failure_key)
        self.redis.delete(self.state_key)

    def safe_call(self, fn, *args, **kwargs):
        if self.is_open():
            raise RuntimeError(f"Circuit OPEN for {self.service}. Skipping call.")
        try:
            result = fn(*args, **kwargs)
            self.record_success()
            return result
        except Exception as e:
            self.record_failure()
            raise

# Usage: All 40 pipelines share the same circuit state via Redis
redis_client  = redis.Redis(host="redis-host")
db_circuit    = SharedCircuitBreaker(redis_client, "oltp-postgres")

def extract_orders_safe(date: str) -> pd.DataFrame:
    def _extract():
        return pd.read_sql(
            f"SELECT * FROM orders WHERE order_date = '{date}'", engine
        )
    return db_circuit.safe_call(_extract)
```

### Impact

| Metric | Before | After |
|---|---|---|
| Black Friday incident duration | 45 minutes | 6 minutes (circuit opened, load dropped) |
| DB connections during incident | 1,200 (from retries) | ~40 (circuit rejected calls) |
| Data freshness degradation | 45 min stale | 6 min stale |

---

## Case Study 2: Building a Production DLQ System

### Problem

A streaming pipeline processing IoT sensor readings had occasional malformed JSON payloads. Without a DLQ, bad messages caused the consumer to crash and restart repeatedly, causing 10+ minute gaps in real-time monitoring.

### Solution: Multi-Level DLQ with Replay

```python
from confluent_kafka import Consumer, Producer, KafkaError
import json, time, traceback

class RobustIoTConsumer:
    def __init__(self, config: dict):
        self.consumer = Consumer({**config["kafka"], "enable.auto.commit": False})
        self.dlq_producer = Producer(config["kafka"])
        self.dlq_topic    = config["dlq_topic"]
        self.max_retries  = config.get("max_retries", 3)
        self.retry_counts: dict = {}
        self.target_engine = create_engine(config["target_db"])

    def run(self):
        self.consumer.subscribe([self.config["source_topic"]])

        while True:
            msg = self.consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError.PARTITION_EOF:
                    continue
                print(f"Consumer error: {msg.error()}")
                continue

            self._process_with_error_handling(msg)

    def _process_with_error_handling(self, msg):
        msg_key = f"{msg.topic()}:{msg.partition()}:{msg.offset()}"

        try:
            # Attempt to parse and validate
            raw = msg.value().decode("utf-8")
            payload = json.loads(raw)

            # Validate required fields
            required = ["sensor_id", "timestamp", "temperature", "humidity"]
            missing = [f for f in required if f not in payload]
            if missing:
                raise ValueError(f"Missing required fields: {missing}")

            if not (-50 <= payload["temperature"] <= 150):
                raise ValueError(f"Temperature out of range: {payload['temperature']}")

            # Write to time-series DB
            self._write_reading(payload)

            # Commit offset on success
            self.consumer.commit(message=msg, asynchronous=False)
            self.retry_counts.pop(msg_key, None)

        except json.JSONDecodeError as e:
            # Permanent error: malformed JSON will never parse
            print(f"JSON decode error — routing to DLQ immediately: {e}")
            self._send_to_dlq(msg, error=e, permanent=True)
            self.consumer.commit(message=msg, asynchronous=False)

        except ValueError as e:
            # Data validation error — may be permanent
            retries = self.retry_counts.get(msg_key, 0) + 1
            self.retry_counts[msg_key] = retries

            if retries >= self.max_retries:
                print(f"Validation failed after {retries} attempts — routing to DLQ")
                self._send_to_dlq(msg, error=e, permanent=False, attempts=retries)
                self.consumer.commit(message=msg, asynchronous=False)
            else:
                print(f"Validation error (attempt {retries}): {e} — will retry")
                time.sleep(2 ** retries)  # Backoff before Kafka re-delivers

        except Exception as e:
            # Transient error — retry with backoff
            retries = self.retry_counts.get(msg_key, 0) + 1
            self.retry_counts[msg_key] = retries

            if retries >= self.max_retries:
                self._send_to_dlq(msg, error=e, permanent=False, attempts=retries)
                self.consumer.commit(message=msg, asynchronous=False)
            else:
                time.sleep(min(2 ** retries, 60))

    def _send_to_dlq(self, msg, error, permanent: bool, attempts: int = 1):
        headers = {
            "original_topic":  msg.topic().encode(),
            "error_type":      type(error).__name__.encode(),
            "error_message":   str(error)[:1000].encode(),
            "is_permanent":    str(permanent).encode(),
            "attempts":        str(attempts).encode(),
            "failed_at":       str(int(time.time())).encode(),
        }
        self.dlq_producer.produce(
            topic=self.dlq_topic,
            key=msg.key(),
            value=msg.value(),
            headers=list(headers.items())
        )
        self.dlq_producer.flush()

    def _write_reading(self, payload: dict):
        with self.target_engine.begin() as conn:
            conn.execute(sa.text("""
                INSERT INTO sensor_readings (sensor_id, ts, temperature, humidity)
                VALUES (:sid, :ts, :temp, :humidity)
                ON CONFLICT (sensor_id, ts) DO NOTHING
            """), {
                "sid": payload["sensor_id"],
                "ts": payload["timestamp"],
                "temp": payload["temperature"],
                "humidity": payload["humidity"],
            })
```

---

## Case Study 3: Error Handling in a dbt Project

### dbt Error Alerting Setup

```python
# scripts/dbt_run_with_alerting.py
import subprocess
import json
from datetime import datetime

def run_dbt_with_alerting(
    project_dir: str,
    target: str,
    select: str = None,
    slack_webhook: str = None
) -> dict:
    """
    Run dbt and send structured alerts on failure.
    """
    cmd = ["dbt", "run", "--target", target]
    if select:
        cmd += ["--select", select]

    start = datetime.utcnow()
    result = subprocess.run(cmd, cwd=project_dir, capture_output=True, text=True)
    end   = datetime.utcnow()

    # Parse dbt output
    success = result.returncode == 0
    duration = (end - start).total_seconds()

    # Extract failed models from output
    failed_models = []
    for line in result.stdout.splitlines():
        if "ERROR" in line and "completed with" not in line:
            # Extract model name from dbt output format
            parts = line.split()
            if len(parts) > 1:
                failed_models.append(parts[-1].strip("."))

    run_result = {
        "success":       success,
        "duration_s":    duration,
        "failed_models": failed_models,
        "stdout":        result.stdout[-5000:],  # Last 5KB
        "stderr":        result.stderr[-2000:],
        "run_at":        start.isoformat(),
    }

    if not success and slack_webhook:
        send_slack_alert(slack_webhook, run_result)

    return run_result

def send_slack_alert(webhook: str, result: dict):
    import requests
    message = {
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f":red_circle: *dbt Run Failed*\n"
                            f"Duration: {result['duration_s']:.0f}s\n"
                            f"Failed models: `{'`, `'.join(result['failed_models'])}`"
                }
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"```{result['stdout'][-1000:]}```"
                }
            }
        ]
    }
    requests.post(webhook, json=message)
```

---

## Interview Tips

> **Tip 1:** The cascading failure story demonstrates systems thinking. "40 pipelines retrying simultaneously made the DB worse" — this is a real and common production problem, and the circuit breaker pattern (especially with shared state via Redis) is the textbook solution.

> **Tip 2:** Differentiating permanent errors (malformed JSON) from transient errors (network timeout) determines the handling strategy. Permanent errors go to DLQ immediately; transient errors are retried with backoff.

> **Tip 3:** The IoT sensor consumer shows production-level error handling: different treatment for different error types, per-message retry counting, backoff, DLQ routing, and offset management — all in one class.

> **Tip 4:** DLQ monitoring is as important as DLQ writing. A DLQ that fills up silently is a liability. Track DLQ depth as a dashboard metric and alert when it exceeds a threshold.

> **Tip 5:** For dbt pipelines, structured run result capture (failed model names, duration, logs) enables better post-mortems than just "the dbt run failed." Feed these results into a metrics table for trending.
