---
title: "Fault Tolerance — Scenarios"
topic: system-design
subtopic: fault-tolerance
content_type: scenario_question
tags: [fault-tolerance, reliability, resilience, scenarios]
---

# Fault Tolerance — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Handling Failures in a Batch Pipeline

**Scenario:** Your nightly Airflow DAG has 10 tasks. Task 7 fails due to a transient network error. The remaining tasks (8, 9, 10) don't run. Explain how you would configure the DAG to handle this gracefully without rerunning all 10 tasks from the start.

<details>
<summary>💡 Hint</summary>

Airflow has built-in retry logic and supports "clear and re-run from failed task." Key configs: `retries`, `retry_delay`, `depends_on_past`. For idempotent tasks, re-running from the failed task is safe.

</details>

<details>
<summary>✅ Solution</summary>

**Configuration: Retry Logic**

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,  # 5min, 10min, 20min
    'max_retry_delay': timedelta(minutes=30),
    'email_on_failure': True,
    'email': ['data-team@company.com']
}

with DAG(
    'nightly_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False
) as dag:

    # Task with custom retry config
    task_7 = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=load_to_snowflake,
        retries=5,                          # override default
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(hours=1)  # fail-fast if hung
    )
```

**Re-running from Failed Task (Airflow UI):**
1. Go to DAG Run → click failed task → "Clear" (not "Reset")
2. Airflow re-runs only the failed task and its downstream dependencies
3. Previously successful tasks are not re-run

**Making Tasks Idempotent (prerequisite for safe re-runs):**

```python
def load_to_snowflake(ds: str, **kwargs):
    """Idempotent: safe to run multiple times for same date."""
    # Delete+insert pattern for the target date
    snowflake_hook.run(f"DELETE FROM orders WHERE order_date = '{ds}'")
    # OR use MERGE INTO for upsert
    df.write.mode("overwrite")         .option("replaceWhere", f"order_date = '{ds}'")         .saveAsTable("prod.orders")
```

**Alerting on Failures:**

```python
def failure_callback(context):
    task_instance = context['task_instance']
    send_slack_message(
        channel='#data-alerts',
        text=f":red_circle: Task failed: {task_instance.task_id} "
             f"in DAG {task_instance.dag_id} "
             f"at {task_instance.execution_date}"
    )

task_7 = PythonOperator(
    on_failure_callback=failure_callback,
    ...
)
```

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Circuit Breaker Pattern for Unreliable Upstream APIs

**Scenario:** Your pipeline ingests data from a vendor API that is unreliable — it goes down for 30-60 minutes 2-3 times per week. Currently, your pipeline fails hard when the API is down, causing cascading failures in downstream jobs. Implement a circuit breaker pattern.

<details>
<summary>💡 Hint</summary>

A circuit breaker has three states: CLOSED (normal), OPEN (stop calling after N failures), HALF-OPEN (test if service recovered). When OPEN, fail fast without hitting the API. Reset after a cooldown period.

</details>

<details>
<summary>✅ Solution</summary>

**Circuit Breaker Implementation:**

```python
import time
import redis
from enum import Enum
from functools import wraps
from typing import Callable, Any

class CircuitState(Enum):
    CLOSED = "closed"        # Normal operation
    OPEN = "open"            # Failing fast
    HALF_OPEN = "half_open"  # Testing recovery

class CircuitBreaker:
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: int = 300,  # 5 minutes
        half_open_max_calls: int = 1
    ):
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self.redis = redis.Redis(host='redis', port=6379)

    def _get_state(self) -> CircuitState:
        state = self.redis.get(f"circuit:{self.name}:state")
        if state is None:
            return CircuitState.CLOSED
        return CircuitState(state.decode())

    def _get_failure_count(self) -> int:
        count = self.redis.get(f"circuit:{self.name}:failures")
        return int(count) if count else 0

    def _record_failure(self):
        pipe = self.redis.pipeline()
        pipe.incr(f"circuit:{self.name}:failures")
        pipe.expire(f"circuit:{self.name}:failures", 600)  # reset after 10 min
        pipe.execute()

        if self._get_failure_count() >= self.failure_threshold:
            self.redis.setex(
                f"circuit:{self.name}:state",
                self.recovery_timeout,
                CircuitState.OPEN.value
            )

    def _record_success(self):
        self.redis.delete(f"circuit:{self.name}:failures")
        self.redis.set(f"circuit:{self.name}:state", CircuitState.CLOSED.value)

    def call(self, func: Callable, *args, **kwargs) -> Any:
        state = self._get_state()

        if state == CircuitState.OPEN:
            raise Exception(
                f"Circuit {self.name} is OPEN. "
                f"Skipping API call. Retry after cooldown."
            )

        try:
            result = func(*args, **kwargs)
            self._record_success()
            return result
        except Exception as e:
            self._record_failure()
            raise

# Usage in pipeline
vendor_circuit = CircuitBreaker(
    name="vendor_api",
    failure_threshold=3,
    recovery_timeout=600  # 10 minutes
)

def fetch_vendor_data(date: str) -> list:
    def _fetch():
        response = requests.get(
            f"https://vendor.api/data?date={date}",
            timeout=30
        )
        response.raise_for_status()
        return response.json()

    return vendor_circuit.call(_fetch)

# Airflow task with circuit breaker + fallback
def ingest_vendor_data(ds: str, **kwargs):
    try:
        data = fetch_vendor_data(ds)
        process_and_store(data, ds)

    except Exception as e:
        if "Circuit" in str(e):
            # Circuit open: use cached/previous day's data as fallback
            print(f"Circuit open, using fallback data for {ds}")
            use_previous_day_data(ds)
            # Mark task as skipped, not failed (downstream can still run)
            raise AirflowSkipException(f"Vendor API unavailable: {e}")
        else:
            raise  # Real error — fail the task
```

**Monitoring Circuit State:**

```python
def circuit_health_check():
    for circuit_name in ["vendor_api", "crm_api", "payment_api"]:
        state = redis.get(f"circuit:{circuit_name}:state")
        failures = redis.get(f"circuit:{circuit_name}:failures")
        
        if state and state.decode() == "open":
            send_alert(f"Circuit OPEN: {circuit_name}, failures: {failures}")
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Designing a Resilient Multi-Region Data Pipeline

**Scenario:** Your data platform must achieve 99.9% availability (< 8.7 hours downtime/year). The primary region is us-east-1. Design a multi-region active-passive failover architecture for the critical path: ingestion → processing → serving.

<details>
<summary>💡 Hint</summary>

99.9% SLA requires automated failover — manual failover is too slow. Key components: active-passive S3 replication, Kafka MirrorMaker 2 for stream replication, DNS failover (Route53), and RTO/RPO targets per tier.

</details>

<details>
<summary>✅ Solution</summary>

**RTO/RPO Targets:**

| Tier | RTO | RPO | Tier |
|------|-----|-----|------|
| Ingestion (Kafka) | 5 min | 0 (no data loss) | Critical |
| Processing (Spark) | 15 min | 15 min | High |
| Serving (Trino/Snowflake) | 10 min | 5 min | Critical |

**Architecture:**

```
Primary (us-east-1)          DR (us-west-2)
─────────────────────        ──────────────────
Kafka Cluster ──MM2──────→  Kafka Cluster (replica)
     ↓                              ↓ (standby)
Flink Cluster                Flink Cluster (hot standby)
     ↓                              ↓
S3 Bucket ────replication──→ S3 Bucket (replica)
     ↓                              ↓
Trino Cluster                Trino Cluster (standby)
     ↓                              ↓
Route53 Health Check → DNS Failover
```

**Component 1: Kafka MirrorMaker 2 (Zero Data Loss)**

```yaml
# mirrormaker2.yaml
clusters: us-east-1, us-west-2
us-east-1.bootstrap.servers: kafka-east:9092
us-west-2.bootstrap.servers: kafka-west:9092

# Replicate all topics east → west
us-east-1->us-west-2.enabled: true
us-east-1->us-west-2.topics: .*
us-east-1->us-west-2.emit.heartbeats.enabled: true
us-east-1->us-west-2.emit.checkpoints.enabled: true
# Checkpoint interval: commit consumer offsets to DR cluster
us-east-1->us-west-2.emit.checkpoints.interval.seconds: 60
```

**Component 2: S3 Cross-Region Replication**

```python
# Enable cross-region replication for all lakehouse buckets
s3_control = boto3.client('s3control')

replication_config = {
    'Rules': [{
        'ID': 'replicate-all-to-dr',
        'Status': 'Enabled',
        'Filter': {'Prefix': ''},
        'Destination': {
            'Bucket': 'arn:aws:s3:::datalake-dr-us-west-2',
            'StorageClass': 'STANDARD_IA',  # Cheaper for DR
            'ReplicationTime': {
                'Status': 'Enabled',
                'Time': {'Minutes': 15}  # S3 RTC: 99.99% within 15 min
            }
        }
    }]
}
```

**Component 3: Automated Failover with Route53**

```python
import boto3

route53 = boto3.client('route53')

def check_primary_health() -> bool:
    """Returns True if primary is healthy."""
    try:
        r = requests.get("https://trino-east.internal/v1/status", timeout=5)
        return r.status_code == 200
    except:
        return False

def trigger_failover():
    """Update DNS to point to DR region."""
    route53.change_resource_record_sets(
        HostedZoneId='Z123ABC',
        ChangeBatch={
            'Changes': [{
                'Action': 'UPSERT',
                'ResourceRecordSet': {
                    'Name': 'trino.data.company.com',
                    'Type': 'CNAME',
                    'TTL': 60,  # Low TTL for fast failover
                    'ResourceRecords': [
                        {'Value': 'trino-west.internal'}  # DR endpoint
                    ]
                }
            }]
        }
    )
    send_pagerduty_alert("FAILOVER INITIATED: primary us-east-1 unhealthy")

# Health check Lambda (runs every 30s)
def lambda_handler(event, context):
    consecutive_failures = int(
        ssm.get_parameter(Name='/failover/consecutive_failures')['Parameter']['Value']
    )

    if not check_primary_health():
        consecutive_failures += 1
        ssm.put_parameter(Name='/failover/consecutive_failures',
                          Value=str(consecutive_failures), Overwrite=True)

        if consecutive_failures >= 3:  # 3 consecutive failures = failover
            trigger_failover()
    else:
        ssm.put_parameter(Name='/failover/consecutive_failures',
                          Value='0', Overwrite=True)
```

**Component 4: Flink Hot Standby**

```python
# Primary Flink job writes checkpoints to S3 (replicated to DR)
env.getCheckpointConfig().setCheckpointStorage(
    "s3://checkpoints-east/pipeline/"  # auto-replicated to west
)

# DR Flink cluster: restore from latest checkpoint
# Triggered by failover automation
dr_job_command = """
flink run     -s s3://checkpoints-west/pipeline/chk-12345/     -c com.company.pipeline.MainJob     pipeline.jar
"""
```

**Failover Runbook (automated):**
1. Health check fails 3× (90 seconds)
2. Lambda triggers Route53 DNS update (TTL=60s → propagates in ~2 minutes)
3. DR Flink jobs start from latest checkpoint (5 minutes)
4. Total RTO: ~8 minutes (within 15-minute target)
5. PagerDuty alert fired; on-call engineer validates DR health

**Chaos Engineering (quarterly drills):**
```python
# Chaos Monkey: randomly terminate primary Kafka brokers
def chaos_drill_kafka_failover():
    # Kill primary broker
    ec2.terminate_instances(InstanceIds=[primary_kafka_broker_id])
    
    # Measure time until Kafka MirrorMaker routes to DR
    start = time.time()
    while not check_kafka_dr_health():
        time.sleep(5)
    rto_seconds = time.time() - start
    
    assert rto_seconds < 300, f"Kafka failover took {rto_seconds}s > 5min SLA"
```

</details>

</article>

---

## Interview Tips

> **Tip 1:** "What is the difference between RTO and RPO?" — RTO (Recovery Time Objective) is how long the system can be down. RPO (Recovery Point Objective) is how much data can be lost. A payment system might have RTO=5min, RPO=0 (no data loss). An analytics dashboard might have RTO=2h, RPO=1h.
> **Tip 2:** "How do you test fault tolerance?" — Chaos engineering: intentionally inject failures in production-like environments (Chaos Monkey, Gremlin). Run quarterly DR drills with real failovers. Measure actual RTO/RPO vs targets.
> **Tip 3:** "What is a bulkhead pattern in data pipelines?" — Isolate failures to prevent cascading: separate thread pools per upstream source, separate Kafka consumer groups per downstream consumer, separate Snowflake warehouses per use case (analytics vs ingestion). A slow analytics query won't block real-time ingestion.
