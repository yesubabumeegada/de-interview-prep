---
title: "Airflow Sensors - Scenario Questions"
topic: airflow
subtopic: sensors
content_type: scenario_question
tags: [airflow, sensors, poke, reschedule, deferrable, deadlock, ExternalTaskSensor]
---

# Airflow Sensors — Scenario Questions

<article data-difficulty="junior">

## 🟢 Scenario 1: Choosing the Right Sensor Mode

Your team has a DAG that runs at 6 AM daily. It starts with a `FileSensor` that waits for a vendor to drop a file on the network share. The vendor typically delivers the file anywhere between 6 AM and 9 AM. Your Airflow cluster has 8 Celery worker slots. In the past week, the team noticed that after 6 AM, no other DAGs in the cluster can run — everything queues up until around 9 AM when the vendor file arrives.

What is the root cause of this issue, and how do you fix it?

<details>
<summary>💡 Hint</summary>
Think about how the FileSensor holds resources while waiting. What happens when 8 sensors are all waiting at the same time? What configuration option changes this behavior?
</details>

<details>
<summary>✅ Solution</summary>

**Root Cause:** The `FileSensor` is running in the default `mode='poke'`. In poke mode, the sensor holds a Celery worker slot for its entire duration — it loops between `poke()` calls with a `time.sleep()` inside the worker process. With multiple DAGs running sensors simultaneously, all 8 slots become occupied by idle sensors, leaving no slots for actual processing tasks. This is **sensor deadlock**.

**Fix:** Switch to `mode='reschedule'`:

```python
from airflow.sensors.filesystem import FileSensor

wait_for_vendor_file = FileSensor(
    task_id='wait_for_vendor_file',
    filepath='/mnt/network_share/vendor/sales_{{ ds }}.csv',
    fs_conn_id='fs_default',
    mode='reschedule',        # Release worker slot between pokes
    poke_interval=300,        # Check every 5 minutes
    timeout=10800,            # Fail after 3 hours
    dag=dag,
)
```

**How reschedule mode fixes this:** In reschedule mode, the sensor writes a `TaskReschedule` record to the metadata DB with the next wake-up time, then raises `AirflowRescheduleException`, which terminates the worker process and frees the slot. The scheduler re-queues the sensor when `reschedule_date` passes. Between pokes, the worker slot is completely free for other tasks.

**Additional improvement:** Set an explicit `timeout`. The default is 7 days — without it, the sensor could run indefinitely if the vendor never delivers.
</details>

</article>

---

<article data-difficulty="junior">

## 🟢 Scenario 2: ExternalTaskSensor Not Detecting Upstream Failure

You have two DAGs: `upstream_etl` runs at 5 AM and `downstream_report` runs at 6 AM. The downstream DAG starts with an `ExternalTaskSensor` waiting for `upstream_etl`'s final task to reach `success` state. One morning, `upstream_etl` failed at 5:30 AM due to a Snowflake timeout. You expect `downstream_report` to also fail quickly, but instead it just keeps waiting until it times out 2 hours later, causing the report to be very late.

How do you fix the ExternalTaskSensor to propagate upstream failures immediately?

<details>
<summary>💡 Hint</summary>
ExternalTaskSensor has parameters that specify which states it should treat as success AND which states should trigger an immediate failure. Look at the `failed_states` parameter.
</details>

<details>
<summary>✅ Solution</summary>

**Root Cause:** `ExternalTaskSensor` by default only watches for `allowed_states` (default: `['success']`). It has no knowledge of failure states unless you configure `failed_states`. Without this, if the upstream task reaches `failed` state, the sensor doesn't recognize it as a terminal condition and keeps polling until timeout.

**Fix:** Add `failed_states`:

```python
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import timedelta

wait_for_upstream = ExternalTaskSensor(
    task_id='wait_for_upstream_etl',
    external_dag_id='upstream_etl',
    external_task_id='final_load_task',
    allowed_states=['success'],
    failed_states=['failed', 'skipped', 'upstream_failed'],  # Fail immediately
    mode='reschedule',
    poke_interval=120,
    timeout=3600,
    dag=dag,
)
```

**How it works:** When the sensor's next poke checks the upstream task state and finds it in `failed_states`, it raises `AirflowException` immediately rather than returning `False`. This fails the sensor task right away, triggering alerts and preventing the 2-hour wait.

**Best practice:** Always set `failed_states` when using `ExternalTaskSensor`. The typical set is `['failed', 'skipped', 'upstream_failed']`. Without it, you'll always wait the full timeout when upstream fails, which delays incident response.
</details>

</article>

---

<article data-difficulty="mid-level">

## 🟡 Scenario 3: Building a Custom Sensor for an Unsupported System

Your company uses an internal job orchestration platform called "JobRunner" that exposes a REST API. You need to submit a job to JobRunner and then wait for it to complete before running downstream transformations. There's no built-in Airflow sensor for JobRunner. The API endpoint `GET /jobs/{job_id}` returns:
- `{"status": "RUNNING", "progress": 45}` while running
- `{"status": "DONE", "output_path": "s3://..."}` on success
- `{"status": "FAILED", "error": "OOM"}` on failure

Design and implement a custom sensor for this use case.

<details>
<summary>💡 Hint</summary>
Subclass `BaseSensorOperator` and implement `poke()`. Consider: what should happen when the job fails (return False or raise an exception)? How do you pass the output_path to downstream tasks? What makes a field templatable?
</details>

<details>
<summary>✅ Solution</summary>

```python
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.http.hooks.http import HttpHook

class JobRunnerSensor(BaseSensorOperator):
    """
    Polls JobRunner API until the job reaches a terminal state.
    """

    # Allow job_id to accept Jinja templates like {{ ti.xcom_pull(...) }}
    template_fields = ['job_id']

    def __init__(
        self,
        job_id: str,
        http_conn_id: str = 'jobrunner_api',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_id = job_id
        self.http_conn_id = http_conn_id

    def poke(self, context) -> bool:
        hook = HttpHook(method='GET', http_conn_id=self.http_conn_id)

        try:
            response = hook.run(
                endpoint=f'/jobs/{self.job_id}',
                headers={'Accept': 'application/json'},
                extra_options={'timeout': 10},
            )
        except Exception as e:
            self.log.warning("Transient error: %s. Retrying.", e)
            return False

        data = response.json()
        status = data.get('status', 'UNKNOWN')
        self.log.info("Job %s status: %s", self.job_id, status)

        if status == 'FAILED':
            # Raise immediately — don't wait for timeout
            raise Exception(f"JobRunner job {self.job_id} failed: {data.get('error')}")

        if status == 'DONE':
            # Push output_path to XCom for downstream tasks
            context['ti'].xcom_push(key='output_path', value=data.get('output_path'))
            return True

        return False   # RUNNING or UNKNOWN — keep waiting
```

**Usage:**
```python
monitor_job = JobRunnerSensor(
    task_id='wait_for_jobrunner',
    job_id="{{ ti.xcom_pull(task_ids='submit_job', key='job_id') }}",
    http_conn_id='jobrunner_api',
    mode='reschedule',
    poke_interval=120,
    timeout=7200,
    dag=dag,
)
```

**Key decisions:**
1. `template_fields = ['job_id']` — makes `job_id` accept Jinja (e.g., XCom pull)
2. Raise on `FAILED` — immediate failure rather than waiting for timeout
3. Return `False` on transient errors — connectivity blips shouldn't fail the pipeline
4. XCom push in `poke()` when success — output path available to downstream tasks
5. Use `HttpHook` — handles connection credentials from Airflow Connections, not hardcoded URLs
</details>

</article>

---

<article data-difficulty="mid-level">

## 🟡 Scenario 4: Sensor Timeout and soft_fail for Optional Data Sources

Your pipeline processes sales data and optionally enriches it with partner marketing attribution data. The partner sends their data to S3 daily, but they're only contractually obligated to deliver it 3 business days per week. If the partner data doesn't arrive within 2 hours of your pipeline start, you want to proceed without it rather than failing the entire run.

Design the sensor setup to handle this gracefully.

<details>
<summary>💡 Hint</summary>
Look at the `soft_fail` parameter on sensors. What state does a task enter when `soft_fail=True` and the sensor times out? How do downstream tasks react to a skipped upstream task?
</details>

<details>
<summary>✅ Solution</summary>

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.utils.state import TaskInstanceState

def enrich_with_partner_data(**context):
    """
    Enrich sales with partner attribution data.
    Only runs if the sensor found partner data (not skipped).
    """
    partner_data_path = f"s3://partner-data/attribution/dt={context['ds']}/"
    # ... load and merge partner data
    print(f"Enriching with partner data from {partner_data_path}")

def process_sales(**context):
    """Core sales processing — runs regardless of partner data."""
    print("Processing core sales data...")

with DAG(...) as dag:

    process_core = PythonOperator(
        task_id='process_core_sales',
        python_callable=process_sales,
    )

    # soft_fail=True: if timeout fires, task becomes SKIPPED (not FAILED)
    wait_for_partner = S3KeySensor(
        task_id='wait_for_partner_data',
        bucket_name='partner-data',
        bucket_key='attribution/dt={{ ds }}/data.parquet',
        aws_conn_id='aws_default',
        mode='reschedule',
        poke_interval=600,        # Check every 10 min
        timeout=7200,             # Give up after 2 hours
        soft_fail=True,           # SKIPPED on timeout, not FAILED
    )

    enrich = PythonOperator(
        task_id='enrich_with_partner_data',
        python_callable=enrich_with_partner_data,
        trigger_rule='all_success',   # Only runs if sensor succeeded
        # If sensor is SKIPPED, this task is also SKIPPED automatically
    )

    done = EmptyOperator(
        task_id='done',
        trigger_rule='none_failed_min_one_success',  # Succeeds even if enrich skipped
    )

    process_core >> wait_for_partner >> enrich >> done
```

**How it works:**
- If partner data arrives within 2 hours: sensor succeeds → `enrich` runs → DAG succeeds
- If partner data doesn't arrive: sensor times out → becomes **SKIPPED** (not FAILED) → `enrich` is also **SKIPPED** (default trigger_rule propagates skip) → `done` task uses `none_failed_min_one_success` to succeed anyway

**Key points:**
- `soft_fail=True` converts timeout from `FAILED` to `SKIPPED`
- With default `trigger_rule='all_success'`, downstream tasks are automatically skipped when upstream is skipped
- The terminal `done` task uses a permissive trigger rule so the DAG run shows `success` overall, not `failed` or `skipped`
</details>

</article>

---

<article data-difficulty="senior">

## 🔴 Scenario 5: Designing a High-Scale Sensor Architecture

Your company has 200 DAGs, each starting with an `S3KeySensor` waiting for daily data partitions. All DAGs start between 6 AM and 8 AM. Your Airflow cluster runs on Kubernetes with a KubernetesExecutor (one pod per task). The team reports that between 6–8 AM, the cluster spawns 200 sensor pods. Many sit idle for hours. Kubernetes HPA is scaling out aggressively, costing $3K/month in wasted compute. Additionally, the scheduler is slow to detect when sensors should re-queue, adding 10–20 minute lag to pipeline starts.

Propose a comprehensive architecture to reduce cost and latency.

<details>
<summary>💡 Hint</summary>
Think about the three levels of improvement: (1) individual sensor configuration (mode, pod resource requests), (2) cluster-level architecture (deferrable sensors, triggerer sizing), (3) pipeline design changes (sensor DAGs vs compute DAGs). Consider what each approach addresses.
</details>

<details>
<summary>✅ Solution</summary>

**Problem Analysis:**
- 200 pods × (0–2h wait) = massive wasted KubernetesExecutor resources
- `reschedule` mode creates pod churn (spawn, poke, terminate, repeat) — K8s overhead
- Scheduler lag on reschedule re-queuing adds detection latency

**Solution: Three-Layer Approach**

**Layer 1: Migrate to Deferrable Sensors (highest impact)**

```python
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensorAsync

# Replace S3KeySensor with deferrable variant
wait_for_partition = S3KeySensorAsync(
    task_id='wait_for_partition',
    bucket_name='data-lake',
    bucket_key='raw/{{ dag.dag_id }}/dt={{ ds }}/data.parquet',
    aws_conn_id='aws_default',
    poke_interval=60,
    timeout=14400,
    # No pod runs during the wait — triggerer handles it
)
```

Result: 200 sensors → 0 pods during wait. The triggerer process handles all polling via asyncio coroutines. Scale the triggerer to 2 replicas for HA.

**Layer 2: Right-size the triggerer**

```yaml
# Kubernetes deployment for triggerer
resources:
  requests:
    cpu: "500m"
    memory: "512Mi"
  limits:
    cpu: "2"
    memory: "2Gi"
# One triggerer handles 1000+ concurrent triggers
replicas: 2   # HA
```

**Layer 3: Separate Sensor DAGs from Compute DAGs**

```python
# sensor_dag.py — lightweight DAG that only waits
with DAG('sensor_daily_sales', schedule_interval='0 6 * * *', ...) as sensor_dag:
    wait = S3KeySensorAsync(
        task_id='wait_for_data',
        bucket_name='data-lake',
        bucket_key='raw/sales/dt={{ ds }}/data.parquet',
        ...
    )
    trigger_compute = TriggerDagRunOperator(
        task_id='trigger_compute',
        trigger_dag_id='compute_daily_sales',
        conf={'ds': '{{ ds }}'},
        wait_for_completion=False,
    )
    wait >> trigger_compute

# compute_dag.py — only runs when data is confirmed
with DAG('compute_daily_sales', schedule_interval=None, ...) as compute_dag:
    # No sensor at start — data is already confirmed
    transform = PythonOperator(...)
    load = PythonOperator(...)
    transform >> load
```

**Layer 4: Reduce K8s pod overhead for remaining reschedule sensors**

```python
# Minimize pod resource requests for sensors
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from kubernetes.client import models as k8s

wait = S3KeySensor(
    task_id='wait',
    ...,
    mode='reschedule',
    executor_config={
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[k8s.V1Container(
                    name="base",
                    resources=k8s.V1ResourceRequirements(
                        requests={"cpu": "50m", "memory": "64Mi"},
                        limits={"cpu": "200m", "memory": "256Mi"},
                    ),
                )]
            )
        )
    }
)
```

**Expected Outcomes:**
- Deferrable sensors: 200 idle pods → ~0 idle pods (triggerer handles all waiting)
- Cost reduction: $2,500+/month in wasted EC2/GKE node capacity
- Latency reduction: Triggerer detects S3 key existence within 60s; no scheduler re-queue lag
- Scheduler load: Fewer reschedule records, less DB polling

**Monitoring:**
```python
# Alert if deferrable sensors aren't actually deferring
# (can happen if triggerer is down)
SELECT classpath, COUNT(*) as trigger_count
FROM trigger
WHERE created_date > NOW() - INTERVAL '1 hour'
GROUP BY classpath;
```
</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is a sensor in Airflow and how does it differ from a regular operator?**
A: A sensor inherits from `BaseSensorOperator` and repeatedly calls its `poke()` method until a condition is met or a timeout is reached. Unlike regular operators that execute once and complete, sensors wait for an external condition (file arrival, partition availability, API response) before allowing downstream tasks to proceed.

**Q: What is the difference between `poke_mode` and `reschedule_mode` in sensors?**
A: In `poke` mode, the sensor occupies a worker slot for its entire wait duration, polling on an interval. In `reschedule` mode, the sensor releases its worker slot between poke attempts and is rescheduled by the Airflow scheduler — freeing the slot for other tasks. `reschedule` is preferred for long-wait sensors to avoid worker starvation.

**Q: What happens when a sensor times out?**
A: When `timeout` seconds elapse without the condition being met, Airflow raises `AirflowSensorTimeout`, marking the task as `failed`. Configure `soft_fail=True` to mark it `skipped` instead of `failed` — useful when the condition's absence should not block the entire pipeline.

**Q: What is `ExternalTaskSensor` and when would you use it?**
A: `ExternalTaskSensor` waits for a task (or entire DAG) in a different DAG to reach a specified state (success by default). Use it to create cross-DAG dependencies without coupling DAG code together — e.g., wait for an upstream data pipeline's daily run to succeed before triggering a downstream ML pipeline.

**Q: What is the `poke_interval` parameter and how do you choose an appropriate value?**
A: `poke_interval` (seconds) controls how often the sensor checks the condition. Choose based on: how quickly the condition is expected to be met (use shorter intervals for time-sensitive waits), and the cost of checking (avoid hammering APIs with very short intervals). A typical range is 30-300 seconds.

**Q: How do you implement a custom sensor for a proprietary internal API?**
A: Subclass `BaseSensorOperator`, implement `poke(self, context)` to call the API and return `True` when the condition is met, `False` otherwise. Handle exceptions within `poke()` — unhandled exceptions cause task failure immediately rather than triggering a retry poke. Optionally use a custom Hook for connection management.

**Q: What is the `deferrable` sensor mode introduced in Airflow 2.2 and why is it important?**
A: Deferrable operators (including sensors) pause execution and defer to a Triggerer process instead of holding a worker slot. The Triggerer is an async process that can manage thousands of deferred tasks with minimal resource usage. This is the most efficient approach for high-scale environments with many waiting sensors.

---

## 💼 Interview Tips

- The `poke` vs. `reschedule` vs. `deferrable` progression is a key interview discussion point — showing you know all three and when each is appropriate signals progressive familiarity with Airflow's evolution.
- Always mention the worker slot starvation problem with poke-mode sensors — this is a real production incident waiting to happen and senior interviewers know to probe for it.
- `ExternalTaskSensor` with `execution_date_fn` for non-matching schedules is an advanced but commonly needed pattern — mentioning it distinguishes you from candidates with only basic sensor knowledge.
- When discussing deferrable operators, show awareness of the Triggerer component — it's a relatively new architectural addition that requires a separate process and is not available in all managed Airflow versions.
- Discuss timeout strategy: always set `timeout` explicitly. A sensor without a timeout can wait forever, holding a worker slot or blocking downstream tasks indefinitely.
- A common mistake to highlight: using sensors to poll rapidly at 5-second intervals — this generates excessive metadata database writes and API load. Discuss appropriate backoff and interval selection.
