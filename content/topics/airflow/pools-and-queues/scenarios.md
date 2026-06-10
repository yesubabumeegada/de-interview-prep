---
title: "Airflow Pools and Queues - Scenario Questions"
topic: airflow
subtopic: pools-and-queues
content_type: scenario_question
tags: [airflow, pools, queues, concurrency, celery, resource-management]
---

# Airflow Pools and Queues — Scenario Questions

<article data-difficulty="junior">

## 🟢 Question 1: What Is an Airflow Pool and When Would You Use One?

Your team is building a DAG that runs 20 parallel tasks, all querying a PostgreSQL database. The DBA tells you the database can handle at most 5 concurrent connections before performance degrades. How do you enforce this limit in Airflow?

<details>
<summary>💡 Hint</summary>

Think about a mechanism in Airflow that lets you limit the number of tasks running simultaneously — not globally, but for a specific resource.

</details>

<details>
<summary>✅ Solution</summary>

### Use an Airflow Pool

A **pool** is a named bucket of slots that limits how many tasks can run concurrently. Create a pool with 5 slots and assign all PostgreSQL tasks to it.

**Step 1: Create the pool**
```bash
airflow pools set postgres_pool 5 "Limits concurrent PostgreSQL connections to 5"
```

**Step 2: Assign tasks to the pool**
```python
from airflow.operators.python import PythonOperator

query_task = PythonOperator(
    task_id='query_customer_data',
    python_callable=run_query,
    pool='postgres_pool',     # ← all 20 tasks use this pool
    pool_slots=1,
)
```

**What happens:**
- Tasks 1–5 acquire slots and start running immediately
- Tasks 6–20 stay in `queued` state waiting for a slot
- As each task completes and releases its slot, the next queued task starts
- At no point will more than 5 tasks query PostgreSQL simultaneously

**Key points to remember:**
- Without a pool, all 20 tasks could run simultaneously (if workers allow it)
- The pool is stored in the Airflow metadata database, not in airflow.cfg
- If you don't specify a pool, tasks go to `default_pool` (128 slots by default)
- Pools work with all executor types (Local, Celery, Kubernetes)

</details>
</article>

---

<article data-difficulty="junior">

## 🟢 Question 2: Task Is Stuck in 'Queued' State — What Do You Check?

A task has been in the `queued` state for 45 minutes. The DAG is not paused, the scheduler is running, and you can see other tasks completing. What are the most likely causes, and how do you diagnose them?

<details>
<summary>💡 Hint</summary>

A queued task is ready to run but hasn't been picked up yet. Think about what prerequisites need to be met before a task transitions from queued to running.

</details>

<details>
<summary>✅ Solution</summary>

### Diagnosis Checklist

A task stays queued when one or more of these conditions is not met:

**1. Pool slots are exhausted**
```bash
# Check pool usage in the Airflow UI: Admin → Pools
# Or query the metadata DB:
airflow pools list

# If running_slots == total_slots, the pool is full
# Fix: temporarily increase pool size or wait for running tasks to finish
airflow pools set my_pool 10 "Temporarily increased to unblock queue"
```

**2. No workers available (CeleryExecutor)**
```bash
# Check if workers are running
airflow celery inspect active

# Check if the task's queue has any workers
airflow celery inspect active_queues
# If the task has queue='special_workers' but no workers listen to it, it stays queued forever
```

**3. Executor capacity reached**
```bash
# Check airflow.cfg: [core] parallelism = X
# If total running tasks == parallelism, new tasks queue
# Also check: [core] dag_concurrency and max_active_runs
```

**4. The task has depends_on_past=True**
```python
# If the previous run's same task didn't succeed,
# this run's task won't start — check previous DAG run status
```

**Investigation steps:**
1. Admin → Pools → check if the pool is full
2. Check the task's `pool` and `queue` parameters
3. Check `parallelism` setting vs current running tasks
4. Look at the task's logs for any hint
5. Check Celery worker logs if using CeleryExecutor

</details>
</article>

---

<article data-difficulty="mid-level">

## 🟡 Question 3: Design a Pool Strategy for a Multi-Team Airflow Instance

Your company has three teams (Analytics, Platform, Finance) sharing one Airflow instance. Analytics runs heavy Snowflake queries; Platform runs Spark jobs; Finance runs time-sensitive compliance reports that must complete by 8 AM. How do you design a pool strategy that prevents teams from starving each other and ensures Finance SLAs are met?

<details>
<summary>💡 Hint</summary>

Consider both team-level isolation and resource-level protection. Think about how `priority_weight` interacts with pool slot allocation.

</details>

<details>
<summary>✅ Solution</summary>

### Multi-Layer Pool Design

**Pool Architecture:**

```bash
# Resource pools (protect downstream systems)
airflow pools set snowflake_pool     8  "Max 8 concurrent Snowflake queries"
airflow pools set spark_pool         3  "Max 3 concurrent Spark submissions"

# Priority pool for Finance SLA compliance
airflow pools set finance_sla_pool  10  "Finance time-sensitive tasks — high priority"
```

**Finance DAG (SLA-bound):**
```python
compliance_report = SnowflakeOperator(
    task_id='run_compliance_report',
    sql="CALL sp_compliance_report('{{ ds }}')",
    pool='snowflake_pool',          # shared resource pool
    priority_weight=100,            # highest priority — gets slots first
    weight_rule='absolute',
    sla=timedelta(hours=2),         # alert if not done by SLA
)
```

**Analytics DAG (non-SLA):**
```python
heavy_analysis = SnowflakeOperator(
    task_id='run_analysis',
    sql="SELECT ...",
    pool='snowflake_pool',
    priority_weight=20,             # lower priority — yields to Finance
    weight_rule='absolute',
)
```

**Key design decisions:**

| Decision | Rationale |
|----------|-----------|
| Shared resource pool (not team pools) | One Snowflake pool prevents either team from monopolizing the warehouse |
| priority_weight on Finance tasks | Finance tasks claim slots before Analytics when competing |
| weight_rule='absolute' | Predictable priority — no inflation from downstream tasks |
| Separate Spark pool | Spark jobs have different resource profile from SQL; isolate them |

**Governance:** Enforce pool assignment through a code review checklist or a custom linting rule that rejects PRs where Snowflake operators don't specify `pool='snowflake_pool'`.

</details>
</article>

---

<article data-difficulty="mid-level">

## 🟡 Question 4: Route Tasks to GPU Workers Using Queues

Your Airflow instance uses CeleryExecutor with 10 standard CPU workers and 2 GPU workers. You need to ensure ML inference tasks only run on GPU workers, while ETL tasks run on any worker. How do you configure this?

<details>
<summary>💡 Hint</summary>

Queues in Airflow (with CeleryExecutor) control which workers pick up which tasks. Workers subscribe to named queues at startup.

</details>

<details>
<summary>✅ Solution</summary>

### Queue-Based Worker Routing

**Step 1: Start workers with queue subscriptions**
```bash
# Standard workers — listen to default queue
airflow celery worker --queues default --concurrency 8 --hostname cpu-worker-{n}

# GPU workers — ONLY listen to gpu queue
airflow celery worker --queues gpu --concurrency 2 --hostname gpu-worker-{n}
```

**Step 2: Assign queues to tasks in DAG code**
```python
# ETL task — any worker can run this
extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_fn,
    queue='default',        # any worker picks this up
)

# Inference task — MUST run on GPU worker
inference = PythonOperator(
    task_id='run_inference',
    python_callable=gpu_inference_fn,
    queue='gpu',            # only GPU workers subscribe to 'gpu'
    pool='gpu_pool',        # also limit concurrency
)
```

**Step 3: Add a pool for GPU concurrency control**
```bash
# Each GPU worker handles 2 concurrent jobs (2 workers × 2 = 4 slots)
airflow pools set gpu_pool 4 "GPU inference concurrency — 2 workers × 2 jobs each"
```

**Why combine queue + pool?**
- `queue='gpu'` ensures the task goes to a GPU machine (routing)
- `pool='gpu_pool'` ensures we don't oversubscribe each GPU (rate limiting)
- Together: right hardware + right concurrency

**Common mistake:** Setting `queue='gpu'` but forgetting to start GPU workers with `--queues gpu`. Tasks queue indefinitely in Celery broker but never get consumed.

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Question 5: Diagnose and Fix a Pool-Induced Deadlock

Your data pipeline has been stuck for 3 hours. The Airflow UI shows 10 tasks in `running` state (all holding slots in `etl_pool` which has 10 slots) and 30 tasks in `queued` state waiting for `etl_pool`. The running tasks have been running for 3 hours — far longer than normal. Nobody modified the code recently. What's happening, and how do you resolve it without data loss?

<details>
<summary>💡 Hint</summary>

Think about what could cause tasks to run indefinitely. Consider whether the running tasks might themselves be waiting for something — and whether that something might be stuck in the queued queue.

</details>

<details>
<summary>✅ Solution</summary>

### Root Cause Analysis

This is a **pool-induced deadlock**. The running tasks are likely waiting for an external resource (a sensor check, a subprocess, or a downstream task result via XCom) that itself is queued in the same pool.

**Diagnostic steps:**

```bash
# Step 1: Identify the running tasks and their actual behavior
# In Airflow UI: click each running task → View Log
# Look for: "waiting for...", sensor polling logs, subprocess waiting

# Step 2: Check if running tasks are sensors or blocking calls
# Common culprit: FileSensor, ExternalTaskSensor, HttpSensor
# These tasks stay 'running' while they poll — holding pool slots

# Step 3: Query metadata DB for task details
```

```sql
-- Find running tasks and their duration + type
SELECT 
    dag_id, task_id, state,
    EXTRACT(EPOCH FROM (NOW() - start_date))/3600 as hours_running,
    pool, pool_slots
FROM task_instance
WHERE state = 'running'
  AND pool = 'etl_pool'
ORDER BY hours_running DESC;
```

**The deadlock scenario:**

```
Situation:
- 10 sensors are running (holding all 10 etl_pool slots)
- These sensors check: "has downstream_task_X completed?"
- downstream_task_X tasks are in the queued list (30 queued tasks)
- But queued tasks can't run because all 10 slots are held by sensors
- Sensors can't succeed because the tasks they're waiting for can't run
→ DEADLOCK
```

**Resolution Strategy (in order of preference):**

```bash
# Option 1: Temporarily increase pool size to break the deadlock
airflow pools set etl_pool 20 "Temporarily doubled to resolve deadlock — revert after tasks clear"
# This allows queued tasks to get slots → sensors can complete → deadlock breaks

# Option 2: Clear the stuck sensor tasks (they'll be re-queued and re-run)
airflow tasks clear -d my_dag -t sensor_task -s 2024-01-15 -e 2024-01-16
# Clearing frees slots → downstream tasks can run

# Option 3: If sensors were a design mistake, mark them as success
airflow tasks states-for-dag-run my_dag 2024-01-15T00:00:00
airflow tasks clear -y -d my_dag -t blocking_sensor -s 2024-01-15 -e 2024-01-16
```

**Permanent fix — separate pools by pipeline stage:**

```python
# BEFORE (deadlock-prone): all tasks in same pool
sensor = ExternalTaskSensor(task_id='wait_for_upstream', pool='etl_pool')
process = PythonOperator(task_id='process_data', pool='etl_pool')

# AFTER (deadlock-safe): sensors in separate pool, never compete with processing
sensor = ExternalTaskSensor(
    task_id='wait_for_upstream',
    pool='sensor_pool',    # separate pool for blocking sensors
    poke_interval=60,
    mode='reschedule',     # releases slot between checks (KEY FIX)
)
process = PythonOperator(
    task_id='process_data',
    pool='etl_pool',       # processing tasks in separate pool
)
```

**The `mode='reschedule'` fix is critical:** In `reschedule` mode, a sensor releases its pool slot between checks. This means sensors don't hold slots while they wait — they only hold slots for the brief moment of the poke. This eliminates the most common cause of pool deadlocks.

**Preventive measures:**
1. Always use `mode='reschedule'` for sensors in production
2. Set `dagrun_timeout=timedelta(hours=4)` to auto-kill stuck runs
3. Separate sensor pools from processing pools
4. Monitor for "tasks queued > 20 minutes" in your alerting system

</details>
</article>

---

## ⚡ Quick-fire Q&A

**Q: What is an Airflow pool and what problem does it solve?**
A: A pool is a named resource slot counter that limits the number of concurrently running tasks sharing that pool. It prevents overloading downstream systems (databases, APIs, storage) by capping concurrent access regardless of how many Airflow workers are available.

**Q: How do you assign a task to a pool?**
A: Set the `pool` parameter on any operator: `MyOperator(task_id='...', pool='my_pool', pool_slots=1)`. The `pool_slots` parameter (default 1) lets a single task consume multiple pool slots — useful for resource-intensive tasks that should count more heavily toward the limit.

**Q: What is the default pool in Airflow and what is its default slot count?**
A: The `default_pool` has 128 slots by default. All tasks that don't specify a pool are assigned to `default_pool`. Adjust the slot count based on your executor capacity and downstream system limits.

**Q: What is the difference between a pool and an Airflow queue?**
A: Pools limit concurrency of task execution (resource throttling). Queues (used with CeleryExecutor and KubernetesExecutor) route tasks to specific worker groups — e.g., a GPU worker queue for ML tasks, or a high-memory queue for large Spark submissions. They address resource affinity, not throttling.

**Q: How do pools interact with DAG-level `max_active_runs` and task-level `max_active_tis_per_dag`?**
A: `max_active_runs` limits how many DagRuns of a DAG execute simultaneously. `max_active_tis_per_dag` limits concurrent task instances of a specific task across all DagRuns. Pools apply across all DAGs sharing the pool — they're orthogonal controls that all apply simultaneously, with the most restrictive one winning.

**Q: When would you create multiple queues in a Celery-based Airflow deployment?**
A: Create multiple queues when you have heterogeneous workers — e.g., workers with GPUs, high-memory instances, or specific software installed. Route tasks requiring those resources to the appropriate queue, ensuring they land on capable workers rather than any available worker.

**Q: How do you monitor pool utilization and identify pool bottlenecks?**
A: The Airflow UI's Pools page shows slot usage in real time. Export pool metrics to your monitoring system (CloudWatch, Datadog) via StatsD/OpenMetrics. Tasks queued waiting for pool slots appear in the `queued` state — a sustained spike in queued tasks signals a pool that needs more slots or downstream system capacity.

---

## 💼 Interview Tips

- Lead with the "why" of pools: protecting downstream systems from being overwhelmed by concurrent Airflow tasks is the core use case. Don't just explain mechanics without the motivation.
- The distinction between pools (concurrency throttling) and queues (worker routing) is a frequent interview question — keep these clearly separated in your answer.
- Mention `pool_slots` — many candidates are unaware that tasks can consume multiple slots, which is important for modeling resource-intensive tasks correctly.
- Senior interviewers will ask about contention between backfill runs and production runs — pools are one of the primary tools for preventing backfills from starving production tasks.
- Show awareness that pool configurations are stored in the metadata database and should be managed as code (exported/imported via CLI or Terraform providers) rather than configured manually in the UI.
- Discuss monitoring: a pool that's perpetually at capacity is a signal to either increase slots, add downstream capacity, or restructure task scheduling — showing this diagnostic mindset signals operational experience.
