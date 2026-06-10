---
title: "Airflow Task Dependencies - Senior Deep Dive"
topic: airflow
subtopic: task-dependencies
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [airflow, dependencies, scheduler-internals, datasets, trigger-rule, depends-on-past, testing]
---

# Airflow Task Dependencies — Senior Deep Dive

## Trigger Rule Internals: How the Scheduler Evaluates State

When the scheduler evaluates whether a task is eligible to run, it doesn't just check if upstream tasks are "done" — it evaluates the specific combination of upstream states against the task's `trigger_rule`.

### Scheduler Evaluation Logic (Pseudocode)

```python
def is_task_eligible_to_run(task_instance, trigger_rule, upstream_states):
    """
    Simplified version of Airflow's internal dependency check.
    upstream_states: dict of {task_id: state} for all direct predecessors
    """
    states = list(upstream_states.values())
    
    successes = states.count('success')
    failures = states.count('failed')
    skipped = states.count('skipped')
    done = len(states)  # total upstream tasks
    upstream_failed = states.count('upstream_failed')
    
    if trigger_rule == 'all_success':
        return successes == done and failures == 0 and skipped == 0
    
    elif trigger_rule == 'all_failed':
        return failures == done
    
    elif trigger_rule == 'all_done':
        return done == len(upstream_states)  # all have reached terminal state
    
    elif trigger_rule == 'one_success':
        return successes >= 1
    
    elif trigger_rule == 'one_failed':
        return failures >= 1
    
    elif trigger_rule == 'none_failed':
        return failures == 0 and upstream_failed == 0  # skipped is OK
    
    elif trigger_rule == 'none_failed_min_one_success':
        return failures == 0 and upstream_failed == 0 and successes >= 1
    
    elif trigger_rule == 'always':
        return True
```

**Critical nuance:** The scheduler doesn't check trigger rules at a specific time — it re-evaluates *every scheduler heartbeat* (default: 5 seconds). A task becomes eligible as soon as its trigger conditions are met, not at a predetermined moment.

---

## How the Scheduler Processes Dependency State

The scheduler uses a dependency graph traversal at each heartbeat:

```
Scheduler Heartbeat Loop:
1. Query all task instances in state: ['none', 'scheduled', 'queued', 'up_for_retry']
2. For each task in 'none' state (not yet evaluated):
   a. Fetch all direct upstream task instances for the same DAG run
   b. Evaluate trigger_rule against upstream states
   c. If eligible: transition to 'scheduled'
   d. If blocked (upstream not terminal): leave in 'none'
   e. If will never be eligible (e.g., all_success but upstream failed): 
      transition to 'upstream_failed'
3. Scheduled tasks are sent to the executor
4. Executor reports status changes back to scheduler
```

**Why this matters for production:** If the scheduler is slow (high metadata DB load, many DAGs), the heartbeat rate degrades. Tasks may sit in `none` state longer than expected, not because they're waiting for upstream, but because the scheduler hasn't gotten around to evaluating them.

```sql
-- Measure scheduler lag: time between task becoming eligible and being scheduled
SELECT
    dag_id,
    AVG(EXTRACT(EPOCH FROM (start_date - queued_dttm))) as avg_scheduler_lag_seconds
FROM task_instance
WHERE state = 'success'
  AND queued_dttm IS NOT NULL
  AND start_date > NOW() - INTERVAL '24 hours'
GROUP BY dag_id
ORDER BY avg_scheduler_lag_seconds DESC
LIMIT 20;
```

---

## depends_on_past Deadlock Scenarios

`depends_on_past=True` creates a **temporal dependency** — the same task instance from a prior run must have succeeded. This creates subtle deadlock scenarios.

### Scenario 1: The Initial Bootstrap Problem

```
DAG start_date: 2024-01-01
First run: execution_date = 2024-01-01

Task with depends_on_past=True:
  - Airflow looks for the same task in the run for 2023-12-31
  - No such run exists
  - Airflow skips the depends_on_past check → task runs normally
  
This is correct behavior. But if you add depends_on_past to a task mid-lifecycle,
the "previous" run may have run before the parameter was set.
```

### Scenario 2: Backfill Ordering with depends_on_past

```bash
# This command creates ALL runs simultaneously, which breaks depends_on_past
airflow dags backfill -s 2024-01-01 -e 2024-01-31 my_dag

# Use --run-backwards=False to ensure chronological order
# But depends_on_past still requires each run to complete before the next starts
# → backfill becomes effectively sequential, not parallel
airflow dags backfill -s 2024-01-01 -e 2024-01-31 my_dag --max-active-runs 1
```

### Scenario 3: depends_on_past + max_active_runs=1 Deadlock

```
max_active_runs=1 → only one DAG run at a time
depends_on_past=True on a task

Normal behavior: Run N completes → Run N+1 starts

Deadlock scenario:
1. Run N is running
2. Run N+1 is in queued state (max_active_runs=1 allows it to be created)
3. Run N fails
4. Operator clears Run N to retry
5. Now both Run N (retry) and Run N+1 are trying to run
6. max_active_runs=1 only allows one run at a time
7. Run N+1's task with depends_on_past checks: "did Run N's task succeed?"
8. Run N (retry) hasn't completed yet
9. Run N+1's task is stuck waiting
10. Run N (retry) is waiting for something that depends on Run N+1's task
   → DEADLOCK (or just prolonged blockage, not true deadlock)
```

**Resolution:** During recovery from failure, use `--reset-dagruns` flag or manually mark the old run as failed before clearing for retry.

---

## Dataset-Based Scheduling (Airflow 2.4+)

Airflow 2.4 introduced **Datasets** — a declarative way to express data dependencies between DAGs. Instead of ExternalTaskSensor polling, DAGs can declare what data they produce and consume.

```python
from airflow import DAG, Dataset
from airflow.operators.python import PythonOperator
from datetime import datetime

# Define datasets as logical references to data
orders_dataset = Dataset('s3://data-lake/orders/{{ ds }}/')
customers_dataset = Dataset('snowflake://warehouse/dim_customers')

# Producer DAG: declares that it produces these datasets
with DAG(
    dag_id='orders_etl',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 6 * * *',
) as producer_dag:

    load_orders = PythonOperator(
        task_id='load_orders',
        python_callable=load_fn,
        outlets=[orders_dataset],     # ← declares this task produces orders_dataset
    )

    load_customers = PythonOperator(
        task_id='load_customers',
        python_callable=load_customers_fn,
        outlets=[customers_dataset],  # ← declares this task produces customers_dataset
    )

# Consumer DAG: triggered automatically when its inlets are updated
with DAG(
    dag_id='consolidated_report',
    schedule=[orders_dataset, customers_dataset],   # ← triggered when BOTH datasets updated
    start_date=datetime(2024, 1, 1),
) as consumer_dag:

    build_report = PythonOperator(
        task_id='build_report',
        python_callable=report_fn,
        inlets=[orders_dataset, customers_dataset],  # ← declares data lineage
    )
```

### Datasets vs ExternalTaskSensor

| Aspect | Dataset Scheduling | ExternalTaskSensor |
|--------|-------------------|-------------------|
| **Mechanism** | Event-driven (task completes → dataset updated) | Polling-based (checks every N seconds) |
| **Coupling** | Producer/consumer know about shared datasets | Consumer knows about producer's DAG/task IDs |
| **Worker slots** | No slot consumption while waiting | Holds slot in reschedule mode, brief pokes |
| **Data lineage** | Built-in via `inlets`/`outlets` | No lineage tracking |
| **Cross-team** | Teams agree on dataset URIs, not DAG internals | Consumer team knows producer's internal task IDs |
| **Airflow version** | 2.4+ | All versions |

---

## Testing Dependency Logic

Testing DAG dependencies is often neglected but critical for correctness.

### Unit Testing Trigger Rules

```python
import pytest
from airflow.models import DagBag, TaskInstance
from airflow.utils.state import State
from airflow.utils.trigger_rule import TriggerRule
from unittest.mock import MagicMock

def test_cleanup_runs_on_failure():
    """Verify cleanup task runs even when upstream tasks fail."""
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dagbag.get_dag('my_pipeline_dag')
    
    cleanup_task = dag.get_task('cleanup')
    assert cleanup_task.trigger_rule == TriggerRule.ALL_DONE, \
        "cleanup must use ALL_DONE trigger rule"

def test_notify_uses_none_failed_after_branch():
    """Verify convergence task after branch uses correct trigger rule."""
    dagbag = DagBag(dag_folder='dags/', include_examples=False)
    dag = dagbag.get_dag('branching_dag')
    
    notify_task = dag.get_task('send_notification')
    assert notify_task.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, \
        "Post-branch convergence must use NONE_FAILED_MIN_ONE_SUCCESS"

def test_dependency_chain():
    """Verify task dependency order is correct."""
    dagbag = DagBag(dag_folder='dags/')
    dag = dagbag.get_dag('sales_etl')
    
    # Get task objects
    extract = dag.get_task('extract')
    transform = dag.get_task('transform')
    load = dag.get_task('load')
    
    # Verify dependency chain
    assert transform.task_id in [t.task_id for t in extract.downstream_list]
    assert load.task_id in [t.task_id for t in transform.downstream_list]
    assert extract.task_id not in [t.task_id for t in load.downstream_list]

def test_no_cycles():
    """Verify DAG has no circular dependencies."""
    dagbag = DagBag(dag_folder='dags/')
    dag = dagbag.get_dag('my_dag')
    
    # DagBag validates cycles during import — any cycle raises during load
    assert len(dagbag.import_errors) == 0
```

### Integration Test: State Propagation

```python
from airflow.models import DagRun, TaskInstance
from airflow.utils.session import create_session

def test_upstream_failure_propagates():
    """Verify that a failed task marks downstream tasks as upstream_failed."""
    with create_session() as session:
        # Simulate a failed upstream task
        upstream_ti = session.query(TaskInstance).filter(
            TaskInstance.dag_id == 'test_dag',
            TaskInstance.task_id == 'extract',
            TaskInstance.run_id == 'test_run',
        ).one()
        upstream_ti.state = State.FAILED
        session.commit()
        
        # Run the scheduler loop (in test mode)
        # After evaluation, downstream tasks should be 'upstream_failed'
        downstream_ti = session.query(TaskInstance).filter(
            TaskInstance.task_id == 'transform'
        ).one()
        
        # Trigger scheduler evaluation
        dag.handle_callback(dag_run, success=False, reason='test')
        
        assert downstream_ti.state == State.UPSTREAM_FAILED
```

---

## Production Patterns for Complex Dependencies

### Pattern: Circuit Breaker for Cross-DAG Dependencies

```python
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

def check_data_quality(**context):
    """Check if upstream data meets quality thresholds."""
    # Query data quality metrics from DQ table
    # Raise AirflowSkipException if quality too low
    pass

def handle_sensor_timeout(**context):
    """Called when upstream DAG never completed — escalate."""
    send_pagerduty_alert("Upstream ETL hasn't completed within SLA window")

with DAG('robust_downstream', ...) as dag:
    
    wait_for_upstream = ExternalTaskSensor(
        task_id='wait_upstream',
        external_dag_id='upstream_etl',
        mode='reschedule',
        timeout=4 * 3600,  # 4 hour timeout
        soft_fail=True,    # don't fail the sensor, just skip remaining tasks
    )
    
    quality_check = PythonOperator(
        task_id='validate_upstream_data',
        python_callable=check_data_quality,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
    
    handle_timeout = PythonOperator(
        task_id='escalate_timeout',
        python_callable=handle_sensor_timeout,
        trigger_rule=TriggerRule.ALL_FAILED,  # only if sensor timed out (soft_fail=True → failed)
    )
    
    process = PythonOperator(task_id='process', ...)
    
    wait_for_upstream >> quality_check >> process
    wait_for_upstream >> handle_timeout  # parallel path: escalate on timeout
```

---

## Interview Tips

> **Tip 1:** "Explain how the scheduler evaluates trigger rules." — "On each heartbeat, the scheduler checks all task instances in non-terminal states. For each task, it fetches the states of all direct upstream tasks and evaluates the trigger rule. If the condition is met, the task moves to 'scheduled'. If a task can never be eligible (e.g., `all_success` but an upstream failed), it immediately becomes `upstream_failed`. This evaluation is continuous, not event-driven — the scheduler polls."

> **Tip 2:** "What are Airflow Datasets and when would you use them over ExternalTaskSensor?" — "Datasets (introduced in 2.4) are logical references to data artifacts. A producer task declares `outlets=[my_dataset]`; a consumer DAG declares `schedule=[my_dataset]`. The consumer is automatically triggered when the dataset is updated. I'd use Datasets over ExternalTaskSensor when I want event-driven (not poll-based) cross-DAG dependencies, when I want built-in data lineage tracking, or when producer and consumer teams should agree on data contracts (dataset URIs) rather than internal DAG/task IDs."

> **Tip 3:** "How do you test that your DAG's trigger rules are correct?" — "I test three things: (1) a unit test that reads the task's trigger_rule property from the DagBag and asserts the expected value; (2) an integration test that sets upstream task states manually and verifies that the dependency evaluation produces the correct downstream state; (3) a DAG structure test that verifies no circular dependencies exist and the expected task ordering is enforced. The DagBag will raise import errors for cycles, so that's essentially free."

## ⚡ Cheat Sheet

**Trigger Rule Reference**
| Rule | Condition to Run |
|---|---|
| `all_success` (default) | All upstream succeeded |
| `all_failed` | All upstream failed |
| `all_done` | All upstream in terminal state (any) |
| `one_success` | At least one upstream succeeded |
| `one_failed` | At least one upstream failed |
| `none_failed` | No upstream failed (skipped is OK) |
| `none_failed_min_one_success` | No failures AND at least one success |
| `always` | Unconditional |

**Trigger Rule Selection Guide**
- Post-branch convergence task: `none_failed_min_one_success` (skipped branches are OK)
- Cleanup/teardown task: `all_done` (must run even if upstream fails)
- Notification on failure: `one_failed` or `all_failed`
- Default most tasks: `all_success`

**Scheduler Evaluation Cadence**
- Re-evaluates trigger rules on every heartbeat (~5s) — not event-driven
- Task becomes eligible as soon as conditions met, not at a fixed time
- If scheduler is slow (DB load), tasks sit in `none` state longer than expected
- `upstream_failed`: propagated immediately when a task can never be eligible

**`depends_on_past` Gotchas**
- Bootstrap: first-ever run has no "previous" run → skips the check (runs normally)
- Adding mid-lifecycle: "previous" run may have run before the parameter existed
- Backfill ordering: use `--max-active-runs 1 --run-backwards False`
- Deadlock risk: `depends_on_past=True` + `max_active_runs=1` + retry → clear old run before retrying

**Datasets vs ExternalTaskSensor**
| | Dataset Scheduling | ExternalTaskSensor |
|---|---|---|
| Mechanism | Event-driven | Polling |
| Coupling | Shared URI (data contract) | Internal DAG/task IDs |
| Worker slots | None while waiting | Brief poke per interval |
| Lineage | Built-in via inlets/outlets | None |
| Requires | Airflow 2.4+ | All versions |

**Dataset Pattern**
```python
my_dataset = Dataset('s3://bucket/path/')
# Producer
PythonOperator(..., outlets=[my_dataset])
# Consumer DAG — triggered automatically
DAG(..., schedule=[my_dataset])
```

**Testing Dependency Logic**
```python
# 1. Assert trigger rule value
assert cleanup_task.trigger_rule == TriggerRule.ALL_DONE
# 2. Assert dependency chain
assert 'transform' in [t.task_id for t in extract_task.downstream_list]
# 3. Assert no cycles — DagBag raises on import if cycles exist
assert len(dagbag.import_errors) == 0
```
