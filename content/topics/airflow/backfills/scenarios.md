---
title: "Airflow Backfills - Scenario Questions"
topic: airflow
subtopic: backfills
content_type: scenario_question
tags: [airflow, backfills, catchup, idempotency, depends-on-past, clear, partitioned-tables]
---

# Airflow Backfills — Scenario Questions

<article data-difficulty="junior">

## 🟢 Question 1: What Is catchup and When Should It Be True vs False?

You're deploying a new daily ETL DAG with `start_date=datetime(2024, 1, 1)` and today is February 1, 2024. Explain what `catchup=True` and `catchup=False` would do, and when you'd choose each.

<details>
<summary>💡 Hint</summary>

Think about the 31 intervals between January 1 and February 1. What should Airflow do with those missed intervals when the DAG first activates?

</details>

<details>
<summary>✅ Solution</summary>

### catchup=True: Backfills All Missed Intervals

```python
dag = DAG(
    dag_id='daily_sales_load',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=True,    # DEFAULT behavior
)
```

**What happens:** When this DAG activates on Feb 1, Airflow calculates all intervals between Jan 1 and Feb 1 — that's 31 daily intervals. It creates and runs DAG runs for every one of those intervals (Jan 1 through Jan 31), plus the Feb 1 run.

**When to use:** 
- New pipeline that needs historical data populated (loading 2 months of past sales)
- After recovering from an outage — want to process missed days automatically
- Data warehouse initial load

### catchup=False: Only Run From Now Forward

```python
dag = DAG(
    dag_id='live_dashboard_metrics',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
)
```

**What happens:** Airflow ignores all intervals before today. On Feb 1, it only creates a run for Feb 1. January doesn't exist.

**When to use:**
- Real-time monitoring pipelines (only today's metrics matter)
- Notification systems (don't need to send 31 notification emails)
- Pipelines where historical data either isn't available or isn't needed
- Most production pipelines — teams typically set `catchup_by_default = False` in airflow.cfg and use the CLI for intentional backfills

### Best Practice

```python
# Most production pipelines: catchup=False + explicit CLI backfill when needed
dag = DAG(
    dag_id='daily_sales_load',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,   # don't backfill automatically on deploy
)

# When you need history: run targeted backfill via CLI
# airflow dags backfill --dag-id daily_sales_load -s 2024-01-01 -e 2024-01-31
```

**The danger of `catchup=True`:** If someone accidentally changes `start_date` to a year ago and deploys, Airflow will try to create 365+ DAG runs simultaneously, potentially overwhelming the scheduler, metadata DB, and downstream systems.

</details>
</article>

---

<article data-difficulty="junior">

## 🟢 Question 2: What Is execution_date and Why Does It Matter for Backfills?

A daily DAG has `execution_date = 2024-01-15`. When does it actually run, and what data should the tasks process? Why does this matter when writing backfill-safe code?

<details>
<summary>💡 Hint</summary>

The `execution_date` (also called `logical_date`) is the START of the data interval — not when the pipeline fires. A daily pipeline runs AFTER its interval completes.

</details>

<details>
<summary>✅ Solution</summary>

### The execution_date Timing Model

For a daily DAG with `schedule_interval='@daily'` (midnight UTC):

```
execution_date:         2024-01-15 00:00:00 UTC
data_interval_start:    2024-01-15 00:00:00 UTC  (= execution_date)
data_interval_end:      2024-01-16 00:00:00 UTC
Actual run fires at:    2024-01-16 00:00:00 UTC+ (after the interval ENDS)
Data being processed:   January 15 (yesterday's data)
```

**The pipeline runs the day AFTER the execution_date.**

### Why This Matters for Writing Tasks

```python
def load_daily_sales(**context):
    # CORRECT: use execution_date to identify which day's data to load
    date = context['ds']    # '2024-01-15' — the day whose data you're loading
    
    sql = f"""
        INSERT INTO warehouse.fact_sales
        SELECT * FROM staging.raw_sales
        WHERE sale_date = '{date}'
    """
    
    # WRONG approach: using current date
    # import datetime
    # today = datetime.date.today()   ← NEVER do this!
    # This would load today's data regardless of which historical run is executing
    # During a backfill of 2024-01-15, it would load today's data, not Jan 15's
```

### The Backfill Impact

```
Normal operation (2024-01-16):
  context['ds'] = '2024-01-15' → loads Jan 15 data ✅

Backfill (running 2024-01-15 interval on 2024-02-10):
  context['ds'] = '2024-01-15' → still loads Jan 15 data ✅ (CORRECT!)

If you used datetime.date.today() instead:
  Backfill run: today = '2024-02-10' → loads Feb 10 data ❌ (WRONG!)
```

**Rule:** Always use `context['ds']` or Jinja's `{{ ds }}` to parameterize the data date. Never use `datetime.now()` or `datetime.date.today()` in pipeline logic — this breaks all historical backfills.

### Template Variables for Dates

```python
# In Python tasks
def my_task(**context):
    date = context['ds']                    # '2024-01-15'
    start = context['data_interval_start']  # datetime object
    end = context['data_interval_end']      # datetime object

# In SQL/Bash operators via Jinja
sql_task = SqlOperator(
    sql="SELECT * FROM t WHERE date = '{{ ds }}'"
)
```

</details>
</article>

---

<article data-difficulty="mid-level">

## 🟡 Question 3: Use the Clear Command to Re-run Failed Tasks

Your `daily_sales_pipeline` ran for 2024-01-15 through 2024-01-20, but a downstream service outage caused the `load_to_warehouse` task to fail on Jan 16, 17, and 18. The extract and transform tasks succeeded. The service is now fixed. How do you re-run only the failed load tasks without re-running the successful extract and transform tasks?

<details>
<summary>💡 Hint</summary>

The `clear` command operates on existing DAG runs, not creating new ones. It marks tasks back to a runnable state. Consider which specific task needs clearing and which date range.

</details>

<details>
<summary>✅ Solution</summary>

### Use airflow tasks clear with Filters

Since the DAG runs already exist (the full runs ran Jan 16–18, just the load task failed), use `clear` — not `backfill` (which creates new runs).

```bash
# Option 1: Clear only the failed load tasks via CLI
airflow tasks clear \
    --dag-id daily_sales_pipeline \
    --task-id load_to_warehouse \
    --start-date 2024-01-16 \
    --end-date 2024-01-18 \
    --yes   # skip confirmation prompt

# Option 2: Clear only failed tasks in the date range (don't touch successes)
airflow tasks clear \
    --dag-id daily_sales_pipeline \
    --start-date 2024-01-16 \
    --end-date 2024-01-18 \
    --only-failed \
    --yes
```

**What this does:**
1. Finds `load_to_warehouse` task instances for Jan 16, 17, 18
2. Resets their state from `failed` to `None` (schedulable)
3. The scheduler picks them up and re-runs them on the next heartbeat
4. `extract` and `transform` tasks are NOT affected (they stay `success`)

### Via the UI

1. Browse → DAG Runs → filter by `daily_sales_pipeline`
2. Find the Jan 16 run → click it → find `load_to_warehouse` (red = failed)
3. Click the task → Click "Clear" → deselect "Clear all tasks" → confirm
4. Repeat for Jan 17 and Jan 18

### Key Decision: clear vs backfill

| Situation | Command |
|-----------|---------|
| Runs exist, tasks failed | `airflow tasks clear` |
| Runs don't exist (gaps) | `airflow dags backfill` |
| Runs exist, need full re-run | `airflow dags backfill --reset-dagruns` |
| Re-run one task only | `airflow tasks clear --task-id specific_task` |
| Re-run from a task forward | `airflow tasks clear --downstream` |

**After clearing:** Monitor the tasks in the UI — they should transition from `None` → `scheduled` → `running` → `success` within a few minutes.

</details>
</article>

---

<article data-difficulty="mid-level">

## 🟡 Question 4: Identify and Fix a Non-Idempotent Pipeline Before Backfilling

Your team needs to backfill a pipeline for the past 30 days. Before starting, you review the code:

```python
def load_transactions(**context):
    sql = f"""
        INSERT INTO warehouse.fact_transactions
        SELECT * FROM staging.raw_transactions
        WHERE txn_date = '{context['ds']}'
    """
    snowflake_hook.run(sql)
```

Is this safe to backfill? If not, what's the risk and how do you fix it?

<details>
<summary>💡 Hint</summary>

Think about what happens when this INSERT runs twice for the same date. Is the second run's output the same as the first?

</details>

<details>
<summary>✅ Solution</summary>

### Not Safe: This Is Not Idempotent

**The risk:** If you backfill any date that already has data (even partially), each run adds MORE rows to the table. After backfilling Jan 15 twice:

```
Run 1: 5,000 rows inserted for Jan 15
Run 2: 5,000 MORE rows inserted for Jan 15
Result: 10,000 rows for Jan 15 (DUPLICATES!)
```

This problem occurs when:
- Backfilling dates that already ran (had partial success)
- Retrying failed tasks that already partially inserted
- Clearing and re-running a task

### Fix 1: DELETE + INSERT (most common, simple)

```python
def load_transactions_idempotent(**context):
    date = context['ds']
    
    delete_sql = f"""
        DELETE FROM warehouse.fact_transactions
        WHERE txn_date = '{date}'::date;
    """
    
    insert_sql = f"""
        INSERT INTO warehouse.fact_transactions
        SELECT * FROM staging.raw_transactions
        WHERE txn_date = '{date}'::date;
    """
    
    # Delete first, then insert — result is always the same
    snowflake_hook.run(delete_sql)
    snowflake_hook.run(insert_sql)
```

### Fix 2: MERGE / UPSERT (best for tables with natural keys)

```python
def load_transactions_merge(**context):
    date = context['ds']
    
    sql = f"""
        MERGE INTO warehouse.fact_transactions AS target
        USING (
            SELECT * FROM staging.raw_transactions
            WHERE txn_date = '{date}'::date
        ) AS source
        ON target.transaction_id = source.transaction_id
        WHEN MATCHED THEN UPDATE SET
            target.amount = source.amount,
            target.status = source.status
        WHEN NOT MATCHED THEN INSERT (transaction_id, txn_date, amount, status)
        VALUES (source.transaction_id, source.txn_date, source.amount, source.status);
    """
    snowflake_hook.run(sql)
```

### Fix 3: INSERT OVERWRITE (Spark / Snowflake partition replacement)

```python
def load_transactions_spark(**context):
    date = context['ds']
    
    # Spark: overwrite only the specific date partition
    df = spark.read.table("raw.transactions").filter(f"txn_date = '{date}'")
    
    df.write \
        .mode("overwrite") \
        .option("replaceWhere", f"txn_date = '{date}'") \
        .saveAsTable("warehouse.fact_transactions")
```

### Testing Idempotency Before Backfill

```bash
# Test: run the same date twice, verify row count is identical
# Run 1
airflow tasks test daily_pipeline load_transactions 2024-01-15

# Check count
# SELECT COUNT(*) FROM fact_transactions WHERE txn_date = '2024-01-15'
# Expected: 5000

# Run 2 (same date)
airflow tasks test daily_pipeline load_transactions 2024-01-15

# Check count again — should STILL be 5000 (not 10000)
```

</details>
</article>

---

<article data-difficulty="senior">

## 🔴 Question 5: Design a Safe Backfill Strategy for a 2-Year Historical Load

You need to backfill 2 years (730 intervals) of daily data for a pipeline that:
- Loads into a date-partitioned Snowflake table
- Has `depends_on_past=True` on the aggregation task (cumulative metrics)
- Uses a Snowflake warehouse that costs $8/credit and can handle max 10 concurrent queries
- Has other scheduled DAGs running simultaneously that also use Snowflake
- Must complete within 48 hours (business deadline)

Design the full backfill strategy including concurrency settings, pool configuration, monitoring, and risk mitigation.

<details>
<summary>💡 Hint</summary>

Consider: what does `depends_on_past=True` mean for parallelism? How do you protect Snowflake from both backfill traffic and regular DAG traffic? How do you fit 730 sequential runs into 48 hours?

</details>

<details>
<summary>✅ Solution</summary>

### Analysis

**The constraints:**
1. `depends_on_past=True` on aggregation → must be sequential (Jan 1 before Jan 2)
2. Snowflake handles max 10 concurrent queries → need a pool
3. Other DAGs also use Snowflake → pool must account for live traffic too
4. 48-hour deadline → 730 runs / 48 hours ≈ 15 runs/hour minimum throughput

**The math:**
- If each run takes 4 minutes (extract + aggregate + validate): 730 × 4 min = ~49 hours sequential
- We need parallel runs for the non-cumulative parts, sequential only for aggregation
- Solution: separate the DAG into parallel-safe and sequential parts

### Strategy Design

**Phase 1: Refactor the DAG for Safe Parallelism**

```python
# Split into two separate tasks with different depends_on_past settings
with DAG('daily_warehouse_load', max_active_runs=5, ...) as dag:

    # Part 1: Extract + Load raw data (IDEMPOTENT, parallelizable)
    extract_and_load = PythonOperator(
        task_id='extract_and_load_raw',
        python_callable=idempotent_extract_load,
        pool='snowflake_backfill_pool',
        pool_slots=1,
        depends_on_past=False,   # CAN run in parallel during backfill
    )

    # Part 2: Cumulative aggregation (MUST be sequential)
    aggregate = PythonOperator(
        task_id='compute_cumulative_metrics',
        python_callable=cumulative_aggregate,
        pool='snowflake_backfill_pool',
        pool_slots=2,            # heavier query
        depends_on_past=True,    # MUST run in order Jan1, Jan2, Jan3...
    )

    extract_and_load >> aggregate
```

**Phase 2: Pool Configuration**

```bash
# Reserve slots for backfill without starving live traffic
# Total Snowflake capacity: 10 concurrent queries
# Live scheduled DAGs need: ~4 slots (always reserved)
# Available for backfill: 6 slots

# Create a dedicated backfill pool
airflow pools set snowflake_backfill_pool 6 \
    "Backfill: 6 of 10 Snowflake slots reserved for 730-day backfill"

# Reduce existing live pools slightly during backfill window
airflow pools set snowflake_live_pool 4 \
    "Temporarily reduced to 4 during backfill (normal: 6)"
```

**Phase 3: Backfill Execution Plan**

```bash
#!/bin/bash
# Two-phase backfill:
# Phase A: Run extract_and_load in parallel (max_active_runs=5, depends_on_past=False)
# Phase B: Run cumulative aggregation sequentially (max_active_runs=1)

DAG_ID="daily_warehouse_load"

echo "=== Phase A: Parallel extract + load ==="
airflow dags backfill \
    --dag-id $DAG_ID \
    --start-date 2022-01-01 \
    --end-date 2023-12-31 \
    --task-regex 'extract_and_load_raw' \
    --max-active-runs 5 \
    --verbose 2>&1 | tee backfill_phase_a.log

echo "Phase A complete. Waiting for all loads to stabilize..."
sleep 300

echo "=== Phase B: Sequential cumulative aggregation ==="
airflow dags backfill \
    --dag-id $DAG_ID \
    --start-date 2022-01-01 \
    --end-date 2023-12-31 \
    --task-regex 'compute_cumulative_metrics' \
    --max-active-runs 1 \
    --run-backwards False \
    --verbose 2>&1 | tee backfill_phase_b.log
```

**Time estimate:**
- Phase A: 730 runs ÷ 5 parallel × 2 min each ≈ **4.9 hours**
- Phase B: 730 runs × 1 min each (sequential) ≈ **12.2 hours**
- Total: ~17 hours — well within 48-hour window

**Phase 4: Monitoring Dashboard**

```sql
-- Real-time progress (run every 15 minutes)
SELECT
    task_id,
    state,
    COUNT(*) as count,
    MIN(execution_date) as oldest_pending,
    MAX(execution_date) as newest_done
FROM task_instance
WHERE dag_id = 'daily_warehouse_load'
  AND execution_date BETWEEN '2022-01-01' AND '2023-12-31'
GROUP BY task_id, state
ORDER BY task_id, state;
```

**Phase 5: Validation and Rollback Plan**

```sql
-- Post-backfill validation
SELECT
    DATE_TRUNC('month', metric_date) as month,
    COUNT(DISTINCT metric_date) as days,
    SUM(daily_revenue) as monthly_revenue,
    MAX(cumulative_sales) as end_of_month_cumulative
FROM warehouse.cumulative_daily_metrics
WHERE metric_date BETWEEN '2022-01-01' AND '2023-12-31'
GROUP BY 1
ORDER BY 1;

-- Spot check: cumulative values should be monotonically increasing
SELECT
    metric_date,
    cumulative_sales,
    LAG(cumulative_sales) OVER (ORDER BY metric_date) as prev_cumulative,
    cumulative_sales - LAG(cumulative_sales) OVER (ORDER BY metric_date) as daily_increase
FROM warehouse.cumulative_daily_metrics
WHERE daily_increase < 0   -- alert if cumulative DECREASES (indicates wrong data)
ORDER BY metric_date;
```

**Rollback Plan:**
```sql
-- If validation fails: truncate and start over
TRUNCATE TABLE warehouse.cumulative_daily_metrics;
-- Then restart the backfill

-- If partial failure: identify which month failed
-- Clear only those months and re-run them
```

**Cost estimate:**
- 6 Snowflake slots × 17 hours × $8/credit (assuming 1 credit/hr per slot) = ~$816
- Within normal operating budget; not a concern
- Cost would be higher with full 10-slot allocation

</details>
</article>

---

## ⚡ Quick-fire Q&A

**Q: What is a backfill in Airflow and when would you use it?**
A: A backfill runs a DAG for historical date ranges that were missed or need reprocessing — used when fixing a data bug, onboarding a new DAG with historical data requirements, or recovering from pipeline failures. It respects the DAG's schedule and creates one DagRun per interval.

**Q: What is the difference between `catchup=True` and `catchup=False` in Airflow?**
A: With `catchup=True`, Airflow creates DagRuns for all past intervals since the `start_date` that have not yet run. With `catchup=False`, only the most recent interval is run when the DAG is enabled — past intervals are skipped entirely.

**Q: How does Airflow's backfill command work and how is it different from setting catchup=True?**
A: `airflow dags backfill` is an explicit CLI command that triggers runs for a specified date range on demand. `catchup=True` is automatic — Airflow's scheduler creates missed runs automatically. The CLI backfill gives you explicit control over which range to reprocess.

**Q: What are the risks of running a large backfill and how do you mitigate them?**
A: Risks include overwhelming the task queue, contending with live production runs, and overloading downstream systems (databases, APIs). Mitigate by using `--max-active-runs` to throttle concurrency, scheduling backfills during off-peak hours, and setting pool limits on shared resources.

**Q: How do you make a DAG idempotent so it can be safely backfilled?**
A: Use `INSERT OVERWRITE` or `MERGE` (upsert) patterns instead of plain `INSERT`. Partition data by the execution date so reprocessing a date overwrites only that partition. Avoid side effects that can't be repeated (e.g., sending emails, incrementing counters) without idempotency guards.

**Q: What is `depends_on_past` and how does it affect backfills?**
A: `depends_on_past=True` means a task won't run unless the same task in the previous DagRun succeeded. During backfills, this creates sequential execution across runs — each date interval must complete before the next starts, which can make backfills very slow for long historical ranges.

**Q: How would you handle a backfill when the DAG's logic has changed since the original runs?**
A: Branch the DAG logic using `execution_date` or version parameters to apply different transformation logic for different date ranges. Alternatively, create a separate backfill-specific DAG with the historical logic. Avoid retroactively changing live DAG logic in ways that break past interval semantics.

**Q: What Airflow pool settings would you use to prevent a backfill from starving production pipelines?**
A: Create a dedicated pool with a limited slot count (e.g., 4-8 slots) for backfill tasks. Assign backfill DAG tasks to this pool and keep production tasks in the default pool. This ensures production tasks always have available workers regardless of backfill volume.

---

## 💼 Interview Tips

- Always lead with idempotency when discussing backfills — the ability to rerun any interval safely is the foundational requirement. Interviewers want to see you think about this first.
- Mention the concurrency controls (`max_active_runs`, pools, priority weights) — running an unconstrained backfill is a common production incident that experienced engineers know to prevent.
- `depends_on_past` is a frequent interview trap: explain that it's sometimes necessary for sequential processing but creates backfill bottlenecks — and describe when you'd disable it for a backfill run.
- Senior interviewers often ask about backfill strategy for large historical datasets — partition pruning, parallel backfill windows, and downstream impact assessment show operational depth.
- Avoid saying "just set catchup=True" as a complete answer — discuss the operational implications of enabling it on an existing production DAG that has been paused for weeks.
- Show awareness that backfills interact with data consumers: downstream dashboards or models reading partially reprocessed data need to be handled carefully during a large backfill operation.
