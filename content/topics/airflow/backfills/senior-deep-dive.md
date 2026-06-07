---
title: "Airflow Backfills - Senior Deep Dive"
topic: airflow
subtopic: backfills
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [airflow, backfills, internals, idempotency, metadata-db, incremental, full-refresh, production]
---

# Airflow Backfills — Senior Deep Dive

## Backfill Internals: Scheduler vs Backfill Command

Understanding the distinction between how the scheduler processes runs and how the backfill command works helps you choose the right approach and troubleshoot issues.

### Scheduled Runs (Normal Operation)

The Airflow **scheduler daemon** continuously:
1. Scans for DAGs with pending intervals (enabled DAGs where `execution_date` < `now - schedule_interval`)
2. Creates `DagRun` records in the metadata DB
3. Evaluates task dependencies and creates `TaskInstance` records
4. Dispatches ready tasks to the executor

```
Scheduler process:
  Heartbeat (every ~5s):
    for dag in active_dags:
      next_run = dag.next_dagrun_info(last_run)
      if next_run.data_interval_end <= now:
        create_dagrun(dag, next_run)
```

### Backfill Command (airflow dags backfill)

The **backfill command** is a separate process (not the scheduler daemon). It:
1. Calculates all intervals in the specified date range
2. Creates `DagRun` records with `run_type='backfill'`
3. Manages its own task execution loop (doesn't rely on the scheduler heartbeat)
4. Executes tasks in-process using the LocalExecutor (regardless of configured executor!)

**Critical implication:** The backfill command always uses `LocalExecutor`, even if your cluster uses `CeleryExecutor`. This means:
- Backfill tasks run on the machine where you run the CLI command
- Tasks don't benefit from worker distribution during backfill
- For large backfills, use the Airflow UI or programmatic DAG triggering instead

```bash
# For CeleryExecutor clusters: use the UI or trigger runs programmatically
# The backfill CLI command is best for LocalExecutor environments

# Production alternative: use the API to trigger runs that go through Celery
for date in $(seq 0 30 | xargs -I {} date -d "2024-01-01 + {} days" '+%Y-%m-%dT00:00:00'); do
    airflow dags trigger --dag-id daily_load --exec-date $date
    sleep 5  # throttle triggering
done
```

---

## Impact on the Metadata Database

Large backfills generate significant metadata DB load:

### What Gets Written Per DAG Run

```sql
-- Insertions per DAG run during backfill:
INSERT INTO dag_run ...           -- 1 row per run
INSERT INTO task_instance ...     -- N rows (one per task × num attempts)
INSERT INTO xcom ...              -- varies (XCom values per task)
INSERT INTO log ...               -- K rows (log entries per task attempt)

-- For a 730-run backfill with 10 tasks each:
-- dag_run: 730 rows
-- task_instance: 730 × 10 = 7,300+ rows
-- log: could be 50,000+ rows (multiple log entries per task)
```

### Database Impact Mitigation

```sql
-- Before large backfill: check current DB size and performance
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname || '.' || tablename)) as size
FROM pg_tables
WHERE schemaname = 'public'
  AND tablename IN ('dag_run', 'task_instance', 'xcom', 'log')
ORDER BY pg_total_relation_size(schemaname || '.' || tablename) DESC;
```

```bash
# Archive old runs before a large backfill to keep metadata DB lean
# Airflow 2.x built-in cleanup
airflow db clean --clean-before-timestamp '2023-01-01T00:00:00' \
    --tables dag_run,task_instance,xcom,log \
    --dry-run   # preview first

# After confirming:
airflow db clean --clean-before-timestamp '2023-01-01T00:00:00' \
    --tables dag_run,task_instance,xcom,log \
    --yes
```

---

## Idempotency: The Foundation of Safe Backfills

A task is **idempotent** if running it multiple times for the same execution date produces the same result. This is non-negotiable for safe backfills.

### Non-Idempotent Patterns (DANGEROUS)

```python
# Pattern 1: Unconditional INSERT — creates duplicates on re-run
def load_data(**context):
    sql = f"""
        INSERT INTO warehouse.fact_sales
        SELECT * FROM staging.raw_sales
        WHERE sale_date = '{context['ds']}'
    """
    run_query(sql)
    # Run once: 1000 rows loaded
    # Run again: 1000 more rows → 2000 rows total (DUPLICATE!)
```

```python
# Pattern 2: Appending to files
def export_data(**context):
    with open('/data/output.csv', 'a') as f:    # 'a' = append mode
        f.write(generate_csv(context['ds']))
    # Re-run: file grows, duplicates added
```

### Idempotent Patterns (SAFE)

```python
# Pattern 1: DELETE + INSERT (partition-aware)
def load_data_idempotent(**context):
    date = context['ds']
    sql = f"""
        -- Remove existing data for this date first
        DELETE FROM warehouse.fact_sales
        WHERE sale_date = '{date}';
        
        -- Then insert fresh
        INSERT INTO warehouse.fact_sales
        SELECT * FROM staging.raw_sales
        WHERE sale_date = '{date}';
    """
    run_query(sql)
    # Re-run: deletes old rows first → same result every time

# Pattern 2: MERGE / UPSERT
def upsert_data_idempotent(**context):
    sql = f"""
        MERGE INTO warehouse.dim_customers AS target
        USING (
            SELECT * FROM staging.customers WHERE updated_date = '{context['ds']}'
        ) AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET
            target.name = source.name,
            target.updated_at = source.updated_at
        WHEN NOT MATCHED THEN INSERT (customer_id, name, updated_at)
        VALUES (source.customer_id, source.name, source.updated_at);
    """
    # Re-run: updates existing rows in-place, no duplicates

# Pattern 3: Overwrite partition
def write_partition_idempotent(**context):
    date = context['ds']
    # Spark: overwrite the specific partition
    df.write \
        .mode('overwrite') \
        .partitionBy('sale_date') \
        .option('replaceWhere', f"sale_date = '{date}'") \
        .parquet('s3://bucket/fact_sales/')
    # Re-run: overwrites the partition → same data every time
```

### Testing Idempotency

```python
def test_task_idempotency():
    """
    Production-readiness test: running task twice for same date
    should produce identical results.
    """
    date = '2024-01-15'
    
    # Run 1
    run_load_task(date)
    count_run1 = query("SELECT COUNT(*) FROM fact_sales WHERE sale_date = '2024-01-15'")
    
    # Run 2 (same date)
    run_load_task(date)
    count_run2 = query("SELECT COUNT(*) FROM fact_sales WHERE sale_date = '2024-01-15'")
    
    assert count_run1 == count_run2, \
        f"Not idempotent! Run 1: {count_run1} rows, Run 2: {count_run2} rows (duplicates!)"
```

---

## Incremental vs Full-Refresh Backfill Strategy

Choosing the right strategy for backfill depends on the table type and business requirements.

### Incremental Backfill

Process only data for the specific date interval. Fast, targeted, correct for event-based tables.

```python
def incremental_load(**context):
    """Loads only the data for this execution's date interval."""
    start_dt = context['data_interval_start']  # e.g., 2024-01-15 00:00:00
    end_dt = context['data_interval_end']      # e.g., 2024-01-16 00:00:00
    
    # Delete the partition for this date
    delete_sql = f"""
        DELETE FROM warehouse.fact_events
        WHERE event_timestamp >= '{start_dt}'
          AND event_timestamp < '{end_dt}'
    """
    
    # Re-insert fresh
    insert_sql = f"""
        INSERT INTO warehouse.fact_events
        SELECT * FROM staging.raw_events
        WHERE event_timestamp >= '{start_dt}'
          AND event_timestamp < '{end_dt}'
    """
    
    run_query(delete_sql)
    run_query(insert_sql)
```

**When to use:** Event/transaction tables, append-only data, tables partitioned by date.

### Full-Refresh Backfill

Re-process ALL data from scratch, regardless of which intervals are being backfilled. Necessary for dimensional tables where historical values change.

```python
def full_refresh_load(**context):
    """
    Always rebuilds the complete dimension table from source.
    Correct for slowly-changing dimensions where any backfill
    requires seeing the complete history.
    """
    # This runs the same query regardless of execution_date
    # Backfilling any date triggers a full rebuild
    sql = """
        TRUNCATE TABLE warehouse.dim_products;
        INSERT INTO warehouse.dim_products
        SELECT * FROM source.products;
    """
    run_query(sql)
```

> **Caution:** Full-refresh backfill is expensive — each backfill run processes all data. For a 730-day backfill with full refresh, you'd re-process all data 730 times. Use only for small reference tables.

### Hybrid: Snapshot Backfill

For slowly-changing dimensions that were different at different points in history:

```python
def snapshot_load(**context):
    """
    Load a point-in-time snapshot of the dimension as it existed
    at the execution_date. Requires source data to have change history.
    """
    snapshot_date = context['ds']
    
    sql = f"""
        INSERT INTO warehouse.dim_products_snapshots
        SELECT
            '{snapshot_date}' as snapshot_date,
            product_id, name, price, category
        FROM source.products_history
        WHERE valid_from <= '{snapshot_date}'
          AND (valid_to > '{snapshot_date}' OR valid_to IS NULL)
        ON CONFLICT (snapshot_date, product_id) DO UPDATE
        SET name = EXCLUDED.name, price = EXCLUDED.price;
    """
    run_query(sql)
```

---

## Backfill Dependency Ordering Patterns

### Ordered Backfill for Cumulative Metrics

Some pipelines compute cumulative values (running totals, rolling averages) that depend on all prior intervals being correct. Backfilling these out of order produces wrong results.

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def load_cumulative_metrics(**context):
    """
    This task computes running total of sales.
    It depends on all previous dates being correct.
    """
    date = context['ds']
    
    sql = f"""
        INSERT INTO warehouse.cumulative_sales
        SELECT
            '{date}' as metric_date,
            SUM(amount) OVER (ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                as cumulative_sales
        FROM warehouse.fact_sales
        WHERE sale_date <= '{date}'
        ON CONFLICT (metric_date) DO UPDATE
        SET cumulative_sales = EXCLUDED.cumulative_sales;
    """
    run_query(sql)

dag = DAG(
    dag_id='cumulative_metrics',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,  # REQUIRED for cumulative metrics — must process sequentially
)

with dag:
    load = PythonOperator(
        task_id='load_cumulative_metrics',
        python_callable=load_cumulative_metrics,
        depends_on_past=True,  # REQUIRED — don't skip ahead in time
    )
```

```bash
# Backfill must be sequential for cumulative pipelines
airflow dags backfill \
    --dag-id cumulative_metrics \
    --start-date 2024-01-01 \
    --end-date 2024-01-31 \
    --max-active-runs 1 \
    --run-backwards False
```

---

## Production Backfill Operations Runbook

### Large-Scale Backfill (> 90 days)

```bash
#!/bin/bash
# backfill_runbook.sh

DAG_ID="daily_warehouse_load"
START_DATE="2022-01-01"
END_DATE="2022-12-31"

echo "=== Pre-backfill checks ==="

# 1. Verify DAG is paused during backfill (prevent scheduled runs from interfering)
airflow dags pause $DAG_ID

# 2. Reduce resource pool to throttle backfill
airflow pools set warehouse_pool 4 "Throttled for backfill $START_DATE to $END_DATE"

# 3. Check for already-successful runs (don't re-run these)
airflow dags list-runs --dag-id $DAG_ID --state success \
    | grep -E "2022-" | wc -l
echo "Successful runs already exist — these will be skipped"

echo "=== Starting backfill ==="
airflow dags backfill \
    --dag-id $DAG_ID \
    --start-date $START_DATE \
    --end-date $END_DATE \
    --max-active-runs 5 \
    --verbose

echo "=== Post-backfill cleanup ==="

# 4. Restore pool
airflow pools set warehouse_pool 8 "Restored after backfill"

# 5. Unpause DAG to resume normal scheduling
airflow dags unpause $DAG_ID

echo "=== Backfill complete ==="
```

---

## Interview Tips

> **Tip 1:** "What's the difference between how backfill works vs how the scheduler creates runs?" — "The scheduler daemon creates runs as part of its continuous heartbeat loop — it evaluates all active DAGs and creates runs for upcoming intervals, dispatching tasks through the configured executor (Celery, Kubernetes). The `airflow dags backfill` CLI command is a separate process that creates historical runs and executes them using LocalExecutor directly, regardless of the cluster's executor. For CeleryExecutor clusters, large backfills should be triggered via the API rather than the CLI to use Celery workers."

> **Tip 2:** "Why must a backfill-safe pipeline be idempotent?" — "Backfilling means re-running tasks for past dates. If the task isn't idempotent — for example, if it uses INSERT without first DELETEing the partition — each re-run adds duplicate data. After a backfill of 30 days, you'd have 2× the data for those 30 days. The solution is always DELETE+INSERT for the specific date partition, UPSERT/MERGE, or partition overwrite in Spark. The idempotency guarantee means 'same execution_date → same result, regardless of how many times you run it.'"

> **Tip 3:** "How do you backfill a pipeline that computes cumulative metrics?" — "Cumulative metrics (running totals, rolling averages) depend on all prior intervals being correct. For these, you must backfill sequentially in chronological order — `--max-active-runs 1` and `--run-backwards False` in the CLI, and `depends_on_past=True` on the task. Out-of-order backfill produces wrong cumulative values because Jan 15's cumulative total would be computed before Jan 14's data is loaded. Sequential processing guarantees each interval has correct prior state."
