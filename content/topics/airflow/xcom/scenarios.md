---
title: "Airflow XCom - Scenario Questions"
topic: airflow
subtopic: xcom
content_type: scenario_question
tags: [airflow, xcom, metadata, S3-backend, TaskFlow, anti-patterns, cross-dag]
---

# Airflow XCom — Scenario Questions

<article data-difficulty="junior">

## 🟢 Scenario 1: Passing Data Between Tasks

You're writing a DAG with three tasks: `extract`, `validate`, and `notify`. The `extract` task fetches data from an API and gets a count of records. The `validate` task needs this count to verify at least 100 records arrived. The `notify` task needs the count to include in a Slack message. How do you pass this count from `extract` to both `validate` and `notify`?

<details>
<summary>💡 Hint</summary>
XCom allows one task to write a value that any number of downstream tasks can read. You can either use `ti.xcom_push()` with a named key or use the return value (key='return_value'). Multiple tasks can pull from the same source task.
</details>

<details>
<summary>✅ Solution</summary>

**Approach 1: Using `ti.xcom_push()` and `ti.xcom_pull()`**

```python
from airflow.operators.python import PythonOperator

def extract(**context):
    response = requests.get('https://api.example.com/records')
    records = response.json()
    record_count = len(records)

    # Push with a named key so multiple downstream tasks can pull it
    context['ti'].xcom_push(key='record_count', value=record_count)
    return record_count   # Also stored as key='return_value'

def validate(**context):
    count = context['ti'].xcom_pull(task_ids='extract', key='record_count')
    if count < 100:
        raise ValueError(f"Only {count} records received — expected at least 100")
    print(f"Validation passed: {count} records")

def notify(**context):
    count = context['ti'].xcom_pull(task_ids='extract', key='record_count')
    print(f"Slack notification: Pipeline complete. {count} records processed.")

extract_task = PythonOperator(task_id='extract', python_callable=extract)
validate_task = PythonOperator(task_id='validate', python_callable=validate)
notify_task = PythonOperator(task_id='notify', python_callable=notify)
extract_task >> validate_task >> notify_task
```

**Approach 2: TaskFlow API (cleaner)**

```python
from airflow.decorators import task, dag

@dag(schedule_interval='@daily', ...)
def my_pipeline():

    @task
    def extract() -> int:
        response = requests.get('https://api.example.com/records')
        return len(response.json())   # Auto-pushed as 'return_value'

    @task
    def validate(record_count: int):
        if record_count < 100:
            raise ValueError(f"Only {record_count} records received")

    @task
    def notify(record_count: int):
        print(f"Pipeline complete: {record_count} records processed")

    count = extract()
    validate(count)       # count XCom flows to validate
    notify(count)         # Same count XCom also flows to notify
```

**Key point:** With TaskFlow, `count` is a "task output" object. When you pass it to both `validate(count)` and `notify(count)`, Airflow generates `xcom_pull` calls in both tasks automatically. Both tasks read from the same XCom entry in the metadata DB.
</details>

</article>

---

<article data-difficulty="junior">

## 🟢 Scenario 2: Why Is My XCom Pull Returning None?

A data engineer wrote this DAG and reports that `transform` always receives `None` from `xcom_pull`:

```python
def extract(**context):
    data = fetch_data()
    print(f"Fetched {len(data)} records")
    # No return, no xcom_push

def transform(**context):
    data = context['ti'].xcom_pull(task_ids='extract')
    print(f"Data: {data}")  # Always prints "Data: None"
    process(data)
```

What are the two common causes for this, and how do you fix it?

<details>
<summary>💡 Hint</summary>
`xcom_pull(task_ids='extract')` pulls the key='return_value' XCom. Think about how values get into XCom — what triggers the automatic push?
</details>

<details>
<summary>✅ Solution</summary>

**Cause 1: `extract` doesn't return or push anything**

The automatic push (key='return_value') only happens when the `PythonOperator` callable has an explicit `return` statement. A function that implicitly returns `None` pushes `None` to XCom.

```python
# BROKEN: implicit None return
def extract(**context):
    data = fetch_data()
    print(f"Fetched {len(data)} records")
    # Missing return statement!

# FIXED: explicit return
def extract(**context):
    data = fetch_data()
    context['ti'].xcom_push(key='fetched_data', value=data)
    return data   # Also pushes as 'return_value'
```

**Cause 2: Wrong `task_ids` in `xcom_pull`**

If the `task_id` string in `xcom_pull` doesn't exactly match the `task_id` of the upstream task, it returns `None` silently (no error).

```python
# Suppose the task is defined as:
extract_task = PythonOperator(task_id='extract_sales_data', ...)

# But the pull uses the wrong task_id:
data = ti.xcom_pull(task_ids='extract')   # WRONG — task_id is 'extract_sales_data'
data = ti.xcom_pull(task_ids='extract_sales_data')   # CORRECT
```

**Cause 3 (bonus): DAG run mismatch**

`xcom_pull` by default only pulls from the same `run_id`. If you're testing with manual triggers or there's a mismatch, you might be reading from a different run. Check in the UI: Admin → XComs, filter by dag_id and run_id.

**Prevention:** Always check Admin → XComs in the UI to confirm a value was actually pushed before debugging the pull side.
</details>

</article>

---

<article data-difficulty="mid-level">

## 🟡 Scenario 3: Production Performance Incident from XCom Misuse

Your Airflow cluster starts experiencing scheduler slowdowns every morning between 8–10 AM. The scheduler loop, normally completing in <1 second, takes 15–30 seconds. Task pickup is delayed by 10–20 minutes. A senior engineer suspects XCom misuse. How do you diagnose and fix this?

<details>
<summary>💡 Hint</summary>
The scheduler shares the metadata DB with XCom. Large XCom entries stored as BYTEA cause heavy I/O on the metadata DB. Run a diagnostic query on the xcom table to find large entries, then trace which tasks are responsible.
</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Diagnose — find large XCom entries**

```sql
-- Run against the Airflow metadata DB
SELECT
    dag_id,
    task_id,
    key,
    pg_column_size(value) AS size_bytes,
    round(pg_column_size(value) / 1024.0 / 1024, 2) AS size_mb,
    execution_date
FROM xcom
WHERE pg_column_size(value) > 102400   -- Flag anything > 100 KB
ORDER BY size_bytes DESC
LIMIT 20;
```

**Step 2: Identify the source**

Say the query reveals `dag_id='ml_feature_dag'`, `task_id='compute_features'`, `key='return_value'`, `size_mb=450`. This means someone returned a 450 MB DataFrame from an `@task` function.

**Step 3: Fix the offending task**

```python
# BEFORE (the problem)
@task
def compute_features(ds: str) -> pd.DataFrame:
    df = load_all_events(ds)
    return feature_engineering(df)   # 450 MB DataFrame in XCom!

# AFTER (the fix)
@task
def compute_features(ds: str) -> str:
    df = load_all_events(ds)
    features = feature_engineering(df)

    # Write to S3, return path
    path = f"s3://ml-data/features/dt={ds}/features.parquet"
    features.to_parquet(path, index=False)
    return path   # ~80 bytes in XCom

@task
def use_features(feature_path: str):
    df = pd.read_parquet(feature_path)   # Read from S3, not XCom
    train_model(df)
```

**Step 4: Clean up historical large XComs**

```sql
-- Remove the offending XCom entries to relieve DB pressure
DELETE FROM xcom
WHERE dag_id = 'ml_feature_dag'
  AND task_id = 'compute_features'
  AND key = 'return_value'
  AND pg_column_size(value) > 102400;
```

**Step 5: Prevent recurrence**

Add a monitoring alert that fires when any XCom entry exceeds 50 KB. Establish team convention: all `@task` functions return only JSON-safe scalars, lists, or dicts. Actual data goes to S3.
</details>

</article>

---

<article data-difficulty="mid-level">

## 🟡 Scenario 4: Cross-DAG XCom vs Better Alternatives

Two DAGs need to share state: `dag_a` runs a nightly ETL and records how many rows it loaded. `dag_b` runs a morning report and needs this count to validate the ETL ran correctly before generating the report. A junior engineer suggests using cross-DAG XCom: `ti.xcom_pull(dag_id='dag_a', task_ids='load', key='row_count')`.

What are the risks of this approach, and what alternatives would you recommend?

<details>
<summary>💡 Hint</summary>
Cross-DAG XCom works but creates hidden coupling. Think about: what happens if the execution dates don't align? What if `dag_a` is rerun? Is there a better way to share state between DAGs that doesn't create this coupling?
</details>

<details>
<summary>✅ Solution</summary>

**Risks of Cross-DAG XCom:**

1. **Execution date alignment:** `xcom_pull` from another DAG defaults to the same `execution_date`. If `dag_a` (runs at midnight) and `dag_b` (runs at 8 AM) have different logical dates, the pull returns `None`
2. **No dependency enforcement:** `dag_b` can try to pull before `dag_a` has finished — it gets `None` silently
3. **Tight coupling:** Renaming `dag_a` or its task breaks `dag_b`. No contract, no validation
4. **Rerun issues:** If `dag_a` is cleared and rerun, the new XCom overwrites the old one — `dag_b`'s historical data becomes inconsistent

**Better Alternative 1: Shared Control Table**

```python
# dag_a: write to control table after load
def record_load_metrics(ds: str, **context):
    row_count = context['ti'].xcom_pull(task_ids='load', key='row_count')
    engine = get_engine()
    engine.execute("""
        INSERT INTO pipeline_control (pipeline_date, pipeline_name, row_count, status, completed_at)
        VALUES (%s, 'dag_a_etl', %s, 'success', NOW())
        ON CONFLICT (pipeline_date, pipeline_name) DO UPDATE
        SET row_count = EXCLUDED.row_count, status = 'success', completed_at = NOW()
    """, (ds, row_count))

# dag_b: read from control table — clear contract
def validate_upstream(ds: str, **context):
    result = engine.execute(
        "SELECT row_count, status FROM pipeline_control WHERE pipeline_date = %s AND pipeline_name = 'dag_a_etl'",
        (ds,)
    ).fetchone()

    if not result or result['status'] != 'success':
        raise ValueError(f"dag_a ETL not complete for {ds}")

    if result['row_count'] < 1000:
        raise ValueError(f"dag_a only loaded {result['row_count']} rows — check ETL")
```

**Better Alternative 2: ExternalTaskSensor + Same-DAG XCom**

```python
# dag_b: use ExternalTaskSensor for dependency, read Snowflake directly
wait_for_etl = ExternalTaskSensor(
    task_id='wait_for_dag_a',
    external_dag_id='dag_a',
    external_task_id='load',
    failed_states=['failed'],
    mode='reschedule',
    poke_interval=120,
)

def validate(**context):
    # Read count directly from Snowflake — no XCom coupling
    count = SnowflakeHook().get_first(
        f"SELECT COUNT(*) FROM fact_sales WHERE sale_date = '{context['ds']}'"
    )[0]
    if count < 1000:
        raise ValueError(f"Only {count} rows in Snowflake for {context['ds']}")
```

**Recommendation:** Use the control table for strong contracts between teams, ExternalTaskSensor for dependency enforcement, and avoid cross-DAG XCom except for very simple cases with well-understood date alignment.
</details>

</article>

---

<article data-difficulty="senior">

## 🔴 Scenario 5: Designing XCom Strategy for a 500-DAG Platform

Your company runs a shared Airflow platform with 30 data engineering teams, 500 DAGs, and 5,000 daily task executions. The platform team wants to establish an XCom governance policy to prevent performance incidents. Design a comprehensive strategy covering conventions, enforcement, monitoring, and infrastructure.

<details>
<summary>💡 Hint</summary>
Think at three levels: (1) what rules prevent abuse (conventions + enforcement), (2) how you detect violations in production (monitoring), and (3) what infrastructure changes enable safe large-payload handling (custom backend). Also consider team education and CI enforcement.
</details>

<details>
<summary>✅ Solution</summary>

**1. Conventions (Team Standards)**

Document and enforce these rules:
- XCom values must be JSON-serializable (no pickle, no DataFrames, no NumPy arrays)
- Maximum XCom value size: 48 KB for metadata, unlimited via S3 backend for structured data
- Permitted XCom types: `str`, `int`, `float`, `bool`, `list[str/int/float]`, `dict` of primitives
- For all actual data: write to S3, XCom the path
- No cross-DAG XCom — use control tables or ExternalTaskSensor instead

**2. Infrastructure: S3 XCom Backend**

```ini
# airflow.cfg
[core]
xcom_backend = airflow.providers.amazon.aws.xcom_backends.s3.S3XComBackend

[aws]
xcom_bucket = company-airflow-xcom-prod
xcom_key_prefix = xcom/

# S3 lifecycle policy: delete objects older than 30 days
```

This handles any accidental large pushes gracefully — they go to S3 instead of bloating the DB.

**3. CI Enforcement: Static Analysis**

```python
# tests/check_xcom_usage.py (run in CI)
import ast
import sys
from pathlib import Path

class XComChecker(ast.NodeVisitor):
    """Check for return statements in @task functions that return non-JSON types."""

    BANNED_RETURN_TYPES = {'DataFrame', 'ndarray', 'Series', 'Tensor'}

    def __init__(self):
        self.violations = []

    def visit_Return(self, node):
        if isinstance(node.value, ast.Call):
            func = node.value.func
            if hasattr(func, 'attr') and func.attr in {'read_csv', 'read_parquet', 'read_sql'}:
                self.violations.append(
                    f"Line {node.lineno}: Likely returning a DataFrame from @task — "
                    "return S3 path instead"
                )

def check_dag_files():
    dag_dir = Path('dags/')
    violations = []
    for dag_file in dag_dir.rglob('*.py'):
        tree = ast.parse(dag_file.read_text())
        checker = XComChecker()
        checker.visit(tree)
        violations.extend(f"{dag_file}: {v}" for v in checker.violations)

    if violations:
        print("XCom policy violations found:")
        for v in violations:
            print(f"  {v}")
        sys.exit(1)
```

**4. Production Monitoring**

```python
# monitoring_dag.py — runs every hour
@dag(dag_id='xcom_monitoring', schedule_interval='0 * * * *', ...)
def xcom_monitoring():

    @task
    def check_large_xcoms():
        from sqlalchemy import create_engine, text
        engine = create_engine(conf.get('database', 'sql_alchemy_conn'))

        with engine.connect() as conn:
            large = conn.execute(text("""
                SELECT dag_id, task_id, key,
                       pg_column_size(value) as size_bytes,
                       execution_date
                FROM xcom
                WHERE pg_column_size(value) > 10240   -- 10 KB threshold
                  AND execution_date > NOW() - INTERVAL '1 day'
                ORDER BY size_bytes DESC LIMIT 20
            """)).fetchall()

        if large:
            violations = [
                f"{r.dag_id}/{r.task_id} [{r.key}]: {r.size_bytes/1024:.1f} KB"
                for r in large
            ]
            # Send to PagerDuty / Slack #data-platform-alerts
            alert_platform_team(f"Large XCom entries detected:\n" + "\n".join(violations))

    @task
    def cleanup_old_xcoms():
        """Delete XCom entries older than 7 days."""
        with create_session() as session:
            deleted = session.query(XCom).filter(
                XCom.execution_date < datetime.utcnow() - timedelta(days=7)
            ).delete(synchronize_session=False)
            session.commit()
        logger.info("Deleted %d stale XCom entries", deleted)

    check_large_xcoms() >> cleanup_old_xcoms()
```

**5. Team Education**

- DAG template repository with correct S3 path pattern pre-wired
- Pre-commit hook that fails if a `@task` returns a type annotation of `DataFrame`, `pd.DataFrame`, etc.
- Quarterly platform review: look at top 10 largest XCom entries per team, provide feedback
- Runbook: what to do when you need to pass large data (S3 path pattern, example code)

**Summary Table:**

| Layer | Tool | Goal |
|-------|------|------|
| Convention | Team standards doc | Define rules clearly |
| Infrastructure | S3 XCom backend | Safe large payload handling |
| CI | AST checker | Catch obvious violations pre-deploy |
| Monitoring | Hourly size query + alerts | Detect production violations |
| Cleanup | Nightly maintenance DAG | Prevent DB bloat |
| Education | Templates + runbooks | Prevent violations by default |
</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is XCom in Airflow and what is it designed for?**
A: XCom (cross-communication) is a mechanism for tasks to share small pieces of data by pushing values to and pulling values from Airflow's metadata database. It's designed for lightweight metadata sharing — task IDs, file paths, record counts, status flags — not for transferring large datasets.

**Q: How do you push and pull XCom values in Airflow?**
A: Push: `ti.xcom_push(key='my_key', value=my_value)` or return a value from `execute()` (auto-pushed as `return_value`). Pull: `ti.xcom_pull(task_ids='upstream_task', key='my_key')` in a downstream task's callable or operator. In Jinja templates: `{{ ti.xcom_pull(task_ids='task', key='key') }}`.

**Q: What is the maximum recommended XCom payload size and what happens if you exceed it?**
A: The default XCom backend stores values in the metadata database — typically limited to a few MB before causing database performance issues. Storing large payloads (DataFrames, model artifacts, large result sets) in XCom causes database bloat and slow task state queries. The practical limit is a few kilobytes to a few hundred kilobytes.

**Q: What is a custom XCom backend and when would you use one?**
A: A custom XCom backend (Airflow 2.x) overrides where XCom values are stored — instead of the metadata database, values are stored in external systems (S3, GCS). Use it when your workflow legitimately needs to pass larger intermediate results between tasks without introducing manual storage boilerplate in every DAG.

**Q: How do you access XCom values within a Jinja-templated operator parameter?**
A: Use `{{ ti.xcom_pull(task_ids='upstream_task_id', key='return_value') }}` inside any `template_fields` parameter. For example, a SQL query can incorporate a dynamically generated table name from a previous task without Python code in the downstream operator.

**Q: What are the risks of using XCom for task communication at scale?**
A: Each XCom push/pull is a read/write to the metadata database. At high task concurrency or with large DAG runs, XCom operations can generate significant database load. Old XCom records accumulate over time — periodically clean them with `airflow db clean` or configure `xcom_expiration` to auto-delete old records.

**Q: What is the `do_xcom_push` parameter on operators?**
A: `do_xcom_push=True` (default on most operators) controls whether the operator's return value is automatically pushed to XCom. Set `do_xcom_push=False` on operators where you know the return value is large or unnecessary — preventing unintended XCom pushes that bloat the metadata database.

---

## 💼 Interview Tips

- Lead with XCom's design intent: it's for metadata, not data. Candidates who describe passing DataFrames through XCom signal they haven't thought through the operational implications.
- The custom XCom backend is a strong differentiator — mentioning S3-backed XCom as the right solution for legitimately larger inter-task payloads shows you know Airflow's extensibility model.
- Be ready to describe the anti-pattern and the fix in the same breath: "don't pass large data through XCom — instead write to S3 and push the path." This shows you've dealt with this in practice.
- Senior interviewers will ask about XCom cleanup — accumulated XCom records are a real production database bloat issue. Know about `airflow db clean` and `xcom_expiration` settings.
- Jinja template XCom access is a powerful pattern for dynamic SQL and operator parameters — demonstrate familiarity with `{{ ti.xcom_pull(...) }}` syntax to show Airflow template depth.
- Avoid over-engineering simple DAGs with XCom when the data flow can be expressed through deterministic naming conventions (S3 paths based on execution_date) — showing this judgment signals senior-level DAG design thinking.
