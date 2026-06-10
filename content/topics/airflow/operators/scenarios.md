---
title: "Airflow Operators - Scenario Questions"
topic: airflow
subtopic: operators
content_type: scenario_question
tags: [airflow, operators, pythonoperator, bashoperator, provider-operators, interview, scenarios]
---

# Scenario Questions — Airflow Operators

<article data-difficulty="junior">

## 🟢 Junior: Build a Three-Task ELT DAG

**Scenario:** You need to build a daily Airflow DAG that: (1) runs a Python function to extract data from an API and save it to `/tmp/data_{{ ds }}.json`, (2) runs a Bash command `python /scripts/transform.py --date {{ ds }}` to transform the file, (3) runs another Python function to load the result into a database. Wire the tasks in sequence.

<details>
<summary>💡 Hint</summary>

Use `PythonOperator` for tasks 1 and 3, `BashOperator` for task 2. Set dependencies with the `>>` operator. The Bash command uses Jinja templating so `{{ ds }}` resolves to the execution date automatically.

</details>

<details>
<summary>✅ Solution</summary>

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests, json

def extract_from_api(**context):
    ds = context['ds']
    response = requests.get(f"https://api.example.com/data?date={ds}")
    response.raise_for_status()
    with open(f"/tmp/data_{ds}.json", 'w') as f:
        json.dump(response.json(), f)
    return {"rows": len(response.json())}

def load_to_database(**context):
    ds = context['ds']
    with open(f"/tmp/data_{ds}.json") as f:
        data = json.load(f)
    # connect and insert data...
    print(f"Loaded {len(data)} records for {ds}")

with DAG(
    dag_id='daily_elt_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=5)},
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_from_api,
    )

    transform = BashOperator(
        task_id='transform',
        bash_command='python /scripts/transform.py --date {{ ds }}',
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_to_database,
    )

    extract >> transform >> load
```

**Key points:**
- `context['ds']` gives the execution date as a string (`YYYY-MM-DD`)
- `{{ ds }}` in `BashOperator.bash_command` is Jinja-templated automatically
- `>>` sets downstream dependency — left task must succeed before right task starts

</details>

</article>

<article data-difficulty="junior">

## 🟢 Junior: Parallel Fan-Out with a Convergence Point

**Scenario:** A pipeline needs to load data for three regions (US, EU, APAC) in parallel, and only after all three succeed, run a final consolidation step. Design the DAG structure.

<details>
<summary>💡 Hint</summary>

Create one task per region using `PythonOperator`. Use a list on the right side of `>>` to fan out to all three tasks simultaneously. Then use a list on the left side of `>>` to converge all three into the consolidation task.

</details>

<details>
<summary>✅ Solution</summary>

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def load_region(region, **context):
    print(f"Loading data for {region} on {context['ds']}")
    # your region-specific load logic

with DAG(
    dag_id='multi_region_load',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
) as dag:

    start = EmptyOperator(task_id='start')

    load_us = PythonOperator(
        task_id='load_us',
        python_callable=load_region,
        op_kwargs={'region': 'US'},
    )
    load_eu = PythonOperator(
        task_id='load_eu',
        python_callable=load_region,
        op_kwargs={'region': 'EU'},
    )
    load_apac = PythonOperator(
        task_id='load_apac',
        python_callable=load_region,
        op_kwargs={'region': 'APAC'},
    )

    consolidate = PythonOperator(
        task_id='consolidate',
        python_callable=lambda **ctx: print("All regions loaded — consolidating"),
    )

    # Fan-out
    start >> [load_us, load_eu, load_apac]
    # Fan-in (all three must succeed)
    [load_us, load_eu, load_apac] >> consolidate
```

**Default behaviour:** `consolidate` only runs if ALL of `load_us`, `load_eu`, `load_apac` succeed. This is controlled by `trigger_rule`, which defaults to `'all_success'`.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Conditional Branching Based on Data

**Scenario:** A daily pipeline should run a `full_refresh` task on Mondays and an `incremental_load` task on all other days. After either task completes, a `send_report` task should always run. Implement this with `BranchPythonOperator`.

<details>
<summary>💡 Hint</summary>

`BranchPythonOperator` returns the `task_id` of the branch to take. Tasks on the unchosen branch are automatically skipped. The convergence task (`send_report`) must use `trigger_rule='none_failed_min_one_success'` or `'one_success'` so it runs even though one branch was skipped.

</details>

<details>
<summary>✅ Solution</summary>

```python
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

def choose_load_strategy(**context):
    # execution_date is the logical date of the run
    weekday = context['execution_date'].weekday()  # 0=Monday
    if weekday == 0:
        return 'full_refresh'
    return 'incremental_load'

def full_refresh(**context):
    print(f"Running full refresh for {context['ds']}")

def incremental_load(**context):
    print(f"Running incremental load for {context['ds']}")

def send_report(**context):
    print(f"Sending report for {context['ds']}")

with DAG(
    dag_id='adaptive_load_strategy',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
) as dag:

    branch = BranchPythonOperator(
        task_id='choose_strategy',
        python_callable=choose_load_strategy,
    )

    full   = PythonOperator(task_id='full_refresh',     python_callable=full_refresh)
    incr   = PythonOperator(task_id='incremental_load', python_callable=incremental_load)

    report = PythonOperator(
        task_id='send_report',
        python_callable=send_report,
        trigger_rule='one_success',  # Run if at least one upstream succeeded
    )

    branch >> [full, incr]
    [full, incr] >> report
```

**Why `trigger_rule='one_success'`?**  
With the default `all_success`, `send_report` would never run because one of `full_refresh` or `incremental_load` is always skipped. `one_success` means "run if at least one upstream task succeeded."

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Retry Strategy for a Flaky External API

**Scenario:** Your `PythonOperator` calls a third-party payment API that occasionally returns 429 (rate limited) or 503 (temporarily unavailable). Failures on the first attempt are common. Design retry logic with exponential backoff, a maximum retry delay, and a custom callback that posts to Slack on final failure.

<details>
<summary>💡 Hint</summary>

Set `retries`, `retry_delay`, `retry_exponential_backoff=True`, and `max_retry_delay` on the operator. Provide `on_failure_callback` that only fires after all retries are exhausted (Airflow calls it on the final failure state, not on each retry — use `on_retry_callback` for per-retry hooks).

</details>

<details>
<summary>✅ Solution</summary>

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests, logging

def post_to_slack(context):
    dag_id  = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    ds      = context['ds']
    err     = context.get('exception', 'Unknown error')
    # Call your Slack webhook
    requests.post(
        'https://hooks.slack.com/services/XXX/YYY/ZZZ',
        json={'text': f":red_circle: *{dag_id}.{task_id}* FAILED on {ds}\n```{err}```"}
    )

def call_payment_api(**context):
    response = requests.post(
        'https://api.payments.example.com/settle',
        json={'date': context['ds']},
        timeout=30,
    )
    if response.status_code == 429:
        raise Exception("Rate limited (429) — retry after backoff")
    if response.status_code >= 500:
        raise Exception(f"Server error {response.status_code} — retrying")
    response.raise_for_status()
    logging.info(f"Settled {response.json()['count']} payments")

with DAG(
    dag_id='payment_settlement',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
) as dag:

    settle = PythonOperator(
        task_id='settle_payments',
        python_callable=call_payment_api,
        retries=4,
        retry_delay=timedelta(minutes=2),      # First retry after 2m
        retry_exponential_backoff=True,         # 2m → 4m → 8m → 16m
        max_retry_delay=timedelta(minutes=20),  # Cap at 20m
        execution_timeout=timedelta(hours=1),   # Kill if running > 1h total
        on_failure_callback=post_to_slack,      # Fires only on final failure
        on_retry_callback=lambda ctx: logging.warning(
            f"Retrying settle_payments — attempt {ctx['ti'].try_number}"
        ),
    )
```

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Idempotent Operator Design for a Stateful Load

**Scenario:** A `PythonOperator` loads daily transaction data into a Snowflake table. The pipeline occasionally fails mid-run and is retried. The naive implementation uses `INSERT INTO` which causes duplicate rows on retry. Redesign it to be fully idempotent — safe to run multiple times with the same execution date and always produce the same result.

<details>
<summary>💡 Hint</summary>

Idempotency for a daily load means: "running for 2024-03-15 twice produces the same rows as running once." The standard patterns are: (1) DELETE + INSERT for the partition/date, (2) MERGE (upsert) keyed on a natural key, (3) CREATE OR REPLACE on a date-partitioned table. Avoid `INSERT INTO` without a guard.

</details>

<details>
<summary>✅ Solution</summary>

```python
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def load_transactions_idempotent(**context):
    ds = context['ds']
    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')

    # Pattern 1: DELETE the partition then INSERT fresh
    # Safe because: if we crash after DELETE before INSERT, the next retry
    # starts from an empty partition and inserts cleanly.
    hook.run(f"""
        BEGIN TRANSACTION;

        -- Remove existing data for this date (idempotent delete)
        DELETE FROM warehouse.transactions
        WHERE DATE(created_at) = '{ds}';

        -- Insert current data for this date
        INSERT INTO warehouse.transactions
        SELECT *
        FROM staging.transactions_staging
        WHERE DATE(created_at) = '{ds}';

        COMMIT;
    """)

    # Pattern 2 (alternative): MERGE on natural key
    hook.run(f"""
        MERGE INTO warehouse.transactions AS tgt
        USING (
            SELECT * FROM staging.transactions_staging
            WHERE DATE(created_at) = '{ds}'
        ) AS src
        ON tgt.tx_id = src.tx_id
        WHEN MATCHED THEN UPDATE SET
            tgt.status     = src.status,
            tgt.updated_at = CURRENT_TIMESTAMP()
        WHEN NOT MATCHED THEN INSERT
            (tx_id, account_id, amount, created_at, status)
        VALUES
            (src.tx_id, src.account_id, src.amount, src.created_at, src.status);
    """)

idempotent_load = PythonOperator(
    task_id='load_transactions',
    python_callable=load_transactions_idempotent,
    retries=3,
    retry_delay=timedelta(minutes=5),
)
```

**Why this is idempotent:**
- **DELETE + INSERT:** The transaction ensures atomicity. On retry, the partition is empty (previous partial insert was rolled back or we re-delete it), so we get exactly the same final state regardless of how many times it runs.
- **MERGE:** Natural key (`tx_id`) deduplicates automatically — existing rows are updated, missing rows are inserted. Running twice produces identical output.

**Anti-patterns to avoid:**
```python
# ❌ NOT idempotent — creates duplicates on retry
hook.run(f"INSERT INTO warehouse.transactions SELECT * FROM staging...")

# ❌ NOT safe — truncates ALL data, not just today's partition
hook.run("TRUNCATE TABLE warehouse.transactions")
```

**Verification test:**
```python
def test_idempotency():
    # Run the operator twice for the same date
    op.execute(context={'ds': '2024-03-15', ...})
    op.execute(context={'ds': '2024-03-15', ...})

    # Row count should be the same as running once
    count_after_two_runs = hook.get_first(
        "SELECT COUNT(*) FROM warehouse.transactions WHERE DATE(created_at)='2024-03-15'"
    )[0]
    count_from_source = hook.get_first(
        "SELECT COUNT(*) FROM staging.transactions_staging WHERE DATE(created_at)='2024-03-15'"
    )[0]
    assert count_after_two_runs == count_from_source
```

</details>

</article>

---

## ⚡ Quick-fire Q&A

**Q: What is the difference between BashOperator, PythonOperator, and using a provider operator like S3ToRedshiftOperator?**
A: BashOperator runs shell commands — simple but hard to test and debug. PythonOperator calls a Python callable — more testable and Pythonic. Provider operators (like S3ToRedshiftOperator) are purpose-built with built-in connection management, retry logic, and template fields — prefer them for standard integrations to avoid reinventing the wheel.

**Q: What is the `provide_context` parameter in PythonOperator and is it still needed?**
A: In Airflow 1.x, `provide_context=True` was required to inject the Airflow context dict into the Python callable. In Airflow 2.x, context is always provided if the callable accepts `**kwargs` or explicitly named context keys — `provide_context` is deprecated and no longer needed.

**Q: What is the BranchPythonOperator and how does it work?**
A: BranchPythonOperator calls a Python callable that returns a task_id (or list of task_ids) representing which downstream branch(es) to follow. All other downstream tasks are skipped. Used for conditional logic — e.g., choosing between a full refresh and incremental load based on a condition.

**Q: How does the ShortCircuitOperator differ from BranchPythonOperator?**
A: ShortCircuitOperator evaluates a condition and either allows all downstream tasks to proceed (if True) or skips all of them (if False). Unlike BranchPythonOperator, it doesn't route between branches — it either continues or stops the entire downstream chain.

**Q: What is the DummyOperator (EmptyOperator in Airflow 2.4+) used for?**
A: DummyOperator/EmptyOperator performs no action but acts as a logical grouping node in the DAG graph — used to create fan-in or fan-out points, label pipeline stages visually, or satisfy dependency requirements without executing any real work.

**Q: What happens when a task executed by an operator raises an exception?**
A: The task transitions to a `failed` state. If `retries` is configured, Airflow reschedules the task after `retry_delay`. After exhausting retries, it stays `failed`. `on_failure_callback` is invoked if configured. If `trigger_rule` of downstream tasks allows, they may still run despite the upstream failure.

**Q: What is the `execution_timeout` parameter and when should you set it?**
A: `execution_timeout` sets a maximum duration for a task before Airflow kills it and marks it as failed. Always set it for tasks that interact with external systems (APIs, databases, file systems) — without it, a hung task can occupy a worker slot indefinitely, starving other tasks.

**Q: How do you pass data between two operators without using XCom for large payloads?**
A: Write the data to an intermediate external store (S3, GCS, a staging database table), then pass only a reference (URI, table name, path) via XCom or a templated parameter to the downstream operator. This keeps XCom lightweight and avoids metadata database bloat.

---

## 💼 Interview Tips

- Know the operator hierarchy: `BaseOperator` → provider operators → sensor operators → branch operators. Being able to explain this shows architectural understanding of Airflow's design.
- When asked about PythonOperator, mention that heavy compute should run in a dedicated resource (Spark, Lambda, ECS task) triggered via an operator — not executed directly in the Airflow worker process.
- Discuss `execution_timeout` proactively — it's a frequently overlooked parameter that causes real production issues. Senior interviewers know this and will probe for it.
- Provider operators are increasingly the preferred answer over BashOperator/PythonOperator for standard integrations — knowing `apache-airflow-providers-*` ecosystem signals up-to-date Airflow knowledge.
- Be ready to explain `trigger_rule` options (`all_success`, `all_done`, `one_failed`, `none_failed`) — they're closely related to operator behavior and a common advanced interview question.
- Avoid describing operators as just "wrappers around code" — demonstrate understanding of their role in the broader DAG execution model: scheduling, retries, state management, and worker resource consumption.
