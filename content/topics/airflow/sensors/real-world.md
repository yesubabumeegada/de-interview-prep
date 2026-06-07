---
title: "Airflow Sensors - Real World Scenarios"
topic: airflow
subtopic: sensors
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [airflow, sensors, S3, ExternalTaskSensor, HttpSensor, real-world, production]
---

# Airflow Sensors — Real World Scenarios

## Scenario 1: Waiting for Upstream Data Landing in S3

### Context

A data warehouse pipeline runs at 8 AM daily. It depends on a vendor delivering a Parquet export to an S3 prefix by 7:30 AM. The vendor is generally reliable but occasionally delivers late. The export is chunked into multiple files under a date-partitioned prefix, and the vendor signals completion by writing a `_SUCCESS` marker file.

### Requirements
- Wait for the `_SUCCESS` marker (not individual files — vendor writes it last)
- Fail loudly if data is still absent after 3 hours (SLA breach)
- Log the number of data files found after the marker arrives
- Continue with downstream processing once confirmed

### Solution

```python
from airflow import DAG
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)

BUCKET = "vendor-data-lake"
VENDOR_PREFIX = "vendor_exports/sales/dt={date}/"
MARKER_KEY = VENDOR_PREFIX + "_SUCCESS"

default_args = {
    "owner": "data-engineering",
    "retries": 0,               # Sensor handles its own retries via poke_interval
    "email_on_failure": True,
    "email": ["de-alerts@company.com"],
}

def validate_and_count_files(ds: str, **context):
    """After marker arrives, validate and count files in the partition."""
    hook = S3Hook(aws_conn_id="aws_default")
    prefix = VENDOR_PREFIX.format(date=ds)

    keys = hook.list_keys(bucket_name=BUCKET, prefix=prefix)
    data_files = [k for k in (keys or []) if not k.endswith("_SUCCESS")]

    logger.info("Found %d data files in s3://%s/%s", len(data_files), BUCKET, prefix)

    if not data_files:
        raise ValueError(
            f"Marker file present but no data files found at s3://{BUCKET}/{prefix}"
        )

    total_size_bytes = sum(
        hook.get_key(k, bucket_name=BUCKET).content_length
        for k in data_files
    )
    logger.info("Total data size: %.2f MB", total_size_bytes / 1024 / 1024)

    # Push metadata for downstream tasks
    context["ti"].xcom_push(key="file_count", value=len(data_files))
    context["ti"].xcom_push(key="total_size_mb", value=round(total_size_bytes / 1024 / 1024, 2))
    return data_files


def process_vendor_data(ds: str, **context):
    """Process the vendor files once confirmed."""
    file_count = context["ti"].xcom_pull(task_ids="validate_files", key="file_count")
    size_mb = context["ti"].xcom_pull(task_ids="validate_files", key="total_size_mb")

    logger.info("Processing %d files (%.2f MB) for %s", file_count, size_mb, ds)
    # ... actual processing logic: read from S3, transform, write to Snowflake


with DAG(
    dag_id="vendor_sales_ingestion",
    default_args=default_args,
    schedule_interval="0 8 * * *",      # Run at 8 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["vendor", "sales", "ingestion"],
    dagrun_timeout=timedelta(hours=4),   # Kill the whole run after 4 hours
) as dag:

    # Wait for the _SUCCESS marker — vendor writes this last
    wait_for_vendor_data = S3KeySensor(
        task_id="wait_for_vendor_marker",
        bucket_name=BUCKET,
        bucket_key=MARKER_KEY.format(date="{{ ds }}"),
        aws_conn_id="aws_default",
        mode="reschedule",           # Release worker slot between pokes
        poke_interval=300,           # Check every 5 minutes
        timeout=10800,               # Fail after 3 hours (SLA breach)
        # soft_fail=False (default): failure alerts the team
    )

    # Validate files and push metadata
    validate_files = PythonOperator(
        task_id="validate_files",
        python_callable=validate_and_count_files,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # Process the data
    process_data = PythonOperator(
        task_id="process_vendor_data",
        python_callable=process_vendor_data,
        op_kwargs={"ds": "{{ ds }}"},
    )

    done = EmptyOperator(task_id="done")

    wait_for_vendor_data >> validate_files >> process_data >> done
```

### Key Design Decisions

- **Marker file pattern:** Waiting for `_SUCCESS` rather than individual Parquet files prevents processing a partial dataset if the vendor is mid-upload
- **`mode='reschedule'`:** The sensor will wait up to 3 hours; holding a worker slot that whole time would be wasteful
- **`timeout=10800`:** Hard stop after 3 hours triggers an alert — the on-call team investigates the vendor delivery
- **Validation after sensor:** The sensor only confirms the marker exists; a separate Python task validates file count and total size, preventing silent data quality issues
- **XCom for metadata:** File count and size flow downstream without re-reading S3

---

## Scenario 2: Waiting for an External DAG to Complete

### Context

A reporting DAG generates executive dashboards. It depends on three upstream warehouse loading DAGs completing first: `load_sales`, `load_inventory`, and `load_marketing`. These DAGs run at different times and have different SLAs. The reporting DAG must wait for ALL three before generating the report.

### Solution

```python
from airflow import DAG
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

UPSTREAM_DAGS = [
    {
        "dag_id": "load_sales",
        "task_id": "validate_and_load",      # Specific terminal task
        "execution_delta": timedelta(hours=0),
        "timeout": 7200,                      # Sales should finish within 2h
    },
    {
        "dag_id": "load_inventory",
        "task_id": None,                      # Wait for entire DAG
        "execution_delta": timedelta(hours=0),
        "timeout": 5400,                      # Inventory within 1.5h
    },
    {
        "dag_id": "load_marketing",
        "task_id": "final_attribution",
        "execution_delta": timedelta(hours=1),  # Marketing DAG runs 1h earlier
        "timeout": 3600,
    },
]

def generate_executive_report(**context):
    """Generate the daily executive dashboard."""
    ds = context["ds"]
    logger.info("All upstream sources confirmed for %s. Generating report...", ds)
    # ... dashboard generation logic


with DAG(
    dag_id="daily_executive_report",
    default_args={
        "owner": "analytics",
        "retries": 1,
        "retry_delay": timedelta(minutes=15),
    },
    schedule_interval="0 10 * * *",       # Run at 10 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["reporting", "executive"],
    dagrun_timeout=timedelta(hours=3),
) as dag:

    start = EmptyOperator(task_id="start")

    # Dynamically create one ExternalTaskSensor per upstream DAG
    sensors = []
    for upstream in UPSTREAM_DAGS:
        sensor = ExternalTaskSensor(
            task_id=f"wait_for_{upstream['dag_id']}",
            external_dag_id=upstream["dag_id"],
            external_task_id=upstream["task_id"],   # None = wait for whole DAG
            allowed_states=["success"],
            failed_states=["failed", "skipped"],    # Propagate failures immediately
            execution_delta=upstream["execution_delta"],
            mode="reschedule",
            poke_interval=120,                      # Check every 2 minutes
            timeout=upstream["timeout"],
            # Sensors run in parallel after start task
        )
        sensors.append(sensor)

    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=generate_executive_report,
    )

    notify_stakeholders = PythonOperator(
        task_id="notify_stakeholders",
        python_callable=lambda **kw: print("Report ready — notifying stakeholders"),
    )

    # All sensors must complete before report generation
    start >> sensors >> generate_report >> notify_stakeholders
```

### Handling Schedule Mismatches with `execution_date_fn`

When upstream DAGs run on different schedules (e.g., hourly vs daily), `execution_delta` isn't enough — you need `execution_date_fn`:

```python
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.session import create_session
from airflow.models import DagRun

def get_most_recent_successful_run(logical_date, **kwargs):
    """
    For an hourly upstream DAG, find the most recent successful run
    before our daily execution date.
    """
    with create_session() as session:
        run = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == "hourly_events_aggregator",
                DagRun.state == "success",
                DagRun.logical_date <= logical_date,
            )
            .order_by(DagRun.logical_date.desc())
            .first()
        )
    return run.logical_date if run else None

wait_for_hourly_dag = ExternalTaskSensor(
    task_id="wait_for_events",
    external_dag_id="hourly_events_aggregator",
    external_task_id="aggregate",
    execution_date_fn=get_most_recent_successful_run,   # Custom date resolution
    mode="reschedule",
    poke_interval=300,
    timeout=3600,
)
```

### Key Design Decisions

- **`failed_states=['failed', 'skipped']`:** Without this, if `load_sales` fails, the reporting sensor waits indefinitely instead of propagating the failure
- **`external_task_id=None` for one DAG:** Waits for the entire DAG run, not just a specific task. Safer when you care about all tasks succeeding
- **Parallel sensors:** All three sensors start simultaneously after the `start` task, then the report waits for all three. This parallelism reduces total wait time
- **Per-sensor timeouts:** Different upstream SLAs get different timeouts, so the sensor that fails first triggers the alert

---

## Scenario 3: HTTP Polling for a Long-Running API Job

### Context

An analytics pipeline submits a heavy computation job to an external ML platform API. The job takes between 15 minutes and 4 hours depending on data volume. The platform exposes a REST endpoint to check job status. The team needs to poll efficiently without hammering the API.

### Solution

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.http.hooks.http import HttpHook
from datetime import datetime, timedelta
import json

class MLJobSensor(BaseSensorOperator):
    """
    Polls the ML platform API for job completion.
    Handles transient errors gracefully and provides rich logging.
    """

    template_fields = ["job_id"]

    def __init__(
        self,
        job_id: str,
        http_conn_id: str = "ml_platform",
        success_states: list = None,
        failure_states: list = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_id = job_id
        self.http_conn_id = http_conn_id
        self.success_states = success_states or ["COMPLETE", "SUCCEEDED"]
        self.failure_states = failure_states or ["FAILED", "CANCELLED", "ERROR", "TIMEOUT"]

    def poke(self, context) -> bool:
        hook = HttpHook(method="GET", http_conn_id=self.http_conn_id)

        try:
            response = hook.run(
                endpoint=f"/v2/jobs/{self.job_id}",
                headers={"Accept": "application/json"},
                extra_options={"timeout": 30},    # HTTP request timeout
            )
        except Exception as e:
            # Transient connectivity issues — log and return False to retry
            self.log.warning(
                "Transient error polling job %s: %s. Will retry.", self.job_id, str(e)
            )
            return False

        data = response.json()
        state = data.get("state", "UNKNOWN")
        progress = data.get("progress_pct", 0)
        message = data.get("status_message", "")

        self.log.info(
            "Job %s: state=%s, progress=%d%%, message=%s",
            self.job_id, state, progress, message
        )

        if state in self.failure_states:
            error_detail = data.get("error", {})
            raise Exception(
                f"ML job {self.job_id} failed with state {state}. "
                f"Error: {json.dumps(error_detail)}"
            )

        if state in self.success_states:
            # Push output location for downstream tasks
            context["ti"].xcom_push(
                key="output_path",
                value=data.get("output_uri"),
            )
            context["ti"].xcom_push(
                key="job_metrics",
                value=data.get("metrics", {}),
            )
            return True

        return False   # Still running


def submit_ml_job(ds: str, **context) -> str:
    """Submit the ML computation job and return the job ID."""
    hook = HttpHook(method="POST", http_conn_id="ml_platform")
    response = hook.run(
        endpoint="/v2/jobs",
        data=json.dumps({
            "job_type": "feature_computation",
            "date_range": {"start": ds, "end": ds},
            "config": {
                "model_version": "v3.2",
                "output_format": "parquet",
                "output_uri": f"s3://ml-outputs/features/dt={ds}/",
            }
        }),
        headers={"Content-Type": "application/json"},
    )

    job_id = response.json()["job_id"]
    logger.info("Submitted ML job: %s", job_id)

    # Push job_id to XCom so sensor can reference it
    context["ti"].xcom_push(key="job_id", value=job_id)
    return job_id


def process_ml_output(**context):
    """Read ML output and load to warehouse."""
    output_path = context["ti"].xcom_pull(
        task_ids="wait_for_ml_job", key="output_path"
    )
    metrics = context["ti"].xcom_pull(
        task_ids="wait_for_ml_job", key="job_metrics"
    )
    logger.info("Processing output from %s. Metrics: %s", output_path, metrics)
    # ... read output_path, transform, load to warehouse


with DAG(
    dag_id="ml_feature_pipeline",
    default_args={
        "owner": "ml-platform",
        "retries": 1,
        "retry_delay": timedelta(minutes=30),
        "email_on_failure": True,
        "email": ["ml-alerts@company.com"],
    },
    schedule_interval="0 2 * * *",       # Run at 2 AM UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["ml", "features"],
    dagrun_timeout=timedelta(hours=6),
) as dag:

    submit_job = PythonOperator(
        task_id="submit_ml_job",
        python_callable=submit_ml_job,
        op_kwargs={"ds": "{{ ds }}"},
    )

    wait_for_ml_job = MLJobSensor(
        task_id="wait_for_ml_job",
        # Pull job_id from submit_job's XCom using Jinja template
        job_id="{{ ti.xcom_pull(task_ids='submit_ml_job', key='job_id') }}",
        http_conn_id="ml_platform",
        mode="reschedule",
        poke_interval=180,          # Check every 3 min (avoid rate limiting)
        exponential_backoff=True,   # Back off if job is taking very long
        timeout=14400,              # 4 hour max wait
    )

    process_output = PythonOperator(
        task_id="process_output",
        python_callable=process_ml_output,
    )

    submit_job >> wait_for_ml_job >> process_output
```

### Key Design Decisions

- **Submit-then-sense pattern:** Submit returns a job ID; the sensor references it via XCom template. This separates concerns and makes the sensor reusable
- **Transient error handling in poke():** Network blips shouldn't fail the sensor — returning `False` on transient errors lets the sensor retry naturally
- **Immediate raise on failure states:** When the API returns a terminal failure state, raise immediately rather than waiting for timeout. Provides faster failure feedback and cleaner error messages
- **`exponential_backoff=True`:** Jobs that run 4 hours don't need to be checked every 3 minutes at hour 3. Backoff reduces API calls while maintaining responsiveness for fast completions
- **XCom push from poke():** The sensor pushes output location and metrics when the job succeeds, avoiding the need for an additional API call in the downstream task
