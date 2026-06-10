---
title: "Cloud Composer / Airflow — Real-World Cases"
topic: gcp
subtopic: cloud-composer
content_type: study_material
difficulty_level: mid-level
tags: [gcp, cloud-composer, interview]
---

# Cloud Composer / Airflow — Real-World Cases

These are the kinds of production stories that turn a generic interview answer into a memorable one. Each case includes the symptom, the investigation, the fix, and the numbers.

## Case Study 1: The 4 AM Pipeline That Started Finishing at 9 AM

**Context.** A retail analytics team ran ~80 DAGs on a Composer 2 medium environment (2–6 autoscaled workers). The nightly warehouse build (landing → staging → marts in BigQuery) had finished by 4 AM for a year. Over six weeks, completion crept to 9 AM, breaching the 7 AM SLA for finance dashboards.

**Investigation.**

```bash
# Step 1: is it task duration or scheduling latency?
# Cloud Monitoring: composer.googleapis.com/environment/dag_processing/total_parse_time
# showed parse time climbing from 8s to 55s — above the 30s parse interval.
```

Task durations were flat; the gap was `scheduled → queued → running` latency. Parse time was the smoking gun. The team had recently merged a "config-driven DAG factory" that generated 300 DAGs — and the factory file called BigQuery at import time to list source tables:

```python
# THE PROBLEM — ran on EVERY scheduler parse loop (~every 30s)
client = bigquery.Client()
tables = [t.table_id for t in client.list_tables("proj.landing")]  # 300+ API calls/parse

for table in tables:
    dag = build_ingest_dag(table)
    globals()[dag.dag_id] = dag
```

**Fix.**

```python
# Manifest generated nightly by CI, read locally — parse cost ~50ms
import json, pathlib

MANIFEST = pathlib.Path(__file__).parent / "manifests/landing_tables.json"
tables = json.loads(MANIFEST.read_text())

for table in tables:
    dag = build_ingest_dag(table)
    globals()[dag.dag_id] = dag
```

Plus: raised `scheduler-count` from 1 to 2 and bumped scheduler CPU 1→2.

**Outcome.** Parse time 55s → 4s. Pipeline completion back to 3:40 AM. Cost increase: ~$60/month for the second scheduler. The post-mortem rule that stuck: *no network I/O at DAG module level — CI builds manifests, DAGs read files.*

**Interview takeaway.** When an Airflow pipeline slows down with no task-level change, check **DAG parse time and scheduler latency first** — the scheduler is a shared resource that degrades globally.

## Case Study 2: The $18,000 Sensor Bill

**Context.** A logistics company waited on 40+ partner SFTP/GCS file drops per day. Their DAGs used poke-mode sensors with `poke_interval=60`, `timeout=12h`. As partners grew, the environment autoscaled to its 10-worker max nearly all day.

**Numbers before.**
- ~45 sensors live concurrently for 6–12 hours each
- `worker_concurrency=8` → sensors alone consumed ~5.6 workers continuously
- Composer bill: **~$2,900/month**, trending up; plus real loads queued behind sensors causing SLA misses

**Investigation.** The Airflow UI "Running" view told the story: most running tasks were sensors in `poke` mode, each pinning a Celery slot while sleeping. Worker CPU utilization was under 8% — the team was paying for sleeping processes.

**Fix in two steps.**

```python
# Step 1 (same day): reschedule mode — slot released between pokes
wait = GCSObjectExistenceSensor(
    task_id="wait_partner_file",
    bucket="partner-landing",
    object="acme/{{ ds }}/orders.csv",
    mode="reschedule",
    poke_interval=600,
)

# Step 2 (next sprint): deferrable sensors on the triggerer
wait = GCSObjectExistenceSensor(
    task_id="wait_partner_file",
    bucket="partner-landing",
    object="acme/{{ ds }}/orders.csv",
    deferrable=True,
)
```

For the highest-volume partner they went further — event-driven: GCS notification → Pub/Sub → a tiny Cloud Function calling the Airflow REST API to trigger the DAG, eliminating waiting entirely:

```python
# Cloud Function: trigger DAG on file arrival
import google.auth
from google.auth.transport.requests import AuthorizedSession

def trigger_dag(event, context):
    creds, _ = google.auth.default()
    session = AuthorizedSession(creds)
    web_url = "https://<composer-webserver-url>"
    session.post(
        f"{web_url}/api/v1/dags/partner_acme_ingest/dagRuns",
        json={"conf": {"object": event["name"]}},
    )
```

**Outcome.** Autoscaler steady-state dropped from 10 workers to 2–3. Bill: $2,900 → **~$1,400/month** (≈$18k/year saved). SLA misses from queueing went to zero.

**Interview takeaway.** Poke-mode sensors are the most common Composer cost and throughput bug. The maturity ladder is **poke → reschedule → deferrable → event-driven trigger**.

## Case Study 3: The Backfill That Double-Loaded Revenue

**Context.** A fintech team needed to reprocess 90 days of transactions after a currency-conversion bug fix. An engineer ran a backfill on the `daily_revenue` DAG. The next morning, finance reported revenue numbers roughly **2x** for the backfilled period.

**Investigation.** The load task appended to the target:

```python
# THE PROBLEM — append + reruns = duplicates
load = BigQueryInsertJobOperator(
    task_id="load_revenue",
    configuration={
        "query": {
            "query": """
                INSERT INTO `proj.warehouse.revenue`
                SELECT * FROM `proj.staging.revenue`
                WHERE tx_date = '{{ ds }}'
            """,
            "useLegacySql": False,
        }
    },
)
```

The original runs had already inserted those dates; the backfill inserted them again. Nothing in the DAG was idempotent.

**Fix.** Idempotent partition overwrite (and a cleanup MERGE for the damage):

```sql
-- One-time repair: rebuild affected partitions from staging
MERGE `proj.warehouse.revenue` t
USING (
  SELECT * FROM `proj.staging.revenue`
  WHERE tx_date BETWEEN '2026-02-01' AND '2026-04-30'
) s
ON FALSE
WHEN NOT MATCHED BY SOURCE
     AND t.tx_date BETWEEN '2026-02-01' AND '2026-04-30' THEN DELETE
WHEN NOT MATCHED BY TARGET THEN INSERT ROW;
```

```python
# Permanent fix — write to the partition decorator with truncate
load = BigQueryInsertJobOperator(
    task_id="load_revenue",
    configuration={
        "query": {
            "query": """
                SELECT * FROM `proj.staging.revenue`
                WHERE tx_date = '{{ ds }}'
            """,
            "useLegacySql": False,
            "destinationTable": {
                "projectId": "proj",
                "datasetId": "warehouse",
                "tableId": "revenue${{ ds_nodash }}",
            },
            "writeDisposition": "WRITE_TRUNCATE",
        }
    },
)
```

Process changes: `max_active_runs=1` during backfills to avoid partition write contention; a data-quality task (row-count vs source within 1%) added as a post-load gate; backfills require a peer-reviewed runbook.

**Outcome.** Finance numbers restated within a day. Every warehouse DAG migrated to partition-truncate or MERGE-on-key semantics over the next quarter. Subsequent backfills (there were three more that year) were non-events.

**Interview takeaway.** The phrase to say out loud: *"Every scheduled load must be safe to run twice."* Backfills, retries, and DR replays all assume idempotency — partition `WRITE_TRUNCATE` or keyed `MERGE` are the two standard mechanisms.

## Patterns Across All Three Cases

| Theme | Rule of thumb |
|---|---|
| Scheduler health | No network I/O at DAG top level; watch total parse time like an SLO |
| Worker economics | Workers orchestrate; sensors go deferrable; heavy compute goes to BigQuery/Dataflow |
| Idempotency | Partition truncate or MERGE; templated `{{ ds }}` everywhere; never `now()` |
| Cost control | Steady-state worker count is the bill — find what pins workers |
| Change safety | CI with DagBag tests; backfill runbooks; data-quality gates after loads |

## How to Use These in an Interview

When asked "tell me about a production issue you debugged," structure it as: **symptom → metric that localized it → root cause → fix → number that proves impact → rule you institutionalized**. The Composer-specific metrics worth naming: DAG parse time, scheduler heartbeat, queued task count, worker pod evictions, and Cloud SQL CPU.
