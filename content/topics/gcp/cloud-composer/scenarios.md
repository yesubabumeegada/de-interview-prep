---
title: "Cloud Composer / Airflow — Scenarios"
topic: gcp
subtopic: cloud-composer
content_type: scenario_question
tags: [gcp, cloud-composer, interview]
---

# Cloud Composer / Airflow — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: The DAG That Never Appears

**Scenario:** You wrote a new DAG file, `customer_export.py`, tested it locally, and copied it to the Composer environment's GCS bucket under `dags/`. Twenty minutes later it still doesn't show up in the Airflow UI. Other DAGs are running fine. The interviewer asks: how do you debug this, step by step?

<details>
<summary>💡 Hint</summary>

Think about what happens between "file lands in the bucket" and "DAG appears in the UI" — the file must sync to the scheduler and then be *successfully imported* as Python. Where would an import failure be reported, and what common file-level mistakes stop a DAG object from being registered at all?

</details>

<details>
<summary>✅ Solution</summary>

A structured answer:

**1. Confirm the file is in the right place.**

```bash
gcloud composer environments describe my-env \
    --location us-central1 \
    --format="value(config.dagGcsPrefix)"

gsutil ls gs://us-central1-my-env-abc123-bucket/dags/customer_export.py
```

A very common miss: copying to the bucket root or a `data/` folder instead of `dags/`.

**2. Check for import errors.** The UI shows a red "DAG Import Errors" banner; or via CLI:

```bash
gcloud composer environments run my-env \
    --location us-central1 \
    dags list-import-errors
```

Typical causes: a missing PyPI package on the workers (works locally, not in Composer), a syntax error, or an exception thrown by top-level code.

**3. Check the file actually defines a DAG at module scope.** The scheduler only registers DAG objects reachable at top level:

```python
# BAD — dag is local to a function, never registered
def make_dag():
    with DAG(dag_id="customer_export", ...) as dag:
        ...
    return dag

# GOOD
with DAG(dag_id="customer_export", ...) as dag:
    ...
# or: globals()["customer_export"] = make_dag()
```

**4. Check for a duplicate `dag_id`.** If another file defines the same `dag_id`, one silently wins.

**5. Check the paused state and filters.** New DAGs may start paused (`dags_are_paused_at_creation=True`), and the UI search/tag filter may simply be hiding it.

| Check | Tool |
|---|---|
| File location | `gsutil ls` against `dagGcsPrefix` |
| Import errors | UI banner / `dags list-import-errors` |
| DAG at module scope | Code review |
| Duplicate dag_id | `dags list` |
| Paused / filtered | UI toggle |

Closing line for the interview: "Ninety percent of missing-DAG cases are an import error or a wrong bucket path — I check those two before anything else."

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Tasks Queue for 40 Minutes Every Morning

**Scenario:** Your Composer 2 environment runs 60 DAGs. Most are scheduled between 5:00 and 6:00 AM. Users complain that tasks sit in `queued` state for 30–40 minutes during that window, but by 8 AM everything runs instantly. The environment has `min-workers=1`, `max-workers=4`, `worker_concurrency=8`. The interviewer asks: diagnose the problem and propose fixes, including the trade-offs.

<details>
<summary>💡 Hint</summary>

Work out the arithmetic of available task slots at peak versus the burst of tasks released at 5 AM, and remember the autoscaler reacts to queue depth — it doesn't anticipate it. Also consider environment-level caps other than worker count that gate how many tasks may run at once.

</details>

<details>
<summary>✅ Solution</summary>

**Diagnosis — capacity math.** Peak capacity = `max-workers (4) × worker_concurrency (8) = 32 concurrent tasks`. Sixty DAGs releasing several tasks each at 5 AM can easily mean 150+ runnable tasks against 32 slots, and the autoscaler starts from 1 worker, taking minutes to scale up pod by pod. Also check `parallelism` — if it's at the default for a small/medium env (e.g., 32), it caps everything regardless of workers.

**Verify before fixing:**

- Cloud Monitoring: queued task count, worker pod count over the 5–6 AM window
- Confirm worker CPU/memory — are tasks lightweight (operators polling BigQuery) or heavy?
- Check pools: a default pool of 128 slots is fine, a custom pool of 16 is a hidden cap

**Fixes, in order of preference:**

1. **Raise `min-workers` ahead of the peak.** Set `min-workers=3` so capacity exists at 5 AM rather than reacting at 5:10. Trade-off: pay for idle workers off-peak (often trivial — a few dollars a day).

```bash
gcloud composer environments update my-env \
    --location us-central1 \
    --min-workers 3 --max-workers 8
```

2. **Raise `max-workers` and `parallelism`.** If the burst is genuinely large, allow the ceiling to absorb it. Trade-off: a runaway DAG can now consume more resources; protect shared downstreams (e.g., a fragile API) with pools.

3. **Spread the schedules.** Stagger DAGs across 4:30–6:30 with a deterministic hash or explicit offsets. Trade-off: requires agreement on SLAs; some DAGs genuinely need 5 AM data.

4. **Reduce slot waste.** If many of the queued "tasks" are sensors, convert to `deferrable=True` so real work gets the slots.

5. **Check task duration, not just count.** If most tasks just submit BigQuery jobs and poll, raising `worker_concurrency` to 12–16 is cheap (low CPU per task). Trade-off: memory pressure — watch for pod evictions.

**What a strong answer sounds like:** "It's a thundering-herd problem: 32 slots, 150 tasks, and a reactive autoscaler starting from one worker. I'd pre-scale with `min-workers` before the window, lift `parallelism`, convert sensors to deferrable, and only then talk about staggering schedules — that's an organizational fix, the others are config."

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Consolidate or Federate? 5 Teams, One Composer Decision

**Scenario:** You join a company where five data teams each run their own orchestration: two self-managed Airflow instances on GKE, one Composer 2 environment, one cron-on-VM setup, and one team using Cloud Scheduler + Cloud Functions chains. Leadership wants "one orchestration platform" to cut cost and standardize. You're asked to design the target state on Cloud Composer: one shared environment or several? How do you handle isolation, CI/CD, cost attribution, and the migration itself?

<details>
<summary>💡 Hint</summary>

Resist the instinct that "one platform" means "one environment." Think about blast radius, upgrade windows, IAM trust boundaries, and noisy-neighbor effects within a single Airflow deployment — then weigh that against the per-environment baseline cost. Your migration plan should also sequence teams by risk, not all at once.

</details>

<details>
<summary>✅ Solution</summary>

**Target topology: few environments, not one.** Recommend **one Composer 3 environment per trust boundary**, typically 2–3 environments (e.g., `core-data`, `finance` for stricter compliance, `experimental`), each with dev/prod pairs. Rationale:

| Concern | One shared env | Per-boundary envs |
|---|---|---|
| Blast radius | A bad DAG factory degrades scheduler for all 5 teams | Contained to one team-group |
| Upgrades | One window must suit everyone | Independent windows |
| IAM | All tasks share env service account by default | Distinct SAs per env; simpler least-privilege |
| Noisy neighbor | Shared parallelism/pools, queue contention | Isolated capacity |
| Cost | Cheapest baseline (~1 env) | +$400–700/month per extra small env — usually worth it |

Within an environment, mitigate sharing with **pools per team**, `max_active_runs` policies, and `impersonation_chain` on Google operators for per-DAG credentials — but be explicit that Airflow's intra-environment isolation is weak; that's why trust boundaries get their own environment.

**Platform standards (the actual "one platform"):**

- One **DAG repo structure** (or monorepo with per-team folders), shared CI: lint, DagBag import test, unit tests, then `gsutil rsync` to the right env bucket per branch/tag.
- **Golden DAG patterns library**: idempotent BigQuery load (partition truncate / MERGE), deferrable sensor wrappers, alerting callbacks, dataset-based cross-DAG dependencies.
- **Secrets** exclusively via Secret Manager backend; no Airflow-UI-created connections in prod.
- **Observability**: standard Cloud Monitoring dashboards (parse time, queued tasks, env health) and per-team alert routing via labels.

**Cost attribution:** environments map to teams → bills map naturally. For shared environments, use task-level labels on BigQuery/Dataflow jobs (`labels={"team": "finance"}`) so downstream compute — usually 10x the Composer bill — is attributable.

**Migration sequencing (risk-ordered):**

1. **Cron-on-VM team first** — lowest sophistication, biggest reliability win; wrap existing scripts in `BashOperator`/KPO, then refactor.
2. **Cloud Scheduler + Functions chains** — convert each chain to a DAG; keep event-driven entry points (Pub/Sub → REST trigger) where they're genuinely event-shaped.
3. **Composer 2 team** — upgrade path to Composer 3 via parallel environment + snapshot validation.
4. **Self-managed Airflow teams last** — they have working systems and the most custom config (plugins, custom operators). Audit plugin compatibility, pin matching Airflow version, dual-run critical DAGs (shadow mode comparing outputs) for 2 weeks before cutover.

**Numbers to volunteer:** two self-managed GKE Airflows at ~0.2 FTE each of ops toil (~$4–8k/month loaded) versus 3 Composer prod environments at ~$2–3k/month total — consolidation pays for itself even before incident-cost reduction.

**Closing summary:** "One platform means one set of standards, CI, and golden patterns — not one environment. I'd run 2–3 Composer 3 environments aligned to trust boundaries, migrate teams in ascending order of orchestration maturity, and dual-run the self-managed Airflow workloads before cutover."

</details>

</article>

## Interview Tips

> **Tip 1:** "Why Composer instead of just running Airflow yourself?" — Answer in TCO terms: Composer trades a higher invoice for near-zero ops toil (upgrades, metadata DB, Celery, on-call). Say you'd only self-manage with a dedicated platform team or a hard requirement Composer can't meet.

> **Tip 2:** "How do you deploy DAGs safely?" — Never say "I copy files to the bucket." Say: Git repo → CI runs lint + DagBag import test + unit tests → `gsutil rsync` to the environment bucket, with dev/staging/prod environments and idempotent DAGs so reruns are safe.

> **Tip 3:** "Your pipeline is slow/stuck — what do you check?" — Show a hierarchy: DAG parse time and scheduler health first (global), then queue depth vs worker slots (capacity), then individual task logs (local). Interviewers reward candidates who debug the shared layers before the single task.

## ⚡ Quick-fire Q&A

**Q:** Where does Composer store Airflow metadata?
A: A Google-managed Cloud SQL (PostgreSQL) instance.

**Q:** How do DAGs get into Composer?
A: Sync Python files to the environment's GCS bucket under `dags/`; the scheduler picks them up automatically.

**Q:** What executor does Composer use?
A: Celery-based execution on GKE worker pods (CeleryExecutor / Celery Kubernetes under the hood).

**Q:** Poke vs reschedule vs deferrable sensor?
A: Poke holds a worker slot while sleeping; reschedule frees the slot between checks; deferrable hands the wait to the async triggerer — cheapest at scale.

**Q:** Biggest difference between Composer 2 and Composer 3?
A: Composer 3 fully hides/manages the GKE layer — faster environment ops and upgrades, but no direct cluster access.

**Q:** How do you make a daily BigQuery load idempotent?
A: Write to the partition decorator (`table$YYYYMMDD`) with `WRITE_TRUNCATE`, or use a keyed MERGE — safe to run twice.

**Q:** Why is top-level code in DAG files dangerous?
A: The scheduler re-parses files every ~30 seconds; module-level API calls or queries execute on every parse and degrade the whole environment.

**Q:** How would you trigger a DAG from a GCS file arrival without sensors?
A: GCS notification → Pub/Sub → Cloud Function calling the Airflow REST API `dagRuns` endpoint.
