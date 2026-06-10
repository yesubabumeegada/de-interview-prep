---
title: "Dataproc / Spark & Hadoop — Scenarios"
topic: gcp
subtopic: dataproc
content_type: scenario_question
tags: [gcp, dataproc, interview]
---

# Dataproc / Spark & Hadoop — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Pick the Right Tool for a Nightly Job

**Scenario:** Your team has a nightly job that reads 200GB of Parquet files from GCS, joins them with a reference table, aggregates by customer, and writes results back to GCS for a downstream system. A teammate proposes keeping a 10-node Dataproc cluster running 24/7 so "the job can start instantly at 2 AM." The job takes about 45 minutes. The interviewer asks: do you agree, and what would you propose instead?

<details>
<summary>💡 Hint</summary>

Compare hours of compute actually needed per day to hours the proposed cluster would be billed. Then recall what makes Dataproc clusters disposable on GCP — where does the data live, and how long does a cluster take to start?

</details>

<details>
<summary>✅ Solution</summary>

Disagree, with arithmetic. The job needs ~45 minutes of cluster time daily; a 24/7 cluster bills 24 hours — roughly **32x more compute-hours than used** (~3% utilization). Cluster startup is ~90 seconds, so "instant start" buys nothing meaningful against a 2 AM batch SLA.

**Proposal 1 — ephemeral cluster via workflow template** (create → run → delete automatically):

```bash
gcloud dataproc workflow-templates create nightly-agg --region us-central1

gcloud dataproc workflow-templates set-managed-cluster nightly-agg \
    --region us-central1 \
    --cluster-name nightly-agg-ephemeral \
    --num-workers 8 \
    --worker-machine-type n2-standard-8

gcloud dataproc workflow-templates add-job pyspark \
    gs://etl-code/nightly_agg.py \
    --step-id aggregate \
    --workflow-template nightly-agg \
    --region us-central1
```

**Proposal 2 — even simpler, Dataproc Serverless** (no cluster at all):

```bash
gcloud dataproc batches submit pyspark gs://etl-code/nightly_agg.py \
    --region us-central1 \
    --version 2.2
```

This works because the data is in **GCS, not HDFS** — nothing is lost when the cluster dies. Rough numbers: 10 × n2-standard-8 ≈ $3.9/hr + premium → 24/7 ≈ **$2,900/month**; ephemeral 45 min/night ≈ **$95/month**.

Safety nets if a standing cluster were ever justified: `--max-idle 30m --max-age 8h`.

Also worth one sentence: "If this join+aggregate is expressible as SQL, I'd evaluate loading to BigQuery and doing it there — possibly no Spark needed at all." That sentence scores points even in a junior interview.

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Spot Workers Made the Job Slower

**Scenario:** To cut costs, your team added 16 Spot secondary workers to a cluster with 4 primary workers for a shuffle-heavy nightly join (about 1.5TB of shuffle). Costs dropped, but the job now sometimes takes 2x longer and occasionally fails with `FetchFailedException`. The interviewer asks: explain what's happening and how you'd fix it while keeping most of the savings.

<details>
<summary>💡 Hint</summary>

Think about where Spark stores shuffle output and what happens to those files when a Spot VM is reclaimed mid-job. Then consider the ratio of stable to preemptible capacity, and whether Dataproc has a mode that protects shuffle data from preemption.

</details>

<details>
<summary>✅ Solution</summary>

**Diagnosis.** Shuffle blocks are written to the **local disks of the worker that produced them**. When a Spot VM is reclaimed (30s notice), every shuffle block it held vanishes. Downstream reducers hit `FetchFailedException`, Spark marks the map output lost, and **re-runs the upstream map tasks** to regenerate it. With 16 of 20 nodes preemptible (80%), a reclamation wave can cascade into repeated stage retries — sometimes slower than the smaller all-stable cluster, and job failure when `spark.stage.maxConsecutiveAttempts` (default 4) is exhausted.

**Fixes, combining several levers:**

1. **Rebalance the ratio** — keep preemptible capacity ≤ ~50%: e.g., 8 primary + 8–10 Spot rather than 4 + 16.

2. **Enhanced Flexibility Mode (EFM)** — shuffle written to primary workers, so Spot loss loses only in-flight tasks, not shuffle data:

```bash
gcloud dataproc clusters create nightly-join \
    --region us-central1 \
    --num-workers 8 \
    --num-secondary-workers 10 \
    --secondary-worker-type spot \
    --properties "dataproc:efm.spark.shuffle=primary-worker" \
    --num-worker-local-ssds 2
```

Trade-off: primaries absorb all shuffle I/O — give them local SSDs and enough size.

3. **Raise retry tolerance** for residual preemptions:

```bash
--properties "spark:spark.stage.maxConsecutiveAttempts=8,spark:spark.task.maxFailures=8"
```

4. **Reduce the shuffle itself** — often the biggest win: enable AQE, broadcast a slimmed small side, pre-filter before the join.

**Cost outcome to articulate:** 8 stable + 10 Spot with EFM typically retains 35–45% savings versus all-on-demand, with runtimes back to baseline. Pure 80% Spot "savings" were illusory — re-executed stages are paid compute too.

Closing line: "Spot pricing discounts the VM, not the retry. EFM plus a sane stable-to-Spot ratio keeps the discount real."

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Design the Migration of a 70-Node Hadoop Estate

**Scenario:** A retailer runs a 70-node on-prem Hadoop cluster: 1.2PB HDFS, ~500 jobs (60% Hive SQL, 30% Spark, 10% MapReduce), Oozie orchestration, Kerberos security, and a 10Gbps link to GCP. Leadership wants out of the datacenter in 12 months and "fewer moving parts" afterward. Design the migration: target architecture, sequencing, what you would *not* lift-and-shift, and the biggest risks.

<details>
<summary>💡 Hint</summary>

Don't map the cluster 1:1 onto a big Dataproc cluster — classify workloads first. Sixty percent of the jobs are Hive SQL: where do SQL workloads belong on GCP long-term? Think data-first sequencing, the metastore, and what 1.2PB means over a 10Gbps link.

</details>

<details>
<summary>✅ Solution</summary>

**Target architecture — three workload destinations, not one:**

| Workload | Destination | Why |
|---|---|---|
| Hive SQL (60%) | **BigQuery** (translated), interim: Hive on ephemeral Dataproc | SQL-shaped → serverless warehouse; biggest "fewer moving parts" win |
| Spark (30%) | **Dataproc Serverless** / ephemeral clusters via Composer | Code mostly unchanged; ephemeral kills idle cost |
| MapReduce (10%) | Rewrite to Spark or retire | MR is legacy; usually small, old, half-abandoned |
| HDFS 1.2PB | **GCS** (Standard + lifecycle to Nearline/Coldline) | Durable, decoupled, no NameNode |
| Oozie | **Cloud Composer** | Managed orchestration |
| Hive Metastore | **Dataproc Metastore** interim; BigQuery catalog end-state | Importable from HMS dump |
| Kerberos/Ranger | **IAM** + BigLake/dataset ACLs | Cloud-native authz |

**Sequencing (12 months):**

1. **Months 1–2: inventory + transfer start.** Job census (last-run dates kill ~20–30% immediately), data classification. Start bulk transfer: 1.2PB over 10Gbps ≈ ~12 days at line rate, realistically 4–8 weeks at sustained 30–50% utilization — start early, run `distcp` waves continuously with `-update` for incrementals. Evaluate Transfer Appliance for cold archives.
2. **Months 2–4: foundations.** Landing-zone project structure, VPC-SC, Composer environments, Dataproc Metastore imported, PHS cluster, CI/CD for jobs.
3. **Months 3–8: workload waves.** Wave by *business domain*, not job type, to keep dependencies together. Each wave: convert Oozie → DAGs, repoint `hdfs://` → `gs://`, dual-run, reconcile (counts/checksums/financial totals), cut consumers over.
4. **Months 6–10: Hive → BigQuery translation** using the BigQuery migration/translation tooling for DDL/DML; validated query-by-query against dual-run outputs.
5. **Months 10–12: decommission** — on-prem read-only quarantine, then power-down.

**What I would NOT lift-and-shift:**
- A persistent 70-node Dataproc replica (recreates idle cost and a SPOF master; the monolith dissolves into job-scoped compute).
- HDFS as a storage layer on Dataproc.
- Oozie on a VM, Kerberos KDCs, custom Hive UDF-laden jobs without first checking BigQuery equivalents.

**Top risks to volunteer:**
1. **Small files / deep partitions** punished by GCS — enforce compaction (128MB–1GB) and shallow partitioning during transfer, not after.
2. **Hidden inter-job dependencies** via HDFS paths and Oozie side-effects — the dual-run reconciliation phase exists to catch these.
3. **Transfer pipe contention** with production traffic — schedule distcp windows, monitor link saturation.
4. **Hive semantic drift in BigQuery** (NULL handling, implicit casts, UDFs) — automated row-level diffs on dual-run, not eyeballing.
5. **People risk** — Hadoop admins need a path (platform/SRE roles for Composer/Dataproc), or migration knowledge walks out.

**Cost story:** 70-node estate (hardware refresh + DC + licenses) typically $2–4M over 3 years; target state ≈ GCS storage ($25–35k/month at 1.2PB with lifecycle tiering) + ephemeral/serverless compute billed per job + BigQuery slots for the SQL estate — and the real prize, elimination of the utilization gap (shared Hadoop ~30% utilized vs pay-per-job).

</details>

</article>

## Interview Tips

> **Tip 1:** "When would you choose Dataproc over BigQuery or Dataflow?" — Give the framework, not features: SQL-shaped → BigQuery; new streaming/Beam → Dataflow; existing Spark/Hadoop code, Spark ML, or engine control → Dataproc. Then add the senior caveat: "and I challenge Spark-by-default — a lot of 'Spark pipelines' are SQL in disguise."

> **Tip 2:** "How do you control Dataproc costs?" — Lead with utilization, not machine types: ephemeral clusters / serverless to kill idle time (the 10x lever), then Spot secondaries with EFM (the 2x lever), then right-sizing (the 20% lever). Quoting that order shows you've owned a bill.

> **Tip 3:** "A Spark job on Dataproc is slow — walk me through debugging." — Show the sequence: Spark UI stage histogram first (skew? spill? one slow task?), then input layout (small files? partition explosion?), then resources (executor memory, shuffle disks) — and only then cluster size. Interviewers downgrade candidates whose first move is "add workers."

## ⚡ Quick-fire Q&A

**Q:** Why are Dataproc clusters considered disposable?
A: Data lives in GCS via the `gs://` connector and metadata in an external metastore, so clusters carry no state worth keeping; they start in ~90 seconds.

**Q:** Primary vs secondary workers?
A: Primaries run YARN + HDFS DataNodes; secondaries (often Spot VMs) are compute-only — no HDFS — so preempting them never loses stored data.

**Q:** What does Dataproc autoscaling actually monitor?
A: YARN pending/available memory, evaluated per cooldown period — not CPU.

**Q:** What is Enhanced Flexibility Mode?
A: Shuffle data is written to primary workers (or external service) so Spot secondary preemption doesn't destroy shuffle output and trigger stage retries.

**Q:** Dataproc Serverless limitations vs clusters?
A: Spark only, fixed executor core sizes (4/8/16), no SSH/YARN UI — debugging via Persistent History Server and Cloud Logging.

**Q:** How do you keep Hive table definitions across ephemeral clusters?
A: External metastore — Dataproc Metastore (managed HMS) attached at cluster create, or skip Hive and use BigQuery as the catalog.

**Q:** What's the small-files problem on GCS?
A: Thousands of tiny objects inflate list/open overhead and planning time; target 128MB–1GB files and shallow partition trees.

**Q:** Cheapest way to run a once-nightly 45-minute Spark job?
A: Dataproc Serverless batch (or an ephemeral workflow-template cluster) — pay only for the 45 minutes, zero idle.
