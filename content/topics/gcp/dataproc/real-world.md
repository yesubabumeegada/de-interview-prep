---
title: "Dataproc / Spark & Hadoop — Real-World Cases"
topic: gcp
subtopic: dataproc
content_type: study_material
difficulty_level: mid-level
tags: [gcp, dataproc, interview]
---

# Dataproc / Spark & Hadoop — Real-World Cases

Three production stories with the numbers, the wrong turns, and the fixes — the raw material for "tell me about a time" answers.

## Case Study 1: Killing the $14k/Month Zombie Cluster

**Context.** A media company's data team inherited a 16-node `n1-standard-16` Dataproc cluster created two years earlier "temporarily" for a recommendation batch job. It ran 24/7. Billing export showed **~$13,800/month** for the cluster; actual YARN utilization averaged **11%**.

**Investigation.**

```bash
# What actually runs on this thing?
gcloud dataproc jobs list \
    --region us-central1 \
    --cluster legacy-recs \
    --format="table(reference.jobId, status.state, statusHistory[0].stateStartTime)" \
    | head -50
```

Findings: 6 nightly PySpark jobs (2–4h total), one weekly model training (3h), and a handful of analysts SSH-ing in for ad-hoc `spark-shell` sessions "because it's always there." HDFS held 4TB of data nobody could attribute — copies of GCS data staged years ago.

**Migration plan executed over 3 weeks:**

1. Confirmed every job read/wrote GCS already (HDFS was just stale cache) — archived HDFS contents to Nearline GCS, then ignored it.
2. Converted the 6 nightly jobs to **Dataproc Serverless batches** submitted from Composer:

```python
batch = DataprocCreateBatchOperator(
    task_id="recs_scoring",
    region="us-central1",
    batch={
        "pyspark_batch": {
            "main_python_file_uri": "gs://etl-code/recs/score.py",
            "args": ["--date", "{{ ds }}"],
        },
        "runtime_config": {
            "version": "2.2",
            "properties": {"spark.dynamicAllocation.maxExecutors": "40"},
        },
    },
    batch_id="recs-score-{{ ds_nodash }}",
)
```

3. Weekly training moved to an ephemeral GPU cluster via Workflow Template (create → train → delete).
4. Analysts got a **single-node `--max-idle 60m` notebook cluster** template they create on demand:

```bash
gcloud dataproc clusters create adhoc-$USER \
    --region us-central1 \
    --single-node \
    --master-machine-type n2-highmem-8 \
    --max-idle 60m \
    --enable-component-gateway \
    --optional-components JUPYTER
```

**Outcome.** New steady-state spend: **~$1,900/month** (serverless batches ~$1,100, training ~$450, ad-hoc ~$350). Savings ≈ **$143k/year**. Bonus: nightly jobs ran 30% faster because serverless dynamic allocation gave them more executors at peak than the shared cluster ever did.

**Interview takeaway.** Always pair "the cluster costs X" with "utilization is Y%." Idle time, not compute price, is almost always the headline number.

## Case Study 2: The 6-Hour Job With One 5-Hour Task (Skew)

**Context.** An ad-tech ETL joined a 2TB clickstream table against a 40GB user-profile table on `user_id`, on a 20-worker autoscaled cluster. Runtime had grown from 2h to 6h over months. Autoscaler maxed out, costs tripled, runtime barely improved.

**Investigation.** Spark UI stage view: 1,999 of 2,000 tasks in the join stage finished in under 3 minutes; **one task ran 5+ hours** with 200x the shuffle read of the median. Classic key skew. Digging into the data:

```python
(
    clicks.groupBy("user_id")
    .count()
    .orderBy(F.desc("count"))
    .show(5)
)
# +-----------+----------+
# | user_id   |  count   |
# +-----------+----------+
# | NULL      | 412M     |   <-- 20% of all rows
# | bot-0001  |  38M     |
# ...
```

20% of clicks had `NULL` user_id (logged-out traffic), plus a handful of bot IDs — all hashed to single partitions.

**Fix — three layers:**

```python
# 1. Don't join what can't match: split out NULLs first
matched = clicks.filter(F.col("user_id").isNotNull())
unmatched = clicks.filter(F.col("user_id").isNull())

# 2. Enable AQE skew-join handling (image default had been disabled
#    by a years-old --properties flag nobody remembered)
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# 3. Broadcast the profile table — 40GB was too big, but after
#    selecting only needed columns it compressed to ~3GB
profiles_slim = profiles.select("user_id", "segment", "ltv_bucket")
joined = matched.join(F.broadcast(profiles_slim), "user_id", "left")

result = joined.unionByName(
    unmatched.withColumn("segment", F.lit(None).cast("string"))
             .withColumn("ltv_bucket", F.lit(None).cast("string"))
)
```

**Outcome.** Runtime 6h → **41 minutes** on a *smaller* fixed 12-worker cluster. Monthly cost for this pipeline fell ~70%. The "disable AQE" property was removed from the shared cluster-creation script in code review.

**Interview takeaway.** "Autoscaling can't fix skew — one hot partition is one task on one core no matter how many workers you buy." Then name the toolbox: filter non-joinable keys, AQE skew join, broadcast slimmed dimensions, salting as last resort.

## Case Study 3: On-Prem Hadoop Migration That Hit the Small-Files Wall

**Context.** A bank migrated a 60-node Cloudera cluster (800TB HDFS, ~400 Hive/Spark jobs) to Dataproc + GCS over 9 months. Data moved with distcp waves; Hive metastore imported into Dataproc Metastore; Oozie coordinators rewritten as Composer DAGs.

**The incident.** Two weeks after cutover, the nightly risk-aggregation Hive job — 45 minutes on-prem — was taking **4.5 hours** on an ephemeral 30-worker cluster. VMs were faster than the on-prem nodes, so where did 6x come from?

**Investigation.**

```bash
# Count objects under one partition tree
gsutil ls -r "gs://bank-dl/warehouse/trades/dt=2026-05-*/**" | wc -l
# 1,940,000 objects
```

The upstream on-prem ingestion wrote a file **per source-system per 5 minutes per branch** — ~70k files/day at ~80KB each. On HDFS with a local NameNode this was survivable; on GCS, job planning had to list ~2M objects and the job spent 3+ hours in split computation and per-object open overhead before doing real work.

**Fix.**

1. **Immediate:** a nightly compaction job per hot table:

```python
(
    spark.read.parquet("gs://bank-dl/warehouse/trades/dt=2026-06-09")
    .repartition(64)  # ~target 256MB files
    .write.mode("overwrite")
    .parquet("gs://bank-dl/warehouse_compacted/trades/dt=2026-06-09")
)
```

2. **Structural:** ingestion changed to micro-batch every 30 minutes with a target file size, and the partition scheme flattened from `dt=/hour=/branch=` (26k dirs/day) to `dt=` only — branch became a column, filtered via Parquet predicate pushdown instead of directories.
3. **Connector tuning** while compaction rolled out: increased list parallelism and enabled fadvise AUTO.

**Outcome.** Risk job: 4.5h → **38 minutes** (beating on-prem). GCS Class A operation charges for the lake dropped by ~$2,300/month after object counts fell ~95%.

**Interview takeaway.** Hadoop-era ingestion habits (many small files, deep partition trees) are tolerated by HDFS NameNodes and punished by object stores. State the rule: **128MB–1GB files, shallow partitions, columns over directories** when cardinality is high.

## Cross-Case Patterns

| Theme | Rule institutionalized |
|---|---|
| Cost | Report utilization % next to every cluster; default to ephemeral/serverless |
| Performance | Read the Spark UI stage histogram before adding workers |
| Skew | Filter NULL/bot keys, AQE on, broadcast slim dims |
| Storage layout | File-size and partition-depth standards enforced at ingestion, not fixed by consumers |
| Migration | Dual-run + reconcile before cutover; expect object-store semantics to break HDFS-era assumptions |

## Using These Stories

For each story you tell, land the trio: **a metric that found it** (utilization %, one task's shuffle bytes, object count), **a fix at the right layer** (orchestration, query plan, storage layout — not "bigger cluster"), and **a number after** ($143k/year, 6h→41min, 4.5h→38min). That structure reads as senior even in a mid-level interview.
