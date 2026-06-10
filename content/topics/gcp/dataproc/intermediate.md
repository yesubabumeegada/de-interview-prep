---
title: "Dataproc / Spark & Hadoop — Intermediate"
topic: gcp
subtopic: dataproc
content_type: study_material
difficulty_level: mid-level
tags: [gcp, dataproc, interview]
---

# Dataproc / Spark & Hadoop — Intermediate

## Autoscaling: How It Actually Works

Dataproc autoscaling watches **YARN pending and available memory** (not CPU) and adds/removes workers per an autoscaling policy. It evaluates each `cooldownPeriod`.

```yaml
# autoscaling-policy.yaml
workerConfig:
  minInstances: 2
  maxInstances: 10
secondaryWorkerConfig:
  minInstances: 0
  maxInstances: 30
basicAlgorithm:
  cooldownPeriod: 2m
  yarnConfig:
    scaleUpFactor: 1.0          # claim 100% of pending memory need
    scaleDownFactor: 0.5         # release 50% of idle capacity per cycle
    scaleUpMinWorkerFraction: 0.0
    scaleDownMinWorkerFraction: 0.0
    gracefulDecommissionTimeout: 1h
```

```bash
gcloud dataproc autoscaling-policies import etl-policy \
    --source autoscaling-policy.yaml \
    --region us-central1

gcloud dataproc clusters create etl-cluster \
    --region us-central1 \
    --autoscaling-policy etl-policy \
    --num-workers 2 \
    --num-secondary-workers 0
```

Mid-level details interviewers probe:

- **Graceful decommissioning** lets a removed worker finish running YARN containers before termination (set ≥ your longest task; `1h` typical). Without it, scale-down kills tasks and triggers recomputation.
- **Spark dynamic allocation** (`spark.dynamicAllocation.enabled=true`, default on Dataproc) must be on for autoscaling to be meaningful — otherwise executors hold memory regardless of need.
- **Don't autoscale HDFS-heavy clusters** — removing primary workers triggers HDFS block re-replication. Scale secondaries (no DataNode) instead; keep primaries fixed.
- Autoscaling reacts in minutes — it smooths multi-job or long-job variance, not a 3-minute job's burst.

## Preemptible / Spot Workers Done Right

Secondary workers can be Spot VMs (60–91% cheaper) but can be reclaimed with 30 seconds notice.

Rules of thumb:

| Rule | Why |
|---|---|
| Keep ≥ 50% capacity in primary workers | Spot reclamation storms shouldn't stall the job |
| Secondaries never run HDFS | By design — compute only, no data loss on preemption |
| Shuffle-heavy jobs suffer most | Lost node = lost shuffle files = stage retries |
| Use for: idempotent batch ETL | Avoid for: tight-SLA jobs, streaming |

```bash
gcloud dataproc clusters create batch-cluster \
    --region us-central1 \
    --num-workers 4 \
    --num-secondary-workers 8 \
    --secondary-worker-type spot \
    --properties "spark:spark.stage.maxConsecutiveAttempts=8"
```

Mitigation worth naming: raise stage retry tolerance, and consider **Enhanced Flexibility Mode (EFM)** which writes shuffle data to primary workers so Spot loss doesn't lose shuffle output:

```bash
gcloud dataproc clusters create efm-cluster \
    --region us-central1 \
    --properties "dataproc:efm.spark.shuffle=primary-worker" \
    --num-workers 4 \
    --num-secondary-workers 12
```

## Initialization Actions and Custom Images

**Initialization actions** are scripts run on every node at creation:

```bash
gcloud dataproc clusters create custom-cluster \
    --region us-central1 \
    --initialization-actions gs://my-bucket/init/install-libs.sh \
    --initialization-action-timeout 10m \
    --metadata "PIP_PACKAGES=great-expectations==0.18.12 db-dtypes"
```

```bash
#!/bin/bash
# install-libs.sh — runs on every node
set -euxo pipefail
PIP_PACKAGES=$(/usr/share/google/get_metadata_value attributes/PIP_PACKAGES)
pip install ${PIP_PACKAGES}
```

Pitfalls:
- Init actions run on **every** node including autoscaled ones → slow scripts slow scale-up. Keep them under ~1 minute.
- A flaky `pip install` from PyPI at 3 AM fails cluster creation. For anything beyond trivial, use a **custom image** (pre-baked) instead:

```bash
# Build a custom image once; clusters using it boot with everything installed
python generate_custom_image.py \
    --image-name etl-image-20260601 \
    --dataproc-version 2.2-debian12 \
    --customization-script install-libs.sh \
    --zone us-central1-a \
    --gcs-bucket my-build-bucket
```

Decision rule: **init action for small/fast/changing config; custom image for heavy, stable dependencies.** Custom images also cut node startup time — relevant for autoscaling and ephemeral patterns.

## Spark Property Tuning on Dataproc

Set properties at cluster create (`--properties`) or per job:

```bash
gcloud dataproc jobs submit pyspark gs://bkt/jobs/etl.py \
    --cluster etl-cluster \
    --region us-central1 \
    --properties "\
spark.executor.memory=10g,\
spark.executor.cores=4,\
spark.sql.shuffle.partitions=400,\
spark.sql.adaptive.enabled=true"
```

Key facts:
- Dataproc auto-computes sensible executor sizing from machine types — only override when you measured a reason.
- `spark.sql.adaptive.enabled=true` (AQE) is your friend: coalesces shuffle partitions, handles skewed joins.
- Prefix matters in `--properties` at cluster level: `spark:`, `yarn:`, `core:`, `hdfs:`, `dataproc:` select the config file.

## Dataproc Serverless in Practice

```bash
gcloud dataproc batches submit pyspark gs://bkt/jobs/transform.py \
    --region us-central1 \
    --version 2.2 \
    --properties "\
spark.executor.cores=4,\
spark.dynamicAllocation.initialExecutors=4,\
spark.dynamicAllocation.maxExecutors=50" \
    --service-account etl-sa@proj.iam.gserviceaccount.com \
    -- --run-date 2026-06-10
```

What to know at mid-level:
- Billed in **DCUs** (Data Compute Units ≈ 4GB+1vCPU bundles) per second while the batch runs, plus shuffle storage.
- Executor sizes are constrained (cores must be 4/8/16; memory derives from cores) — less knob-turning than clusters, by design.
- No SSH, no YARN UI; debugging is via **Spark UI in the console (Persistent History Server)** and Cloud Logging.
- Requires a subnet with **Private Google Access**; a common first-run failure is networking, not Spark.

Set up a Persistent History Server (PHS) once per team — essential for post-mortem debugging of both serverless batches and deleted ephemeral clusters:

```bash
gcloud dataproc clusters create phs-cluster \
    --region us-central1 \
    --single-node \
    --enable-component-gateway \
    --properties "spark:spark.history.fs.logDirectory=gs://phs-bucket/*/spark-job-history"

# Point batches at it
gcloud dataproc batches submit pyspark gs://bkt/jobs/etl.py \
    --region us-central1 \
    --history-server-cluster projects/proj/regions/us-central1/clusters/phs-cluster
```

## External Hive Metastore

Ephemeral clusters need table metadata to outlive them. Options:

1. **Dataproc Metastore** — managed Hive Metastore service; attach to any cluster:

```bash
gcloud dataproc clusters create etl-cluster \
    --region us-central1 \
    --dataproc-metastore projects/proj/locations/us-central1/services/my-dms
```

2. **BigQuery as the catalog** via the BigQuery connector (skip Hive tables entirely).
3. Self-hosted Cloud SQL metastore (legacy pattern; mention it as what Dataproc Metastore replaced).

## Common Pitfalls Checklist

1. **Small-files problem on GCS** — thousands of tiny files destroy read performance (per-object overhead). Compact to 128MB–1GB files; coalesce/repartition before write.
2. **Treating GCS like HDFS for shuffle** — shuffle stays on local disks; size local SSDs for shuffle-heavy jobs (`--num-worker-local-ssds`).
3. **Driver on the master undersized** — collect()-heavy jobs OOM the master; raise `spark.driver.memory` or stop collecting.
4. **One mega-cluster shared by all teams** — queue contention and config conflicts; prefer job-scoped ephemeral clusters or per-team policies.
5. **Forgetting `--max-idle`** on interactive clusters — the classic Friday-to-Monday cost leak.
6. **Skewed joins** — one task takes hours: enable AQE skew handling, salt keys, or broadcast the small side:

```python
from pyspark.sql import functions as F

big_df.join(
    F.broadcast(small_df),
    on="customer_id",
    how="left",
)
```

## Orchestration from Composer

```python
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)

run_batch = DataprocCreateBatchOperator(
    task_id="spark_transform",
    region="us-central1",
    batch={
        "pyspark_batch": {
            "main_python_file_uri": "gs://bkt/jobs/transform.py",
            "args": ["--run-date", "{{ ds }}"],
        },
        "runtime_config": {"version": "2.2"},
    },
    batch_id="transform-{{ ds_nodash }}",
)
```

Talking point: serverless batches from Composer give you the ephemeral pattern with zero cluster lifecycle code — the modern default for scheduled Spark on GCP.

## Interview Sound Bites

> "Autoscaling on Dataproc is YARN-memory driven; I scale secondary workers, keep primaries stable for HDFS/shuffle, and always set graceful decommissioning so scale-down doesn't kill tasks."

> "Spot secondaries cut compute 60–90%, but I cap them at ~50% of capacity and enable Enhanced Flexibility Mode so shuffle survives preemption."

> "Init actions for light setup, custom images for heavy dependencies — image build time is paid once, init time is paid on every node, every scale-up."
