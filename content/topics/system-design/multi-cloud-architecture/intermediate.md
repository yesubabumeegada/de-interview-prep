---
title: "Multi-Cloud Architecture — Intermediate"
topic: system-design
subtopic: multi-cloud-architecture
content_type: study_material
difficulty_level: mid-level
layer: intermediate
tags: [system-design, multi-cloud, networking, replication, orchestration]
---

# Multi-Cloud Architecture — Intermediate

## Cross-Cloud Data Replication Patterns

### Pattern 1: GCS → S3 via Google Storage Transfer Service

```
GCS Bucket (europe-west1) → Storage Transfer Service → S3 Bucket (us-east-1)
  - Scheduled: nightly at 2 AM UTC
  - Incremental: only new/changed objects
  - Cost: GCP egress to AWS = $0.08/GB
  - Latency: transfer completes in ~2–4 hours for 1 TB
```

**Configuration:**
```python
# Google Cloud Storage Transfer Service (Python SDK)
from google.cloud import storage_transfer

client = storage_transfer.StorageTransferServiceClient()
transfer_job = client.create_transfer_job(
    transfer_job={
        "project_id": "my-gcp-project",
        "transfer_spec": {
            "gcs_data_source": {"bucket_name": "my-gcs-bucket"},
            "aws_s3_data_sink": {
                "bucket_name": "my-s3-bucket",
                "aws_access_key": {
                    "access_key_id": "AKIAXXXXXXXX",
                    "secret_access_key": "..."
                }
            },
            "transfer_options": {"delete_objects_from_source_after_transfer": False}
        },
        "schedule": {
            "schedule_start_date": {"year": 2024, "month": 1, "day": 1},
            "start_time_of_day": {"hours": 2, "minutes": 0}  # 2 AM UTC
        }
    }
)
```

### Pattern 2: Kafka MirrorMaker 2 (Cross-Cloud Event Replication)

For streaming data, Kafka MirrorMaker 2 (MM2) replicates topics between Kafka clusters across clouds.

```
AWS MSK (us-east-1) ←──── MirrorMaker 2 ────► GCP Confluent (europe-west1)
  [Source Cluster]       [Kafka Connect]        [Target Cluster]
  Topic: payments        Runs in source VPC      Topic: aws.payments (prefixed)
```

**MM2 Configuration:**
```properties
# mirrormaker2.properties
clusters = source, target

source.bootstrap.servers = b-1.msk.us-east-1.amazonaws.com:9092
target.bootstrap.servers = kafka.europe-west1.gcp.confluent.cloud:9092

# Replication: source → target
source->target.enabled = true
source->target.topics = payments, orders, user-events

# Prevent circular replication
source->target.emit.checkpoints.enabled = true
source->target.sync.group.offsets.enabled = true

# TLS for cross-cloud
ssl.truststore.location = /etc/kafka/kafka.client.truststore.jks
```

**Topic naming:** MM2 prefixes topic names with source cluster alias: `payments` → `source.payments` on target. This prevents infinite replication loops.

### Pattern 3: Delta Lake as Cross-Cloud Replication Format

Instead of proprietary replication, write Delta Lake format to both clouds' object storage:

```python
# Write once to S3 (AWS), replicate metadata + parquet files to GCS
# Both clouds read the same logical table

# AWS: primary write
df.write.format("delta").save("s3://lake/orders/")

# Replicate: use rclone or Storage Transfer to copy _delta_log/ + parquet files to GCS
# rclone sync s3:lake/orders/ gcs:lake-replica/orders/ --checksum

# GCP: reads same data as external table
# BigQuery Omni query via Omni (reads S3 directly, no copy needed)
bq query --use_legacy_sql=false "
  SELECT * FROM orders_external_table
  WHERE order_date = '2024-01-15'
"
# BigQuery Omni (AWS admin region) = query S3 without moving data to GCP
```

---

## Networking: Connecting Clouds Securely

### Option 1: Public Internet with Encryption

Simplest but highest latency and egress cost:
- All cross-cloud traffic goes over public internet
- TLS encryption mandatory
- Throughput limited, latency 50–100ms cross-region

### Option 2: AWS PrivateLink + GCP Private Service Connect

```
AWS VPC ─── PrivateLink ─── PrivateLink Endpoint ─── GCP VPC
  (private IP)                                        (private IP)
```
- Traffic stays within cloud provider backbone, not public internet
- Lower latency (20–50ms), no egress surcharge for PrivateLink traffic
- Use case: microservices calling cross-cloud APIs

### Option 3: Dedicated Interconnect (Enterprise)

```
AWS Direct Connect + GCP Cloud Interconnect
  AWS Colocation facility ── 10 Gbps fiber ── GCP Colocation facility
  
Cost: ~$1,500–$5,000/month for 10 Gbps dedicated link
Latency: < 5ms (same metro area)
Use case: petabyte-scale cross-cloud data movement
```

---

## Cloud-Agnostic Orchestration

### Apache Airflow as the Universal Orchestrator

Airflow's abstraction: **operators** talk to cloud APIs, DAGs are cloud-agnostic.

```python
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.s3_to_gcs import S3ToGCSOperator

with DAG("multi_cloud_pipeline", schedule_interval="@daily") as dag:
    
    # Step 1: Run Spark on AWS EMR
    run_on_aws = EmrCreateJobFlowOperator(
        task_id="process_on_aws",
        job_flow_overrides={"Name": "daily-transform", ...},
        aws_conn_id="aws_default"
    )
    
    # Step 2: Copy results to GCS
    copy_to_gcs = S3ToGCSOperator(
        task_id="copy_s3_to_gcs",
        bucket="my-s3-bucket",
        prefix="processed/",
        dest_gcs="gs://my-gcs-bucket/processed/",
        aws_conn_id="aws_default",
        gcp_conn_id="google_cloud_default"
    )
    
    # Step 3: Load into BigQuery
    load_to_bq = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket="my-gcs-bucket",
        source_objects=["processed/*.parquet"],
        destination_project_dataset_table="project.dataset.table",
        source_format="PARQUET",
        write_disposition="WRITE_APPEND"
    )
    
    run_on_aws >> copy_to_gcs >> load_to_bq
```

### Dagster: Cloud-Agnostic Asset Graph

```python
from dagster import asset, io_manager
from dagster_aws.s3 import s3_resource
from dagster_gcp.bigquery import bigquery_resource

@asset(required_resource_keys={"s3", "bigquery"})
def processed_orders(context, raw_orders):
    # Transform on Spark (cloud-agnostic via Databricks)
    df = transform(raw_orders)
    
    # Write to S3
    context.resources.s3.put_object(
        Bucket="lake-bucket",
        Key="orders/processed/",
        Body=df.to_parquet()
    )
    
    # Load to BigQuery
    context.resources.bigquery.load_table_from_dataframe(df, "project.dataset.orders")
```

---

## Identity Federation Across Clouds

The auth problem: How does a GCP service account call an AWS S3 API securely?

### AWS Workload Identity Federation (WIF)

```bash
# Step 1: Create AWS IAM OIDC provider for GCP
aws iam create-open-id-connect-provider \
  --url "https://accounts.google.com" \
  --client-id-list "sts.amazonaws.com"

# Step 2: Create IAM role with trust for GCP service account
aws iam create-role --role-name gcp-dataproc-s3-access \
  --assume-role-policy-document '{
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Federated": "arn:aws:iam::ACCOUNT:oidc-provider/accounts.google.com"},
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringEquals": {
          "accounts.google.com:sub": "SERVICE_ACCOUNT_UNIQUE_ID"
        }
      }
    }]
  }'

# Step 3: GCP Dataproc job assumes AWS role via token exchange
# (GCP SDK handles OIDC token, exchanges for AWS temporary credentials)
```

**Simpler alternative:** Store AWS access keys as GCP Secret Manager secrets, inject into Dataproc jobs as environment variables. Less secure, but operationally simpler for small teams.

---

## Cost Comparison Framework

When choosing which cloud to run a workload on, compare across three dimensions:

### 1. Compute Cost (per vCPU-hour, as of 2024)

| Workload | AWS | GCP | Azure |
|---|---|---|---|
| On-demand Spark (m5.4xlarge / n2-standard-16) | $0.768/hr | $0.636/hr | $0.784/hr |
| Spot/Preemptible Spark | $0.20–0.30/hr | $0.10–0.15/hr | $0.15–0.25/hr |
| Managed DW (Redshift / BigQuery / Synapse) | $0.25/slot-hr | $0.04/slot-hr (on-demand) | $0.04/DWU-hr |

**GCP preemptible VMs are typically 60–70% cheaper than AWS spot for Spark workloads.**

### 2. Storage Cost (per GB/month)

| Storage Type | AWS S3 | GCP GCS | Azure Blob |
|---|---|---|---|
| Standard | $0.023 | $0.020 | $0.018 |
| Infrequent Access | $0.0125 | $0.010 | $0.010 |
| Archive (Glacier) | $0.004 | $0.004 | $0.00099 |

### 3. Egress Cost (per GB leaving cloud)

| Cloud | Same-region egress | Cross-region | Internet egress |
|---|---|---|---|
| AWS | Free | $0.02 | $0.09 |
| GCP | Free | $0.01–0.08 | $0.08 |
| Azure | Free | $0.02 | $0.087 |

**Key insight:** Egress is the hidden cost in multi-cloud. Moving 100 TB cross-cloud costs $8,000–$9,000 in egress alone.

---

## Decision Framework: Multi-Cloud vs Single Cloud

```
Is your data volume > 10 PB?              → Multi-cloud may save cost on storage
Do you have regulatory multi-region reqs? → Multi-cloud required
Is your team > 50 engineers?              → Multi-cloud ops overhead manageable
Do you use best-of-breed services?        → BigQuery + SageMaker = valid multi-cloud
Are you an early-stage startup?           → Single cloud (pick AWS or GCP, go deep)
Is your main bottleneck ops complexity?   → Single cloud (simplify first)
```
