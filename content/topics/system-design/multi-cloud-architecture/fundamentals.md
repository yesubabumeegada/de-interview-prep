---
title: "Multi-Cloud Architecture — Fundamentals"
topic: system-design
subtopic: multi-cloud-architecture
content_type: study_material
difficulty_level: junior
layer: fundamentals
tags: [system-design, multi-cloud, AWS, GCP, azure, cloud-architecture]
---

# Multi-Cloud Architecture — Fundamentals

## What Is Multi-Cloud?

Multi-cloud means running workloads across more than one public cloud provider (AWS, GCP, Azure) simultaneously — not as a backup, but as an intentional architectural choice.

**Analogy:** It's like using two banks — Chase for your checking account (daily transactions) and Fidelity for investments (long-term). Each is best-of-breed for its purpose, and you're not locked into either.

---

## Why Multi-Cloud? The Four Reasons

| Reason | Example |
|---|---|
| **Avoid vendor lock-in** | If AWS raises S3 prices 30%, you want the option to move to GCS |
| **Best-of-breed services** | BigQuery for analytics + AWS SageMaker for ML + Azure Cognitive for AI |
| **Compliance/data residency** | EU customer data must stay in GCP europe-west1; US data in AWS us-east-1 |
| **Cost arbitrage** | GCP sustained-use discounts beat AWS reserved instances for long-running Spark |

---

## The Most Common Multi-Cloud DE Pattern: AWS + GCP

| Layer | AWS Component | GCP Component | Why |
|---|---|---|---|
| **Storage** | S3 (raw/landing) | GCS (curated) | S3 ecosystem strongest; BigQuery native to GCS |
| **Analytics DW** | Redshift | BigQuery | BigQuery: serverless, ML-native; Redshift: SQL-compatible |
| **Streaming** | Kinesis / MSK (Kafka) | Pub/Sub | Kafka on MSK for DE; Pub/Sub for event-driven |
| **Compute** | EMR (Spark) | Dataproc (Spark) | Cost arbitrage per workload |
| **Orchestration** | Managed Airflow (MWAA) | Cloud Composer | Same Airflow DAGs, different managed env |

---

## Cloud-Agnostic Storage: The Foundation

The key to multi-cloud is making your **data readable by any cloud's compute engine**. Open table formats do this:

### Apache Iceberg and Delta Lake

```
S3 (AWS)          GCS (GCP)         ADLS (Azure)
   │                 │                   │
   └────────────┬────┘                   │
                ▼                        │
   [Iceberg / Delta Lake metadata]       │
   (manifest files, schema, partitions)  │
                │
     ┌──────────┼───────────┐
     ▼          ▼           ▼
  Spark     BigQuery     Trino/Athena
  (any cloud)  (GCP)     (any cloud)
```

**Why this matters:** If your raw data is Delta Lake format on S3, both AWS Athena and GCP BigQuery Omni can query it without copying the data.

### Parquet: The Universal Format

```
Parquet files on S3:
  - Readable by: Spark, Athena, BigQuery, Snowflake, Databricks, Trino
  - Not readable by: native DW tables (must be external table or copied)
  - Column-oriented: great for analytics queries
  - Compression: Snappy (default), Zstd (better ratio)
```

---

## Data Gravity Problem

**The biggest multi-cloud headache:** data wants to stay where it is because moving it is expensive.

```
AWS S3 → GCP BigQuery
Data: 10 TB
S3 egress cost: $0.09/GB = $900
GCS Storage Transfer: free ingress (GCP covers it)

But: if you do this monthly (incremental 500 GB):
Monthly egress: $45/month = $540/year just for data movement
```

**Rule of thumb:** Put compute near your data. If most data is in S3, run Spark on EMR (AWS), not Dataproc (GCP). If analytics data is in BigQuery, run ML on Vertex AI (GCP), not SageMaker.

---

## Hybrid Cloud: On-Prem + Cloud

Many enterprises run a hybrid: on-premises data center + one or more clouds.

| Pattern | Example |
|---|---|
| **Burst to cloud** | On-prem Spark cluster handles baseline; EMR handles peak loads |
| **Archive to cloud** | On-prem hot data; S3 Glacier for cold archive |
| **Lift and shift** | Migrate on-prem Hadoop to EMR or Dataproc |
| **Cloud-native extension** | On-prem Oracle DW + BigQuery for modern analytics |

### Hybrid Data Platforms

| Platform | On-Prem Option | Notes |
|---|---|---|
| **Databricks** | Databricks on BYOC (Bring Your Own Cloud) or on-prem via VMs | Popular for regulated industries |
| **Snowflake** | Snowflake on AWS GovCloud / Azure Government | Not truly on-prem; private cloud |
| **Cloudera CDP** | CDP Private Cloud Base | Full Hadoop stack on-prem |

---

## When NOT to Go Multi-Cloud

Multi-cloud adds real operational complexity. Avoid it when:

| Situation | Why Single Cloud Is Better |
|---|---|
| Small team (< 10 engineers) | Multi-cloud doubles operational overhead |
| Early startup | Vendor lock-in is not a real risk yet; move fast instead |
| Deep platform integration | Snowflake ML + Streamlit + Cortex — all Snowflake is simpler |
| Tight latency requirements | Cross-cloud data movement adds 50–200ms network latency |
| Compliance is the priority | Simpler to audit one cloud's controls than two |

**Senior quote:** "Multi-cloud is the right answer for 20% of companies. The other 80% should pick one cloud and go deep."

---

## Key Terms

| Term | Definition |
|---|---|
| **Egress cost** | Fee charged by cloud provider when data leaves their network |
| **Data gravity** | Data attracts compute — workloads naturally move toward where data lives |
| **BYOC** | Bring Your Own Cloud — vendor software runs in your cloud account |
| **Data residency** | Regulatory requirement that data physically stays in a specific country/region |
| **Cloud-agnostic** | Software/format that works without modification on any cloud |
