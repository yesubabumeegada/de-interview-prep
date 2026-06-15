---
title: "Multi-Cloud Architecture — Real World"
topic: system-design
subtopic: multi-cloud-architecture
content_type: study_material
difficulty_level: senior
layer: real-world
tags: [system-design, multi-cloud, real-world, case-study]
---

# Multi-Cloud Architecture — Real World

## Case Study 1: Netflix's Multi-Cloud Strategy

Netflix uses AWS as primary with strategic GCP usage for specific workloads.

### What Netflix Uses AWS For
- Primary video encoding (Encoda: custom EMR clusters)
- Content delivery (CloudFront CDN)
- Core databases (DynamoDB, RDS Aurora)
- Data warehouse (Iceberg + Spark on EMR + Presto for querying)

### What Netflix Uses GCP For
- Machine learning: Vertex AI for recommendation model training
- BigQuery for ad-hoc analytics by data scientists who prefer BigQuery's SQL ergonomics
- Google's TPUs for certain deep learning training jobs

### The Data Bridge
Netflix's Metacat (open-sourced) is a federated metadata service that catalogs tables across multiple Hive metastores and S3 paths:
```
Metacat API
  ├── Hive Metastore (AWS EMR) — production tables
  ├── BigQuery (GCP) — analytics tables
  └── Iceberg Catalog (AWS Glue) — new lakehouse tables

Query: "Give me all tables owned by team=recommendations"
→ Metacat searches all catalogs, returns unified result
```

**Interview lesson from Netflix:** Centralized metadata is the glue in multi-cloud. Without a unified catalog, data discovery breaks down across clouds.

---

## Case Study 2: Spotify's Data Platform (AWS + GCP)

Spotify runs what they call a "Hybrid Cloud" architecture:
- **GCP**: Primary analytics (BigQuery for all analyst queries)
- **AWS**: Some ML workloads (SageMaker for specific models)
- **On-prem**: None (fully cloud-native)

### Spotify's Approach to BigQuery Scale

Spotify runs one of the largest BigQuery environments publicly documented:
- **100,000+ BigQuery jobs/day**
- **100+ PB of data** in BigQuery storage
- **5,000+ Airflow DAGs** in Cloud Composer orchestrating pipelines

Key pattern they published:
```python
# Spotify's Luigi → Airflow migration showed that at massive scale,
# DAG parse time becomes a bottleneck. Their solution:
# - Separate Airflow scheduler processes per domain team
# - Each team's Cloud Composer environment runs independently
# - Cross-team dependencies use Pub/Sub triggers, not Airflow sensors

# Cross-DAG dependency without shared Airflow:
# Team A's final task publishes completion event:
from google.cloud import pubsub_v1
publisher = pubsub_v1.PublisherClient()
publisher.publish("projects/spotify/topics/pipeline-complete",
                  b"orders_daily complete",
                  dag_id="orders_daily", run_date="2024-01-15")

# Team B's Airflow listens via Pub/Sub sensor:
from airflow.providers.google.cloud.sensors.pubsub import PubSubPullSensor
wait_for_orders = PubSubPullSensor(
    task_id="wait_for_orders_team",
    project_id="spotify",
    subscription="pipeline-notifications-sub",
    messages_filter={"dag_id": "orders_daily"}
)
```

---

## Case Study 3: Airbnb's Multi-Cloud Journey

Airbnb was an early AWS shop and later expanded to GCP for ML:

### Phase 1 (AWS-only): The Minerva Semantic Layer
Airbnb built Minerva — a centralized metric and dimension store — on top of Spark + Presto + S3 on AWS. Minerva ensured consistent metric definitions across 200+ teams.

### Phase 2 (AWS + GCP): ML Platform Expansion
As ML demand grew, Airbnb moved ML training to GCP (Vertex AI) for:
- Better TPU access for deep learning models
- Google's Kubeflow Pipelines for ML workflow management

**Data bridge:** Airflow DAGs on AWS export features from S3 to GCS nightly. The feature store is in Redis (multi-cloud accessible over VPN).

### The lesson: Shared feature store is the key integration point

```
AWS (feature engineering)        GCP (ML training)
     │                               │
     ▼                               ▼
Redis Cluster (feature serving) ←────┘
(accessible from both clouds over VPN)
     │
     ▼
  Both Triton Inference (AWS)
  and Vertex AI Training (GCP)
  read from same Redis
```

---

## Real Egress Cost War Story

A fintech company's experience (shared at re:Invent 2023):

**The problem:** Their data science team ran BigQuery queries on data stored in S3. The workflow was:
1. Spark on EMR copies data from S3 → GCS ($0.09/GB egress from AWS)
2. BigQuery queries the GCS data
3. Monthly: 500 TB moved → $45,000/month just in egress

**The fix:** BigQuery Omni (runs BigQuery compute on AWS infrastructure):
```sql
-- BigQuery Omni: query S3 directly, no GCS copy
CREATE EXTERNAL TABLE `project.dataset.orders`
OPTIONS (
  format = 'PARQUET',
  uris = ['s3://my-bucket/orders/*.parquet']
);

-- This query runs on AWS infrastructure via BigQuery Omni
-- No data leaves AWS → zero egress cost
SELECT DATE(order_ts), SUM(amount)
FROM `project.dataset.orders`
GROUP BY 1;
```

**Result:** $45,000/month → ~$3,000/month (BigQuery Omni compute charge only). $504,000/year savings.

---

## Practical Multi-Cloud Interview Answers

### "Why not just pick one cloud?"

**Good answer:**
> "For most companies, a single cloud is the right answer — it reduces operational complexity and allows deeper optimization. Multi-cloud makes sense when: (1) you have regulatory data residency requirements across regions/countries, (2) you're using genuinely best-of-breed services from different vendors (BigQuery's serverless analytics + SageMaker's ML tooling), or (3) at petabyte scale where storage cost differences between clouds justify the operational overhead. I'd push back on multi-cloud as a default and treat it as a deliberate architectural choice."

### "How do you handle schema management across clouds?"

**Good answer:**
> "Schema management is one of the hardest parts of multi-cloud. I'd use a combination of (1) a centralized schema registry — Confluent Schema Registry for Kafka schemas, with enforcement in CI/CD pipelines — and (2) an open table format like Iceberg, which embeds schema evolution in the table metadata and is readable by Spark, Athena, BigQuery, and Trino without conversion. For governance, Unity Catalog or Apache Ranger can enforce column-level policies across cloud boundaries."

### "What's your DR strategy for a multi-cloud pipeline?"

**Good answer:**
> "I'd use an active-passive pattern for most workloads: AWS as primary with 30-minute RPO async replication to GCP. Automated failover via Route53 health checks + Lambda. For mission-critical pipelines (fraud detection, payment processing), I'd push for active-active with Kafka MirrorMaker 2 bidirectional replication, accepting the conflict resolution complexity. The key metrics to define upfront are RPO and RTO — those drive the architecture choice, not the other way around."
