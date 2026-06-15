---
title: "Multi-Cloud Architecture — Scenario Questions"
topic: system-design
subtopic: multi-cloud-architecture
content_type: scenario_question
tags: [system-design, multi-cloud, scenarios, cloud-architecture]
---

# Multi-Cloud Architecture — Scenario Questions

<article data-difficulty="junior">

## 🟢 Junior: Should We Go Multi-Cloud?

**Scenario:** Your CTO asks you to evaluate whether the company should move from AWS-only to a multi-cloud strategy using both AWS and GCP. The company is a 50-person startup with 5 data engineers and a $200K/year AWS bill. What do you recommend and why?

<details>
<summary>✅ Solution</summary>

**Recommendation: Stay single-cloud (AWS) for now.**

**Reasoning framework:**

| Factor | Assessment | Recommendation |
|---|---|---|
| Team size | 5 data engineers | Too small to manage 2 cloud stacks |
| Current spend | $200K/year | Not at scale where cost arbitrage matters significantly |
| Compliance | Startup — no cross-jurisdiction data residency | Not a requirement |
| Vendor lock-in risk | Real but manageable | Mitigate with open formats, not multi-cloud |

**How to mitigate vendor lock-in without multi-cloud:**

```python
# Use open table formats (Iceberg/Delta Lake) — portable to any cloud
# Instead of Redshift tables (proprietary):
df.write.format("delta").save("s3://lake/orders/")
# → Can be read by BigQuery, Snowflake, Databricks, Athena without conversion

# Use cloud-agnostic orchestration (Airflow)
# Instead of AWS Step Functions (AWS-only):
# Airflow DAGs run on any cloud with operators per provider

# Use Terraform for IaC — same templates work on AWS and GCP
# with provider-specific modules
```

**What would change this recommendation?**
1. Regulatory requirement to keep EU data in GCP (data residency)
2. Company grows to 200+ engineers with a dedicated platform team
3. A specific GCP service (e.g., BigQuery ML, Vertex AI) provides 10× better ROI than AWS equivalent
4. AWS bill exceeds $5M/year where cost arbitrage becomes meaningful

**Key point to make in interview:**
> "Multi-cloud is a solution to specific problems, not a goal in itself. The operational overhead of managing two cloud stacks — separate IAM, networking, monitoring, billing — requires a dedicated platform team. At 5 engineers and $200K spend, AWS-only with open formats is the right call."

</details>

</article>

---

<article data-difficulty="mid">

## 🟡 Mid-Level: Design Cross-Cloud Data Replication

**Scenario:** Your company processes payments in the US (AWS us-east-1) but regulators now require that all EU customer transaction data must be stored and processed within the EU (GCP europe-west1). Design a pipeline that ensures EU data never leaves GCP while still allowing consolidated global reporting from the US.

<details>
<summary>✅ Solution</summary>

**Step 1: Classify data flow**

```
All transactions → Route by customer geography

US customer transactions → AWS us-east-1 (existing flow)
EU customer transactions → GCP europe-west1 (NEW — data never leaves EU)

Global reporting:
  - EU: send only aggregated, anonymized metrics to US
  - Raw PII data stays in EU always
```

**Step 2: Architecture**

```
Payment API (global, Route53 geolocation routing)
    │
    ├── US users → AWS us-east-1
    │     [Kafka MSK] → [Spark EMR] → [Delta Lake S3 us-east-1]
    │
    └── EU users → GCP europe-west1
          [Kafka Confluent EU] → [Dataproc Spark EU] → [BigQuery EU dataset]
          
Global Reporting:
  EU BigQuery EU dataset
      │
      │ Aggregated stats ONLY (no PII)
      │ Cloud DLP: remove all PII before cross-region
      ▼
  BigQuery US dataset (multi-region reporting)
      │
      ▼
  Looker dashboard (global revenue by region)
```

**Step 3: Data classification enforcement**

```python
# Classify transactions at ingestion time
def route_transaction(tx: dict) -> str:
    """Returns 'aws-us' or 'gcp-eu' based on customer region."""
    if tx.get("customer_country") in EU_COUNTRIES:
        return "gcp-eu"
    return "aws-us"

EU_COUNTRIES = {"DE", "FR", "IT", "ES", "NL", "PL", "SE", "AT", ...}

# Kafka routing: separate topics per region
def produce_transaction(tx: dict):
    region = route_transaction(tx)
    if region == "gcp-eu":
        gcp_kafka_producer.produce("transactions-eu", value=tx)
    else:
        aws_kafka_producer.produce("transactions-us", value=tx)
```

**Step 4: PII scrubbing before cross-region transfer**

```python
# Google Cloud DLP: automatically detect and remove PII
from google.cloud import dlp_v2

def anonymize_for_global_reporting(eu_aggregated_data: dict) -> dict:
    """Remove all PII before sending EU data to US reporting."""
    dlp_client = dlp_v2.DlpServiceClient()
    
    # Replace PII with tokens
    response = dlp_client.deidentify_content(
        parent="projects/my-gcp-project",
        deidentify_config={
            "info_type_transformations": {
                "transformations": [{
                    "primitive_transformation": {"replace_with_info_type_config": {}}
                }]
            }
        },
        inspect_config={
            "info_types": [
                {"name": "EMAIL_ADDRESS"},
                {"name": "PERSON_NAME"},
                {"name": "PHONE_NUMBER"},
                {"name": "CREDIT_CARD_NUMBER"}
            ]
        },
        item={"value": str(eu_aggregated_data)}
    )
    return response.item

# Only anonymized aggregates cross the EU→US boundary
```

**Step 5: Compliance audit trail**

```python
# Log every data access and cross-border transfer
audit_log = {
    "timestamp": datetime.utcnow().isoformat(),
    "data_type": "transaction_aggregate",
    "source_region": "gcp-europe-west1",
    "destination_region": "aws-us-east-1",
    "pii_present": False,  # confirmed by DLP scan
    "row_count": 15420,
    "triggered_by": "global_reporting_dag",
    "gdpr_basis": "legitimate_interest_aggregated_analytics"
}
# Write to BigQuery audit table (immutable, append-only)
```

**Trade-offs:**
- EU BigQuery EU dataset has its own SLA — EU analyst reports independent of US
- Aggregation granularity: daily/weekly to minimize cross-border transfers
- BigQuery EU ↔ BigQuery US transfer: done via BigQuery Data Transfer Service (GCP-internal, no internet egress billing for BigQuery → BigQuery within GCP)

</details>

</article>

---

<article data-difficulty="senior">

## 🔴 Senior: Design a Multi-Cloud Data Platform with DR

**Scenario:** A global fintech company ($5B revenue) wants a multi-cloud data platform: AWS as primary for all real-time pipelines and GCP as secondary for analytics and ML. Requirements: (1) RTO < 30 minutes if AWS goes down, (2) EU data residency compliance, (3) a unified data catalog across both clouds, (4) cost < $3M/year. Design the full platform.

<details>
<summary>✅ Solution</summary>

**Step 1: Clarify requirements in detail**
- Data volume: 50 TB/day ingested, 5 PB total
- Users: 200 engineers, 150 analysts
- Pipelines: 300 daily batch + 20 streaming
- Real-time latency: fraud detection < 200ms, dashboards < 5 sec
- DR RPO: 30 minutes (acceptable data loss window)
- DR RTO: 30 minutes (time to recover after AWS outage)

**Step 2: Platform architecture**

```
                    AWS (us-east-1) — PRIMARY
┌───────────────────────────────────────────────────────────────┐
│  Ingestion: Kafka MSK (20 topics, RF=3)                       │
│  Streaming: Flink on EKS (fraud detection, CDC)               │
│  Batch: EMR Spark (300 DAGs via Airflow MWAA)                 │
│  Storage: Delta Lake on S3 (5 PB, lifecycle to Glacier)       │
│  DW: Redshift (operational reporting, < 5 sec queries)        │
│  Feature Store: Redis ElastiCache (100 GB, 8-node cluster)    │
│  Orchestration: Airflow MWAA (300 DAGs)                       │
└───────────────────┬───────────────────────────────────────────┘
                    │
          Replication (async, 15-min lag)
          ┌─────────────────────┐
          │ Kafka MirrorMaker 2 │ (for streaming events)
          │ S3→GCS Transfer Svc │ (for Delta Lake files)
          │ Route53 health check│ (for DNS failover)
          └─────────┬───────────┘
                    │
                    ▼
                    GCP (us-central1) — ANALYTICS + DR STANDBY
┌───────────────────────────────────────────────────────────────┐
│  BigQuery (primary analytics engine — 150 analysts)           │
│  Vertex AI (ML training, model registry)                      │
│  GCS replica (Delta Lake mirror, read-only until failover)    │
│  Confluent Kafka (standby topics, receives MM2 replication)   │
│  Cloud Composer Airflow (standby DAGs, paused until failover) │
└───────────────────────────────────────────────────────────────┘

EU Data (europe-west1) — COMPLIANCE ZONE
┌───────────────────────────────────────────────────────────────┐
│  GCP europe-west1: all EU customer data stored here only      │
│  BigQuery EU dataset: EU analytics                            │
│  Only anonymized aggregates cross to US                       │
└───────────────────────────────────────────────────────────────┘
```

**Step 3: DR Failover Design (RTO 30 min)**

```python
# Automated DR failover: Lambda triggered by CloudWatch alarm
# Alarm: "AWS MSK consumer lag > 1M OR S3 API error rate > 50%"

def initiate_failover(event, context):
    steps = []
    
    # Step 1: Confirm AWS outage (not a false positive)
    if not is_aws_region_down("us-east-1"):
        return {"status": "no_action", "reason": "AWS healthy"}
    
    # Step 2: Pause AWS write endpoints (prevent split-brain)
    update_dns("kafka.internal", new_target="confluent-gcp.us-central1")
    update_dns("api.internal", new_target="gcp-us-central1")
    steps.append("DNS switched to GCP")
    
    # Step 3: Promote GCP Kafka from standby to primary
    # (Confluent Cloud API: un-pause the replication target)
    confluent_api.set_cluster_mode("us-central1-kafka", "ACTIVE")
    steps.append("GCP Kafka promoted to primary")
    
    # Step 4: Resume Cloud Composer Airflow DAGs on GCP
    composer_api.unpause_all_dags(environment="dr-composer")
    steps.append("Cloud Composer DAGs resumed")
    
    # Step 5: Point Flink jobs to GCP Kafka
    kubernetes_api.patch_deployment("flink-fraud-job",
        env={"KAFKA_BOOTSTRAP": "confluent-gcp.us-central1:9092"})
    steps.append("Flink repointed to GCP Kafka")
    
    # Step 6: Alert all teams
    pagerduty.trigger_incident(
        title="AWS us-east-1 DR Failover Complete",
        details={"steps_completed": steps, "rto_elapsed_min": elapsed_minutes()}
    )
    
    return {"status": "failover_complete", "steps": steps}
```

**Step 4: Unified Data Catalog (DataHub)**

```yaml
# DataHub deployment: runs on GKE (GCP) — accessible from both clouds

# Ingestion sources configured in DataHub:
ingestion_sources:
  - type: aws-glue
    config:
      aws_region: us-east-1
      # Crawls all Delta Lake tables registered in Glue catalog
    
  - type: bigquery
    config:
      project_id: fintech-gcp-project
      # Crawls all BigQuery datasets
  
  - type: kafka
    config:
      bootstrap_servers: msk.us-east-1.amazonaws.com:9092
      # Crawls all Kafka topic schemas from Schema Registry

  - type: airflow
    config:
      airflow_url: https://mwaa.us-east-1.amazonaws.com
      # Ingests DAG lineage (which tables each DAG reads/writes)

# Result: DataHub shows unified lineage graph:
# MySQL → Kafka → Spark → Delta Lake (S3) → BigQuery → Looker
# (across AWS + GCP in single view)
```

**Step 5: Cost breakdown (target < $3M/year)**

| Component | Cloud | Monthly Cost | Annual |
|---|---|---|---|
| Kafka MSK (primary) | AWS | $8,000 | $96K |
| EMR Spark (300 DAGs) | AWS | $45,000 | $540K |
| S3 storage (5 PB) | AWS | $115,000 | $1.38M |
| Redshift (operational) | AWS | $15,000 | $180K |
| Redis ElastiCache | AWS | $8,000 | $96K |
| Confluent Kafka (DR) | GCP | $3,000 | $36K |
| BigQuery (analytics) | GCP | $25,000 | $300K |
| Vertex AI (ML training) | GCP | $12,000 | $144K |
| GCS replica storage | GCP | $25,000 | $300K |
| Networking/egress | Both | $5,000 | $60K |
| **Total** | | **$261,000** | **$3.13M** |

Slightly over $3M. Cost levers to pull:
- S3 Intelligent Tiering: move cold data to IA/Glacier → saves $30K/month
- EMR Spot instances (70% of fleet): saves $25K/month
- BigQuery committed use discounts: saves $8K/month
→ **Revised: ~$2.76M/year** ✓

**Step 6: Trade-offs to discuss**

| Decision | Alternative | Reason |
|---|---|---|
| Async replication (15-min lag) | Synchronous replication | Sync adds 50ms+ latency to every AWS write; 15-min RPO is acceptable |
| DNS-based failover | BGP anycast | DNS TTL=60s means 60s split-brain window; acceptable for RTO=30min |
| DataHub on GCP | DataHub on AWS | DataHub must survive AWS outage to remain useful during DR |
| Redshift for operational | BigQuery | Redshift < 5sec latency needed; BigQuery on-demand can be slower |
| MM2 for Kafka DR | Manual consumer replay | MM2 maintains consumer offsets, enabling seamless failover |

</details>

</article>
