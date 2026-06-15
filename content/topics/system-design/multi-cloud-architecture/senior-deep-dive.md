---
title: "Multi-Cloud Architecture — Senior Deep Dive"
topic: system-design
subtopic: multi-cloud-architecture
content_type: study_material
difficulty_level: senior
layer: senior-deep-dive
tags: [system-design, multi-cloud, disaster-recovery, governance, cost-optimization]
---

# Multi-Cloud Architecture — Senior Deep Dive

## Designing a Production Multi-Cloud Data Platform

### Reference Architecture: AWS (Primary) + GCP (Analytics)

```
                    ┌─────────────────────────────────────────┐
                    │           AWS (Primary)                  │
                    │                                         │
  Sources ─────────►  Kafka (MSK) → EMR Spark → Delta Lake   │
  (Apps, APIs,     │               (S3)                       │
   Databases)      │                    │                     │
                    └────────────────────┼────────────────────┘
                                         │ nightly replication
                                         │ Storage Transfer Service
                                         ▼
                    ┌─────────────────────────────────────────┐
                    │           GCP (Analytics)                │
                    │                                         │
                    │  GCS (replica) → BigQuery               │
                    │                      │                  │
                    │                 Vertex AI (ML)          │
                    │                      │                  │
                    │              Looker / Looker Studio      │
                    └─────────────────────────────────────────┘
```

**Why this split:**
- **AWS primary**: strongest ecosystem for Kafka (MSK), mature Spark on EMR, largest engineer talent pool
- **GCP analytics**: BigQuery is unmatched for serverless OLAP at scale (no cluster management), Vertex AI has best-in-class AutoML, Looker native integration

---

## Iceberg as the Universal Table Format

Apache Iceberg is the best current answer to cloud-agnostic table format. It decouples storage from compute.

### Iceberg Table Architecture

```
S3://lake/orders/
  metadata/
    v1.metadata.json   ← table schema, partition spec, snapshot history
    v2.metadata.json
  data/
    dt=2024-01-15/
      part-00001-abc.parquet
      part-00002-def.parquet
  
Iceberg catalog (AWS Glue / GCP Dataplex / Hive Metastore):
  Tracks: current metadata file pointer per table
  Query engines ask catalog: "where is the metadata for orders?"
  Catalog returns: "s3://lake/orders/metadata/v2.metadata.json"
```

### Iceberg Query from Multiple Engines

```python
# Spark (runs on EMR or Dataproc) — same query
spark.read.format("iceberg").load("orders").filter("dt = '2024-01-15'")

# Athena (AWS serverless) — SQL
SELECT * FROM "glue_catalog"."lake"."orders" WHERE dt = '2024-01-15';

# BigQuery Omni (GCP, reads S3 directly) — no data movement
SELECT * FROM `project.dataset.orders` WHERE dt = '2024-01-15';

# Trino (self-hosted, cloud-agnostic)
SELECT * FROM iceberg.lake.orders WHERE dt = '2024-01-15';
```

### Iceberg Schema Evolution

```python
# Add a new column without rewriting data — metadata-only operation
from pyiceberg.catalog import load_catalog
from pyiceberg.types import LongType

catalog = load_catalog("glue", **{"type": "glue", "region": "us-east-1"})
table = catalog.load_table("lake.orders")

with table.update_schema() as update:
    update.add_column("discount_amount", LongType(), doc="Discount in cents")
# Old parquet files: discount_amount reads as NULL (backward compatible)
# New parquet files: include discount_amount
```

### Iceberg vs Delta Lake vs Hudi

| Feature | Iceberg | Delta Lake | Apache Hudi |
|---|---|---|---|
| Multi-engine support | Best (Spark, Flink, Trino, Dremio, BigQuery) | Good (Spark-first, others via connector) | Good |
| ACID transactions | Yes | Yes | Yes |
| Schema evolution | Yes (additive + rename) | Yes (additive) | Yes |
| Time travel | Yes | Yes | Yes |
| Merge-on-read upserts | Yes | Yes | Primary design |
| Catalog requirements | Yes (Glue/Hive/REST) | No (uses _delta_log in path) | Yes |
| Cloud-agnostic | Best | Good | Good |

**Senior recommendation:** For new greenfield multi-cloud systems, choose Iceberg. For existing Databricks/Spark-heavy shops, Delta Lake is fine.

---

## Multi-Cloud Disaster Recovery

### Active-Passive vs Active-Active

**Active-Passive (most common):**
```
AWS (PRIMARY — all writes go here)
  │
  │ async replication (15–60 min lag)
  ▼
GCP (STANDBY — reads only, failover target)

RPO: 15–60 minutes (data loss window)
RTO: 30–90 minutes (time to switch)
Cost: ~1.5× single cloud (pay for standby infra)
```

**Active-Active (complex, expensive):**
```
AWS ←──── Kafka MirrorMaker 2 (bidirectional) ────► GCP
  (50% of writes)                                    (50% of writes)
  
Conflict resolution: Last-writer-wins with Lamport timestamps
RPO: Near-zero (seconds)
RTO: < 5 minutes
Cost: ~2× single cloud
Use when: RTO/RPO requirements < 15 minutes
```

### Failover Runbook Pattern

```python
# Automated failover using Route53 health checks + Lambda
# When AWS primary Kafka is unhealthy → switch producers to GCP backup

def check_and_failover(event, context):
    health = check_kafka_health("msk.us-east-1.amazonaws.com")
    if not health["is_healthy"]:
        # Update DNS: failover.kafka.internal → GCP Confluent endpoint
        route53.change_resource_record_sets(
            HostedZoneId="...",
            ChangeBatch={
                "Changes": [{
                    "Action": "UPSERT",
                    "ResourceRecordSet": {
                        "Name": "kafka.internal",
                        "Type": "CNAME",
                        "TTL": 60,
                        "ResourceRecords": [{"Value": "kafka.europe-west1.gcp.confluent.cloud"}]
                    }
                }]
            }
        )
        alert_oncall("Kafka failover triggered: AWS MSK → GCP Confluent")
```

---

## Federated Governance in Multi-Cloud

### The Problem

Different clouds have different access control models:
- AWS: IAM roles + S3 bucket policies
- GCP: Service accounts + IAM bindings + VPC Service Controls
- Azure: RBAC + AD groups + Storage Account ACLs

**You need a single governance plane that spans all three.**

### Solutions

**Option 1: Apache Ranger (open source)**
```
Apache Ranger (central policy server)
  │
  ├── Ranger plugin (AWS EMR) — intercepts Spark queries
  ├── Ranger plugin (GCP Dataproc) — intercepts Spark queries
  └── Ranger plugin (Hive/Trino) — intercepts SQL queries

Policy: "Data scientists can read 'orders' table but PII columns masked"
→ Ranger applies this policy on any query engine, any cloud
```

**Option 2: Unity Catalog (Databricks, cloud-agnostic)**
```
Unity Catalog Metastore (Databricks control plane)
  │ governs
  ├── AWS: Databricks workspace → EMR clusters can query Unity Catalog tables
  ├── GCP: Databricks workspace → Dataproc with Unity Catalog connector
  └── Azure: Databricks workspace (native)

Policy enforcement: column-level masking, row-level security, audit logs
```

**Option 3: OpenLineage + DataHub (lineage + discovery)**
```python
# Emit lineage events from any cloud via OpenLineage
from openlineage.client import OpenLineageClient
from openlineage.client.run import RunEvent, Run, Job, Dataset

client = OpenLineageClient(url="https://datahub.company.com/openlineage")

# Works whether running on AWS EMR or GCP Dataproc
client.emit(RunEvent(
    eventType="COMPLETE",
    run=Run(runId="job-run-uuid"),
    job=Job(namespace="aws-emr", name="daily_orders_transform"),
    inputs=[Dataset(namespace="s3://lake", name="raw_orders")],
    outputs=[Dataset(namespace="s3://lake", name="curated_orders")]
))
# DataHub ingests this → shows cross-cloud lineage graph
```

---

## Cost Optimization at Multi-Cloud Scale

### Egress Cost Reduction Strategies

| Strategy | Mechanism | Savings |
|---|---|---|
| **BigQuery Omni** | Query S3 data from BigQuery without copying to GCS | Eliminates S3→GCS egress cost |
| **Athena Federated Query** | Query GCS/BigQuery from Athena via custom connector | Avoids full dataset copies |
| **Compress before transfer** | Gzip/Zstd before cross-cloud transfer | 3–5× fewer bytes = 3–5× lower egress |
| **Incremental transfers** | Only transfer changed files (delta) | 90%+ reduction vs full copies |
| **Regional affinity** | Use GCP us-central1 + AWS us-east-1 (same geography) | Faster transfer, same egress cost |

### Unified Cost Dashboard

```sql
-- BigQuery table that aggregates all cloud costs
-- AWS Billing → S3 → BigQuery; GCP Billing native in BigQuery; Azure → Storage → BigQuery

SELECT
  cloud_provider,
  service_name,
  team_tag,
  project_tag,
  SUM(cost_usd) AS total_cost_usd,
  DATE_TRUNC(usage_date, MONTH) AS month
FROM `cost_analytics.unified_cloud_costs`
WHERE usage_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
GROUP BY 1, 2, 3, 4, 5
ORDER BY total_cost_usd DESC;
```

---

## Multi-Cloud Anti-Patterns to Call Out in Interviews

1. **Anti-pattern: Copying data for every cross-cloud query**
   - Solution: Use BigQuery Omni (query S3 in-place) or Trino with multi-catalog connectors

2. **Anti-pattern: Different pipeline code for each cloud**
   - Solution: Cloud-agnostic orchestration (Airflow/Dagster) with provider-specific operators

3. **Anti-pattern: Manual failover procedures**
   - Solution: Automated health checks + DNS failover + runbooks as code

4. **Anti-pattern: Untagged resources across clouds**
   - Solution: Tagging policy enforced at IaC layer (Terraform) before provisioning

5. **Anti-pattern: Per-cloud identity management**
   - Solution: Centralized IdP (Okta/Azure AD) with OIDC federation to each cloud
