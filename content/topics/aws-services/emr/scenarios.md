---
title: "AWS EMR - Scenario Questions"
topic: aws-services
subtopic: emr
content_type: scenario_question
tags: [aws, emr, interview, scenarios, spark, cluster]
---

# Scenario Questions — AWS EMR

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Design a Cost-Effective EMR Cluster

**Scenario:** Your nightly Spark ETL processes 2 TB of data in 3 hours on a 20-node cluster costing $1,200/month. Leadership wants 50% cost reduction without increasing runtime beyond 4 hours. Design the optimization.

<details>
<summary>✅ Solution</summary>

**Current cost:** 20 × m5.2xlarge × 3 hrs × 30 days × $0.384/hr = $1,382/month

**Optimization strategy:**

```python
# Strategy: Spot Task nodes + smaller Core fleet + auto-scaling

# Fixed Core (On-Demand): 4 nodes for stability
# Variable Task (Spot): 0-20 nodes based on workload
# Spot pricing: ~$0.12/hr vs $0.384/hr On-Demand (70% savings)

instance_fleets = [
    {'Type': 'MASTER', 'OnDemand': 1, 'Instance': 'm5.xlarge'},     # $0.19/hr
    {'Type': 'CORE', 'OnDemand': 4, 'Instance': 'r5.2xlarge'},      # 4 × $0.50 = $2/hr
    {'Type': 'TASK', 'Spot': 16, 'Instance': 'r5.2xlarge'},         # 16 × $0.15 = $2.40/hr
]
# Total hourly: $0.19 + $2 + $2.40 = $4.59/hr
# Monthly: $4.59 × 4 hrs × 30 = $551/month

# With auto-scaling policy:
# Scale up to 20 Task nodes during heavy shuffle stages
# Scale down to 4 Task nodes during I/O stages
# Average: ~12 Task nodes → $3.80/hr × 4 hrs × 30 = $456/month
```

**Additional optimizations:**
- Use S3 instead of HDFS (Core nodes don't need to store data)
- Enable Spark AQE for automatic partition optimization
- Use Graviton instances (r6g) for 15% better price/performance

**Result:** $1,382 → ~$500/month (64% reduction), runtime stays under 4 hours.

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: EMR Serverless vs On-EC2 Migration

**Scenario:** Your team runs 50 daily Spark jobs on a persistent EMR cluster (always on, $3,000/month). Jobs run between 1 AM-7 AM, cluster idle 75% of the day. Should you migrate to EMR Serverless? Calculate cost impact and identify which jobs are good/bad candidates.

<details>
<summary>✅ Solution</summary>

**Current state analysis:**
- 50 jobs, run 1 AM-7 AM (6 hours active, 18 hours idle)
- Cluster: 10 × r5.2xlarge On-Demand ≈ $5.04/hr compute + ~$1.26/hr EMR fee ≈ $6.3/hr
- Monthly on-demand 24/7: ~$6.3 × 24hr × 30 ≈ $4,500 → actual ~$3,000 with reserved instances
- With reservations: effective ~$4.17/hr ($3,000 / 720 hours)

**EMR Serverless cost estimate:**
- Each job: average 5 vCPU-hours and 20 GB-hours
- Serverless pricing: $0.052624/vCPU-hour + $0.0057785/GB-hour
- Per job: (5 × $0.052) + (20 × $0.0058) = $0.26 + $0.116 = $0.376
- 50 jobs × $0.376 × 30 days = **$564/month**

**Decision matrix:**

| Factor | Stay on EC2 | Migrate to Serverless |
|--------|-------------|----------------------|
| Cost (this workload) | $3,000/month | $564/month |
| Startup latency | 0 (always running) | 30-60 seconds (acceptable for batch) |
| Custom libraries | Full control | Limited (but improving) |
| Shared state (Hive Metastore) | On-cluster | Use Glue Catalog instead |
| Interactive notebooks | Yes (Jupyter on master) | No (use SageMaker/Glue Studio) |

**Candidates for migration:**

| Good for Serverless | Bad for Serverless |
|--------------------|--------------------|
| Independent batch ETL jobs | Jobs needing shared HDFS state |
| Standard PySpark transforms | Jobs using custom C libraries |
| Jobs under 1 hour | Jobs needing persistent connections |
| Jobs with variable size | Streaming/always-on jobs |

**Recommendation:** Migrate 45 of 50 jobs to Serverless. Keep 5 complex jobs on a smaller on-demand cluster (spin up only during 1-7 AM window).

**Hybrid cost:** Serverless ($564) + a smaller on-demand cluster (~4 nodes) running only the 6-hour window (~$2.5/hr × 6 × 30 ≈ $450-500) = **~$1,064/month (65% savings)**

</details>

</article>
