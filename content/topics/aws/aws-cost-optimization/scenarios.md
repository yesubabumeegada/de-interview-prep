---
title: "AWS Cost Optimization Scenario Questions"
description: "Practice scenarios for AWS cost optimization: S3 storage tier selection, EMR Spot pipeline design, and FinOps strategy for multi-account data platforms"
content_type: scenario_question
topic: aws-services
subtopic: aws-cost-optimization
tags: [aws, cost-optimization, s3, emr, spot-instances, finops, scenario, interview]
---

<article data-difficulty="junior">

## Scenario: Choosing the Right S3 Storage Tier for Log Data

Your company collects application logs at a rate of 500 GB per day. These logs are stored in S3 and used for:
- Active debugging and monitoring during the first 30 days
- Occasional incident investigation from 30–90 days old
- Compliance retention requirement: logs must be kept for 7 years
- Logs older than 7 years can be deleted

Currently all logs are stored in S3 Standard, and the team lead has asked you to reduce the S3 storage bill without violating the retention policy. The logs are never accessed after 90 days except in rare compliance audits (once or twice a year).

**Question:** Which S3 storage tiers would you use and when? Design a lifecycle policy that minimizes cost while meeting all requirements.

<details>
<summary>✅ Solution</summary>

### Storage Tier Selection

Given the access pattern, the optimal configuration is:

| Age | Storage Class | Reasoning |
|-----|--------------|-----------|
| 0–30 days | S3 Standard | Active debugging, fast access needed |
| 30–90 days | S3 Standard-IA | Occasional incident investigation, pay-per-retrieval is fine |
| 90 days – 7 years | S3 Glacier Flexible Retrieval | Compliance audits once/twice a year; Standard retrieval (3-5 hours) is acceptable |
| After 7 years (2555 days) | Delete | Retention requirement satisfied |

**Why not Glacier Deep Archive for 90+ days?**
- Deep Archive has 12–48 hour retrieval time
- Compliance audits require retrieval within a few hours
- Glacier Flexible Retrieval's Standard tier (3-5 hours) meets audit requirements

**Why not Glacier Instant Retrieval?**
- At $0.004/GB vs Glacier Flexible at $0.0036/GB, the cost difference is small
- But at massive scale (500 GB/day over years), it adds up
- The audit access pattern doesn't require instant (millisecond) retrieval

### Lifecycle Policy

```json
{
  "Rules": [
    {
      "ID": "ApplicationLogLifecycle",
      "Status": "Enabled",
      "Filter": {"Prefix": "logs/"},
      "Transitions": [
        {
          "Days": 30,
          "StorageClass": "STANDARD_IA"
        },
        {
          "Days": 90,
          "StorageClass": "GLACIER"
        }
      ],
      "Expiration": {
        "Days": 2555
      }
    },
    {
      "ID": "CleanupIncompleteUploads",
      "Status": "Enabled",
      "AbortIncompleteMultipartUpload": {
        "DaysAfterInitiation": 7
      }
    }
  ]
}
```

### Cost Estimate (per month at steady state)

Assuming 500 GB/day, after 90+ days of accumulation:
- Standard (0-30 days): 15 TB × $0.023 = $345
- Standard-IA (30-90 days): 30 TB × $0.0125 = $375
- Glacier (90 days - 7 years): ~1,165 TB × $0.0036 = $4,194

Compared to all-Standard: ~1,210 TB × $0.023 = **$27,830**

**Savings: ~83% reduction in storage costs.**

### Follow-up Considerations

- Enable S3 Versioning only if you need protection against accidental deletion; if not, leave it off to avoid unexpected version accumulation costs
- S3 Standard-IA has a 30-day minimum storage charge — if you delete objects before 30 days, you're charged for 30 days anyway. Since logs are kept 7 years, this doesn't matter here.
- Consider S3 Intelligent-Tiering instead if the access pattern is unpredictable

</details>

</article>

<article data-difficulty="mid">

## Scenario: Cost-Optimized Nightly EMR Pipeline with Spot Checkpointing

You are a data engineer at an e-commerce company. You have a nightly EMR Spark job that:
- Runs daily at 1 AM, must complete by 6 AM (5-hour SLA)
- Processes 3 TB of daily transaction data
- Runs four sequential Spark stages: ingestion → enrichment → aggregation → reporting
- Currently uses 15x `m5.2xlarge` On-Demand instances ($1.08/hour each = $16.20/hour)
- Monthly cost: ~$500/month (runs ~31 hours/month)

Management wants you to reduce costs by at least 50% while maintaining the 5-hour SLA and tolerating occasional pipeline restarts (but not complete failures).

**Question:** Design a cost-optimized EMR configuration using Spot instances with appropriate checkpointing. Address: cluster configuration, Spark tuning, checkpoint strategy, and failure handling.

<details>
<summary>✅ Solution</summary>

### Target: Reduce from $500/month to ~$150/month (70% reduction)

### 1. EMR Cluster Configuration

Use Instance Fleets to diversify Spot pools and reduce interruption risk:

```json
{
  "InstanceFleets": [
    {
      "InstanceFleetType": "MASTER",
      "TargetOnDemandCapacity": 1,
      "InstanceTypeConfigs": [
        {"InstanceType": "m5.xlarge", "WeightedCapacity": 1}
      ]
    },
    {
      "InstanceFleetType": "CORE",
      "TargetOnDemandCapacity": 2,
      "InstanceTypeConfigs": [
        {"InstanceType": "m5.xlarge", "WeightedCapacity": 1},
        {"InstanceType": "m5d.xlarge", "WeightedCapacity": 1}
      ]
    },
    {
      "InstanceFleetType": "TASK",
      "TargetSpotCapacity": 30,
      "TargetOnDemandCapacity": 0,
      "InstanceTypeConfigs": [
        {"InstanceType": "m5.2xlarge", "WeightedCapacity": 2},
        {"InstanceType": "m5d.2xlarge", "WeightedCapacity": 2},
        {"InstanceType": "m4.2xlarge", "WeightedCapacity": 2},
        {"InstanceType": "r5.xlarge", "WeightedCapacity": 1},
        {"InstanceType": "m5a.2xlarge", "WeightedCapacity": 2}
      ],
      "LaunchSpecifications": {
        "SpotSpecification": {
          "TimeoutDurationMinutes": 5,
          "TimeoutAction": "SWITCH_TO_ON_DEMAND"
        }
      }
    }
  ]
}
```

**Why 2 On-Demand core nodes?** They hold HDFS metadata and coordinate YARN. Task nodes (Spot) handle all processing — if interrupted, only in-progress tasks restart.

### 2. Spark Tuning for Spot Resilience

```python
spark_conf = {
    # Smaller tasks = less wasted work per interruption
    "spark.sql.shuffle.partitions": "400",      # Was 200
    "spark.default.parallelism": "400",
    
    # Speculative execution to handle stragglers
    "spark.speculation": "true",
    "spark.speculation.threshold": "0.75",
    "spark.speculation.multiplier": "1.5",
    
    # Graceful node decommission window
    "spark.blacklist.enabled": "true",
    "spark.blacklist.task.maxTaskAttemptsPerNode": "2",
    
    # Dynamic allocation to scale with available Spot capacity
    "spark.dynamicAllocation.enabled": "true",
    "spark.dynamicAllocation.minExecutors": "4",
    "spark.dynamicAllocation.maxExecutors": "20",
    "spark.dynamicAllocation.executorIdleTimeout": "120s"
}
```

### 3. Stage Checkpoint Strategy

Write intermediate results to S3 after each major stage:

```python
CHECKPOINT_BUCKET = "s3://etl-checkpoints/nightly-transactions/"
DATE = datetime.now().strftime("%Y-%m-%d")

def stage_complete(stage_name):
    """Check if a stage has already been completed for today."""
    key = f"{CHECKPOINT_BUCKET}{DATE}/{stage_name}/_SUCCESS"
    return s3_key_exists(key)

def run_nightly_pipeline():
    # Stage 1: Ingestion
    raw_path = f"{CHECKPOINT_BUCKET}{DATE}/raw/"
    if not stage_complete("raw"):
        raw = ingest_transactions()  # Read from S3 source
        raw.write.mode("overwrite").parquet(raw_path)
    raw = spark.read.parquet(raw_path)
    
    # Stage 2: Enrichment
    enriched_path = f"{CHECKPOINT_BUCKET}{DATE}/enriched/"
    if not stage_complete("enriched"):
        enriched = enrich_with_product_catalog(raw)
        enriched.write.mode("overwrite").parquet(enriched_path)
    enriched = spark.read.parquet(enriched_path)
    
    # Stage 3: Aggregation
    agg_path = f"{CHECKPOINT_BUCKET}{DATE}/aggregated/"
    if not stage_complete("aggregated"):
        aggregated = compute_daily_metrics(enriched)
        aggregated.write.mode("overwrite").parquet(agg_path)
    aggregated = spark.read.parquet(agg_path)
    
    # Stage 4: Write to Redshift (idempotent upsert)
    write_to_redshift(aggregated, mode="upsert", key="date,product_id")
    
    # Cleanup checkpoints (keep last 3 days for debugging)
    cleanup_old_checkpoints(keep_days=3)
```

### 4. Failure Handling and SLA Management

**Automated retry via Step Functions or Airflow:**
```python
# Airflow DAG with retry logic
emr_task = EmrCreateJobFlowOperator(
    task_id='run_nightly_etl',
    job_flow_overrides=JOB_FLOW_CONFIG,
    retries=2,                        # Up to 2 retries
    retry_delay=timedelta(minutes=15), # Wait 15 min before retry
    execution_timeout=timedelta(hours=4)  # Fail if >4 hours (leaves 1hr buffer)
)
```

**If job fails and retry starts:** Checkpoints mean the restarted job skips completed stages and resumes from the last successful stage. A full 3-stage restart from scratch takes ~4 hours; a checkpoint-based restart typically takes 1-2 hours depending on which stage failed.

**SLA monitoring:**
```python
# Alert if job hasn't completed by 5:30 AM (30-min before SLA)
CloudWatchAlarm(
    metric='PipelineDuration',
    threshold=16200,  # 4.5 hours in seconds
    comparison='GreaterThan',
    alarm_actions=['arn:aws:sns:...:on-call-alerts']
)
```

### Cost Estimate

| Component | Configuration | Cost/night | Monthly |
|-----------|--------------|-----------|---------|
| Master node | 1x m5.xlarge On-Demand, 3hrs | $0.192 × 3 = $0.58 | $17.40 |
| Core nodes | 2x m5.xlarge On-Demand, 3hrs | $0.384 × 3 = $1.15 | $34.50 |
| Task nodes | ~70% Spot savings, 3hrs | ~$2.50 | $77.50 |
| **Total** | | **~$4.23/night** | **~$129/month** |

**Result: 74% cost reduction** ($500 → $129) while maintaining the 5-hour SLA with a checkpoint-based restart capability.

</details>

</article>

<article data-difficulty="senior">

## Scenario: FinOps Strategy for a Multi-Account Data Platform at $500K/Month

You've just joined a fast-growing tech company as a senior data engineer. The VP of Engineering tells you the AWS bill for the data platform has grown to $500,000/month, up from $200,000/month a year ago. There's no clear explanation for the 2.5× growth, and no cost governance in place.

The platform spans:
- 3 AWS accounts (data production, data dev/staging, data science)
- Services: EMR, Glue, Redshift (4 × ra3.4xlarge clusters), Athena, S3 (2 PB), Kinesis, MSK (Kafka)
- ~20 data engineers and analysts across 4 teams
- No cost allocation tags on most resources
- No Reserved Instances or Savings Plans purchased
- No budget alerts configured

You have a mandate to reduce costs by 30% ($150K/month) within 90 days without significant architectural changes.

**Question:** Design a comprehensive 90-day FinOps strategy. Cover: immediate wins, medium-term optimizations, governance implementation, and measurement/accountability.

<details>
<summary>✅ Solution</summary>

### Phase 0: Baseline and Visibility (Days 1-7)

Before optimizing, you must understand where money is going.

**Enable Cost and Usage Report:**
```bash
aws cur put-report-definition \
    --report-definition '{
        "ReportName": "DataPlatformCUR",
        "TimeUnit": "HOURLY",
        "Format": "Parquet",
        "Compression": "Parquet",
        "AdditionalSchemaElements": ["RESOURCES"],
        "S3Bucket": "billing-reports-bucket",
        "S3Prefix": "cur/",
        "S3Region": "us-east-1"
    }'
```

**Query CUR to identify top cost drivers:**
```sql
-- Top 10 cost drivers last month
SELECT
    product_product_name AS service,
    line_item_usage_type,
    SUM(line_item_unblended_cost) AS monthly_cost,
    SUM(line_item_unblended_cost) / 500000 AS pct_of_total
FROM cur_report
WHERE month = '2024-01'
GROUP BY 1, 2
ORDER BY monthly_cost DESC
LIMIT 20;
```

**Typical findings at this scale:**
- S3 storage + transfer: ~25-30% ($125-150K)
- EMR (likely over-provisioned On-Demand): ~25-30%
- Redshift (4 clusters, no RIs): ~15-20%
- Glue: ~10-15%
- Data transfer: ~10-15%

### Phase 1: Immediate Wins — Target $50K/month (Days 7-30)

**Win 1: Terminate idle resources ($20-30K/month)**

```python
# Auto-discover and flag idle resources
def audit_idle_resources():
    # EMR clusters in WAITING state for >2 hours
    idle_emr = find_idle_emr_clusters(idle_hours=2)
    
    # Redshift clusters with <5% CPU for 7+ days
    idle_redshift = find_idle_redshift_clusters(cpu_threshold=5, days=7)
    
    # Unattached EBS volumes
    unattached_ebs = find_unattached_ebs_volumes()
    
    return {
        "idle_emr_estimated_monthly_cost": sum(c['monthly_cost'] for c in idle_emr),
        "idle_redshift_estimated_monthly_cost": sum(c['monthly_cost'] for c in idle_redshift),
        "unattached_ebs_monthly_cost": sum(v['monthly_cost'] for v in unattached_ebs)
    }
```

**Win 2: Enable Athena workgroup scan limits ($10-15K/month)**

```python
# Create workgroups with scan limits (see intermediate case study)
# Most analytics teams have 10-20% of queries doing full-table scans
# Workgroup limits force analysts to add partition filters
```

**Win 3: Apply S3 lifecycle policies ($10-15K/month)**

Run S3 inventory, identify Standard-tier data >30 days old, apply lifecycle:
```bash
# Apply lifecycle to top 5 buckets by Standard-tier volume older than 30 days
for bucket in $(get_top_buckets_by_old_standard_data):
    aws s3api put-bucket-lifecycle-configuration \
        --bucket $bucket \
        --lifecycle-configuration file://standard-lifecycle.json
```

**Win 4: Enable incomplete multipart upload cleanup ($2-5K/month)**
```bash
# Apply to ALL buckets — pure savings with zero risk
aws s3api list-buckets | jq '.Buckets[].Name' | xargs -I{} \
    aws s3api put-bucket-lifecycle-configuration \
    --bucket {} \
    --lifecycle-configuration '{"Rules":[{"ID":"CleanupMPU","Status":"Enabled","AbortIncompleteMultipartUpload":{"DaysAfterInitiation":7}}]}'
```

### Phase 2: Medium-Term Optimizations — Target $70K/month (Days 30-60)

**Win 5: Migrate EMR to Spot instances ($25-35K/month)**

Convert all batch EMR jobs to use Instance Fleets with Spot task nodes:
- Expected savings: 60-70% on compute for batch workloads
- Requires checkpointing implementation (2-3 days per pipeline)
- Prioritize longest-running, most frequent jobs first

**Win 6: Purchase Redshift Reserved Nodes ($15-20K/month)**

4 × ra3.4xlarge On-Demand: $1.008/hr × 4 = $4.032/hr × 720 hrs = $2,903/month
4 × ra3.4xlarge 1-yr RI (All Upfront effective rate): ~$0.546/hr × 4 = $2.184/hr × 720 = $1,572/month

**Savings: $1,331/month per cluster × 4 clusters = $5,324/month** (~$64K over the year)

But first verify: are all 4 clusters actually needed? If 2 are underutilized, consolidate or switch to Redshift Serverless.

**Win 7: Purchase Compute Savings Plans ($15-20K/month)**

```python
# Analyze last 30 days of compute spend
# Identify stable baseline that runs 24/7 (Kafka MSK, always-on Glue crawlers, etc.)
# Commit to that baseline spend via Compute Savings Plans

baseline_hourly_compute = calculate_consistent_compute_spend()
# Purchase Compute Savings Plan for baseline_hourly_compute/hour
```

### Phase 3: Governance Implementation (Days 60-90)

**Mandatory tagging via AWS Config:**
```python
# Config Rule: check all new EC2, EMR, S3 buckets for required tags
{
    "ConfigRuleName": "RequiredTags",
    "Source": {"Owner": "AWS", "SourceIdentifier": "REQUIRED_TAGS"},
    "InputParameters": json.dumps({
        "tag1Key": "Team",
        "tag2Key": "Project", 
        "tag3Key": "Environment",
        "tag4Key": "CostCenter"
    })
}
```

**Budget alerts for each team:**
```python
teams = ['data-engineering', 'analytics', 'ml-platform', 'data-science']
for team in teams:
    create_team_budget(
        name=f"{team}-monthly",
        amount=allocated_budget[team],
        filter_tag={'Team': team},
        alert_thresholds=[80, 100, 120]  # % of budget
    )
```

**Weekly cost review process:**
- Monday: Automated cost report emailed to each team (their costs vs. budget)
- Wednesday: Data platform cost review (30 min, team leads + FinOps)
- Friday: Anomaly investigation and resolution

**Cost as a code review criterion:**
```
# PR template addition:
## Cost Impact
- [ ] No new AWS resources created
- [ ] New resources have required tags
- [ ] Estimated monthly cost impact: $___
- [ ] DPU count / cluster size justified by profiling
```

### Measurement and Accountability

**KPIs to track:**

| KPI | Baseline | 30-day Target | 90-day Target |
|-----|---------|---------------|---------------|
| Total monthly cost | $500K | $450K | $350K |
| S3 Standard-tier data >30 days old | 100% | 50% | 10% |
| EMR Spot task node % | 0% | 40% | 80% |
| Redshift RI coverage | 0% | 0% | 100% |
| Resources with required tags | 15% | 60% | 95% |
| Savings Plans coverage | 0% | 30% | 60% |
| Athena full-table scan queries | 35% | 15% | 5% |

**Unit economics dashboard:**
```
Cost per GB processed:     $0.043 → target $0.025
Cost per active analyst:   $25,000/month → target $17,500/month
Cost per pipeline run:     Track top 20 pipelines
```

### Summary: 90-Day Expected Savings

| Initiative | Monthly Savings | Timeline |
|-----------|----------------|---------|
| Terminate idle resources | $25,000 | Week 1 |
| Athena workgroup limits | $12,000 | Week 2 |
| S3 lifecycle policies | $18,000 | Weeks 2-4 |
| Multipart upload cleanup | $3,000 | Week 1 |
| EMR → Spot migration | $30,000 | Weeks 4-8 |
| Redshift Reserved Nodes | $20,000 | Week 6 |
| Savings Plans purchase | $15,000 | Week 7 |
| Governance (tag enforcement) | $10,000 (reduced waste) | Weeks 8-12 |
| **Total** | **$133,000/month** | **90 days** |

This exceeds the $150K target — achievable with disciplined execution. The key is sequencing: visibility first, then quick wins, then structural changes, then governance.

**Critical success factors:**
1. Executive sponsorship (VP Engineering mandate helps here)
2. Team leads own their cost budgets (accountability)
3. Engineering time allocated to optimization (not just business-as-usual work)
4. Celebrate wins publicly to build momentum

</details>

</article>
