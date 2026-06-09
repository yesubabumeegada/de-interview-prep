---
title: "Cost Optimization — Scenarios"
topic: system-design
subtopic: cost-optimization
content_type: scenario_question
tags: [cost, optimization, cloud, scenarios]
---

# Cost Optimization — Interview Scenarios

<article data-difficulty="junior">

## 🟢 Junior: Identifying Quick Cloud Cost Wins

**Scenario:** Your team's AWS bill for the data platform is $80K/month and growing 20% each quarter. Your manager asks you to find quick wins to reduce costs. What do you look for first?

<details>
<summary>💡 Hint</summary>

Quick wins are usually: unused resources (stopped EC2 still paying EBS), oversized instances, S3 without lifecycle policies, and data transfer costs. Use AWS Cost Explorer and Trusted Advisor as starting points.

</details>

<details>
<summary>✅ Solution</summary>

**Step 1: Use AWS Cost Explorer to identify top spenders**

Top cost categories for data platforms:
1. S3 storage + requests
2. EMR/EC2 compute
3. Data transfer (egress)
4. RDS/Redshift

**Quick Win Checklist:**

**S3 (often 30-40% of bill):**
```python
import boto3

s3 = boto3.client('s3')

# Find buckets without lifecycle policies
def find_buckets_without_lifecycle():
    buckets = s3.list_buckets()['Buckets']
    unmanaged = []
    for bucket in buckets:
        try:
            s3.get_bucket_lifecycle_configuration(Bucket=bucket['Name'])
        except s3.exceptions.from_code('NoSuchLifecycleConfiguration'):
            unmanaged.append(bucket['Name'])
    return unmanaged

# Apply intelligent-tiering lifecycle
def apply_lifecycle(bucket_name):
    s3.put_bucket_lifecycle_configuration(
        Bucket=bucket_name,
        LifecycleConfiguration={
            'Rules': [{
                'ID': 'move-to-ia',
                'Status': 'Enabled',
                'Filter': {'Prefix': ''},
                'Transitions': [
                    {'Days': 30, 'StorageClass': 'STANDARD_IA'},
                    {'Days': 90, 'StorageClass': 'GLACIER_INSTANT_RETRIEVAL'},
                ],
                'NoncurrentVersionExpiration': {'NoncurrentDays': 30}
            }]
        }
    )
```

**EMR/Compute:**
- Switch from On-Demand to Spot Instances for batch jobs (60-80% savings)
- Terminate clusters after jobs complete (not "always-on")
- Rightsizing: check CloudWatch CPU — if consistently < 30%, downsize

```python
# Spot instance for EMR
emr_config = {
    'InstanceFleets': [{
        'InstanceFleetType': 'TASK',
        'TargetSpotCapacity': 20,
        'InstanceTypeConfigs': [
            {'InstanceType': 'r5.4xlarge', 'BidPriceAsPercentageOfOnDemandPrice': 80},
            {'InstanceType': 'r5.8xlarge', 'BidPriceAsPercentageOfOnDemandPrice': 80},
        ]
    }]
}
```

**Data Transfer:**
- Use S3 Transfer Acceleration only when needed
- Keep compute in same region as S3 (cross-region transfer = $0.02/GB)
- Use VPC endpoints for S3 (eliminates NAT Gateway charges)

**Typical Savings:** S3 lifecycle (20-30%), Spot instances (50-70% compute reduction), rightsizing (15-20%).

</details>

</article>

<article data-difficulty="mid-level">

## 🟡 Mid-Level: Optimizing Databricks Spend

**Scenario:** Your Databricks bill is $120K/month. Jobs are running on all-purpose clusters left on overnight by data scientists. Analytics jobs run on oversized clusters. Design a cost governance strategy.

<details>
<summary>💡 Hint</summary>

Key levers: cluster auto-termination, job clusters vs all-purpose clusters, instance pool reuse, cluster policies to enforce limits, and DBU (Databricks Unit) monitoring per team.

</details>

<details>
<summary>✅ Solution</summary>

**Root Cause Analysis:**

```python
# Query Databricks API for cluster usage
import requests

def get_cluster_costs(workspace_url: str, token: str):
    r = requests.get(
        f"{workspace_url}/api/2.0/clusters/list",
        headers={"Authorization": f"Bearer {token}"}
    )
    clusters = r.json()['clusters']

    expensive = [c for c in clusters
                 if c.get('cluster_source') == 'UI'  # manually created
                 and c.get('state') == 'RUNNING'
                 and time_since_last_activity(c) > 60]  # idle >60 min

    return expensive
```

**Fix 1: Enforce Auto-Termination via Cluster Policy**

```json
{
  "cluster_type": {
    "type": "fixed",
    "value": "dbt_generic"
  },
  "autotermination_minutes": {
    "type": "range",
    "minValue": 10,
    "maxValue": 60,
    "defaultValue": 20
  },
  "num_workers": {
    "type": "range",
    "minValue": 1,
    "maxValue": 8
  },
  "node_type_id": {
    "type": "allowlist",
    "values": ["i3.xlarge", "i3.2xlarge", "r5.2xlarge"]
  }
}
```

**Fix 2: Job Clusters Instead of All-Purpose**

```python
# All-purpose cluster: $0.55/DBU, always-on → expensive
# Job cluster: $0.15/DBU, spins up per job, auto-terminates → 70% cheaper

# Airflow DAG using job clusters
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

transform_task = DatabricksSubmitRunOperator(
    task_id='run_etl',
    databricks_conn_id='databricks',
    new_cluster={
        'spark_version': '13.3.x-scala2.12',
        'node_type_id': 'i3.2xlarge',
        'num_workers': 4,
        'aws_attributes': {'availability': 'SPOT_WITH_FALLBACK'}
    },
    notebook_task={'notebook_path': '/jobs/etl_transform'}
)
```

**Fix 3: Instance Pools for Fast Startup Without Idle Costs**

```python
# Instance pool keeps pre-warmed instances (pay for EBS only, not EC2)
pool_config = {
    "instance_pool_name": "analytics-pool",
    "min_idle_instances": 2,
    "max_capacity": 20,
    "node_type_id": "r5.4xlarge",
    "idle_instance_autotermination_minutes": 10,
    "aws_attributes": {"availability": "SPOT_WITH_FALLBACK"}
}
# Clusters from pool start in 30s instead of 5 minutes
```

**Fix 4: Cost Attribution with Tags**

```python
# Tag every cluster with team + project for chargeback
cluster_tags = {
    "team": "data-engineering",
    "project": "customer-churn",
    "cost-center": "CC-4521",
    "environment": "production"
}

# Monthly chargeback report
spark.sql("""
    SELECT tags['team'], sum(dbu_hours * dbu_rate) as monthly_cost
    FROM system.billing.usage
    WHERE usage_date >= date_trunc('month', current_date)
    GROUP BY 1
    ORDER BY 2 DESC
""").show()
```

**Expected Savings:**
- Auto-termination: eliminate overnight idle clusters (-$30K/month)
- Job clusters for scheduled jobs: 70% DBU rate reduction (-$25K/month)
- Spot instances: 50% EC2 savings (-$15K/month)
- **Total: ~$70K/month (58% reduction)**

</details>

</article>

<article data-difficulty="senior">

## 🔴 Senior: Building a FinOps Practice for a Data Platform Team

**Scenario:** Your data platform costs $2M/year across AWS (EMR, S3, Glue), Databricks, and Snowflake. The CFO wants a 25% reduction ($500K) without reducing capabilities. Build a comprehensive FinOps strategy including governance, tooling, and process changes.

<details>
<summary>💡 Hint</summary>

FinOps has three phases: Inform (visibility), Optimize (action), Operate (culture). Start with cost allocation tags and unit economics (cost per pipeline run, cost per TB processed). Then optimize the top 3 cost drivers. Finally embed cost awareness into engineering culture via dashboards and team budgets.

</details>

<details>
<summary>✅ Solution</summary>

**Phase 1: Inform — Cost Visibility**

```python
# Build cost allocation by service, team, and job
import boto3
from datetime import datetime, timedelta

ce = boto3.client('ce')

def get_cost_by_tag(tag_key: str, start: str, end: str):
    response = ce.get_cost_and_usage(
        TimePeriod={'Start': start, 'End': end},
        Granularity='MONTHLY',
        Filter={'Tags': {'Key': tag_key, 'Values': [], 'MatchOptions': ['EXISTS']}},
        GroupBy=[{'Type': 'TAG', 'Key': tag_key}],
        Metrics=['UnblendedCost']
    )
    return response['ResultsByTime']

# Unit economics dashboard
unit_metrics = {
    "cost_per_pipeline_run": total_compute_cost / pipeline_runs,
    "cost_per_tb_processed": total_cost / tb_processed,
    "cost_per_active_analyst": total_cost / active_analysts,
    "cost_per_dashboard_view": query_cost / dashboard_views
}
```

**Phase 2: Optimize — Top 3 Initiatives**

**Initiative 1: Snowflake Query Optimization (-$150K)**

```sql
-- Find expensive queries by team
SELECT query_tag, 
       sum(credits_used_compute) as credits,
       sum(credits_used_compute) * 3 as cost_usd,  -- $3/credit
       count(*) as query_count
FROM snowflake.account_usage.query_history
WHERE start_time >= dateadd('month', -1, current_timestamp)
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;

-- Auto-suspend idle warehouses
ALTER WAREHOUSE analytics_wh SET AUTO_SUSPEND = 60;  -- 1 min
ALTER WAREHOUSE analytics_wh SET AUTO_RESUME = TRUE;

-- Query result cache (free re-use)
ALTER SESSION SET USE_CACHED_RESULT = TRUE;
```

**Initiative 2: S3 + EMR Optimization (-$200K)**

```python
# Automated rightsizing: analyze job metrics, recommend smaller clusters
def analyze_emr_job_efficiency():
    metrics = cloudwatch.get_metric_data(
        MetricDataQueries=[
            {
                'Id': 'cpu_util',
                'MetricStat': {
                    'Metric': {'Namespace': 'AWS/ElasticMapReduce',
                               'MetricName': 'CoreNodesRunning'},
                    'Period': 300,
                    'Stat': 'Average'
                }
            }
        ]
    )
    
    if metrics['avg_cpu'] < 0.30:
        recommendation = f"Reduce cluster size by 50% — currently at {metrics['avg_cpu']:.0%} CPU"
        return recommendation

# S3 + EMR in same AZ (eliminate inter-AZ transfer)
emr_config['Ec2SubnetId'] = 'subnet-same-az-as-s3'
```

**Initiative 3: Reserved Capacity for Predictable Workloads (-$150K)**

```python
# Identify predictable daily jobs → buy Savings Plans
# 1-year Savings Plan: 40% off On-Demand EC2

# Snowflake: purchase capacity for base load
# Databricks: commit to $50K/month for 20% discount

commitments = {
    "EC2_Savings_Plan": {
        "hourly_commitment": 5.0,  # $/hour
        "term": "1_year",
        "savings_vs_od": 0.40
    },
    "Snowflake_Capacity": {
        "credits_per_month": 1000,
        "discount": 0.15
    }
}
```

**Phase 3: Operate — Embedding Cost Culture**

```python
# Weekly cost anomaly detection
def detect_cost_anomalies():
    ce = boto3.client('ce')
    anomalies = ce.get_anomalies(
        MonitorArn='arn:aws:ce::123456789:anomalymonitor/...',
        DateInterval={'StartDate': '2024-01-01', 'EndDate': '2024-01-31'}
    )
    
    for anomaly in anomalies['Anomalies']:
        if anomaly['Impact']['TotalImpact'] > 1000:  # >$1000 anomaly
            send_slack_alert(
                channel='#platform-finops',
                message=f"Cost anomaly: ${anomaly['Impact']['TotalImpact']:.0f} "
                        f"in {anomaly['RootCauses'][0]['Service']}"
            )

# Monthly team budget review
TEAM_BUDGETS = {
    "data-engineering": 80_000,
    "data-science": 40_000,
    "analytics": 30_000
}

def enforce_budget_alerts():
    for team, budget in TEAM_BUDGETS.items():
        actual = get_team_cost(team, current_month)
        if actual > budget * 0.80:
            notify_team_lead(team, actual, budget)
```

**Cost Governance Policies (enforced via SCP/IAM):**
1. No On-Demand EMR clusters > 20 nodes without VP approval
2. Snowflake warehouse size > XL requires justification ticket
3. All S3 buckets must have lifecycle policy within 7 days of creation
4. Auto-terminate any idle Databricks cluster after 30 minutes

**Projected 12-Month Savings:**
| Initiative | Annual Savings |
|-----------|---------------|
| Snowflake query optimization | $150K |
| EMR rightsizing + Spot | $200K |
| Reserved capacity commitments | $150K |
| S3 lifecycle + compaction | $80K |
| Idle resource elimination | $70K |
| **Total** | **$650K (32.5%)** |

</details>

</article>

---

## Interview Tips

> **Tip 1:** "How do you explain cloud cost optimization to non-technical stakeholders?" — Frame it as unit economics: cost per pipeline run, cost per analyst query, cost per customer record processed. These metrics make cost tangible and link to business value.
> **Tip 2:** "What is the difference between Reserved Instances and Savings Plans?" — Reserved Instances commit to a specific instance type in a region (less flexible, higher discount up to 72%). Savings Plans commit to an hourly dollar spend on EC2/Fargate/Lambda (more flexible, up to 66% discount). Savings Plans are generally preferred for data workloads with mixed instance types.
> **Tip 3:** "How do you prevent cost overruns from runaway Spark jobs?" — Set per-job resource limits in cluster policies, configure Spark job timeouts, use auto-scaling with max nodes cap, and set up CloudWatch billing alarms at 80% of monthly budget.
