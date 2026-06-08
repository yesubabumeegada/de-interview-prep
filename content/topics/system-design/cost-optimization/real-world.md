---
title: "Cost Optimization — Real World"
topic: system-design
subtopic: cost-optimization
content_type: study_material
difficulty_level: mid-level
layer: real-world
tags: [system-design, cost-optimization, production, finops, cloud]
---

# Cost Optimization — Real World

## Pattern 1: Cost Review Dashboard

```sql
-- Weekly cost review queries for a data platform team

-- Snowflake: top credit consumers last 7 days
SELECT
  user_name,
  warehouse_name,
  COUNT(*) AS query_count,
  ROUND(SUM(execution_time) / 3600000.0, 2) AS total_hours,
  ROUND(SUM(credits_used_cloud_services), 3) AS cloud_credits,
  ROUND(AVG(bytes_scanned) / 1e9, 2) AS avg_gb_scanned
FROM snowflake.account_usage.query_history
WHERE start_time > DATEADD(day, -7, CURRENT_TIMESTAMP)
  AND execution_status = 'SUCCESS'
GROUP BY user_name, warehouse_name
ORDER BY total_hours DESC
LIMIT 20;

-- Queries with no partition filter (expensive full scans)
SELECT query_text,
       ROUND(bytes_scanned/1e9, 2) AS gb_scanned,
       execution_time/1000 AS exec_sec,
       partitions_scanned, partitions_total
FROM snowflake.account_usage.query_history
WHERE start_time > DATEADD(day, -7, CURRENT_TIMESTAMP)
  AND partitions_scanned = partitions_total   -- scanned ALL partitions = no pruning
  AND bytes_scanned > 1e10                    -- > 10GB scan
ORDER BY bytes_scanned DESC
LIMIT 10;
-- Share these with query authors: "Your query scanned 500GB. Add a date filter to scan 1GB."

-- AWS: S3 cost by prefix (requires Storage Lens or Athena + billing export)
SELECT year, month,
       line_item_resource_id,
       SUM(line_item_unblended_cost) AS cost_usd
FROM aws_billing.cost_and_usage
WHERE product_code = 'AmazonS3'
  AND year = '2024'
GROUP BY year, month, line_item_resource_id
ORDER BY cost_usd DESC;
```

---

## Pattern 2: Spot Instance Strategy for Spark

```python
# Databricks or EMR: use spot instances for worker nodes only
# Driver/master stays on on-demand (can't be interrupted mid-job)

# EMR cluster configuration (boto3):
import boto3

emr = boto3.client('emr')
response = emr.run_job_flow(
    Name='orders-pipeline-spot',
    Instances={
        'MasterInstanceType': 'm5.xlarge',
        'SlaveInstanceType': 'm5.xlarge',
        'InstanceFleets': [
            {
                'Name': 'MasterFleet',
                'InstanceFleetType': 'MASTER',
                'TargetOnDemandCapacity': 1,   # driver always on-demand
            },
            {
                'Name': 'WorkerFleet',
                'InstanceFleetType': 'CORE',
                'TargetSpotCapacity': 20,       # workers on spot (60-70% cheaper)
                'TargetOnDemandCapacity': 2,    # 2 fallback on-demand workers
                'LaunchSpecifications': {
                    'SpotSpecification': {
                        'TimeoutDurationMinutes': 10,
                        'TimeoutAction': 'SWITCH_TO_ON_DEMAND'  # fallback on timeout
                    }
                }
            }
        ]
    },
    # ... other config
)

# With spot: ~$0.05/hour per m5.xlarge vs $0.192/hour on-demand
# 20 workers: $1.00/hour spot vs $3.84/hour on-demand → 74% savings
# For a 2-hour daily job: $2/day vs $7.68/day → saves $2,100/year on just this job
```

---

## Cost Optimization Quick Wins (30-Day Plan)

| Week | Action | Expected Savings | Effort |
|---|---|---|---|
| 1 | Enable auto-suspend on all Snowflake warehouses (60 sec) | 20-30% of compute | 1 hour |
| 1 | Terminate idle dev/test clusters | 5-10% total | 30 min |
| 2 | S3 lifecycle policy: IA after 30d, Glacier after 90d | 20-40% of storage | 2 hours |
| 2 | Reduce Snowflake TIME_TRAVEL on staging tables to 0 days | 5-15% of storage | 1 hour |
| 3 | Move Spark batch workers to spot | 60-70% of batch compute | 4 hours |
| 3 | Find top 5 full-scan queries, add partition filter | 30-50% of DW compute | 8 hours |
| 4 | Review pipeline schedules: hourly → daily for non-critical | 20-30% of batch compute | 4 hours |
| 4 | Enable Delta VACUUM nightly (clean up old file versions) | 10-20% of S3 storage | 2 hours |

---

## Interview Tips

> **Tip 1:** "A Databricks pipeline's cost tripled last month. How do you investigate?" — Check Databricks Jobs UI → cost per job run. Then check cluster configuration: was cluster resized? Look at Spark History Server → Stages → shuffle read/write to see if data volume changed. Check auto-scaling: did the cluster scale to max capacity every run? Check if a new table join was added that causes a huge shuffle. Usually root cause is: new query without broadcast join on a growing table.

> **Tip 2:** "How do you balance cost optimization with developer productivity?" — Don't optimize developer environments (use slightly over-provisioned dev clusters — engineer time is expensive). Optimize production (where cost scales with data volume). Give developers cost visibility — a dashboard showing "your pipeline cost $40 this week" motivates optimization. Avoid micro-optimization at the expense of readability. Big wins: right-size production clusters, spot instances for batch, auto-suspend for idle DW — none of these impact developer experience.

> **Tip 3:** "How do you make cost governance sustainable (not a one-time project)?" — Three things: (1) Tagging policy enforced via CI/CD (reject deploys without required tags), (2) Weekly cost email to team leads showing their spend vs last week (automated via AWS Cost Explorer API), (3) Cost metric in the team's quarterly OKRs (unit cost target or total budget). Cost optimization done as a "project" always regresses. Built into culture and tooling: it compounds.
